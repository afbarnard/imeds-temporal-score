# Copyright (c) 2014 Aubrey Barnard.  This is free software.  See
# LICENSE.txt for details.
#
# Program and API for temporal score IMEDS method.

from __future__ import print_function

import argparse
import collections
import getpass
import logging
import os
import re
import shutil
import socket
import string
import subprocess
import sys
import tempfile
import traceback

defaultParameters = collections.OrderedDict((
        ('dbConnectionName', 'lsomop'),
        ('dbUser', None), # Prompt if None
        ('dbPass', None), # Prompt if None
        ('dbSchemaName', None), # Default to dbUser
        ('drugEraTableName', 'drug_era'),
        ('condEraTableName', 'condition_era'),
        ('conditionWindowStart', -100000), # in days
        ('conditionWindowEnd', 100000), # in days
        ('drugOccurrenceOffset', 0), # in days
        ('pseudocount', 1),
        ('countsScoresTableName', 'counts_scores'),
        ('reportFileName', None), # Default to stdout
        ('condIdsTuple', None), # Generated
        ('drugIdsTuple', None), # Generated
        ))

sqlScriptTemplate = '''
-- Script that collects counts of drugs and conditions in their temporal
-- orders and uses them to compute adverse drug event likelihood scores.

-- Session parameters
whenever sqlerror exit sql.sqlcode;
set autocommit off
set echo off
set feedback off
set trimout on
set trimspool on

-- Switch to the specified schema
alter session set current_schema = ${dbSchemaName};

-- Create temporary tables.  Use PLSQL to emulate "create or replace
-- table"

begin
  execute immediate 'drop table first_drugs';
exception
  when others then if sqlcode != -0942 then raise; end if;
end;
/
create global temporary table first_drugs (
    person number(15) not null,
    drug number(15) not null,
    drug_date date not null
);

begin
  execute immediate 'drop table first_conds';
exception
  when others then if sqlcode != -0942 then raise; end if;
end;
/
create global temporary table first_conds (
    person number(15) not null,
    cond number(15) not null,
    cond_date date not null
);

begin
  execute immediate 'drop table first_drugs_conds';
exception
  when others then if sqlcode != -0942 then raise; end if;
end;
/
create global temporary table first_drugs_conds (
    person number(15) not null,
    drug number(15) not null,
    cond number(15) not null,
    drug_date date not null,
    cond_date date not null
);

-- Create table to hold counts and scores
begin
  execute immediate 'drop table ${countsScoresTableName}';
exception
  when others then if sqlcode != -0942 then raise; end if;
end;
/
create table ${countsScoresTableName} (
    drug number(15) not null, -- Drug ID
    cond number(15) not null, -- Condition ID
    -- Temporal score fields
    ct_d_bef_c number(9), -- Count people having drug before condition
    ct_c_bef_d number(9), -- Count people having condition before drug
    ct_d_c number(9), -- Count people having drug and condition
    ct_d_bef_anyc number(9), -- Count people having drug before any condition
    ct_d_anyc number(9), -- Count people having drug and any condition
    ct_anyd_bef_c number(9), -- Count people having any drug before condition
    ct_anyd_c number(9), -- Count people having any drug and condition
    -- Totals fields that can be used to construct a 2x2 table (with ct_d_c above)
    ct_d number(9), -- Count people having drug
    ct_c number(9), -- Count people having condition
    ct_ppl number(9), -- Count people
    -- Scores based on above counts
    temporal_score real
);

-- Create a type for column literals so that lists of drug and condition
-- IDs can be used directly as tables
create or replace type number15_table as table of number(15);
/

-- Find all the first drug occurrences
insert into first_drugs
select de.person_id as person,
       de.drug_concept_id as drug,
       (min(de.drug_era_start_date) + ${drugOccurrenceOffset}) as drug_date
from ${drugEraTableName} de
where de.drug_concept_id in ${drugIdsTuple}
group by person_id, drug_concept_id;

-- Find all the first condition occurrences
insert into first_conds
select ce.person_id as person,
       ce.condition_concept_id as cond,
       min(ce.condition_era_start_date) as cond_date
from ${condEraTableName} ce
where ce.condition_concept_id in ${condIdsTuple}
group by person_id, condition_concept_id;

-- Put the first drug occurrences and first condition occurrences together
insert into first_drugs_conds
select fd.person, fd.drug, fc.cond, fd.drug_date, fc.cond_date
from first_drugs fd,
     first_conds fc
where fd.person = fc.person
  and fc.cond_date >= (fd.drug_date + ${conditionWindowStart})
  and fc.cond_date <= (fd.drug_date + ${conditionWindowEnd});

-- Insert all drug-condition pairs into the temporal scores
insert into ${countsScoresTableName}
    (drug, cond,
     ct_d_bef_c, ct_c_bef_d, ct_d_c,
     ct_d_bef_anyc, ct_d_anyc, ct_anyd_bef_c, ct_anyd_c,
     ct_d, ct_c, ct_ppl,
     temporal_score)
select drugs.column_value as drug, conds.column_value as cond, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0
from table(number15_table${drugIdsTuple}) drugs,
     table(number15_table${condIdsTuple}) conds;

-- Populate all the counts of people
update ${countsScoresTableName} cst
set
  -- Drug before condition
  ct_d_bef_c =
    (select count(distinct person)
     from first_drugs_conds fdc
     where drug_date < cond_date
       and cst.drug = fdc.drug
       and cst.cond = fdc.cond),
  -- Condition before drug
  ct_c_bef_d =
    (select count(distinct person)
     from first_drugs_conds fdc
     where drug_date >= cond_date
       and cst.drug = fdc.drug
       and cst.cond = fdc.cond),
  -- Drug and condition
  ct_d_c =
    (select count(distinct person)
     from first_drugs_conds fdc
     where cst.drug = fdc.drug
       and cst.cond = fdc.cond),
  -- Drug before any condition
  ct_d_bef_anyc =
    (select count(distinct person)
     from first_drugs_conds fdc
     where drug_date < cond_date
       and cst.drug = fdc.drug),
  -- Drug and any condition
  ct_d_anyc =
    (select count(distinct person)
     from first_drugs_conds fdc
     where cst.drug = fdc.drug),
  -- Any drug before condition
  ct_anyd_bef_c =
    (select count(distinct person)
     from first_drugs_conds fdc
     where drug_date < cond_date
       and cst.cond = fdc.cond),
  -- Any drug and condition
  ct_anyd_c =
    (select count(distinct person)
     from first_drugs_conds fdc
     where cst.cond = fdc.cond),
  -- Drugs
  ct_d =
    (select count(distinct person)
     from first_drugs fd
     where cst.drug = fd.drug),
  -- Conditions
  ct_c =
    (select count(distinct person)
     from first_conds fc
     where cst.cond = fc.cond),
  -- Patients
  ct_ppl =
    (select count(distinct person)
     from (select person from first_drugs
           union
           select person from first_conds));

-- Compute temporal scores
update ${countsScoresTableName} cst
set temporal_score =
    (((ct_d_bef_c + ${pseudocount}) / (ct_d_c + ${pseudocount} + ${pseudocount}))
     / ((ct_d_bef_anyc + ${pseudocount}) / (ct_d_anyc + ${pseudocount} + ${pseudocount})
      * (ct_anyd_bef_c + ${pseudocount}) / (ct_anyd_c + ${pseudocount} + ${pseudocount}))
    );

-- End transaction
commit;

-- Set parameters for CSV-like output
set pagesize 0
set linesize 1000
set numwidth 15
set null ''
set colsep ,
set termout off

-- Write counts and scores to a file
spool ${reportFileName}
select *
from ${countsScoresTableName}
order by drug, cond;
spool off
-- See errors again
set termout on

-- Clean up: drop all things except results table
drop table first_drugs;
drop table first_conds;
drop table first_drugs_conds;
drop type number15_table;

exit
'''

_settingPattern = re.compile(r'^\s*([\w~!@$%^&*+|;,./?-]+)\s*[:=]\s*(.*?)\s*$')

# Can't use ConfigParser because it case-converts all names
def parseConfig(fileName):
    logger = logging.getLogger(__name__)
    settings = collections.OrderedDict()
    logger.info('Loading configuration file: %s', fileName)
    with open(fileName, 'r') as inputFile:
        for lineNumber, line in enumerate(inputFile, start=1):
            strippedLine = line.strip()
            # Skip blank lines and comments
            if not strippedLine or strippedLine.startswith('#'):
                continue
            # Parse settings
            match = _settingPattern.match(line)
            if match is None:
                logger.warning("Skipping non-setting at %s:%s: '%s'", fileName, lineNumber, line.rstrip('\n'))
            else:
                name, value = match.group(1, 2)
                settings[name] = value
    return settings

def dictToPrettyString(dict_):
    lines = ['{']
    for name, value in dict_.viewitems():
        lines.append(str(name) + ': ' + str(value) + ',')
    lines.append('}')
    return '\n'.join(lines)

_integerPattern = re.compile(r'\s*\d+\s*')

def parseIds(inputFile):
    ids = []
    for lineNumber, line in enumerate(inputFile, start=1):
        strippedLine = line.strip()
        # Skip empty lines and comments
        if not line or strippedLine.startswith('#'):
            continue
        # Convert integer IDs to integers
        id_ = strippedLine
        match = _integerPattern.match(id_)
        if match is not None:
            id_ = int(match.group())
        ids.append(id_)
    return ids

def temporalScore(drugIds, condIds, parameters=defaultParameters):
    logger = logging.getLogger(__name__)
    logger.info('Computing temporal scores')
    # Copy the parameters to avoid modifying the original
    parameters = dict(parameters)
    # Construct the tuples of IDs (in string form)
    parameters['drugIdsTuple'] = repr(tuple(drugIds))
    parameters['condIdsTuple'] = repr(tuple(condIds))
    # Create a temporary file for the report output
    reportOutput = tempfile.NamedTemporaryFile(suffix='.csv')
    parameters['reportFileName'] = reportOutput.name
    # Build the SQL script
    sqlTemplate = string.Template(sqlScriptTemplate)
    sqlScript = sqlTemplate.substitute(parameters)
    # Run the SQL script
    scriptOutput = runOracleSqlScript(
        parameters['dbConnectionName'],
        parameters['dbUser'],
        parameters['dbPass'],
        sqlScript,
        )
    # Prepare report output for reading as input
    reportOutput.flush()
    reportOutput.seek(0)
    return reportOutput, scriptOutput

class OracleError(Exception):

    def __init__(self, message=None, exitCode=None, signal=None):
        self.exitCode = exitCode
        self.signal = signal
        if message is None:
            if exitCode is not None:
                message = 'Oracle failed with code: {}'.format(exitCode)
            elif signal is not None:
                message = 'Oracle killed by signal: {}'.format(signal)
            else:
                message = 'Oracle error unknown.'
        super(Exception, self).__init__(message)

# Shells out like this to run the given Oracle script:
# echo <dbPass> | sqlplus -s <dbUser>@<dbName> @oracleScript.sql 1> out.log 2> err.log
def runOracleSqlScript(dbName, dbUser, dbPass, script):
    logger = logging.getLogger(__name__)
    logger.info('Running Oracle script')
    logger.debug('Oracle script:\n%s', script)
    # Dump the script out to a temporary file such that it can be opened
    # by name
    scriptFile = tempfile.NamedTemporaryFile(suffix='.sql')
    scriptFile.write(script)
    scriptFile.flush()
    os.fsync(scriptFile.fileno())
    # Create temporary file for IO
    output = tempfile.TemporaryFile()
    # Create the command line
    command = ['sqlplus', '-s', dbUser + '@' + dbName, '@' + scriptFile.name]
    try:
        # Run the Oracle script as a sub-process
        logger.info('Running Oracle sub-process: %s', ' '.join(command))
        process = subprocess.Popen(command, stdin=subprocess.PIPE,
                                   stdout=output, stderr=subprocess.STDOUT)
        # Write the password to the standard input of the process.
        # Since stdout and stderr are not pipes, communicate() should
        # not return any data.  Communicate needs to be called for some
        # reason or else Oracle hangs.  Oracle also hangs if stdin is
        # not a pipe.  I think this is because sqlplus will not exit
        # unless input is closed or an exit command is given.
        process.communicate(input=dbPass)
        # Wait for termination
        process.wait()
        # Prepare captured output for reading as input
        output.flush()
        output.seek(0)
        # Check the exit status
        if process.returncode == 0:
            logger.info('Oracle sub-process finished successfully.')
        else:
            if process.returncode > 0:
                logger.error('Oracle sub-process failed with code: %s', process.returncode)
            else:
                logger.error('Oracle sub-process killed by signal: %s', -process.returncode)
            logger.info('Oracle output:\n%s', ''.join(output.readlines()))
            output.seek(0)
    except Exception as e:
        logger.exception('Oracle sub-process failed with exception:')
        raise
    else:
        # Raise exceptions for abnormal terminations
        if process.returncode > 0:
            raise OracleError(exitCode=process.returncode)
        elif process.returncode < 0:
            raise OracleError(signal=-process.returncode)
    finally:
        # Close the temporary file (which deletes it)
        scriptFile.close()
    logger.info('Oracle sub-process done.')
    return output

# Define the CLI
_argParser = argparse.ArgumentParser(
    prog='temporalScore',
    description='''Evaluates the adverse drug event likelihood of
    drug-condition pairs using the temporal score from page 4 of (Page,
    et al. AAAI 2012) and outputs each pair with its counts and scores
    in CSV format.  Runs only on an Oracle DB in IMEDS common data model
    format.

    The drug-condition pairs are constructed as a Cartesian product of
    the lists of drugs and conditions in the given files.

    The parameters for this program are described in accompanying
    documentation.
    ''',
    )
_argParser.add_argument(
    '-p', '--parameters',
    help='Input file containing Oracle DB and algorithm parameters in "config" format.',
    metavar='PARAMS-FILE',
    type=argparse.FileType('r'),
    )
_argParser.add_argument(
    'drugIdsFile',
    help='Input file containing a list of drug IDs, one per line.',
    metavar='DRUG-IDS-FILE',
    type=argparse.FileType('r'),
    )
_argParser.add_argument(
    'condIdsFile',
    help='Input file containing a list of condition IDS, one per line.',
    metavar='COND-IDS-FILE',
    type=argparse.FileType('r'),
    )
_argParser.add_argument(
    '-o', '--output',
    help='Output file containing the report of counts and scores in CSV format.  Overrides the parameters file.  Default is standard output.',
    metavar='OUTPUT',
    type=argparse.FileType('w'),
    )
_argParser.add_argument(
    '--db-conn',
    help='Name of the Oracle DB connection.  Overrides the parameters file.  Default is \'lsomop\'.',
    metavar='NAME',
    )
_argParser.add_argument(
    '--db-user',
    help='Oracle username.  Overrides the parameters file.  Default is to prompt.',
    metavar='USERNAME',
    )
_argParser.add_argument(
    '--db-pass',
    help='Oracle password.  Overrides the parameters file.  Default is to prompt.',
    metavar='PASSWORD',
    )
_argParser.add_argument(
    '--db-schema',
    help='Schema for all DB operations.  Overrides the parameters file.  Default is username.',
    metavar='NAME',
    )
_argParser.add_argument(
    '--debug',
    help='Print stack traces.',
    action='store_true',
    default=False,
    )

def main(args=None):
    '''Exposes the functionality of this module as a command line API.

    args: A sequence of strings, the command line arguments without any
    initial program name.  When args is None it defaults to
    sys.argv[1:].
    '''
    # Default args
    if args is None:
        args = sys.argv[1:]
    # Parse the arguments
    environment = _argParser.parse_args(args)
    # Set up logging
    logging.basicConfig(
        format='%(asctime)s %(name)s.%(funcName)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
        level=logging.INFO,
        stream=sys.stderr,
        )
    logger = logging.getLogger(__name__)
    # Log basic information about this run
    logger.info('Run identifier (host/pid): %s/%s', socket.gethostname(), os.getpid())
    logger.info('Current working directory: %s', os.getcwd())
    # Log the arguments
    logger.info('Main invoked with arguments: %s', args)

    # Start with the default parameters
    parameters = collections.OrderedDict(defaultParameters)
    # Load configuration (if any)
    if environment.parameters:
        configuration = parseConfig(environment.parameters.name)
        parameters.update(configuration)

    # Load IDs from files (they are already open)
    logger.info('Loading drug IDs from file: %s', environment.drugIdsFile.name)
    drugIds = parseIds(environment.drugIdsFile)
    environment.drugIdsFile.close()
    logger.info('Loading cond IDs from file: %s', environment.condIdsFile.name)
    condIds = parseIds(environment.condIdsFile)
    environment.condIdsFile.close()

    # Open the report file
    reportFile = sys.stdout
    if environment.output:
        reportFile = environment.output
    elif parameters['reportFileName']:
        reportFile = open(parameters['reportFileName'], 'w')

    # Fill in parameter values from the command line
    if environment.db_conn is not None:
        parameters['dbConnectionName'] = environment.db_conn
    if environment.db_user is not None:
        parameters['dbUser'] = environment.db_user
    if environment.db_pass is not None:
        parameters['dbPass'] = environment.db_pass
    if environment.db_schema is not None:
        parameters['dbSchemaName'] = environment.db_schema

    # Fill in missing parameter values
    # Prompt for DB username
    if parameters['dbUser'] is None:
        parameters['dbUser'] = raw_input('Oracle username: ')
    # Prompt for DB password
    dbPass = parameters['dbPass']
    if dbPass is None:
        dbPass = getpass.getpass('Oracle password: ')
        parameters['dbPass'] = '***redacted***'
    # Set schema
    if parameters['dbSchemaName'] is None:
        parameters['dbSchemaName'] = parameters['dbUser']
    # Log parameters (password excluded unless already public)
    logger.info('Parameters:\n%s', dictToPrettyString(parameters))
    # Store the password in the parameters
    parameters['dbPass'] = dbPass

    # Compute the temporal score
    reportOutput, oracleOutput = temporalScore(drugIds, condIds, parameters)

    # Output the report
    logger.info('Writing report')
    shutil.copyfileobj(reportOutput, reportFile)

    # Done
    logger.info('Done.')

def mainProgram():
    '''Calls 'main(sys.argv[1:])', handles its exceptions, and exits.
    Not intended for API use.
    '''
    try:
        main(sys.argv[1:])
    except Exception as e:
        if '--debug' in sys.argv:
            traceback.print_exc(file=sys.stderr)
        print('temporalScore: Error:', e, file=sys.stderr)
        sys.exit(1)
    else:
        sys.exit(0)

# Run module as main program
if __name__ == '__main__':
    # Make __name__ meaningful
    __name__ = 'temporalScore'
    mainProgram()
