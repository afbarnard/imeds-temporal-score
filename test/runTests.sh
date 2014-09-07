#!/bin/bash
# Copyright (c) 2014 Aubrey Barnard.  This is free software.  See
# LICENSE.txt for details.
#
# Shell script for setting up the test data, running Python unit tests,
# and cleaning up the test data

# Check command line arguments
if [[ "${#}" -ne 1 ]]; then
    echo "Error: Incorrect command line arguments" >&2
    echo "Usage: <username>[/<password>]@<oracle-db>" >&2
    exit 2
fi

# Get the command line arguments
dbConn="${1}"
# Parse the pieces of the DB connection string
if [[ "$dbConn" =~ ([[:print:]]+?)/([[:print:]]*)@([[:print:]]+) ]]; then
    dbUser="${BASH_REMATCH[1]}"
    dbPass="${BASH_REMATCH[2]}"
    dbName="${BASH_REMATCH[3]}"
elif [[ "$dbConn" =~ ([[:print:]]+?)@([[:print:]]+) ]]; then
    dbUser="${BASH_REMATCH[1]}"
    dbName="${BASH_REMATCH[2]}"
fi

# Prompt for Oracle password if it wasn't given on the command line
if [[ -z "$dbPass" ]]; then
    read -s -p "Oracle password: " dbPassInput
    echo >&2
fi

# Create the testing tables
echo "$(date +'%FT%T') runTests.sh INFO Creating Oracle DB testing tables" >&2
# I can't get sqlplus to work with a single here document that has a
# password and then the SQL script (and I can't use multiple here
# documents because Oracle appends '.sql' to the script name), so use a
# here document for the password and an explicit temporary file for the
# script
scriptFile=$(mktemp -t runTests.XXXXXXX.sql)
cat <<EOF > "${scriptFile}"
set feedback off;
-- Create condition occurrences testing table
create table test_cond_era (
    condition_era_id numeric(15) not null,
    person_id numeric(12) not null,
    condition_concept_id numeric(10) not null,
    condition_era_start_date date,
    condition_era_end_date date,
    constraint test_cond_era_pk primary key (condition_era_id)
);
-- Create drug occurrences testing table
create table test_drug_era (
    drug_era_id numeric(15) not null,
    person_id numeric(12) not null,
    drug_concept_id numeric(10) not null,
    drug_era_start_date date,
    drug_era_end_date date,
    constraint test_drug_era_pk primary key (drug_era_id)
);
exit;
EOF
sqlplus -s "${dbConn}" "@${scriptFile}" <<EOF
${dbPassInput}
EOF

# Load the test data
echo "$(date +'%FT%T') runTests.sh INFO Loading testing data" >&2
for file in testDataConds.dat testDataDrugs.dat; do
    sqlldr "${dbConn}" control="${file}" log="${file}.log" discard="${file}.discard" bad="${file}.bad" silent="(header,feedback)" errors=0 <<EOF
${dbPassInput}
EOF
    if [[ ! -e "${file}.discard" && ! -e "${file}.bad" ]]; then
        rm -f "${file}.log"
    fi
done

# Run Python unit tests
echo "$(date +'%FT%T') runTests.sh INFO Running Python tests" >&2
if [[ -n "${dbPass}" ]]; then
    python testTemporalScore.py "${dbUser}" "${dbPass}"
else
    python testTemporalScore.py "${dbUser}" <<EOF
${dbPassInput}
EOF
fi

# Testing clean-up
echo "$(date +'%FT%T') runTests.sh INFO Cleaning up Oracle DB testing data" >&2
cat <<EOF > "${scriptFile}"
set feedback off;
-- Drops tables created by testing setup
drop table test_cond_era;
drop table test_drug_era;
exit;
EOF
sqlplus -s "${dbConn}" "@${scriptFile}" <<EOF
${dbPassInput}
EOF

# Remove temporary files
rm -f "${scriptFile}"
