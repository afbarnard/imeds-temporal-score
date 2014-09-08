Temporal Score IMEDS Method
===========================


Description
-----------

The Temporal Score IMEDS Method is a program that evaluates the adverse
drug event likelihood of drug-condition pairs and outputs each pair with
its counts and scores in CSV format.  The program only runs with an
Oracle database having electronic medical records data in IMEDS common
data model (CDM) format.  This software is submitted as a research
method of the Innovation in Medical Evidence Development and
Surveillance (IMEDS) program.

This software implements the temporal score described on page 4 of the
following paper.  A detailed description of the temporal score is in its
own section below.

<blockquote>
Identifying Adverse Drug Events by Relational Learning.
David Page, Vitor Santos Costa, Sriraam Natarajan, Aubrey Barnard, Peggy Peissig, Michael Caldwell.
AAAI 2012.
http://www.aaai.org/ocs/index.php/AAAI/AAAI12/paper/view/4941
</blockquote>

For more information on the IMEDS program and the research methods it
and collaborators are developing see http://imeds.reaganudall.org.
Historical information can be found at the site of IMEDS's predecessor,
the Observational Medical Outcomes Partnership (OMOP) http://omop.org.


License
-------

The Temporal Score IMEDS Method is free, open source software.  It is
released under the Apache License, Version 2.0, a copy of which can be
found as `LICENSE.txt` in your distribution as a sibling of this file.


Requirements
------------

* Python 2.7
* Oracle database with data in IMEDS CDM (version 2) format
* `sqlplus`, the Oracle client


How to Use
----------

[Download this
project](https://github.com/afbarnard/imeds-temporal-score/archive/master.zip)
and extract it into your preferred directory.  Then run the program at
the command line like in the following example:

    $ python2.7 <...>/temporalScore.py --db-user <oracle-username> -p <params-file> <drug-IDs-file> <condition-IDs-file> > <report-file>

The normal operation is to collect counts from the database for each
drug-condition pair and output those counts with computed scores to
standard output (or a file).  The results table is also left in the
database for later inspection or processing.  For more information on
how to run the program, access the command line help:

    $ python2.7 <...>/temporalScore.py -h


Parameters File
---------------

Database and algorithm parameters can be specified in a configuration
file.  The file is in "config" format without any section headers.  The
parameters are described below and some can also be specified on the
command line.  Any command line arguments override any settings from the
parameters file.

It is possible to run this software without a parameters file, but one
will usually have to specify the names of the drug and condition era
tables.

* `dbConnectionName`: Identifier of the Oracle DB connection to use.
  Default is 'lsomop'.  Also settable on the command line.
* `dbUser`: Oracle username.  Default is to prompt.  Also settable on
  the command line.
* `dbPass`: Oracle password.  Default is to prompt.  Also settable on
  the command line.
* `dbSchemaName`: Schema to use for temporary tables and the results
  table.  Defaults to the user schema.  Also settable on the command
  line.
* `drugEraTableName`: Name of table containing drug era records.
  Specify a fully-qualified table name if the table is not in the
  specified schema.  Default is 'drug_era'.
* `condEraTableName`: Name of the table containing condition era
  records.  Specify a fully-qualified table name if the table is not in
  the specified schema.  Default is 'condition_era'.
* `conditionWindowStart`: Number of days after a drug occurrence to
  allow the earliest associated condition occurrence.  Use a negative
  number to make the window start before the drug.  Default is -100,000.
* `conditionWindowEnd`: Number of days after a drug occurrence to allow
  the latest associated condition occurrence.  Default is 100,000.
* `drugOccurrenceOffset`: Number of days to shift the date of drug
  occurrences.  Default is 0.
* `pseudocount`: Pseudocount to add to all counts to avoid zero counts.
  Default is 1.
* `countsScoresTableName`: Name of the table to contain the results
  report.  Default is 'counts_scores'.
* `reportFileName`: Name of the file to contain the results report.
  Default is standard output.


Report Format
-------------

This software reports its results as a table in the Oracle DB (see the
'countsScoresTableName' parameter) and also outputs the table in CSV
format (see the 'reportFileName' parameter).  These are the fields of
the table.

* `drug`: Drug ID
* `cond`: Condition ID
* `ct_d_bef_c`: Count of people having the drug before the condition
* `ct_c_bef_d`: Count of people having the condition before the drug
* `ct_d_c`: Count of people having both the drug and condition
* `ct_d_bef_anyc`: Count of people having the drug before any of the
  conditions
* `ct_d_anyc`: Count of people having the drug and any of the conditions
* `ct_anyd_bef_c`: Count of people having any of the drugs before the
  condition
* `ct_anyd_c`: Count of people having any of the drugs and the condition
* `ct_d`: Count of people having the drug
* `ct_c`: Count of people having the condition
* `ct_ppl`: Count of people having any of the drugs and conditions
* `temporal_score`: Temporal score

One can use the above counts to do further epidemiology-style 2-by-2
table analysis.


Temporal Score Explanation
--------------------------

TODO


Contact
-------

Contact me about this software through GitHub.  First, see if there are
any [relevant
issues](https://github.com/afbarnard/imeds-temporal-score/issues?q=is%3Aissue).
If not, then [open a new
issue](https://github.com/afbarnard/imeds-temporal-score/issues/new) to
report a bug or ask a new question.  If you have academic inquiries you
can find my e-mail address in the git log.


Copyright (c) 2014 Aubrey Barnard.  This is free software.  See
LICENSE.txt for details.
