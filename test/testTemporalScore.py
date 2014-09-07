# Copyright (c) 2014 Aubrey Barnard.  This is free software.  See
# LICENSE.txt for details.
#
# Data generation and testing for temporal score.

from __future__ import print_function

import csv
import datetime
import getpass
import itertools as itools
import logging
import random
import sys
import unittest

sys.path.append('..')
import temporalScore


########################################
# Test data creation

# Chinese-restaurant-process-like population ID sampler for sampling
# patient IDs
class IdProcess(object):
    def __init__(self):
        self._maxId = 1
    def sample(self):
        id = random.randint(1, self._maxId)
        if id == self._maxId:
            self._maxId += 1
        return id

def sampleEvents(initialDistribution, causesToEffects):
    distribution = list(initialDistribution)
    events = []
    numberEvents = int(round(random.gammavariate(3.0, 1.5)))
    for i in xrange(numberEvents):
        event = random.choice(distribution)
        events.append(event)
        if event in causesToEffects:
            distribution.append(causesToEffects[event])
    return events

def sampleDatesForEvents(events, sameStartProb=0.2):
    timeline = []
    start = random.randrange(727) # 2-year starting range
    dayIncrement = 181 # 6-month increment
    for event in events:
        duration = random.choice((7, 14, 30, 60, 90))
        timeline.append((event, start, start + duration))
        # Keep the same start part of the time
        if random.random() > sameStartProb:
            start = random.randint(start, start + dayIncrement)
    return tuple(timeline)

def uniqueInOrder(iterable):
    seen = set()
    for item in iterable:
        if item not in seen:
            seen.add(item)
            yield item

def convertTimelinesToUniqueEventSequences(timelines):
    for timeline in timelines:
        # Throw away times
        events = itools.imap(lambda x: x[0], timeline)
        # Limit to first occurrences of events
        yield tuple(uniqueInOrder(events))

def timelinesToEventRecords(timelines):
    for patientId, eventSequence in enumerate(timelines, start=1):
        for event in eventSequence:
            yield (patientId,) + event

def filterSortEventRecords(eventRecords, ids, idIndex=1, keyIndex=2):
    records = filter(lambda r: r[idIndex] in ids, eventRecords)
    for recordId, record in enumerate(sorted(records, key=lambda r: r[keyIndex])):
        yield (recordId,) + record

def daysToDatesInEventRecordsWRecIds(eventRecords, referenceDate):
    for record in eventRecords:
        recId, personId, eventId, startDays, endDays = record
        startDate = referenceDate + datetime.timedelta(days=startDays)
        endDate = referenceDate + datetime.timedelta(days=endDays)
        startDateString = startDate.strftime('%Y-%m-%d')
        endDateString = endDate.strftime('%Y-%m-%d')
        yield (recId, personId, eventId, startDateString, endDateString)

def printItemPerLine(iterable, start='[', end=']', delim=','):
    print(start)
    for item in iterable:
        print(item, end='')
        print(delim)
    print(end)

# Test data creation commands to be run by hand in the interpreter
_testDataCreationCommands = '''
import itertools as itools
import testTemporalScore as tts

# Sample data
timelines = tuple(tts.sampleDatesForEvents(tts.sampleEvents(tts.initialDistribution, tts.causesToEffects)) for i in xrange(11))
timelines = tts.timelines
eventRecords = tuple(tts.timelinesToEventRecords(timelines))
condRecs = tuple(tts.filterSortEventRecords(eventRecords, tts.condIds))
drugRecs = tuple(tts.filterSortEventRecords(eventRecords, tts.drugIds))
tts.printItemPerLine(timelines, start='(', end=')')
tts.printItemPerLine(condRecs, start='(', end=')')
tts.printItemPerLine(drugRecs, start='(', end=')')
import datetime as dt
refDate = dt.date(2002, 02, 20)
tts.printItemPerLine(tts.daysToDatesInEventRecordsWRecIds(condRecs, refDate))
tts.printItemPerLine(tts.daysToDatesInEventRecordsWRecIds(drugRecs, refDate))

# Calculate temporal scores
execfile('../../mnAde/temporalScore.py')
eventSequences = tuple(tts.convertTimelinesToUniqueEventSequences(timelines))
eventsToPatients = patientsPerEvent(eventSequences)
crossSets = calcCrossOccurrenceSets(tts.drugIds, tts.condIds, eventsToPatients)
eventPairs = list(itools.product(tts.drugIds, tts.condIds))
temporalScores = calcTemporalScores(eventPairs, eventsToPatients, crossSets)
tts.printItemPerLine(sorted(temporalScores.viewitems()))
tables = calcTables(eventPairs, eventsToPatients)
'''


########################################
# Created test data

# Causal model:
# c1 -> d1 -> c2  (421 -> 773 -> 443)
# c3 -> d2        (479 -> 797)
condIds = (421, 443, 479)
drugIds = (773, 797)
initialDistribution = (421, 421, 421, 443, 479, 479, 773, 797)
causesToEffects = {
    421: 773,
    773: 443,
    479: 797,
    }

# (eventId, startDay, endDay)
timelines = (
    ((443, 406, 466), (479, 470, 560), (479, 593, 607), (421, 720, 780), (797, 720, 780), (479, 720, 750), (797, 858, 872), (479, 927, 934)),
    ((773, 486, 516), (443, 507, 537), (421, 514, 574), (773, 514, 604)),
    ((421, 47, 77), (443, 154, 184)),
    ((797, 485, 545), (797, 599, 629), (773, 762, 792)),
    ((773, 647, 677), (797, 758, 788)),
    ((797, 323, 383), (443, 436, 526)),
    ((421, 383, 473), (773, 409, 439)),
    ((421, 400, 460), (421, 400, 490), (421, 423, 513), (421, 444, 451)),
    ((773, 199, 289), (443, 206, 213), (479, 385, 399), (443, 526, 586), (773, 663, 723)),
    ((443, 434, 441), (421, 434, 494)),
    ((797, 233, 240), (773, 357, 447), (421, 464, 524)),
    )

# drug, cond, drug_bef_cond, drug_aft_cond, drug_cond, drug_bef_anycond, drug_anycond, anydrug_bef_cond, anydrug_cond, #drug, #cond, #patients, temporal_score
countsTable = (
    (773, 421,   2, 1, 3,   3, 4,   2, 4,   6, 7, 11, 1.80), # (2/3)/((3/4)*(2/4)); (3/5)/((4/6)*(3/6))
    (773, 443,   2, 0, 2,   3, 4,   3, 4,   6, 6, 11, 1.69), # (2/2)/((3/4)*(3/4)); (3/4)/((4/6)*(4/6))
    (773, 479,   1, 0, 1,   3, 4,   1, 2,   6, 2, 11, 2.00), # (1/1)/((3/4)*(1/2)); (2/3)/((4/6)*(2/4))
    (797, 421,   1, 1, 2,   2, 3,   2, 4,   5, 7, 11, 1.67), # (1/2)/((2/3)*(2/4)); (2/4)/((3/5)*(3/6))
    (797, 443,   1, 1, 2,   2, 3,   3, 4,   5, 6, 11, 1.25), # (1/2)/((2/3)*(3/4)); (2/4)/((3/5)*(4/6))
    (797, 479,   0, 1, 1,   2, 3,   1, 2,   5, 2, 11, 1.11), # (0/1)/((2/3)*(1/2)); (1/3)/((3/5)*(2/4))
    )


########################################
# Unit tests

# Default parameters for testing
_defaultParameters = dict(temporalScore.defaultParameters)
_defaultParameters.update((
        ('condEraTableName', 'test_cond_era'),
        ('drugEraTableName', 'test_drug_era'),
        ))

def getOracleUsername():
    return raw_input('Oracle username for testing: ')

def getOraclePassword():
    return getpass.getpass('Oracle password for testing: ')

def fillInParameters(parameters):
    if parameters['dbUser'] is None:
        parameters['dbUser'] = getOracleUsername()
    if parameters['dbPass'] is None:
        parameters['dbPass'] = getOraclePassword()
    if parameters['dbSchemaName'] is None:
        parameters['dbSchemaName'] = parameters['dbUser']

def readTemporalScoreOutputAsTable(file_, func=lambda x: x):
    reader = csv.reader(file_)
    return tuple(func(row) for row in reader if row)

def convertTsResultRow(row):
    # Columns 1-12 are ints, column 13 is a float
    return tuple(int(i) for i in row[:12]) + (round(float(row[12]), 2),)

class TemporalScoreTest(unittest.TestCase):

    def setUp(self):
        # Get missing parameters
        fillInParameters(_defaultParameters)
        # Make a local copy of parameters for this test
        self.parameters = dict(_defaultParameters)

    def test_temporalScore(self):
        # Calculate and output the temporal scores
        reportOutput, oracleOutput = temporalScore.temporalScore(
            drugIds, condIds,
            parameters=self.parameters,
            )
        # Check for correct output
        expectedTable = countsTable
        actualTable = readTemporalScoreOutputAsTable(
            reportOutput,
            convertTsResultRow,
            )
        self.assertEqual(expectedTable, actualTable)

    def test_temporalScore_noDataIds(self):
        # Add extra drug and cond IDs that have no data
        extraDrugIds = drugIds + (701,)
        extraCondIds = condIds + (499,)
        # Calculate and output the temporal scores
        reportOutput, oracleOutput = temporalScore.temporalScore(
            extraDrugIds, extraCondIds,
            parameters=self.parameters,
            )

        # Build the amended counts table
        expectedTable = list(countsTable)
        # Make the row template
        rowTemplate = [0] * 13
        # Add row for no-data pair
        rowTemplate[0] = 701
        rowTemplate[1] = 499
        rowTemplate[11] = 11 # number people stays constant
        rowTemplate[12] = 2.0
        expectedTable.append(tuple(rowTemplate))
        # Add rows for no-data drug and other conds
        for index, condId in enumerate(condIds):
            rowTemplate[1] = condId
            rowTemplate[7] = countsTable[index][7]
            rowTemplate[8] = countsTable[index][8]
            rowTemplate[10] = countsTable[index][10]
            rowTemplate[12] = round(
                (1.0 / 2.0)
                / ((1.0 / 2.0) *
                   (float(rowTemplate[7] + 1)
                    / float(rowTemplate[8] + 2))),
                2)
            expectedTable.append(tuple(rowTemplate))
        rowTemplate[7], rowTemplate[8], rowTemplate[10] = 0, 0, 0
        # Add rows for no-data cond and other drugs
        rowTemplate[1] = 499
        for index, drugId in enumerate(drugIds):
            index = index * len(condIds)
            rowTemplate[0] = drugId
            rowTemplate[5] = countsTable[index][5]
            rowTemplate[6] = countsTable[index][6]
            rowTemplate[9] = countsTable[index][9]
            rowTemplate[12] = round(
                (1.0 / 2.0)
                / ((float(rowTemplate[5] + 1)
                    / float(rowTemplate[6] + 2))
                   * (1.0 / 2.0)),
                2)
            expectedTable.append(tuple(rowTemplate))
        # Put the table in order by drug and cond
        expectedTable.sort()

        # Check for correct output
        actualTable = readTemporalScoreOutputAsTable(
            reportOutput,
            convertTsResultRow,
            )
        self.assertEqual(tuple(expectedTable), actualTable)


########################################
# Main

# Logging setup for testing
logging.basicConfig(
    format='%(asctime)s %(name)s.%(funcName)s %(levelname)s %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    level=logging.WARNING,
    #level=logging.DEBUG,
    stream=sys.stderr,
    )

if __name__ == '__main__':
    # Get the command line arguments
    if len(sys.argv) >= 2:
        _defaultParameters['dbUser'] = sys.argv[1]
        del sys.argv[1]
    if len(sys.argv) >= 2:
        _defaultParameters['dbPass'] = sys.argv[1]
        del sys.argv[1]
    else:
        dbPass = raw_input()
        if dbPass:
            _defaultParameters['dbPass'] = dbPass
    unittest.main()
