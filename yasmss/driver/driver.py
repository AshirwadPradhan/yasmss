"""Takes the parsed query from parser and send it to MapReduce and Spark jobs
"""
from parsetemplate import parser
from sparkmapper import sparkmapper


class RunQuery:
    def __init__(self, parsedQuery):
        self.parsedQuery = parsedQuery

    def _initiateSparkJob(self):
        if isinstance(self.parsedQuery, parser.QuerySetJoin):
            sparkj = sparkmapper.SparkJob()
            sparkj.startjob(self.parsedQuery, 'QuerySetJoin')
        elif isinstance(self.parsedQuery, parser.QuerySetGroupBy):
            sparkj = sparkmapper.SparkJob()
            sparkj.startjob(self.parsedQuery, 'QuerySetGroupBy')
        else:
            raise TypeError('Unidentified Class Type')

    def _initiateMRjob(self):
        pass

    def run(self, runMR=True, runSpark=True):
        if runMR:
            self._initiateMRjob
        if runSpark:
            self._initiateSparkJob()
