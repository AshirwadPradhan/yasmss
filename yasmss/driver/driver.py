"""Takes the parsed query from parser and send it to MapReduce and Spark jobs
"""
from yasmss.parsetemplate import parser
from yasmss.sparkmapper import sparkmapper


class RunQuery:

    def __init__(self, parsedquery):
        self.parsedquery = parsedquery
        self._initiateSparkJob()

    def _initiateSparkJob(self):
        if isinstance(self.parsedQuery, parser.QuerySetJoin):
            sparkj = sparkmapper.SparkJob()
            sparkj.startjob(self.parsedquery, 'QuerySetJoin')
        elif isinstance(self.parsedQuery, parser.QuerySetGroupBy):
            sparkj = sparkmapper.SparkJob()
            sparkj.startjob(self.parsedquery, 'QuerySetGroupBy')
        else:
            print('Unknown class error')

    def _initiateMRjob(self):
        pass
