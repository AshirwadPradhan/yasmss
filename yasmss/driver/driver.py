"""Takes the parsed query from parser and send it to MapReduce and Spark jobs
"""
from parsetemplate import parser
from sparkmapper import sparkmapper
from sparkresult import sparkresult
from mrmapper import mrjob

class RunQuery:
    """Initialize the MR and Spark jobs
    """

    def __init__(self, parsedQuery):

        self.parsedQuery = parsedQuery
        self.sparkOutput = None
        self.mrOutput = None

    def _initiateSparkJob(self):

        if isinstance(self.parsedQuery, parser.QuerySetJoin):

            sparkj = sparkmapper.SparkJob()
            sparkjob_op = sparkj.startjob(self.parsedQuery, 'QuerySetJoin')
            return sparkjob_op

        elif isinstance(self.parsedQuery, parser.QuerySetGroupBy):

            sparkj = sparkmapper.SparkJob()
            sparkjob_op = sparkj.startjob(self.parsedQuery, 'QuerySetGroupBy')
            return sparkjob_op

        else:
            raise TypeError('Unidentified Class Type')
            return None

    def _initiateMRjob(self):
        if isinstance(self.parsedQuery, parser.QuerySetJoin):
            mrj = mrjob.MRJob()
            mrres = mrj.start_mrjob(self.parsedQuery, 'QuerySetJoin')
            return mrres
        elif isinstance(self.parsedQuery, parser.QuerySetGroupBy):
            mrj = mrjob.MRJob()
            mrres = mrj.start_mrjob(self.parsedQuery, 'QuerySetGroupBy')
            return mrres
        else:
            raise TypeError('Unidentified Class Type in MapReduce')

    def run(self, runMR=True, runSpark=True):
        if runMR:
            mrjob_output = self._initiateMRjob()
            self.mrOutput = mrjob_output
        if runSpark:
            sparkjob_op = self._initiateSparkJob()
            self.sparkOutput = sparkresult.SparkJSON().covertToJSON(sparkjob_op)
        return self
