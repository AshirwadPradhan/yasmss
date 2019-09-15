"""Process the output of spark job and convert it into JSON object
"""
import json


class SparkJSON:

    def __init__(self):
        self._finaldict = None
        self.exectime = None
        self.trans_actions = None

    def _getRows(self, row):
        data = json.loads(row)
        print(type(data))
        self._finaldict.update(dict(data))
        SparkJSON.finaldict.update(dict(data))

    def _convertToDict(self, obj):
        return obj.__dict__

    def covertToJSON(self, sparkjob_op):

        res = sparkjob_op.queryresult.toJSON().map(lambda j: json.loads(j)).collect()
        self._finaldict = res
        self.exectime = sparkjob_op.exectime
        self.trans_actions = sparkjob_op.trans_actions
        data = json.dumps(self, default=self._convertToDict, indent=4)
        return data
