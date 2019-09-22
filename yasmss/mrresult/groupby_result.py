"""
    returns the the MR result stored in HDFS directory, maps it to columns names and transforms it into an array of dict.
"""
import subprocess
import os
import yaml
from schema import schema
import collections

class MrResult:
    def __init__(self,queryset):
        self.json_result = []
        self.col_names = []
        self.queryset = queryset
        self.output = None
        self.opfile = "part-00000"
        self.parentdir = None
        self.outputdir = None

    def _get_outputpath(self):
        conf = {}
        with open("config.yaml", 'r') as file:
            conf = yaml.load(file, Loader=yaml.FullLoader)
        self.parentdir = conf['pathconfig']['parentdir']
        self.outputdir = conf['pathconfig']['outputdir']
        return "hdfs://localhost:9000/{path}/{outputdir}/{outputfile}".format(
                path=self.parentdir, outputdir=self.outputdir, outputfile=self.opfile)


    def _get_col_names(self):
        selcols = self.queryset.selcolumns[:len(self.queryset.selcolumns)-1]
        selcols.append(self.queryset.aggcol)
        return selcols

    def _groupby_res(self):
        """
        mapping the column names to the output stored in hdfs
        """
        self.col_names = self._get_col_names()
        self.output = self._get_outputpath()
        cat = subprocess.Popen(["hadoop", "fs", "-cat", self.output], stdout=subprocess.PIPE)
        for line in cat.stdout:
            temp_dict = dict(zip(self.col_names, line.decode('utf-8').split('\n')[0].replace(",", "\t").split('\t')))
            self.json_result.append(temp_dict)
        self._cleanup()

    def _cleanup(self):
        rm_output = "hdfs dfs -rm -r hdfs://localhost:9000/{parent}/{outputdir}".format(parent=self.parentdir, outputdir=self.outputdir)
        os.system(rm_output)

    def _join_res(self):
        self.output = self._get_outputpath()
        t1_cols = list(schema.Schema().getSchemaDict(table=self.queryset.fromtable).keys())
        t2_cols = list(schema.Schema().getSchemaDict(table=self.queryset.jointable).keys())
        all_cols = t1_cols + t2_cols
        all_cols = list(collections.OrderedDict.fromkeys(all_cols))
        cat = subprocess.Popen(["hadoop", "fs", "-cat", self.output], stdout=subprocess.PIPE)
        for line in cat.stdout:
            temp_dict = dict(zip(all_cols, line.decode('utf-8').split('\n')[0].split('\t')))
            self.json_result.append(temp_dict)
        self._cleanup()


    def get_result(self, groupby=None):
        if groupby:
            self._groupby_res()
        else:
            self._join_res()
        return self.json_result
