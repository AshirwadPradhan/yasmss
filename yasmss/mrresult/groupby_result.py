"""
    returns the the MR result stored in HDFS directory, maps it to columns names and transforms it into an array of dict.
"""
import subprocess
import os
import yaml

class MrResult:
    def __init__(self,queryset):
        self.json_result = []
        self.col_names = []
        self.queryset = queryset
        self.output = None
        self.opfile = "part-00000"
        self.outputpath = None
        self.outputdir = None

    def _get_outputpath(self):
        conf = {}
        with open("config.yaml", 'r') as file:
            conf = yaml.load(file, Loader=yaml.FullLoader)
        self.outputpath = conf['output']
        self.outputdir = conf['outputdir']
        return "hdfs://localhost:9000/{path}/{outputdir}/{outputfile}".format(
                path=self.outputpath, outputdir=self.outputdir, outputfile=self.opfile)


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
        rm_output = "hdfs dfs -rm -r hdfs://localhost:9000/{path}/{outputdir}".format(path=self.outputpath, outputdir=self.outputdir)
        os.system(rm_output)

    def get_result(self):
        self._groupby_res()
        return self.json_result