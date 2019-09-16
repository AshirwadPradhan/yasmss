import subprocess
import os

class MrResult:
    def __init__(self,queryset):
        self.output = "hdfs://localhost:9000/test/output_gx2/part-00000"
        self.json_result = []
        self.col_names = []
        self.queryset = queryset

    def _get_col_names(self):
        selcols = self.queryset.selcolumns[:len(self.queryset.selcolumns)-1]
        selcols.append(self.queryset.aggcol)
        return selcols

    def _groupby_res(self):
        self.col_names = self._get_col_names()
        cat = subprocess.Popen(["hadoop", "fs", "-cat", self.output], stdout=subprocess.PIPE)
        for line in cat.stdout:
            self.json_result.append(dict(zip(self.col_names, line.decode('utf-8').split('\n')[0].replace(",", "\t").split('\t'))))
            # t = dict(zip(self.[col_names], line.decode('utf-8').split('\n')[0].split('\t')))
        self._cleanup()

    def _cleanup(self):
        os.system("hdfs dfs -rm -r hdfs://localhost:9000/test/output_gx2")

    def get_result(self):
        self._groupby_res()
        return self.json_result