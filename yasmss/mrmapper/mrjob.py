"""
    This module Drives the MR activity. Takes the query parameters from the driver and runs the map and reduce
    jobs accordingly. These query parameters(config/conf) are stored in a yaml so that the independant map and reduce jobs
    can access it(as Import wasnt working in mapreduce).
    Gets the exit code for MR and mrresult, calculates the time taken and returns the result and time to the driver.
"""

import os
from mrmapper import groupby_mapper
from schema import schema
import yaml
import time


class MRJob:
    def __init__(self):
        self.hadoop_streaming_jar = None
        self.mapper = "groupby_mapper"
        self.reducer = "groupby_reducer"
        self.input = None
        self.parentdir = None
        self.table = None
        self.table1 = None
        self.table2 = None


    def _get_set_config(self, queryset, sel_col_indexes, agg_index):
        conf = {}
        with open("config.yaml", 'r') as file:
            conf = yaml.load(file, Loader=yaml.FullLoader)

        self.input = conf['pathconfig']['input_dir']
        self.parentdir = conf['pathconfig']['parent_output_dir']
        self.hadoop_streaming_jar = conf['pathconfig']['hadoop_streaming_jar']
        self.outputdir = conf['pathconfig']['child_output_dir']

        conf['sel_col_indexes'] = sel_col_indexes
        conf['agg_index'] = agg_index
        conf['havop'] = queryset.havop
        conf['havrval'] = queryset.havrval
        conf['havlval'] = queryset.havlval.split('(')[0]

        with open("config.yaml", 'w') as target:
            yaml.dump(conf, target)

    def _get_where_col_index(self,  queryset, table, wherecol):
        index  = None
        if table == self.table1:
            table_schema_keys = list(schema.Schema().getSchemaDict(table=self.table1).keys())
            index = table_schema_keys.index(wherecol)
        else:
            table_schema_keys = list(schema.Schema().getSchemaDict(table=self.table2).keys())
            index = table_schema_keys.index(wherecol)
        return index


    def _get_col_len(self, table):
        tab_len = None
        if table == 1:
            tab_len = len(list(schema.Schema().getSchemaDict(table=self.table1).keys()))
        else:
            tab_len = len(list(schema.Schema().getSchemaDict(table=self.table2).keys()))
        return tab_len


    def start_mrjob(self, queryset, classtype):
        if classtype == 'QuerySetGroupBy':
            table_schema_keys = list(schema.Schema().getSchemaDict(table=queryset.fromtable).keys())
            selcols = queryset.selcolumns[:len(queryset.selcolumns)-1]
            sel_col_indexes = []

            for i in selcols:
                sel_col_indexes.append(table_schema_keys.index(i))
            agg_index = table_schema_keys.index(queryset.aggcol)
            
            self._get_set_config(queryset, sel_col_indexes, agg_index)
            
            command = 'mapred streaming -mapper "python mrmapper/groupby_mapper.py" -reducer "python mrmapper/groupby_reducer.py" -input /{input}/{table}.csv -output /{parent}/{outputdir}'.format(
                            table=queryset.fromtable, input=self.input, parent=self.parentdir, hadoop_streaming_jar=self.hadoop_streaming_jar, outputdir=self.outputdir)
            
            start= time.time()
            os.system(command)

            time_delta = time.time() - start

            from mrresult import groupby_result
            res = groupby_result.MrResult(queryset)
            mrout = res.get_result('groupby')
            mrresult = {'mrout': mrout, 'Time taken': time_delta}
            return mrresult

        else:
            table_schema_keys = list(schema.Schema().getSchemaDict(table=queryset.fromtable).keys())
            join_col = queryset.onlval.split('.')[1]
            join_col_index = table_schema_keys.index(join_col)

            self.table1 = queryset.fromtable
            self.table2 = queryset.jointable

            wheretable = queryset.wherelval.split('.')[0]

            wherelval = queryset.wherelval.split('.')[1]

            wherelval_index = self._get_where_col_index(queryset, wheretable, wherelval)

            whererval = queryset.whererval

            whereop = queryset.whereop

            conf = {}
            with open("config.yaml", 'r') as file:
                conf = yaml.load(file, Loader=yaml.FullLoader)

            self.input = conf['pathconfig']['input_dir']
            self.parentdir = conf['pathconfig']['parent_output_dir']
            self.outputdir = conf['pathconfig']['child_output_dir']

            conf['joinconfig']['join_col_index'] = join_col_index
            conf['joinconfig']['wherelval'] = wherelval_index
            conf['joinconfig']['whererval'] = whererval
            conf['joinconfig']['whereop'] = whereop
            conf['joinconfig']['table1'] = self.table1
            conf['joinconfig']['table2'] = self.table2
            conf['joinconfig']['wheretable'] = 1 if wheretable == self.table1 else 2
            conf['joinconfig']['table1_len'] = self._get_col_len(1)
            conf['joinconfig']['table2_len'] = self._get_col_len(2)

            with open("config.yaml", 'w') as target:
                yaml.dump(conf, target)

            command = 'mapred streaming -mapper "python mrmapper/join_mapper.py" -reducer "python mrmapper/join_reducer.py" -input /{input}/{table1}.csv -input /{input}/{table2}.csv -output /{parentdir}/{output}'.format(
                input=self.input, parentdir=self.parentdir, output=self.outputdir, table1=self.table1, table2=self.table2)

            start= time.time()
            os.system(command)

            time_delta = time.time() - start

            from mrresult import groupby_result
            res = groupby_result.MrResult(queryset)
            mrout = res.get_result()
            mrresult = {'mrout': mrout, 'Time taken': time_delta}
            return mrresult
