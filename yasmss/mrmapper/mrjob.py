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
        self.output = None
        self.table = None


    def _get_set_config(self, queryset, sel_col_indexes, agg_index):
        conf = {}
        with open("config.yaml", 'r') as file:
            conf = yaml.load(file, Loader=yaml.FullLoader)

        self.input = conf['input']
        self.output = conf['output']
        self.hadoop_streaming_jar = conf['hadoop_streaming_jar']
        self.outputdir = conf['outputdir']

        conf['sel_col_indexes'] = sel_col_indexes
        conf['agg_index'] = agg_index
        conf['havop'] = queryset.havop
        conf['havrval'] = queryset.havrval
        conf['havlval'] = queryset.havlval.split('(')[0]

        with open("config.yaml", 'w') as target:
            yaml.dump(conf, target)


    def start_mrjob(self, queryset, classtype):
        if classtype == 'QuerySetGroupBy':
            table_schema_keys = list(schema.Schema().getSchemaDict(table=queryset.fromtable).keys())
            selcols = queryset.selcolumns[:len(queryset.selcolumns)-1]
            sel_col_indexes = []

            for i in selcols:
                sel_col_indexes.append(table_schema_keys.index(i))
            agg_index = table_schema_keys.index(queryset.aggcol)
            
            self._get_set_config(queryset, sel_col_indexes, agg_index)
            
            command = 'hadoop jar {hadoop_streaming_jar} -mapper "python mrmapper/groupby_mapper.py" -reducer "python mrmapper/groupby_reducer.py" -input /{input}/{table}.csv -output /{output}/{outputdir}'.format(
                            table=queryset.fromtable, input=self.input, output=self.output, hadoop_streaming_jar=self.hadoop_streaming_jar, outputdir=self.outputdir)
            
            start= time.time()
            os.system(command)
            
            time_delta = time.time() - start
            
            from mrresult import groupby_result
            res = groupby_result.MrResult(queryset)
            mrout = res.get_result()
            mrresult = {'mrout': mrout, 'Time taken': time_delta}
            return mrresult

