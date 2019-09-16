import os
from mrmapper import groupby_mapper
from schema import schema
import yaml
import time
import pprint


class MRJob:
    def __init__(self):
        self.hadoop_streaming = "/usr/share/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar"
        self.mapper = "groupby_mapper"
        self.reducer = "groupby_reducer"
        self.input = "test"
        self.output = "test"
        self.table = "rating"
        # self.basepath = "hdfs://localhost:9000/"

    # @staticmethod
    # def get_outputpath(g_type = None):
    #     if g_type == 'generate':
    #         count = count + 1
    #     return str(count)


    def start_mrjob(self, queryset, classtype):
        if classtype == 'QuerySetJoin':
            conf = {}
            
            table_schema_keys = list(schema.Schema().getSchemaDict(table=queryset.fromtable).keys())
            # import pdb; pdb.set_trace()
            selcols = queryset.selcolumns[:len(queryset.selcolumns)-1]
            sel_col_indexes = []
            for i in selcols:
                sel_col_indexes.append(table_schema_keys.index(i))
            agg_index = table_schema_keys.index(queryset.aggcol)
            
            with open("config.yaml", 'r') as file:
                conf = yaml.load(file, Loader=yaml.FullLoader)
            conf['sel_col_indexes'] = sel_col_indexes
            conf['agg_index'] = agg_index
            conf['havop'] = queryset.havop
            conf['havrval'] = queryset.havrval
            conf['havlval'] = queryset.havlval.split('(')[0]

            with open("config.yaml", 'w') as target:
                yaml.dump(conf, target)
            
            
            command = """ hadoop jar {jar} -mapper "python {mapper}.py" -reducer "python {reducer}.py" -input /{input}/{table}.csv -output /{output}/output""".format(
                            jar=self.hadoop_streaming, mapper=self.mapper, reducer = self.reducer, input =self.input,
                            output= self.output, table = self.table)
            command = 'hadoop jar /usr/share/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -mapper "python mrmapper/groupby_mapper.py" -reducer "python mrmapper/groupby_reducer.py" -input /test/rating.csv -output /test/output_gx2'
            

            start= time.time()
            os.system(command)

            
            print("Time taken : " + str(time.time() - start))
            
            from mrresult import groupby_result
            res = groupby_result.MrResult()
            
            pprint.pprint(res.get_result())
            

            # with open("/home/shash/Documents/yasmss1/yasmss/config.yaml", 'r') as file:
            #     conf = yaml.load(file, Loader=yaml.FullLoader)
            #     # print(conf)
            
    
    
    def get_output_path(self):
        d = {'path': self.output }
        return d 
