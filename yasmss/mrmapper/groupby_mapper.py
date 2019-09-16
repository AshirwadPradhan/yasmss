#!/usr/bin/python

import sys
import time
import yaml

class Mapper:
    def __init__(self):
        self.sel_col_indexes = []
        self.agg_col_index = None


    def getdata(self, queryset):
        table_schema_keys = list(schema.Schema().getSchemaDict(table=queryset.fromtable).keys())
        self.sel_col_indexes = [table_schema_keys.index('%s'%col for col in queryset.selcolumns)]
        self.agg_indexes = table_schema_keys.index(queryset.aggcol)
        

    def startjob(self):
        start_time = time.time()
        for line in sys.stdin:
            line = line.strip()
            line = line.split(',')

            sel_cols = [line[x] for x in self.sel_col_indexes]
            agg_cols = self.agg_col_index

            print("{}\t{}".format(",".join(sel_cols), agg_cols))


if __name__ == '__main__':
    mapr = Mapper()
    import os
    
    conf = {}
    
    with open("config.yaml", 'r') as file:
        conf = yaml.load(file, Loader=yaml.FullLoader)
    
    mapr.sel_col_indexes = conf['sel_col_indexes']
    mapr.agg_col_index = conf['agg_index']
    mapr.startjob()

