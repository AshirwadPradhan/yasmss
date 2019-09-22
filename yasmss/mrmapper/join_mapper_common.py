'''
eg :
python select_mapper.py -t1 rating -t2 movies -on  movieid=movieid -w movieid=<value>
'''
#!/usr/bin/python

import sys
import argparse
import os
import json
import operator
from collections import OrderedDict
import re


class Mapper:
    left_table = None
    right_table = None
    onlval = None
    onrval = None
    wherecond = None
    wherelval = None
    whererval = None

    def __init__(self,args):
        self.left_table = args.table1
        self.right_table = args.table2
        self.onlval = args.on.split('=')[0]
        self.onrval = args.on.split('=')[1]
        if args.__dict__().where:
            where = self.get_operator_l_r(args.where)
            self.wherecond = where[1]
            self.wherelval = where[0]
            self.whererval = where[2]

    @staticmethod
    def get_operator_l_r(str):
        if str.find("<>") != -1:
            return (str.split("<>")[0], Mapper.get_operator_from_char("<>"),
                str.split("<>")[1])
        elif str.find("<") != -1:
            return (str.split("<")[0], Mapper.get_operator_from_char("<"),
                str.split("<")[1])
        elif str.find(">") != -1:
            return (str.split(">")[0], Mapper.get_operator_from_char(">"),
                str.split(">")[1])
        elif str.find("=") != -1:
            return (str.split("=")[0], Mapper.get_operator_from_char("="),
                str.split("=")[1])

    def __str__(self):
        # returns string representation of mapper args
        return (self.left_table +
                self.right_table +
                self.onlval +
                self.onrval +
                self.wherelval +
                self.wherecond.__name__ +
                self.whererval)

    @staticmethod
    def get_operator_from_char(operator_char):
        op = {"=":operator.eq,
              ">":operator.gt,
              "<":operator.lt,
             "<>":operator.ne}

        return op[operator_char]

    def get_schema(self, left_table, right_table):
        with open("../schema/schema.json", 'r') as file:
                data = json.load(file,object_pairs_hook=OrderedDict)
                ltable_schema = data[left_table]
                rtable_schema = data[right_table]
                return (ltable_schema, rtable_schema)



    def startjob(self):
        ltable_schema,rtable_schema = self.get_schema(self.left_table,self.right_table)
        # if on condition is not specified then find common column
        if self.onlval == None and self.onrval == None:
            join_on = list(set(ltable_schema & rtable_schema))[0]

        elif self.onlval in ltable_schema.keys():
            join_on = self.onlval

        elif self.onrval in rtable_schema.keys():
            join_on = self.onrval

        result_columns = list(set(list(ltable_schema) + list(rtable_schema))) #check  for resultant columns in result
        # common column should be on top

        result_columns.remove(join_on)
        result_columns.insert(0,join_on)
        for line in sys.stdin:
            line = line.strip()
            line = re.split(",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))",line)
            try:
                input_file = os.environ['mapreduce_map_input_file']
            except KeyError:
                input_file = os.environ['map_input_file']

            map_line = {}

            if input_file == self.left_table+'.csv':
                if self.wherelval in ltable_schema.keys():
                    if not self.wherecond(line[ltable_schema.keys().index(self.wherelval)],self.whererval):
                        continue
                if self.onlval == None and self.onrval == None:
                    map_line[join_on] = line[ltable_schema.keys().index(join_on)]
                    rem_col = ltable_schema.keys()
                    rem_col.remove(join_on)
                    rem_col.insert(0,join_on)
                    for col in rem_col:
                        map_line[col] = line[ltable_schema.keys().index(col)]
                else:
                    map_line[self.onlval] = line[ltable_schema.keys().index(self.onlval)]
                    rem_col = ltable_schema.keys()
                    rem_col.remove(self.onlval)
                    rem_col.insert(0,self.onlval)
                    for col in rem_col:
                        map_line[col] = line[ltable_schema.keys().index(col)]
            elif input_file == self.right_table+'.csv':
                print("table")
                # second table is being mapped

                if self.wherelval in ltable_schema.keys():
                    if not self.wherecond(line[ltable_schema.keys().index(self.wherelval)],self.whererval):
                        continue

                if self.onlval == None and self.onrval == None:
                    map_line[join_on] = line[rtable_schema.keys().index(join_on)]
                    rem_col = rtable_schema.keys()
                    rem_col = rem_col.remove(join_on)
                    rem_col.insert(0,join_on)
                    for col in rem_col:
                        map_line[col] = line[rtable_schema.keys().index(col)]

                else:
                    map_line[self.onlval] = line[rtable_schema.keys().index(self.onrval)]
                    rem_col = rtable_schema.keys()
                    rem_col = rem_col.remove(self.onrval)
                    rem_col.insert(0,self.onrval)
                    for col in rem_col:
                        map_line[col] = line[rtable_schema.keys().index(col)]
            stdout = []
            for c in result_columns:
                if c in map_line.keys():
                    stdout.append(map_line[c])
                else:
                    stdout.append(" ")
            print("^".join(map(str,stdout)))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--table1', '-t1', help="table1 - left table for join", type= str)
    parser.add_argument('--table2', '-t2', help="table2 - right table for join", type= str)
    parser.add_argument('--on', '-on', help="on condition", type=str)
    parser.add_argument('--where','-w',help="where condition", type=str)

    try:
        args = parser.parse_args()
    except:
        pass

    mapr = Mapper(args)
    mapr.startjob()


