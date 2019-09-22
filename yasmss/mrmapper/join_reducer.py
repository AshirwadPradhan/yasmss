#!/usr/bin/python3

import sys
import yaml
import operator
import json
import collections


a1= []
a2 =[]

class Reducer:
    def __init__(self):
        self.dd = {}
        self.clauses={ '<':  operator.lt, 
                '<=': operator.le,
                '>':  operator.gt, 
                '>=': operator.ge,
                '=': operator.eq,
                '!=': operator.ne
            }
        self.wherelval = None
        self.whererval = None
        self.whereop = None
        self.join_col_index = None


    def get_config(self):
        conf = {}
        with open('config.yaml', 'r') as file:
            conf = yaml.load(file, Loader=yaml.FullLoader)
            self.join_col_index = conf['joinconfig']['join_col_index']
            self.wherelval = conf['joinconfig']['wherelval']
            self.whererval = conf['joinconfig']['whererval']
            self.whereop = conf['joinconfig']['whereop']
            self.wheretable = conf['joinconfig']['wheretable']


    def start_reduce(self):
        self.get_config()
        for line in sys.stdin:
            line = line.strip()
            vals = line.split(',')
            
            if vals[0] == '1':
                a1.append(vals[1:])
            else:
                a2.append(vals[1:])

        for c1 in a1:
            for c2 in a2:
                if c1[self.join_col_index] in c2:
                    if int(self.wheretable) == 1:
                        tab = c1
                    else:
                        tab = c2
                    if self.clauses[self.whereop](tab[self.wherelval], self.whererval):
                        op = c1+c2
                        op = list(collections.OrderedDict.fromkeys(op))
                        print("\t".join(op))


if __name__ == "__main__":
    reducer = Reducer()
    reducer.start_reduce()