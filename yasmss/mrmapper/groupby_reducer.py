#!/usr/bin/python

import sys
import operator
import yaml

class Reducer:
    def __init__(self):
        self.res = {}
        self.having = True
        # self.clause = ">"
        # self.threshold_val = 2
        # self.agg_operation = "sum"

        self.clauses = { '<':  operator.lt, 
                    '<=': operator.le,
                    '>':  operator.gt, 
                    '>=': operator.ge,
                    '==': operator.eq,
                    '!=': operator.ne
                }
        confg = {}
        with open("config.yaml", 'r') as file:
            conf = yaml.load(file, Loader=yaml.FullLoader)
        
        self.clause = conf['havop']
        self.threshold_val = int(conf['havrval'])
        self.agg_operation = conf['havlval']


    def get_sum(self):
        for k, v in self.res.items():
            val_sum = sum(v)
            if self.clauses[self.clause](val_sum, self.threshold_val):
                print("{}\t{}".format(k, val_sum))


    def get_min(self):
        for k, v in self.res.items():
            val_min = min(v)
            if self.clauses[self.clause](val_min, self.threshold_val):
                print("{} \t {}".format(k, val_min))

            
    def get_max(self):
        for k, v in self.res.items():
            val_max = max(v)
            if self.clauses[self.clause](val_max, self.threshold_val):
                print("{} \t {}".format(k, val_max))

    def count(self):
        for k, v in self.res.items():
            val_len = len(v)
            if self.clauses[self.clause](val_len, self.threshold_val):
                print("{} \t {}".format(k, val_len))



if __name__ == "__main__":
    red = Reducer()

    for line in sys.stdin:
        line = line.strip()
        line = line.split('\t')

        try:
            if line[0] in red.res:
                red.res[line[0]].append(int(line[1]))
            else:
                red.res[line[0]] = [int(line[1])]
        
        except:
            print("Invalid Int conversion")
        
    if red.agg_operation == "sum":
        red.get_sum()
    elif red.agg_operation == "min":
        red.get_min()
    elif red.agg_operation == "max":
        red.get_max()
    elif red.agg_operation == "count":
        red.count()
    else:
        print("Invalid operation")
