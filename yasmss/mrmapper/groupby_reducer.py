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
        print(self.clause, self.having, self.agg_operation, self.threshold_val)
        for k, v in self.res.items():
            if not self.having:
                print("{}\t{}".format(k, sum(v)))
            else:
                temp = [i for i in v if self.clauses[self.clause](i, self.threshold_val)]
                if len(temp):
                    print("{}\t{}".format(k, sum(temp)))


    def get_min(self):
        for k, v in self.res.items():
            if not self.having:
                print("{} \t {}".format(k, min(v)))
            else:
                temp = [i for i in v if self.clauses[self.clause](i, self.threshold_val)]
                if len(temp):
                    print("{}\t{}".format(k, min(temp)))
            
    def get_max(self):
        for k, v in self.res.items():
            if not self.having:
                print(k, max(v))
            else:
                temp = [i for i in v if self.clauses[self.clause](i, self.threshold_val)]
                if len(temp):
                    print("{}\t{}".format(k, max(i for i in v if self.clauses[self.clause](i, self.threshold_val))))

    def count(self):
        for k, v in self.res.items():
            if not self.having:
                print(k, len(v))
            else:
                temp = [i for i in v if self.clauses[self.clause](i, self.threshold_val)]
                if len(temp):
                    print("{}\t{}".format(k, len(temp)))



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
    # mrr = groupby_result.MrResult()
    # mrr.groupby_res()