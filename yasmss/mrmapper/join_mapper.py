#!/usr/bin/python3

import sys
import yaml
import json
import collections


class Mapper:
    def __init__(self):
        self.table1 = None
        self.table2 = None

    def get_config(self):
        conf = {}
        with open('config.yaml', 'r') as file:
            conf = yaml.load(file, Loader=yaml.FullLoader)
        self.table1 = conf['joinconfig']['table1_len']
        self.table2 = conf['joinconfig']['table2_len']

    def start_job(self):
        self.get_config()
        for line in sys.stdin:
            line = line.strip()
            line = line.split(',')

            # if self.table1 != self.table2:
            if len(line) == int(self.table1):
                print("1,{}".format(",".join(line)))
            else:
                print("2,{}".format(",".join(line)))


if __name__ == "__main__":
    mapper = Mapper()
    mapper.start_job()

