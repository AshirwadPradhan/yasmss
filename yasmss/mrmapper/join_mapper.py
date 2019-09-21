#!/usr/bin/python3

import sys
import yaml
import json
import collections


class Mapper:
    def start_job(self):
        for line in sys.stdin:
            line = line.strip()
            line = line.split(',')

            if len(line) == 5:
                print("1,{}".format(",".join(line)))
            else:
                print("2,{}".format(",".join(line)))


if __name__ == "__main__":
    mapper = Mapper()
    mapper.start_job()

