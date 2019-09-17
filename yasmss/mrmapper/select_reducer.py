# -*- coding: utf-8 -*-
"""
Created on Sun Sep 15 09:56:37 2019

@author: pallav
"""
#!/usr/bin/env python
import sys
# input comes from STDIN (standard input)
 
last_id = None
dic_1 = []
dic_2 = []
for line in sys.stdin:
    try:
        line= line.strip()   #strip whitespaces from stdin
        splits = line.split('^')
        
        if splits[-1]=="|1" and dic_1 == []:
            dic_1 = dic_1 + splits[0:-1]
            
        if splits[-1]=="|2" and dic_1 != [] and splits[0] == dic_1[0]:
            #print(dic_1+splits[1:-1])
            print(' '.join(map(str,dic_1+splits[1:-1])))
            
        if splits[-1]=="|1" and dic_1 != [] and splits[0] != dic_1[0]:
            dic_1 = splits[0:-1]
            
    except:
        pass
        
        
        