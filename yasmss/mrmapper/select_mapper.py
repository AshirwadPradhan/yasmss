# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 17:56:49 2019

@author: Pallav
@format: python mapper.py -t1 <table1> -t2 <table2> -w "attribute=|<>|<|>value"
<table1> & <table2> schema should be declared in schema.py 
"""

#!/usr/bin/env python
import sys
import operator
import argparse
# input comes from STDIN (standard input)
DELIMITER = '^' # this is delimiter for Key value pair
# parse command python mapper.py -t1 <TableName1> -t2 <TableName2> -w <Conditions> 

# All the Schema of Data files should be declared here in format "TableName" : "ColumnName1  |  ColumnName2  |  ... ColummnNameN"
Schema = {"users" : " userID  |  age  |  gender  |  occupation  |  zipcode ",
         "zipcodes" : " zipcode  |  zipcodetype  |  city  |  state ",
          "movies" : " movieID | title | release | unknown | action | adventure | animation | children | comedy | crime | documentary | drama | fantasy | filmnoir | horror | musical | mystery | romance | scifi | thriller | war | western | unknown2 | unknown3 ",
          "rating" : " userID  |  movieID  |  rating  |  timestamp "}


parser = argparse.ArgumentParser()
parser.add_argument('--table1', '-t1', help="table1 - left table for join", type= str)
parser.add_argument('--table2', '-t2', help="table2 - right table for join", type= str)
parser.add_argument('--where','-w',help="where condition", type=str )
where_clause_str = None
try:
    args = parser.parse_args()
    # get columns name from schema.py
    t1_schema = [c.strip() for c in Schema[args.__dict__['table1']].split('|')]
    t2_schema = [c.strip() for c in Schema[args.__dict__['table2']].split('|')]
    try:
        where_clause_str = args.__dict__['where']
    except:
        pass
    

    
except:
    print("ERROR: Missing Tables or Conditions")
    sys.exit(1)
    
    
if where_clause_str != None:
    # where clause is present ! Lets Parse it

    if where_clause_str.find("<>")!=-1:        
        # not equal to operator present
        f = where_clause_str.split("<>")
        f.append(operator.ne)

        
    elif where_clause_str.find("<")!=-1:
        # less than operator
        f = where_clause_str.split("<")
        f.append(operator.lt)       
        
    
    elif where_clause_str.find(">")!=-1:
        # more than
        f = where_clause_str.split(">")
        f.append(operator.gt)      
        
    elif where_clause_str.find("=")!=-1:
        # equal to        
        f = where_clause_str.split("=")
        f.append(operator.eq)
        
    
# check if two tables that user wants to join have common columns
# currently this code skips for ON clause and looks for common columns that exist between two tables for join
#checks for intersection between t1_schema and t2_schema
if (len(set(t1_schema) & set(t2_schema))<1):  
    print("NOT ENOUGH COMMON COLUMNS TO JOIN")
    sys.exit(1)
    
common_column = list(set(t1_schema) & set(t2_schema))[0]   # check for common column btw two schema 
        
result_columns = list(set(t1_schema+t2_schema)) #check  for resultant columns in result
# common column should be on top        
result_columns.remove(common_column)
result_columns.insert(0,common_column)
dic_index = {}
for x in result_columns:
    dic_index[x] = -1 # index missing    
    


def checkIfHeader(splits):
    for split in splits:
        if(split[0:2]!='||' or split[-2:]!='||'):  
            return False
    return True
current_stdin_table = None

for line in sys.stdin:
    
    try: #sometimes bad data can cause errors use this how you like to deal with lint and bad data
#        line= "||userID||,||age||,||gender||,||occupation||,||zipcode||"
        line = line.strip()  # strip leading and trailing ws from STDIN 
        splits = line.split(",") # splits on basis of comma       
        if checkIfHeader(splits):
            # STDIN is header of some table
            columns = [x.strip('||') for x in line.split(",")]
            
        
        # check if splits are header 
   
        splits = line.split(",")
        if checkIfHeader(splits)==True:
            headers = [split.strip('||') for split in splits]
            # table header , check which table is in stdin
            if headers == t1_schema:
                #STDIN is handling table1
                current_stdin_table = 1
                for header in enumerate(headers):
                    dic_index[header[1]]= header[0]
                
            if headers == t2_schema:
                current_stdin_table = 2
                #STDIN is handling table2
                for header in enumerate(headers):
                    dic_index[header[1]]= header[0]
                    
        else:
            if current_stdin_table == 1:
                
                if not f[-1](int(splits[dic_index[f[0]]]),int(f[1])) and where_clause_str != None:
                    continue
                    
                stdout = splits[dic_index[common_column]].strip('"')  #strips " if it exists
                for column in result_columns:
                    if dic_index[column]!=-1 and column!=common_column:    
                        stdout+=DELIMITER+splits[dic_index[column]].strip('"')
                print(stdout+DELIMITER+"|1")   # stdout with |1 to identify table - it will help reducer
            
            elif current_stdin_table == 2:
                
                if not f[-1](int(splits[dic_index[f[0]]]),int(f[1])) and where_clause_str != None:
                    continue
                    
                stdout = splits[dic_index[common_column]].strip('"') #strips " if it exists
                for column in result_columns:
                    if dic_index[column]!=-1 and column!=common_column:    
                        stdout+=DELIMITER+splits[dic_index[column]].strip('"')
                print(stdout+DELIMITER+"|2")
            
            else:
                print("Table Headers not Found ! Please add Headers on First Row ||Column_Name||...")

    except: #errors are going to make your job fail which you may or may not want
        pass