import json
from rdflib import *
import re
from pymongo import *
import os
import time
import sys



def process_hpoteam(filepath):
    con = MongoClient('localhost', 27017)
    my_db = con.hpoew
    with open(filepath, 'r', encoding='UTF-8') as f:
        #line = f.readline()
        head = ['DB', 'DB_Object_ID', 'DB_Name', 'Qualifier', 'HPO_ID', 'DB_Reference', 'Evidence_Code', 'Onset modifier', 'Frequency', 'Sex', 'Modifier', 'Aspect', 'Date_Created', 'Assigned_By']
        # print(len(head))
        line = f.readline()
        # line_list = line.split('\t')
        # print(len(line_list))
        while line:
            line_list = line[:-1].split('\t')
            tmp_dict = dict(zip(head, line_list))
            my_db.hpoteamnew.insert_one(tmp_dict)
            line = f.readline()


if __name__ == "__main__":
    start = time.clock()
    goapath = sys.argv[1]
    process_hpoteam(goapath)
    end = time.clock()
    total_time = (end - start)
    print(total_time)