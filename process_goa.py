import json
from rdflib import *
import re
from pymongo import *
import os
import time
import sys
import os


def process_goa(filepath):
    con = MongoClient('localhost', 27017)
    my_db = con.goa
    i = 0
    with open(filepath, 'r', encoding='UTF-8') as f:
        #line = f.readline()
        head = ['DB', 'DB_Object_ID', 'DB_Object_Symbol', 'Qualifier', 'GO_ID', 'DB_Reference', 'Evidence_Code', 'With/From', 'Aspect', 'DB_Object_Name', 'DB_Object_Synonym', 'DB_Object_Type', 'Taxon', 'Date', 'Assigned_By', 'Annotation_Extension', 'Gene_Product_Form_ID']
        # print(len(head))
        line = f.readline()
        # line_list = line.split('\t')
        # print(len(line_list))

        while line:
            if line[0] == '!':
                line = f.readline()
            else:
                line_list = line[:-1].split('\t')
                tmp_dict = dict(zip(head, line_list))
                my_db.goa_tmp.insert_one(tmp_dict)
                line = f.readline()
                i = i+1
    return i


if __name__ == "__main__":
    goapath = '/home/qz/Desktop/XMLdata/goa/'
    f = open('/home/qz/Desktop/XMLdata/goa_time.txt', 'a')
    for file in os.listdir(goapath):
        print(file)
        file_path = os.path.join(goapath, file)
        start = time.clock()
        s = process_goa(file_path)
        end = time.clock()
        total_time = (end - start)
        f.write(file+'\t'+str(s)+'\t'+str(total_time)+'\n')
    # start = time.clock()
    # process_goa(goapath)
    # end = time.clock()
    # total_time = (end - start)
        print(total_time)
    f.close()