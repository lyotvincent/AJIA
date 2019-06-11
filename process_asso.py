import json
import datetime
import time
import re
import argparse
import os
from pymongo import *
import sys
from bson import Code


def CreatIndex(collection):
    map = Code("""
    function(){
        for (var key in this) { 
          emit(key, null);
        }
    }
    """)
    reduce=Code("""
        function (key, values) {
            return key;
        }
    """)
    keys = collection.map_reduce(map, reduce, out = {'inline' : 1} , full_response = True)
    for key in keys['results']:
        try:
            collection.create_index([(key['value'], 1)], background=True)
        except:
            continue
    return


def process_asso(filepath, ele1, ele2, IsCreatIndex):
    con = MongoClient('localhost', 27017)
    my_db = con.asso
    collectionname = ele1 + '_' + ele2
    with open(filepath, 'r', encoding='UTF-8') as f:
        head = [ele1, ele2]
        line = f.readline()
        bf = []
        i = 0
        while line:
            line_list = line[:-1].split(',')
            tmp_dict = dict(zip(head, line_list))
            bf.append(tmp_dict)
            if len(bf) == 10000:
                my_db.collectionname.insert_many(bf)
                bf=[]
                i+=1
                print(i)
            line = f.readline()
        my_db[collectionname].insert_many(bf)
    if IsCreatIndex == 'Y':
        CreatIndex(my_db[collectionname])


if __name__ == '__main__':
    input_path = sys.argv[1]
    IsCreatIndex = sys.argv[2]
    for file in os.listdir(input_path):
        print(file)
        file_path = os.path.join(input_path, file)
        process_asso(file_path, file.split('#')[0], file.split('#')[1], IsCreatIndex)