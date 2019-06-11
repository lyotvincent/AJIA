import json
import numpy as np
from rdflib import *
import re
from pymongo import *
import os
import time
import sys
import datetime

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (np.int_, np.intc, np.intp, np.int8,
                            np.int16, np.int32, np.int64, np.uint8,
                            np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32,
                              np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return list(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        else:
            return json.JSONEncoder.default(self, obj)


def importGFF3(filename):
    start = time.clock()
    Fields = ['seqid', 'source', 'type', 'start', 'end', 'score', 'strand', 'phase']
    attributePos = 8
    i = 0
    isContent = False
    with open(filename, 'r') as fp:
        for line in fp:
            if line[0:3] != '###' and not isContent:
                continue
            elif line[0:3] == '###':
                isContent = True
                continue
            elif isContent:
                i += 1
                tabstr = line.strip('\n').split('\t')
                record1 = {
                    item: tabstr[i] for item, i in zip(Fields, range(len(Fields)))
                }
                semicolonstr = tabstr[attributePos].strip('\n').split(';')
                record2 = {
                    semicolonstr[i].split('=')[0] : semicolonstr[i].split('=')[1] for i in range(len(semicolonstr))
                }
                record3 = {
                    'attributes': record2
                }
                record = dict(record1, **record3)
                con.gff3.gff3.insert_one(record)

    end = time.clock()
    total_time = (end - start)
    return i,  total_time



if __name__ == '__main__':
    con = MongoClient('localhost', 27017)
    filepath = sys.argv[1]
    files = os.listdir(filepath)
    for file in files:
        file_ap = os.path.join(filepath, file)
        if not os.path.isdir(file_ap):
            i, time_1 = importGFF3(file_ap)
            with open('GFFF3_time', 'a') as fp:
                result = (file_ap + '\t' + '{0}' + '\t' + '{1}' + '\n').format(i, time_1)
                fp.write(result)
                print(result)
