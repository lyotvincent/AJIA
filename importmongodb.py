import os
import json
from pymongo import MongoClient
import re
import time
import sys

def ImportJson2Mongodb(filepath_json, collection):
    with open(filepath_json) as file:
        while True:
            line = file.readline()
            if not line:
                break
            else:
                #delete space and \r\n
                line = line[:-1]
                line = RenameJsonKey(line)
                buf = json.loads(line)
                collection.insert_many(buf)
    return


def RenameJsonKey(strJson):
    if isinstance(strJson,dict):
        strJson = json.dumps(strJson)
    #先默认json的key中没有特殊符号
    pattern = re.compile(r"\"([\w.$:]+)\":")
    strJson = pattern.sub(lambda m: m.group(0).replace('.', "_").replace('$', "^"), strJson)
    return strJson



if __name__ == "__main__":
    start = time.time()
    filepath = sys.argv[1]
    connection = MongoClient('localhost', 27017)
    collection = connection.vcf1000g.chr21
    ImportJson2Mongodb(filepath, collection)
    end = time.time()
    print("done!")
    print(end-start)
