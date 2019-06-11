import json
import numpy as np
import sys
from pymongo import MongoClient
import re

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


def RenameJsonKey(strJson):
    if isinstance(strJson,dict):
        strJson = json.dumps(strJson)
    #先默认json的key中没有特殊符号
    pattern = re.compile(r"\"([\w.$:]+)\":")
    strJson = pattern.sub(lambda m: m.group(0).replace('.', "_").replace('$', "^"), strJson)
    return strJson



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
                collection.insert_one(buf)
    return


if __name__ == '__main__':
    filename = sys.argv[1]
    Fields = ['CHROM', 'POS', 'ID', 'REF', 'ALT', 'FILTER']
    InfoPos = 7
    isContent = False
    with open("clinvr37.json", 'a') as outfp:
        with open(filename, 'r') as fp:
            for line in fp:
                if line[0] == '#':
                    continue
                else:
                    tabstr = line.strip('\n').split('\t')
                    record1 = {
                        item: tabstr[i] for item, i in zip(Fields, range(len(Fields)))
                    }
                    semicolonstr = tabstr[InfoPos].split(';')
                    record2 = {
                        semicolonstr[i].split('=')[0] : semicolonstr[i].split('=')[1] for i in range(len(semicolonstr))
                    }
                    record3 = {
                        'INFO': record2
                    }
                    record = dict(record1, **record3)
                    recordstring = json.dumps(record, cls=MyEncoder) + '\n'
                    outfp.write(recordstring)
    print("done!")
    #
    # connection = MongoClient('localhost', 27017)
    # collection = connection.vcf.clinvar
    # ImportJson2Mongodb(filename, collection)