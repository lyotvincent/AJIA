#-*- coding: utf-8 -*-
#from bson import Code
# from django.shortcuts import render
# from django.http import HttpResponse, JsonResponse
# from django.views.decorators.csrf import csrf_exempt
import os
import allel
import numpy as np
import json
import multiprocessing
import pickle
import copy
from pymongo import MongoClient
import re
import zipfile
#from django.http import StreamingHttpResponse
from wsgiref.util import FileWrapper
from functools import partial
import time
import sys
if sys.platform.startswith('linux'):
    import fcntl
else:
    from lockfile import LockFile
#import zerorpc

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


#class Transform(object):
def addhead(header, filepath_json):
    record_head = []
    for line in header:
        line = line.strip('\n')
        record_head.append(line)
    record = {
       "header": record_head
    }
    with open(filepath_json, 'a') as fp:
        recordstring = json.dumps(record, cls=MyEncoder) + '\n'
        fp.write(recordstring)
    return

def chunker2string(chunker, fields, samples, mode='MergeSamples'):
    li = []
    infoNum = 0
    infoSpecial = 0
    #把NaN转换成-1
    for i in range(chunker[1]):
        for field in fields:
            if isinstance(chunker[0][field][i], np.ndarray) and not isinstance(chunker[0][field][i][0], np.str):
                nanpos = np.isnan(chunker[0][field][i])
                chunker[0][field][i][nanpos] = -1.0

    if mode == 'MergeAll':
        for i in range(chunker[1]):
            #basic
            recorddict1 = {
                "CHROM": chunker[0]['variants/CHROM'][i],
                "POS" : chunker[0]['variants/POS'][i],
                "ID": chunker[0]['variants/ID'][i],
                "REF": chunker[0]['variants/REF'][i],
                "ALT": chunker[0]['variants/ALT'][i],
                "QUAL": chunker[0]['variants/QUAL'][i],
            }
            #filter
            recorddict2 = {
                "FILTER": {
                    k_filter[9:] : chunker[0][k_filter][i] for k_filter in fields if 'variants/FILTER' in k_filter
                }
            }

            #Info
            recorddict3 = {
                "INFO": {
                    k_Info[9:] : chunker[0][k_Info][i] for k_Info in fields if k_Info not in ['variants/CHROM', 'variants/POS', 'variants/ID', 'variants/REF', 'variants/ALT', 'variants/QUAL', 'variants/numalt', 'variants/svlen', 'variants/is_snp']
                    and 'variants/FILTER' not in k_Info and 'calldata/' not in k_Info
                }
            }

            infoNum += len(recorddict3['INFO'])
            try:
                if recorddict3['INFO']['AC'][0] == 1:
                    infoSpecial += 1
            except Exception as e:
                pass


            #Samples
            recordsamples = []
            for k_sample, j in zip(samples, range(samples.size)):
                recordsample1 = {
                    "SAMPLENO": k_sample
                }
                recordsample2 = {
                    k_field[9:]: [chunker[0][k_field][i][j][n] for n in
                                  range(chunker[0][k_field][i][j].size)] if isinstance(
                        chunker[0][k_field][i][j], np.ndarray) else chunker[0][k_field][i][j] for k_field in
                    fields if "calldata/" in k_field
                }
                recordsample = dict(recordsample1, **recordsample2)
                recordsamples.append(recordsample)
            recorddict4 = {
                "SAMPLES": recordsamples
            }

            recorddictMerge = dict(dict(dict(recorddict1, **recorddict2), **recorddict3),**recorddict4)
            #recorddictMerge = dict(recorddict1, **recorddict2, **recorddict3, **recorddict4)
            #recorddictMerge = dict(recorddict1, recorddict2, recorddict3, recorddict4)
            li.append(recorddictMerge)

    elif mode == 'MergeSamples':
        for i in range(chunker[1]):
            recorddict1 = {
                k_field[9:]: [chunker[0][k_field][i][m] for m in range(chunker[0][k_field][i].size)] if isinstance(
                    chunker[0][k_field][i], np.ndarray) else chunker[0][k_field][i] for k_field in fields if
                'variants/' in k_field and k_field not in  ['variants/numalt', 'variants/svlen', 'variants/is_snp']
            }
            recordsamples = []
            for k_sample, j in zip(samples, range(samples.size)):
                recordsample1 = {
                    "SAMPLENO": k_sample
                }
                recordsample2 = {
                    k_field[9:]: [chunker[0][k_field][i][j][n] for n in
                                  range(chunker[0][k_field][i][j].size)] if isinstance(
                        chunker[0][k_field][i][j], np.ndarray) else chunker[0][k_field][i][j] for k_field in
                    fields if "calldata/" in k_field
                }
                recordsample = dict(recordsample1, **recordsample2)
                recordsamples.append(recordsample)
            recorddict2 = {
                "SAMPLES": recordsamples
            }

            recorddict = dict(recorddict1, **recorddict2)
            li.append(recorddict)

    recordstring = json.dumps(li, cls=MyEncoder) + '\n'
    return recordstring, infoNum, infoSpecial


def IoOperat_multi(tmpfile, mode, statisticArr, chunker):
    # tmpfile = "value_" + md5 + ".dat"
    with open(tmpfile, "rb") as f:
        fields = pickle.load(f)
        samples = pickle.load(f)
        headers = pickle.load(f)
        filepath_json = pickle.load(f)
    recordstring, infonum, infoSpecial = chunker2string(chunker, fields, samples, mode)
    if sys.platform.startswith('linux'):
        with open(filepath_json, "a") as fp:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX)
            statisticArr[0] += chunker[1]
            statisticArr[1] += infonum
            statisticArr[2] += infoSpecial
            fp.write(recordstring)
    else:
        lock = LockFile(filepath_json)
        lock.acquire()
        with open(filepath_json, "a") as fp:
            statisticArr[0] += chunker[1]
            statisticArr[1] += infonum
            statisticArr[2] += infoSpecial
            fp.write(recordstring)
        lock.release()
    return


def vcf2json_Single(filepath_vcf, filepath_json, mode):
    fields, samples, headers, chunks = allel.iter_vcf_chunks(filepath_vcf, fields=['*'], chunk_length=50)

    if os.path.exists(filepath_json):
        os.remove(filepath_json)
    addhead(headers[0], filepath_json)

    for chunker in chunks:
        with open(filepath_json, 'a') as fp:
            recordstring = chunker2string(chunker, fields, samples, mode)
            fp.write(recordstring)

    return


def vcf2json_multi2(filepath_vcf, filepath_json, md5, mode):
    #统计数据
    time_start = time.time()
    manager = multiprocessing.Manager()
    statisticArr = manager.Array("i", [0, 0, 0])

    fields, samples, headers, chunks = allel.iter_vcf_chunks(filepath_vcf, fields=['variants/*', 'calldata/*'],chunk_length=500)
    print(filepath_vcf)
    if os.path.exists(filepath_json):
        os.remove(filepath_json)
    #增加原vcf文件的头部信息, 用于逆向转换
    #addhead(headers[0], filepath_json)

    tmpfile = "value_" + md5 + ".dat"
    with open(tmpfile, "wb") as f:
        pickle.dump(fields, f)
        pickle.dump(samples, f)
        pickle.dump(headers, f)
        pickle.dump(filepath_json, f)

    cores = multiprocessing.cpu_count()
    processnum = max(int(cores / 2), 2)
    #processnum = min(cores, 20)
    #processnum = int(cores / 2)

    #自己调度迭代器 防止内存溢出
    pool = multiprocessing.Pool(processes=processnum)
    index = 0
    tmpchunks = []
    i = 0
    # for chunker in chunks:
    #     index+=1
    #     tmpchunks.append(chunker)
    #     if index % (processnum*10) == 0:
    #         # i += 1
    #         # print(("{0} - 1").format(i))
    #         pool.map(partial(IoOperat_multi, tmpfile, mode, statisticArr), tmpchunks)
    #         #print(("{0} - 2").format(i))
    #         #pool.map(partial(IoOperat_multi, tmpfile, mode, statisticArr), tmpchunks)
    #         # time.sleep(10)
    #         tmpchunks.clear()
    first = True
    realchunks=[]

    for chunker in chunks:
        index+=1
        tmpchunks.append(chunker)
        if index % (processnum*10) == 0:
            if not first:
                AppResult.get()
                realchunks.clear()
            realchunks = copy.deepcopy(tmpchunks)
            tmpchunks.clear()
            first=False
            AppResult = pool.map_async(partial(IoOperat_multi, tmpfile, mode, statisticArr), realchunks)

    if "AppResult" in locals().keys():
        AppResult.get()
    #print("last section")
    pool.map(partial(IoOperat_multi, tmpfile, mode, statisticArr), tmpchunks)
    tmpchunks.clear()
    if realchunks:
        realchunks.clear()
    pool.close()
    pool.join()  # 主进程阻塞等待子进程的退出
    os.remove(tmpfile)  # 删除临时文件,节约空间

    #保存统计数据
    filesize = os.path.getsize(filepath_json)
    time_end = time.time()
    time_cost = time_end - time_start
    dir = os.path.splitext(filepath_vcf)[0]
    #statisticFile = dir + '.txt'
    statisticFile = "vcf2json_results.txt"
    # with open(statisticFile, mode='a') as fp:
    #     result = (filepath_vcf + '\t' + 'chrom: ' + '{0}' + '\t' + 'info: ' + '{1}' + '\t' + 'sample: ' + '{2}' + '\t' +'total cost: ' + '{3}' +
    #               '\t' + 'jsonfilesize: ' + '{4}' + 'infoSpecial: {5}' + '\n').format(statisticArr[0], statisticArr[1], samples.size, time_cost, filesize, statisticArr[2])
    #     fp.write(result)
    #os.remove(filepath_json)  # 删除文件,节约空间


def dotranform(filepath_vcf, mode):
    basename = os.path.basename(filepath_vcf)
    file_json = os.path.splitext(basename)[0] + ".json"
    #file_json = os.path.splitext(filepath_vcf)[0] + ".json"
    vcf2json_multi2(filepath_vcf, file_json, "tmpdat", mode)
    #vcf2json_Single(filepath_vcf, file_json, mode)


#with output path
def dotransformWithOutPath(filepath_vcf, filepath_json, mode):
    vcf2json_multi2(filepath_vcf, filepath_json, "tmpdat", mode)


def preview(filepath_vcf, mode):
    fields, samples, headers, chunks = allel.iter_vcf_chunks(filepath_vcf, fields=['*'], chunk_length=2)
    #get first 2 lines for example
    #get json
    for chunker in chunks:
        recordstring = chunker2string(chunker, fields, samples, mode)
        recordstring = RenameJsonKey(recordstring)
        break

    #get vcf
    linenum = 0
    vcfline = str()
    with open(filepath_vcf) as file:
        while True:
            line = file.readline()
            if not line:
                break
            else:
                if line[1] != '#':
                    vcfline += line
                    linenum += 1
                    if linenum == 3:
                        break

    result = {"vcf": vcfline, "json": recordstring}
    return result


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


if __name__ == "__main__":
    type = sys.argv[1]
    filepath = sys.argv[2]
    if (type == '-t'):  #input a vcf file
        #filepath = sys.argv[2]
        dotranform(filepath, mode='MergeAll')
    elif (type == '-r'):    #input a txt with a list of vcf files
        #listfilepath = sys.argv[2]
        with open(filepath, 'r') as f:
            lines = f.readlines()
        for line in lines:
            line = line.strip('\n')
            dotranform(line, mode='MergeAll')
    elif (type == '-f'):    #input a folder, and then transform all vcf files into json
        files = os.listdir(filepath)
        for file in files:
            file_ap = os.path.join(filepath, file)
            if not os.path.isdir(file_ap):
                if os.path.splitext(file_ap)[1] == '.gz':
                    dotranform(file_ap, mode='MergeAll')


    # basename = os.path.basename(filepath)
    # file_json = os.path.splitext(basename)[0] + ".json"
    # # file_json = '/home/qz/WebstormProjects/vcf2json/clinvar_20190520.vcf.json'
    # connection = MongoClient('localhost', 27017)
    # collection = connection.vcf.clinvar
    # ImportJson2Mongodb(file_json, collection)

    # filepath = sys.argv[1]
    # with open(filepath, 'r') as f:
    #     lines = f.readlines()
    # for line in lines:
    #     line = line.strip('\n')
    #     dotranform(line, mode='MergeAll')
    #transform = Transform()
    #dotranform(filepath, mode='MergeAll')

