import json
from rdflib import *
import re
from pymongo import *
import os
import time
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


if __name__ == "__main__":
    con = MongoClient('localhost', 27017)
    collections = [con.goa.goa, con.hpo.hpoteam, con.owl.obo, con.vcf.clinvar]
    for collection in collections:
        CreatIndex(collection)
