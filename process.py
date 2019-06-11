import json
from rdflib import *
import re
from pymongo import *
import os
import time
import sys

def findrealation(input):
    con = MongoClient('localhost', 27017)
    detail = input.split(':')
    type = detail[0]
    value = detail[1]
    if type.upper() == 'DOID':
        results_doid = con.owl.obo.find({"url": "http://purl.obolibrary.org/obo/" + type + '_' + value},{"_id": 0})
        OMIMs = []
        if results_doid.count():
            for result_doid in results_doid:
                DbXref = result_doid['oboInOwlu003AhasDbXref']
                if isinstance(DbXref, list):
                    for xref in DbXref:
                        if 'OMIM' in xref['value']:
                            OMIMs.append(xref['value'])
                else:
                    if 'OMIM' in DbXref['value']:
                        OMIMs.append(xref['value'])
        #print(OMIMs)
        for omim in OMIMs:
            HPO_IDs = []
            print(omim)
            omimid = omim.split(':')[1]
            results_omim = con.hpo.hpoteam.find({"DB_Object_ID": omimid})
            for result_omim in results_omim:
                HPO_IDs.append(result_omim['HPO_ID'])
            print(HPO_IDs)

            # for omim in OMIMs:
            #     omimid = omim.split(':')[1]
            #     regx = re.compile('.×' + 'OMIM:608565' + '.×', re.IGNORECASE)
            #     #results_clinvar = con.vcf.clinvar.find({"INFO.CLNDISDB" : {'$regex': ''/OMIM:608565/}})
            #     results_clinvar = con.vcf.clinvar.find({"INFO.CLNDISDB":regx})
            #
            #     for result_clinvar in results_clinvar:
            #         geneinfo = result_clinvar['INFO']['GENEINFO']
    elif type.upper() == 'HPO':
        pass
    elif type.upper() == 'GO':
        pass
    elif type.upper() == 'SO':
        pass


def OutputAllInfo(chrom, ID):
    try:
        con = MongoClient('localhost', 27017)
        TotalInfo = []
        result_clinvar = con.vcf.clinvar.find_one({"CHROM": chrom, "ID": ID})
        if result_clinvar:
            if "OMIM" in result_clinvar['INFO']["CLNDISDB"]:
                for str in result_clinvar['INFO']["CLNDISDB"].split(','):
                    if "OMIM" in str:
                        OMIMstr = str
                        break
                #OMIM
                tmpline = {}
                OMIM_id = OMIMstr.split(':')[1]
                tmpline["OMIM"] = OMIM_id

                #DOID
                result_OMIM = con.owl.obo.find_one({"oboInOwlu003AhasDbXref.value": OMIMstr})
                if result_OMIM:
                    DOID = result_OMIM["oboInOwlu003Aid"]["value"]
                    DOID_id = DOID.split(':')[1]
                    tmpline["DOID"] = DOID_id
                else:
                    tmpline["DOID"] = ''

                #HPO
                tmpline_HPO = []
                results_HPO = con.hpo.hpoteam.find({"DB":"OMIM", "DB_Object_ID":OMIM_id})
                for result_HPO in results_HPO:
                    tmpline2 = []
                    tmpline2 = tmpline.copy()
                    tmpline2["HPO"] = result_HPO["HPO_ID"].split(":")[1]
                    tmpline_HPO.append(tmpline2)

                #SO
                for ele in tmpline_HPO:
                    ele["CLNVCSO"] = result_clinvar['INFO']['CLNVCSO'].split(':')[1]

                #MC
                for ele in tmpline_HPO:
                    try:
                        ele["MC"] = result_clinvar['INFO']['MC'].split('|')[0].split(':')[1]
                    except Exception as e:
                        pass

                #GENEINFO
                for ele in tmpline_HPO:
                    try:
                        ele["GENEINFO"] = result_clinvar['INFO']['GENEINFO']
                    except Exception as e:
                        pass

                #GO
                genesymbal = result_clinvar['INFO']['GENEINFO'].split(':')[0]
                results_GO = con.goa.goa.find({"DB_Object_Symbol": genesymbal})
                for result_GO in results_GO:
                    for ele in tmpline_HPO:
                        tmpline_GO = ele.copy()
                        tmpline_GO["GO_ID"] = result_GO["GO_ID"]
                        TotalInfo.append(tmpline_GO)

        if TotalInfo:
            print(TotalInfo)
    except Exception as e:
        pass


def OutputAllInfo2(chrom, ID, input, outtype):
    try:
        if outtype == "VCF":
            return
        detail = input.split(':')
        type = detail[0]
        value = detail[1]
        con = MongoClient('localhost', 27017)
        TotalInfo = []
        result_clinvar = con.vcf.clinvar.find_one({"CHROM": chrom, "ID": ID})
        if result_clinvar:
            if type != "OMIM":
                if "OMIM" in result_clinvar['INFO']["CLNDISDB"]:
                    for str in result_clinvar['INFO']["CLNDISDB"].split(','):
                        if "OMIM" in str:
                            OMIMstr = str
                            break
                    #OMIM
                    TotalInfo.append(OMIMstr)

            # DOID
            if type != "DOID" and outtype == "DO":
                if type == "OMIM":
                    result_OMIM = con.owl.obo.find_one({"oboInOwlu003AhasDbXref.value": input})
                else:
                    result_OMIM = con.owl.obo.find_one({"oboInOwlu003AhasDbXref.value": OMIMstr})
                if result_OMIM:
                    DOID = result_OMIM["oboInOwlu003Aid"]["value"]
                    TotalInfo.append(DOID)
                    result_DOID = con.owl.obo.find_one({"url": "http://purl.obolibrary.org/obo/" + DOID.split(":")[0] + '_' + DOID.split(":")[1]})
                    if result_DOID:
                        children_DOID = con.owl.obo.find({"rdfsu003AsubClassOf.rdfu003Aresource": "http://purl.obolibrary.org/obo/" + DOID.split(":")[0] + '_' + DOID.split(":")[1]})
                        #e.g. DOID_0050565
                        parent_DOID = result_DOID["rdfsu003AsubClassOf"]["rdfu003Aresource"].split('/')[-1]

            # HPO
            if type != "HP" and outtype == "HP":
                if type == "OMIM":
                    results_HPO = con.hpo.hpoteam.find({"DB":"OMIM", "DB_Object_ID":value})
                else:
                    results_HPO = con.hpo.hpoteam.find({"DB": "OMIM", "DB_Object_ID": OMIMstr.split(':')[1]})

                for result_HPO in results_HPO:
                    TotalInfo.append(result_HPO["HPO_ID"])
                    result_HP = con.owl.obo.find_one({"url": "http://purl.obolibrary.org/obo/" + result_HPO["HPO_ID"].split(':')[0] + '_' + result_HPO["HPO_ID"].split(':')[1]})
                    if result_HP:
                        children_HP = con.owl.obo.find({"rdfsu003AsubClassOf.rdfu003Aresource": "http://purl.obolibrary.org/obo/" + result_HPO["HPO_ID"].split(':')[0] + '_' + result_HPO["HPO_ID"].split(':')[1]})
                        parent_HP = result_DOID["rdfsu003AsubClassOf"]["rdfu003Aresource"].split('/')[-1]

            #SO
            if outtype == "SO":
                TotalInfo.append(result_clinvar['INFO']['CLNVCSO'])
                SOstr = result_clinvar['INFO']['CLNVCSO'].split(':')[0] + '_' + result_clinvar['INFO']['CLNVCSO'].split(':')[1]
                result_SO = con.owl.obo.find_one({"url": "http://purl.obolibrary.org/obo/" + SOstr})

                #MC
                TotalInfo.append(result_clinvar['INFO']['MC'])
                MCstr = result_clinvar['INFO']['MC'].replace(':', '_')
                result_MC = con.owl.obo.find_one({"url": "http://purl.obolibrary.org/obo/" + MCstr})

            #GENEINFO
            TotalInfo.append(result_clinvar['INFO']['GENEINFO'])

            # GO
            if type != "GO" and outtype == "GO":
                genesymbal = result_clinvar['INFO']['GENEINFO'].split(':')[0]
                results_GO = con.goa.goa.find({"DB_Object_Symbol": genesymbal})
                for result_GO in results_GO:
                    TotalInfo.append(result_GO["GO_ID"])
                    GOstr = result_GO["GO_ID"].replace(':', '_')
                    result_GO = con.owl.obo.find_one({"url": "http://purl.obolibrary.org/obo/" + GOstr})


        if TotalInfo:
            print(TotalInfo)
    except Exception as e:
        pass



def clinvarfind(input, outtype):
    con = MongoClient('localhost', 27017)
    detail = input.split(':')
    type = detail[0]
    value = detail[1]
    if type.upper() == 'DOID':
        results_doid = con.owl.obo.find({"url": "http://purl.obolibrary.org/obo/" + type + '_' + value},{"_id": 0}, no_cursor_timeout = True, batch_size=5)
        OMIMs = []
        if results_doid.count():
            for result_doid in results_doid:
                DbXref = result_doid['oboInOwlu003AhasDbXref']
                if isinstance(DbXref, list):
                    for xref in DbXref:
                        if 'OMIM' in xref['value']:
                            OMIMs.append(xref['value'])
                else:
                    if 'OMIM' in DbXref['value']:
                        OMIMs.append(xref['value'])
            results_doid.close()

        #OMIM  e.g. OMIM = OMIM:608565
        if OMIMs:
            for OMIM in OMIMs:
                regx = re.compile(".*" + OMIM + ".*", re.IGNORECASE)
                results_clinvar = con.vcf.clinvar.find({"INFO.CLNDISDB": regx}, no_cursor_timeout = True, batch_size=5)
                if results_clinvar.count():
                    for result_clinvar in results_clinvar:
                        OutputAllInfo2(result_clinvar['CHROM'],result_clinvar['ID'], input, outtype)
                    results_clinvar.close()

    elif type.upper() == 'HP':
        results_OMIM = con.hpo.hpoteam.find({"HPO_ID" : input, "DB" : "OMIM"}, no_cursor_timeout = True, batch_size=5)
        if results_OMIM.count():
            for result_OMIM in results_OMIM:
                regx = re.compile(".*" + result_OMIM['DB_Reference'] + ".*", re.IGNORECASE)
                results_clinvar = con.vcf.clinvar.find({"INFO.CLNDISDB": regx}, no_cursor_timeout = True, batch_size=5)
                if results_clinvar.count():
                    for result_clinvar in results_clinvar:
                        OutputAllInfo2(result_clinvar['CHROM'],result_clinvar['ID'], input, outtype)
                    results_clinvar.close()
            results_OMIM.close()

    elif type.upper() == 'GO':
        results_GO = con.goa.goa.find({"GO_ID" : input}, no_cursor_timeout = True, batch_size=5)
        if results_GO.count():
            for result_GO in results_GO:
                regx = re.compile(".*" + result_GO['DB_Object_Symbol'] + ".*", re.IGNORECASE)
                results_clinvar = con.vcf.clinvar.find({"INFO.GENEINFO":regx}, no_cursor_timeout = True, batch_size=5)
                if results_clinvar.count():
                    for result_clinvar in results_clinvar:
                        OutputAllInfo2(result_clinvar['CHROM'], result_clinvar['ID'], input, outtype)
                    results_clinvar.close()
            results_GO.close()

    elif type.upper() == 'SO':
        regx = re.compile(".*" + input + ".*", re.IGNORECASE)
        results_clinvar1 = con.vcf.clinvar.find({"INFO.CLNVCSO": regx}, no_cursor_timeout = True, batch_size=5)
        if results_clinvar1.count():
            for result_clinvar in results_clinvar1:
                OutputAllInfo2(result_clinvar['CHROM'], result_clinvar['ID'], input, outtype)
            results_clinvar1.close()

        results_clinvar2 = con.vcf.clinvar.find({"INFO.MC": regx}, no_cursor_timeout = True, batch_size=5)
        if results_clinvar2.count():
            for result_clinvar in results_clinvar2:
                OutputAllInfo2(result_clinvar['CHROM'], result_clinvar['ID'], input, outtype)
            results_clinvar2.close()

    elif type.upper() == 'OMIM':
        regx = re.compile(".*" + input + ".*", re.IGNORECASE)
        results_clinvar = con.vcf.clinvar.find({"INFO.CLNDISDB": regx}, no_cursor_timeout = True, batch_size=5)
        if results_clinvar.count():
            for result_clinvar in results_clinvar:
                OutputAllInfo2(result_clinvar['CHROM'], result_clinvar['ID'], input, outtype)
            results_clinvar.close()


# input
# DOID:0110493
# HP:0001761
# SO:0001483
# GO:0000978
# OMIM:608565
# outputtype = [VCF, SO, GO, HP, DO]
if __name__ == "__main__":
    start = time.time()
    input = sys.argv[1]
    outtype = sys.argv[2]
    clinvarfind(input, outtype)
    end = time.time()
    total_time = (end - start)
    print(total_time)