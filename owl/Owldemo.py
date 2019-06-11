# -*- coding:utf-8 -*-
from rdflib import *
import json
import datetime
import time
import re
import os
import sys

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


class OwlDemo:
    def __init__(self, file_path, file_name):
        with open(file_path, "r", encoding="utf-8") as f:
            self.g = Graph().parse(f, format="application/rdf+xml")
            self.namespaces = dict()
            self.output_path = '/home/qz/Desktop/XMLdata/result/'
            self.file_name = file_name
            for tup in self.g.namespaces():
                #trans to the string
                self.namespaces[tup[0]] = str(tup[1])
            # print(self.namespaces)

    def owl_convert(self):
        #for the rdf:ID
        rdf_ids = dict()
        resources = dict()
        resources["namespaces"] = self.namespaces
        subjects = set(self.g.subjects())
        i = 0
        for subject in subjects:
            has_axiom = False
            is_rdfid = False
            axiom_dict = dict()
            properties = dict()
            if isinstance(subject, BNode):
                continue
            i += 1
            subject_axiom = list(self.g.subjects(URIRef("http://www.w3.org/2002/07/owl#annotatedSource"), subject))
            if len(subject_axiom) > 0:
                axiom_dict = self.process_axiom(subject_axiom)
                has_axiom = True
            # subject_qname = self.g.qname(subject)
            for tup in self.g.predicate_objects(subject):
                tmp = dict()
                tup_property_qname = self.g.qname(tup[0])
                if isinstance(tup[1], Literal):
                    if has_axiom and str(tup[1]) in axiom_dict.keys():
                        tmp["owl:Axiom"] = axiom_dict.get(str(tup[1]))
                    if str(Literal(tup[1]).value) == 'nan':
                        tmp["value"] = ''
                    else:
                        tmp["value"] = Literal(tup[1]).value
#                    tmp["value"] = Literal(tup[1]).value
#                    tmp["type"] = "literal"
                    state = Literal(tup[1]).__getstate__()[1]
                    if state["language"] is not None:
                        tmp["lang"] = state["language"]
                    if state["datatype"] is not None:
                        tmp["datatype"] = state["datatype"].toPython()
                    if tup_property_qname in properties.keys():
                        re_val = properties.get(tup_property_qname)
                        if type(re_val).__name__ == 'dict':
                            properties[tup_property_qname] = [re_val, tmp]
                        elif type(re_val).__name__ == 'list':
                            re_val.append(tmp)
                            properties[tup_property_qname] = re_val
                    else:
                        properties[tup_property_qname] = tmp
                elif isinstance(tup[1], URIRef):
                    if has_axiom and str(tup[1]) in axiom_dict.keys():
                        tmp["owl:Axiom"] = axiom_dict.get(str(tup[1]))
                    if tup_property_qname == "rdf:subject":
                        is_rdfid = True
                    tmp["rdf:resource"] = URIRef(tup[1]).toPython()
#                    tmp["type"] = "url"
                    if tup_property_qname in properties.keys():
                        re_val = properties.get(tup_property_qname)
                        if type(re_val).__name__ == 'dict':
                            properties[tup_property_qname] = [re_val, tmp]
                        elif type(re_val).__name__ == 'list':
                            re_val.append(tmp)
                            properties[tup_property_qname] = re_val
                    else:
                        properties[tup_property_qname] = tmp
                elif isinstance(tup[1], BNode):
                    is_collection = False
                    for tup1 in self.g.predicate_objects(tup[1]):
                        if str(tup1[0]).endswith("#first") or str(tup1[0]).endswith("#rest"):
                            is_collection = True
                    if is_collection is False:
                        properties[tup_property_qname] = self.process_blank_node(tup[1])
                    else:
                        properties[tup_property_qname] = self.process_general_Collection(tup[1])
                    # properties[tup_property_qname] = self.process_blank_node(tup[1])
            if not is_rdfid:
                resources[str(subject)] = properties
            else:
                rdf_ids[str(subject)] = properties
        #deal with the rdf:ID
        for item in rdf_ids.items():
            id = item[0]
            id_qname = self.g.qname(URIRef(id))
            content = item[1]
            sub = content["rdf:subject"]["rdf:resource"]
            pre = content["rdf:predicate"]["rdf:resource"]
            if 'rdf:resource' in content["rdf:object"].keys():
                obj = content["rdf:object"]["rdf:resource"]
            else:
                obj = content["rdf:object"]["value"]
            pre_qname = self.g.qname(URIRef(pre))
            if sub in resources.keys():
                objs = resources[sub][pre_qname]
                if type(objs).__name__ == "dict":
                    objs["rdfID"] = id_qname
                if type(objs).__name__ == "list":
                    for j in range(len(objs)):
                        if 'rdf:resource' in objs[j].keys():
                            if objs[j]["rdf:resource"] == obj:
                                objs[j]["rdfID"] = id_qname
                        else:
                            if objs[j]["value"] == obj:
                                objs[j]["rdfID"] = id_qname
                resources[sub][pre_qname] = objs
            else:
                objs = rdf_ids[sub][pre_qname]
                if type(objs).__name__ == "dict":
                    objs["rdfID"] = id_qname
                if type(objs).__name__ == "list":
                    for j in range(len(objs)):
                        if 'rdf:resource' in objs[j].keys():
                            if objs[j]["rdf:resource"] == obj:
                                objs[j]["rdfID"] = id_qname
                        else:
                            if objs[j]["value"] == obj:
                                objs[j]["rdfID"] = id_qname
                rdf_ids[sub][pre_qname] = objs
            content.pop("rdf:subject")
            content.pop("rdf:predicate")
            content.pop("rdf:object")
            resources[id] = content
        fp = open(self.file_name+'.txt', 'w')
        json.dump(resources, fp, cls=DateEncoder)
        fp.close()
        print('finished!')
        return i
        # for subject in subjects:
        #     if isinstance(subject, BNode):
        #         print('judge if the collection or the equivalentClass')
                # for tup in self.g.predicate_objects(subject):
                #     if str(tup[1]).endswith("#ObjectProperty"):
                #         print('call the process')
                #     if str(tup[1]).endswith("#AnnotationProperty"):
                #         print('call the process')

    def get_namespaces(self):
        namespace_map = dict()
        for name_tuple in self.g.namespaces():
            namespace_map[name_tuple[0]] = name_tuple[1].toPython()

        return namespace_map

    def process_object_property(self, object_property_url):
        qname = self.g.qname(object_property_url)
        obj_dic = {}
        for tup in self.g.predicate_objects(object_property_url):
            tmp = dict()
            property_name = self.g.qname(tup[0])
            #not record the namespace of the property
            print('process the object property')
            if isinstance(tup[1], Literal):
                if str(Literal(tup[1]).value) == 'nan':
                    tmp["value"] = ''
                else:
                    tmp["value"] = Literal(tup[1]).value
#                tmp["value"] = Literal(tup[1]).value
#                tmp["type"] = "literal"
                state = Literal(tup[1]).__getstate__()[1]
                if state["language"] is not None:
                    tmp["lang"] = state["language"]
                if state["datatype"] is not None:
                    tmp["datatype"] = state["datatype"].toPython()
                obj_dic[property_name] = tmp
            elif isinstance(tup[1], URIRef):
                tmp["rdf:resource"] = URIRef(tup[1]).toPython()
#                tmp["type"] = "url"
                obj_dic[property_name] = tmp
            elif isinstance(tup[1], BNode):
                tmp = self.process_blank_node(tup[1])
                obj_dic[property_name] = tmp
        return qname, obj_dic

    def process_axiom(self, axiom_list):
        axiom_dict = dict()
        for subject in axiom_list:
            axiom_properties = dict()
            axiom_target = ""
            for tup in self.g.predicate_objects(subject):
                if str(tup[0]).endswith("#annotatedSource"):
                    continue
                elif str(tup[0]).endswith("#annotatedProperty"):
                    continue
                elif str(tup[0]).endswith("#type"):
                    continue
                elif str(tup[0]).endswith("#annotatedTarget"):
                    axiom_target = str(tup[1])
                else:
                    tmp = dict()
                    tup_property_qname = self.g.qname(tup[0])
                    # tup_property_qname = '{}:{}'.format(self.g.compute_qname(tup[0])[0], self.g.compute_qname(tup[0])[2])
                    if isinstance(tup[1], Literal):
                        if str(Literal(tup[1]).value) == 'nan':
                            tmp["value"] = ''
                        else:
                            tmp["value"] = Literal(tup[1]).value
#                        tmp["type"] = "literal"
                        state = Literal(tup[1]).__getstate__()[1]
                        if state["language"] is not None:
                            tmp["lang"] = state["language"]
                        if state["datatype"] is not None:
                            tmp["datatype"] = state["datatype"].toPython()
                        if tup_property_qname in axiom_properties.keys():
                            re_val = axiom_properties.get(tup_property_qname)
                            if type(re_val).__name__ == 'dict':
                                axiom_properties[tup_property_qname] = [re_val, tmp]
                            elif type(re_val).__name__ == 'list':
                                re_val.append(tmp)
                                axiom_properties[tup_property_qname] = re_val
                        else:
                            axiom_properties[tup_property_qname] = tmp
                    elif isinstance(tup[1], URIRef):
                        tmp["rdf:resource"] = URIRef(tup[1]).toPython()
#                        tmp["type"] = "url"
                        if tup_property_qname in axiom_properties.keys():
                            re_val = axiom_properties.get(tup_property_qname)
                            if type(re_val).__name__ == 'dict':
                                axiom_properties[tup_property_qname] = [re_val, tmp]
                            elif type(re_val).__name__ == 'list':
                                re_val.append(tmp)
                                axiom_properties[tup_property_qname] = re_val
                        else:
                            axiom_properties[tup_property_qname] = tmp
                    elif isinstance(tup[1], BNode):
                        is_collection = False
                        for tup1 in self.g.predicate_objects(tup[1]):
                            if tup1[0] == RDF.first or tup1[0] == RDF.rest:
                                is_collection = True
                        if is_collection:
                            axiom_properties[tup_property_qname] = self.process_general_Collection(tup[1])
                        else:
                            axiom_properties[tup_property_qname] = self.process_blank_node(tup[1])
            axiom_dict[axiom_target] = axiom_properties
        return axiom_dict

    def process_blank_node(self, node_id):
        properties = dict()
        container_type = ""
        for tup in self.g.predicate_objects(node_id):
            tmp = dict()
            tup_property_qname = self.g.qname(tup[0])
            if isinstance(tup[1], Literal):
                if str(Literal(tup[1]).value) == 'nan':
                    tmp["value"] = ''
                else:
                    tmp["value"] = Literal(tup[1]).value
#                tmp["type"] = "literal"
                state = Literal(tup[1]).__getstate__()[1]
                if state["language"] is not None:
                    tmp["lang"] = state["language"]
                if state["datatype"] is not None:
                    tmp["datatype"] = state["datatype"].toPython()
                properties[tup_property_qname] = tmp
            elif isinstance(tup[1], URIRef):
                if tup[1] == RDF.Seq:
                    container_type = "Seq"
                elif tup[1] == RDF.Alt:
                    container_type = "Alt"
                elif tup[1] == RDF.List:
                    container_type = "List"
                tmp["rdf:resource"] = URIRef(tup[1]).toPython()
#                tmp["type"] = "url"
                properties[tup_property_qname] = tmp
            elif isinstance(tup[1], BNode):
                is_collection = False
                for tup1 in self.g.predicate_objects(tup[1]):
                    if tup1[0] == RDF.first or tup1[0] == RDF.rest:
                        is_collection = True
                if is_collection:
                    properties[tup_property_qname] = self.process_general_Collection(tup[1])
                else:
                    properties[tup_property_qname] = self.process_blank_node(tup[1])
        if container_type is not "":
            container_properties = dict()
            container_properties["container"] = container_type
            properties.pop(str(RDF.type))
            container_properties["value"] = properties
            return container_properties
        return properties

    def process_general_Collection(self, node_id):
        properties = dict()
        properties["parseType"] = "collection"
        collection = []
        collection = self.general_each_item(collection, node_id)
        properties["value"] = collection

        return properties

    def general_each_item(self, collection, node_id):
        # print(collection)
        # print("################################")
        for tup in self.g.predicate_objects(node_id):
            tmp = dict()
            if isinstance(tup[1], Literal):
                if str(Literal(tup[1]).value) == 'nan':
                    tmp["value"] = ''
                else:
                    tmp["value"] = Literal(tup[1]).value
#                tmp["type"] = "url"
                state = Literal(tup[1]).__getstate__()[1]
                if state["language"] is not None:
                    tmp["lang"] = state["language"]
                if state["datatype"] is not None:
                    tmp["datatype"] = state["datatype"].toPython()
                collection.append(tmp)
            elif isinstance(tup[1], URIRef):
                if str(URIRef(tup[1]).toPython()).endswith("#nil"):
                    continue
                tmp["rdf:resource"] = URIRef(tup[1]).toPython()
#                tmp["type"] = "url"
                collection.append(tmp)
            elif isinstance(tup[1], BNode):
                is_collection = False
                for tup1 in self.g.predicate_objects(tup[1]):
                    if str(tup1[0]).endswith("#first") or str(tup1[0]).endswith("#rest"):
                        is_collection = True
                if is_collection is False:
                    collection.append(self.process_blank_node(tup[1]))
                else:
                    collection = self.general_each_item(collection, tup[1])
        return collection

    def RenameJsonKey(strJson):
        if isinstance(strJson, dict):
            strJson = json.dumps(strJson)
        # 先默认json的key中没有特殊符号
        pattern = re.compile(r"\"([\w.$:]+)\":")
        strJson = pattern.sub(lambda m: m.group(0).replace('.', "u002E").replace('$', "u0024"), strJson)
        return strJson


# obj = OwlDemo("hp.owl")
# start = time.clock()
# obj.owl_convert()
# end = time.clock()
# print('running time: ', end-start)
if __name__ == '__main__':
    input_path = sys.argv[1]
	#input_path = '/home/qz/Desktop/XMLdata/obo/'
    for file in os.listdir(input_path):
        # print(file)
        file_path = os.path.join(input_path, file)
        obj = OwlDemo(file_path, file[:-4])
        start = time.clock()
        subject_sum = obj.owl_convert()
        end = time.clock()
        total_time = (end-start)
        # print("subject num: ", subject_sum)
        # print("time: ", total_time)
        # f.write(file+'\t')
        # f.write(str(subject_sum)+'\t')
        # f.write(str(total_time)+'\n')
        log = file + '\t' + str(subject_sum) + '\t' + str(total_time) + '\n'
        print(log)
        with open('time.txt', 'a') as f:
            f.write(log)

