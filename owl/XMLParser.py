#!/usr/bin/env python -S
# -*- coding: utf-8 -*-
import sys
import re
import xml.sax
import time
from importlib import reload
from pymongo import *
import json


class XMLtoJSON_ContentHandler(xml.sax.handler.ContentHandler):

    def __init__(self, output=sys.stdout, pretty_print=True, indent=2):
        self.output = output
        self.indent = indent
        self.indent_space = ' '*self.indent
        self.pretty_print = pretty_print
        self.last_called = "__init__"

    def startDocument(self):
        self.data = {}
        self.p_data = [self.data]
        self.continuations = []
        self.push_text_node = []

        self.last_called = "startDocument"

    def endDocument(self):
        # self.print_json()
        # json.dump(self.data, self.output)
        client = MongoClient('localhost', 27017)
        db = client.disease
        collection = db.hmdb
        collection.insert_many(self.data['hmdb']['metabolite'])

    def characters(self, content):
        #process the concurrent element's text
        this_push_text_node = self.push_text_node[-1]
        line = re.match('^\s*(.*)$', content)
        if line and len(line.groups()[0]) > 0:
            this_push_text_node(line.groups()[0], self.last_called == "characters")
            self.last_called = "characters"

    def startElement(self, name, attr):
        this_p_data = self.p_data[-1]
        this_data = {'d': {}}

        def this_push_text_node(node, continued_p):
            data = this_data['d']
            if not data.get('#text'):
                data['#text'] = node
            elif isinstance((data['#text']), list):
                if continued_p:
                    data['#text'][-1] = data['#text'][-1] + '\\n' + node
                else:
                    data['#text'].append(node)
            else:
                if continued_p:
                    data['#text'] = data['#text'] + '\\n' + node
                else:
                    data['#text'] = [data['#text'], node]

        self.push_text_node.append(this_push_text_node)
        #extract attributes
        for key in attr.getNames():
            value = attr.getValue(key)
            this_data['d'][key] = value

        def cont():
            keys = this_data['d'].keys()
            #the most inner element
            if keys == ['#text']:
                if not isinstance((this_data['d']['#text']), list):
                    this_data['d'] = this_data['d']['#text']

            if keys:
                #not exist
                if not this_p_data.get(name):
                    this_p_data[name] = this_data['d']
                #has existed more than ones
                elif isinstance((this_p_data[name]), list):
                    this_p_data[name].append(this_data['d'])
                #only one
                else:
                    this_p_data[name] = [this_p_data[name], this_data['d']]

        self.continuations.append(cont)
        self.p_data.append(this_data['d'])

        self.last_called = "startElement"

    def endElement(self, name):
        self.p_data.pop()
        self.push_text_node.pop()
        cont = self.continuations.pop()
        cont()
        self.last_called = "endElement"

    def print_json(self):
        first_time = [1]

        def it(h, nesting=0):
            if isinstance(h, dict):
                if self.pretty_print:
                    if not first_time[0]:
                        self.output.write("\n")
                    else:
                        first_time[0] = 0

                    for i in range(nesting):
                        self.output.write(self.indent_space)

                self.output.write("{")
                l, i = len(h), 0
                for k, v in h.items():
                    self.output.write('"' + k + '"' + ':')
                    it(v, nesting+1)
                    if i < (l-1):
                        self.output.write(",")
                        if self.pretty_print:
                            self.output.write("\n ")
                            for j in range(nesting):
                                self.output.write(self.indent_space)
                    i += 1
                self.output.write("}")

            if isinstance(h, list):
                self.output.write("[")
                l, i = len(h), 0
                for a in h:
                    it(a, nesting+1)
                    if i < (l-1):
                        self.output.write(',')
                    i += 1
                self.output.write("]")

            if isinstance(h, str):
                h = h.replace('"', '\\"')
                self.output.write('"' + h + '"')

        it(self.data)

        if self.pretty_print:
            self.output.write("\n")


class XMLtoJSON():
    def __init__(self, output=None, input=None, indent=False, output_file_append=False):
        self.indent = indent
        pretty_print = (self.indent is not False) and (self.indent > -1)
        self.handler = XMLtoJSON_ContentHandler(None, pretty_print, self.indent or 0)
        self.output_file_append = output_file_append
        self.output = output
        self.input = input

    def parse_file(self, path=None):
        # argument is given the priority.
        def parsing():
            with open((path or self.input), 'r', encoding='utf-8') as f:
                xml.sax.parse(f, self.handler)

        return self.parse_base(parsing)

    def parse_base(self, parsing):
        with open(self.output, 'w', encoding='utf-8') as f:
            self.handler.output = f
            parsing()

    def parse(self):
        return self.parse_file()


if __name__ == '__main__':
    p = XMLtoJSON(output="result.json", input="hmdb_metabolites.xml", indent=False)
    start = time.clock()
    p.parse()
    end = time.clock()
    print('running time: ', end-start)
