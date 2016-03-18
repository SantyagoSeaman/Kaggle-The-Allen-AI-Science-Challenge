#sudo ssh -L localhost:9200:10.7.0.4:9200 santyago@kaggleteamelastic1.westeurope.cloudapp.azure.com
#sudo ssh -L localhost:5601:10.7.0.4:5601 santyago@kaggleteamelastic1.westeurope.cloudapp.azure.com

import json
import requests
import bz2
import re
import string
import xml.etree.cElementTree as ET
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'timeout': 100}])
print(es.ping())

reCommonList = [
    re.compile('\{\{.*?\}\}'),
    re.compile('<!--.*?-->'),
    re.compile('=='),
    re.compile('<ref>.*?</ref>'),
    re.compile('\[http://.*?\]'),
    re.compile('\[\[File.*?\]\]'),
    re.compile('\[\[Image.*?\]\]'),
]
reCategoryList = [
    re.compile('\[\[Category:.*?\]\]'),
    re.compile('\|.*?\]\]'),
    re.compile('\[\['),
    re.compile('\]\]'),
]
reGetCategories = re.compile('\[\[Category:(.*?)\]\]')

counter = 0
currentPage = {}
currentNode = ''
redirect = False
packageOfPages = []
skipDocuments = 1000000
lastDocument = 100000000
with bz2.BZ2File("d:/Kaggle/enwiki-latest-pages-articles.xml.bz2") as xmlFlow:
    for event, elem in ET.iterparse(xmlFlow, events=("start", "end")):
        if '}' in elem.tag:
            elem.tag = elem.tag.split('}', 1)[1]

        if event == 'start':
            # print("Start: " + elem.tag)
            # if elem.text:
            #     print("Value: " + elem.text)

            if elem.tag == 'page':
                currentPage = {}
                redirect = False
                currentNode = elem.tag
            elif elem.tag == 'revision' or elem.tag == 'contributor':
                currentNode = elem.tag
            elif elem.tag == 'redirect':
                redirect = True
            elif elem.tag == 'title':
                currentPage['title'] = elem.text
            elif elem.tag == 'id':
                if currentNode == 'page':
                    if elem.text:
                        currentPage['id'] = int(elem.text)
                    else:
                        redirect = True
                elif currentNode == 'revision':
                    if elem.text:
                        currentPage['revisionid'] = int(elem.text)
            elif elem.tag == 'text':
                if not redirect:
                    if elem.text:
                        txt = elem.text.replace('\n', ' ').replace('\r', ' ')

                        currentPage['categories'] = [i.strip('| ') for i in re.findall(reGetCategories, txt)]
                        # if currentPage['categories'].__len__() == 0:
                        #     aaa = 10

                        for r in reCommonList:
                            txt = re.sub(r, '', txt)
                        for r in reCategoryList:
                            txt = re.sub(r, '', txt)

                        txt = re.sub('\'{2,}', '\'', txt)
                        txt = re.sub('[*] ', ' ', txt)
                        txt = re.sub('[ ]{2,}', ' ', txt)

                        currentPage['text'] = txt.strip()
        elif event == 'end':
            # print("End: " + elem.tag)
            if elem.tag == 'page':
                if not redirect and ('text' in currentPage):
                    counter += 1
                    if counter % 100 == 0: print(counter)

                    # print("Total:" + currentPage.__str__())
                    # jsn = json.loads(r.content.decode("utf-8"))
                    # es.index(index='ai_wiki',
                    #          doc_type='articles',
                    #          id=currentPage['id'],
                    #          body=json.dumps(currentPage),
                    #          request_timeout=60)

                    if counter > skipDocuments:
                        packageOfPages.append({
                            "index": {
                                "_index": "ai_wiki_full",
                                "_type": "articles",
                                "_id": currentPage['id']
                            }
                        })
                        packageOfPages.append(currentPage)

                        if counter % 500 == 0:
                            res = es.bulk(index="ai_wiki_full", body=packageOfPages, fields='id', refresh=False)
                            packageOfPages = []

                            if counter >= lastDocument:
                                break

        elem.clear()
        # while elem.getprevious() is not None:
        #     del elem.getparent()[0]


# 0:40

