import re
from elasticsearch import Elasticsearch
from bs4 import BeautifulSoup

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'timeout': 100}])
print(es.ping())

for article in range(1, 124):
    print(article)

    f = open('../data/OEBPS/' + str(article) + '.html', 'r')
    html = f.read()
    # parsed_html = BeautifulSoup(html, "html.parser")
    # #print(parsed_html.body.find('div', attrs={'class':'container'}).text)
    #
    # print(parsed_html.title.text)

    html = html.replace("\n", " ")
    html = html.replace("\r", " ")
    html = html.replace("\t", " ")
    html = re.sub(r"[ ]{2,}", " ", html)


    title = re.search('<title>(.*?)</title>', html).group(1).strip()
    print(title)

    #zzz = re.findall('(<h1.*?</h1>.*?<h1|</html>)', html)
    chapters = re.split('<h1', html)
    index = 0
    packageOfPages = []
    for chapter in chapters[2:chapters.__len__()-1]:
        parsed_chapter = BeautifulSoup('<h1 ' + chapter, "html.parser")
        category = parsed_chapter.find('h1').text.strip()
        #subcategories = [x for x in parsed_chapter.find_all('h2')]
        print(category)

        text = re.sub(r"[ ]{2,}", " ", parsed_chapter.text).strip()
        text = re.sub('Figure below', '', text)
        text = re.sub('Figure [0-9]+[.][0-9]+', '', text)
        id = 100000000 * article + index

        index += 1
        page = {
            'id': id,
            'revision': -100,
            'categories': [title, category],
            'title': title + ' : ' + category,
            'text': parsed_chapter.text.strip()
        }

        packageOfPages.append({
            "index": {
                "_index": "ai_c12",
                "_type": "articles",
                "_id": id
            }
        })
        packageOfPages.append(page)

    res = es.bulk(index="ai_c12", body=packageOfPages, fields='id', refresh=False)

