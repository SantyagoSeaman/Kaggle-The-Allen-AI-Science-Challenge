import datetime
import json
import requests
import bz2
import re
import string
import pandas as pd
import nltk
from nltk.corpus import stopwords
from elasticsearch import Elasticsearch, helpers

#nltk.download()
cachedStopWordsFullList = stopwords.words("english")
cachedStopWordsFullList = cachedStopWordsFullList + (['.', ',', '?', '_', '->'])
cachedStopWordsFullList = [word for word in cachedStopWordsFullList if word not in ['have', 'has', 'should', 'would', 'only', 'no', 'yes']]

cachedStopWordsShortList = ['is', 'are', 'a', 'an', 'the', 'in', 'on', 'of', '.', ',', '?', '_', '->']

def unique_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]

def remove_stop_words(text, shortList):
    if shortList:
        stop_words = cachedStopWordsShortList
    else:
        stop_words = cachedStopWordsFullList
    return ' '.join([word for word in text.split() if word not in stop_words]).strip()

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'timeout': 100}])
#print(es.ping())

training_set = pd.read_csv('data/training_set.tsv', sep='\t')
#print(training_set[1:10])

validation_set = pd.read_csv('data/validation_set.tsv', sep='\t')
sample_submission = pd.read_csv('data/sample_submission.csv')

start_index = int(input('Start index: '))

try:
    for index, row in validation_set[start_index:(validation_set.question.count()-1)].iterrows():
    # for index, row in validation_set.iterrows():

        print('=============================================================================')
        print(row)

        question = row['question'].lower()
        # question = re.sub(r"wh(ich|at)", "", question)
        # question = re.sub(r"[ ](is|a|an|and|the|in|on)[ ]", " ", question)
        # question = re.sub(r"[_]", "", question)
        # question = re.sub(r"[->]", "", question)
        # question = re.sub(r"[?]", "", question)
        question = remove_stop_words(question, False)
        question = question.strip()

        for minimum_should_match in range(95, 50, -5):
            # res = es.search(
            #         index="ai_wiki_full",
            #         body={"query":
            #                   {"match":
            #                        {'text': {
            #                                 'query': row['question'],
            #                                 'operator': 'and'
            #                            }
            #                        }
            #                   }
            #             }
            # )
            res = es.search(
                index="ai_wiki_full",
                body={"query":
                          {"match":
                               {'text': {
                                        'query': question,
                                        #'operator': 'and',
                                        'cutoff_frequency' : 0.001,
                                        'fuzziness': 1,
                                        "minimum_should_match": str(minimum_should_match) + '%'
                                   }
                               }
                          }
                    },
                size=20
            )
            # res = es.search(
            #         index="ai_wiki_full",
            #         body={"query": {
            #               "common": {
            #                 "body": {
            #                   "query": row['question'],
            #                   "cutoff_frequency": 0.0001,
            #                 }
            #               }
            #         }}
            # )
            if res['hits']['total'] > 2:
                break

        print("Got %d Hits with %d%%" % (res['hits']['total'], minimum_should_match))
        #break

        categories = []
        ids = []
        for hit in res['hits']['hits']:
            source = hit["_source"]
            categories = categories + source['categories']
            ids = ids + [source['id']]
        categories = unique_list(categories)
        ids = unique_list(ids)

        hits = []
        scores = []
        for answer in row[['answerA', 'answerB', 'answerC', 'answerD']]:
            answer = answer.lower()
            # answer = ' ' + answer.lower() + ' '
            # answer = re.sub(r"[ ](is|a|an|and|the|in|on)[ ]", " ", answer)
            # answer = re.sub(r"[.]", "", answer)
            answer = remove_stop_words(answer, True)
            answer = answer.strip()

            for fuzziness in (0, 1, 2, 3):
                # res = es.search(
                #     index="ai_wiki_full",
                #     body={"query":
                #               {"match":
                #                    {'text': {
                #                             'query': question + ' ' + answer,
                #                             #'operator': 'and',
                #                             'cutoff_frequency' : 0.001,
                #                             'fuzziness': 2,
                #                             "minimum_should_match": str(minimum_should_match) + '%'
                #                        },
                #                    }
                #               # },
                #               # "filter" : {
                #               #       "terms" : {
                #               #           "id" : ids,
                #               #           "execution" : "or"
                #               #       },
                #               #       # "bool": {
                #               #       #     "should": [
                #               #       #       { "term": { "categories": categories[0] }},
                #               #       #       { "term": { "categories": categories[1] }}
                #               #       #     ]
                #               #       #   }
                #               }
                #         }
                # )
                # res = es.search(
                #     index="ai_wiki_full",
                #     body={"query":
                #               {"bool":{
                #                       'must': {
                #                           'match': {'text': {
                #                               'query': answer,
                #                               'operator': 'and',
                #                               'cutoff_frequency' : 0.001,
                #                               'fuzziness': fuzziness
                #                            }}
                #                       },
                #                       'should': {
                #                           'match': {'text': {
                #                             'query': question + ' ' + answer,
                #                             'operator': 'and',
                #                             'cutoff_frequency' : 0.001,
                #                             'fuzziness': 1,
                #                             "minimum_should_match": str(minimum_should_match) + '%'
                #                            }}
                #                       },
                #                       # "filter" : {
                #                       #       "terms" : {
                #                       #           "id" : ids,
                #                       #           "execution" : "or"
                #                       #       }
                #                       # }
                #                   },
                #               }
                #         }
                # )
                res = es.search(
                    index="ai_wiki_full",
                    body={"query": {
                              "filtered": {
                                 "query": {
                                    "match_all": {}
                                 },"filter":
                              {"bool":{
                                      'must': {
                                          'match': {'text': {
                                              'query': answer,
                                              'operator': 'and',
                                              'cutoff_frequency' : 0.001,
                                              'fuzziness': fuzziness
                                           }}
                                      },
                                      'should': {
                                          'match': {'text': {
                                            'query': question + ' ' + answer,
                                            'cutoff_frequency' : 0.001,
                                            'fuzziness': fuzziness,
                                            "minimum_should_match": str(minimum_should_match) + '%'
                                           }}
                                      },
                                  },
                              }
                        }
                    }}
                )
                if res['hits']['total'] > 0:
                    break

            if res['hits']['total'] > 0:
                ids = []
                for hit in res['hits']['hits']:
                    source = hit["_source"]
                    ids = ids + [source['id']]
                ids = unique_list(ids)

                res = es.search(
                    index="ai_wiki_full",
                    body={"query":
                          {"match":
                               {'text': {
                                        'query': question + ' ' + answer,
                                        'cutoff_frequency' : 0.001,
                                        'fuzziness': fuzziness,
                                        "minimum_should_match": str(minimum_should_match) + '%'
                                   },
                               }
                          },
                          "filter" : {
                                "terms" : {
                                    "id" : ids,
                                    "execution" : "or"
                                }
                          }
                    }
                )

            hits = hits + [res['hits']['total']]
            if res['hits']['total'] > 0:
                hit = res['hits']['hits'][0]
                scores = scores + [hit["_score"]]
            else:
                scores = scores + [0]

        result_array = [a*b for a,b in zip(hits, scores)]
        result_max_index = result_array.index(max(result_array))
        result_answer = ('A', 'B', 'C', 'D')[result_max_index]

        score_max_index = scores.index(max(scores))
        score_answer = ('A', 'B', 'C', 'D')[score_max_index]

        print(hits)
        print(scores)
        print('by score*hits result: ' + result_answer)
        print('by score result: ' + score_answer)
        print('sample result: ' + sample_submission.loc[index, 'correctAnswer'])
        print()
        print()

        sample_submission.loc[index, 'correctAnswer'] = score_answer
        zzz = 0
        if index % 100 == 0:
            sample_submission.to_csv(
                'data/result_set_' + str(index)
                + '_' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                + '.csv',
                index=False
            )

except:
    raise
finally:
    sample_submission.to_csv('data/result_set_' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + '.csv', index=False)
