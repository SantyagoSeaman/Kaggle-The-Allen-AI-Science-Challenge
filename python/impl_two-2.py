import datetime
import json
import itertools
import math
import requests
import bz2
import re
import string
import pandas as pd
import nltk
import numpy
from nltk.corpus import stopwords
from elasticsearch import Elasticsearch, helpers

# ----------------------------------------------------------------------------------------------
# nltk.download()
cachedStopWordsFullList = stopwords.words("english")
# cachedStopWordsFullList = cachedStopWordsFullList + ['->', '-']
cachedStopWordsFullList = cachedStopWordsFullList + ['->']
cachedStopWordsFullList = [word for word in cachedStopWordsFullList if
                           word not in ['have', 'has', 'should', 'would', 'only', 'no', 'yes']]

# cachedStopWordsShortList = ['is', 'are', 'a', 'an', 'the', 'in', 'on', 'of', '->', '-']
cachedStopWordsShortList = ['is', 'are', 'a', 'an', 'the', 'in', 'on', 'of', '->']

punc_reg = re.compile("[" + "".join(['.', ',', '?', '_']) + "]")


def unique_list(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def remove_stop_words(text, shortList):
    text = re.sub(punc_reg, "", text)
    text = re.sub("  ", " ", text)
    if shortList:
        stop_words = cachedStopWordsShortList
    else:
        stop_words = cachedStopWordsFullList
    return ' '.join([word for word in text.split() if word not in stop_words]).strip()


# ----------------------------------------------------------------------------------------------
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'timeout': 30}])
# print(es.ping())

training_set = pd.read_csv('data/training_set.tsv', sep='\t')
# print(training_set[1:10])

validation_set = pd.read_csv('data/validation_set.tsv', sep='\t')
sample_submission_score = pd.read_csv('data/sample_submission.csv')
sample_submission_mult = sample_submission_score.copy()

start_index = 0
# start_index = 197
# start_index = int(input('Start index: '))

right_answers_by_mult_counter = 0
right_answers_by_score_counter = 0

total_answers_short = 0
right_answers_short = 0
total_answers_long = 0
right_answers_long = 0

indexes = "ai_c12,ai_wiki_full"
# indexes = "ai_c12"
# ----------------------------------------------------------------------------------------------
try:
    # for index, row in validation_set[start_index:(validation_set.question.count()-1)].iterrows():
    # for index, row in validation_set[0:100].iterrows():
    for index, row in training_set[start_index:(start_index + 200)].iterrows():

        print('=============================================================================')
        print(row)

        question = row['question'].lower()
        question = remove_stop_words(question, False)
        question = question.strip()

        answers = ' '.join(row[['answerA', 'answerB', 'answerC', 'answerD']])

        # Find a knowledge domain
        for minimum_should_match in range(95, 50, -5):
            res = es.search(
                    body={"query":
                        {"match":
                            {'text': {
                                'query': question,
                                # 'operator': 'and',
                                'cutoff_frequency': 0.001,
                                'fuzziness': (10 - math.floor(minimum_should_match / 10)),
                                "minimum_should_match": str(minimum_should_match) + '%'
                            }
                            }
                        }
                    },
                    index=indexes,
                    size=10
            )
            if res['hits']['total'] > 5:
                break

        print("Got %d hits for domain with %d%%" % (res['hits']['total'], minimum_should_match))

        if res['hits']['total'] == 0:
            continue

        categories = []
        for hit in res['hits']['hits']:
            source = hit['_source']
            categories = categories + source['categories']

        categories = [cat for cat in categories
                      if re.search('^[0-9]{4}s? ', cat) is None
                      and re.search('^[0-9]{2}th-century ', cat) is None
                      and re.search(' in [0-9]{4}s?', cat) is None
                      and re.search(' films$', cat) is None
                      and re.search(' clips$', cat) is None
                      and re.search(' novels$', cat) is None
                      and re.search('^Film[s]? ', cat) is None
                      and re.search('^Screenplays ', cat) is None
                      ]

        categories.sort()
        categories_freq = [(cat[0], len(list(cat[1]))) for cat in itertools.groupby(categories)]
        categories_freq.sort(key=lambda x: x[1], reverse=True)
        # Create knowledge domain
        knowledge_domain = [x[0] for x in categories_freq[0:20]]
        print(knowledge_domain)

        # Search answers inside knowledge domain
        # { "term": { "categories": categories[0] }}
        es_domain_query = [{"match": {"categories": cat}} for cat in knowledge_domain]

        mean_answers_count = numpy.mean([
            x.split(' ').__len__() for x
            in row[['answerA', 'answerB', 'answerC', 'answerD']]
        ])

        hits = []
        scores = []
        fuzziness_arr = []
        for answer in row[['answerA', 'answerB', 'answerC', 'answerD']]:
            answer = answer.lower()
            answer = remove_stop_words(answer, True)
            answer = answer.strip()

            minimum_should_match_answer = 100
            if mean_answers_count > 3:
                minimum_should_match_answer = 80

            # Search documents
            for fuzziness in [2, 3, 4, 5]:
                res = es.search(
                    body={"query": {"filtered": {
                        "query": {
                            "match": {
                                'text': {
                                    'query': question + ' ' + answer,
                                    'cutoff_frequency': 0.001,
                                    'fuzziness': fuzziness,
                                    'minimum_should_match': str(minimum_should_match - fuzziness*5) + '%'
                                }
                            }
                        },
                        "filter": {
                            "and": [
                                {
                                    'bool': {
                                        'must': {
                                            'match': {'text': {
                                                'query': answer,
                                                'minimum_should_match': str(minimum_should_match_answer) + '%',
                                                'fuzziness': fuzziness - 1
                                            }}
                                        }
                                    }
                                },
                                # {
                                #     "bool": {
                                #         "should": [
                                #             es_domain_query,
                                #         ],
                                #         "minimum_should_match": 1
                                #     }
                                # }
                            ]
                        }
                    }}},
                    index=indexes,
                    size=10
                )
                if res['hits']['total'] > 1:
                    break

            fuzziness_arr = fuzziness_arr + [fuzziness]
            hits = hits + [res['hits']['total']]
            if res['hits']['total'] > 1:
                hit1 = res['hits']['hits'][0]
                hit2 = res['hits']['hits'][1]
                scores = scores + [round((hit1["_score"] + hit2["_score"])/2, 3)]
            else:
                scores = scores + [0]
            # breakpointstub = 0

        mult_array = [a * b for a, b in zip(hits, scores)]
        mult_max_index = mult_array.index(max(mult_array))
        mult_answer = ('A', 'B', 'C', 'D')[mult_max_index]

        score_max_index = scores.index(max(scores))
        score_answer = ('A', 'B', 'C', 'D')[score_max_index]

        print('Hits: ' + str(hits))
        print('Fuzziness: ' + str(fuzziness_arr))
        print('Scores: ' + str(scores))
        print('By score*hits result: ' + mult_answer)
        print('By score result: ' + score_answer)

        if mean_answers_count > 5:
            total_answers_long += 1
        else:
            total_answers_short += 1

        if mult_answer == row['correctAnswer']:
            right_answers_by_mult_counter += 1
        print('Right answers by mult: %d%%' % (100 * right_answers_by_mult_counter / (index - start_index + 1)))
        if score_answer == row['correctAnswer']:
            right_answers_by_score_counter += 1

            if mean_answers_count > 5:
                right_answers_long += 1
            else:
                right_answers_short += 1

        print('Right answers by score: %d%%' % (100 * right_answers_by_score_counter / (index - start_index + 1)))

        print('total_answers_short: ' + str(total_answers_short))
        print('right_answers_short: ' + str(right_answers_short))
        print('total_answers_long: ' + str(total_answers_long))
        print('right_answers_long: ' + str(right_answers_long))

        print()
        print()

        # !!!!!!!
        sample_submission_score.loc[index, 'correctAnswer'] = score_answer
        sample_submission_mult.loc[index, 'correctAnswer'] = mult_answer

        if index % 100 == 0:
            sample_submission_score.to_csv(
                'data/impl_3_' + str(index)
                + '_' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                + '.csv',
                index=False
            )
        #     # sample_submission_mult.to_csv(
        #     #     'data/result_set_mult_' + str(index)
        #     #     + '_' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        #     #     + '.csv',
        #     #     index=False
        #     # )

except:
    raise
finally:
   sample_submission_score.to_csv('data/impl_3_' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + '.csv', index=False)
#    # sample_submission_mult.to_csv('data/result_set_total_mult' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + '.csv', index=False)

