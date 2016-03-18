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


def remove_punctuation(text):
    text = re.sub(punc_reg, "", text)
    text = re.sub("  ", " ", text)
    return text.strip()

def remove_stop_words(text, shortStopList):
    if shortStopList:
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
        question = remove_punctuation(question)
        question = remove_stop_words(question, False)
        question = question.strip()

        mean_answers_count = numpy.mean([
            x.split(' ').__len__() for x
            in row[['answerA', 'answerB', 'answerC', 'answerD']]
        ])

        minimum_should_match_answer = 80
        if mean_answers_count > 5:
            minimum_should_match_answer = 60

        hits = []
        scores = []
        for answer in row[['answerA', 'answerB', 'answerC', 'answerD']]:
            filter_cond = {}

            answer = answer.lower()
            if answer.split(' ').__len__() > 5:
                answer = remove_punctuation(answer)
                # answer = remove_stop_words(answer, True)
                answer = answer.strip()
            else:
                answer = remove_punctuation(answer)
                # answer = remove_stop_words(answer, True)
                answer = answer.strip()
                filter_cond = {
                        "and": [
                            {
                                'bool': {
                                    'must': {
                                        'match': {'text': {
                                            'query': answer,
                                            'minimum_should_match': '80%',
                                            'fuzziness': 3
                                        }}
                                    }
                                }
                            }
                        ]
                    }

            res = es.search(
                body={"query": {"filtered": {
                    "query": {
                        "match": {
                            'text': {
                                'query': question + ' ' + answer,
                                'cutoff_frequency': 0.001,
                                'fuzziness': 3,
                                'minimum_should_match': str(minimum_should_match_answer) + '%'
                            }
                        }
                    },
                    "filter": filter_cond
                }}},
                index=indexes,
                size=5
            )

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

