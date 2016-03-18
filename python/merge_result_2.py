import datetime
import pandas as pd

sample_submission = pd.read_csv('data/sample_submission.csv')

v0 = pd.read_csv('data/final_2016-02-07-20-55-15.csv')
v1 = pd.read_csv('data/final_2016-02-07-22-09-53.csv')



sample_submission[10000:21299] = v1[(19430-10000):19430]
sample_submission[0:10100] = v0[0:10100]

sample_submission.to_csv(
        'data/final_total_' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + '.csv',
        index=False
)


# 8132
# 21298

