import datetime
import pandas as pd

sample_submission = pd.read_csv('data/sample_submission.csv')

v0 = pd.read_csv('data/impl_3_2016-02-02-09-46-39__3800.csv')
v1 = pd.read_csv('data/impl_3_2016-02-02-12-28-33__4900.csv')
v2 = pd.read_csv('data/impl_3_2016-02-02-23-16-05__7400.csv')
v3 = pd.read_csv('data/impl_3_2016-02-03-01-21-59__8100.csv')

sample_submission[0:3800] = v0[0:3800]
sample_submission[3800:4900] = v1[3800:4900]
sample_submission[4900:7400] = v2[4900:7400]
sample_submission[7400:8100] = v3[7400:8100]

sample_submission.to_csv(
        'data/total_3_' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + '.csv',
        index=False
)


v0 = pd.read_csv('data/impl_5_2016-02-02-09-46-39__3800.csv')
v1 = pd.read_csv('data/impl_5_2016-02-02-12-28-34__4900.csv')
v2 = pd.read_csv('data/impl_5_2016-02-02-23-16-05__7400.csv')
v3 = pd.read_csv('data/impl_5_2016-02-03-01-21-24__8100.csv')

sample_submission[0:3800] = v0[0:3800]
sample_submission[3800:4900] = v1[3800:4900]
sample_submission[4900:7400] = v2[4900:7400]
sample_submission[7400:8100] = v3[7400:8100]

sample_submission.to_csv(
        'data/total_5_' + datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + '.csv',
        index=False
)


