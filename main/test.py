import numpy as np
import cx_Oracle as cx
import pandas.io.sql as sql
import pandas as pd
from collections import Counter
import os

pickle_path = '/home/hezhenfeng/medical_insurance/data/dataset/pickles/'

df = pd.read_pickle(os.path.join(pickle_path, 'community.pkl'))

cnt = 0
for i in range(len(df)):
    row = df.iloc[i]
    if row['AKB020_1'] == '9100':
        cnt += 1
print(cnt)

