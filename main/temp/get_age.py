import numpy as np
import cx_Oracle as cx
import pandas.io.sql as sql
import pandas as pd
from collections import Counter
import os

data_path = '/home/hezhenfeng/medical_insurance/data/dataset/ages'
year = '2019'

try:
    connection = cx.connect('me/mypassword@192.168.0.241:1521/helowin', encoding="UTF-8")
except Exception as e:
    print('异常：', str(e))

age_sql = """SELECT
                a.AAC001 AS pc_id,
                a.BAE450 AS age,
                a.AKC193 AS in_dcode,
                a.BKC231 AS in_dname,
                a.AKC193 AS out_dcode,
                a.BKC231 AS out_dname
            FROM
                tlf_kc21_%s a
""" % year

age_sql_2020 = """
            SELECT
                a.AAC001 AS pc_id,
                a.BAE450 AS age,
                a.AKC193 AS in_dcode,
                a.BKC231 AS in_dname,
                a.AKC193 AS out_dcode,
                a.BKC231 AS out_dname
            FROM
                tlf_kc21 a
            where substr(a.AKC002, 1, 4) = '2020'
"""

data = sql.read_sql(age_sql, connection)
print('read over')
data.to_pickle(os.path.join(data_path, '%s_data.pkl' % year))
print('finished!')
