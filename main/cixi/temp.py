import numpy as np
import cx_Oracle as cx
import pandas.io.sql as sql
import pandas as pd
from collections import Counter
import os
'''
住院异常数据提取 
住院类型 = 普通住院
医院地区 = 330282（慈溪）
'''

YEAR = "2018"
AGE_THRESHOLD = 50
pickle_path = '/home/hezhenfeng/medical_insurance/data/cixi_data/'

if os.path.exists(pickle_path) is not True:
    os.mkdir(pickle_path)

try:
    connection = cx.connect('YBOLTP/mypassword@192.168.0.241:1521/helowin', encoding="UTF-8")
except Exception as e:
    print('异常：', str(e))
print('ready')

drug_sql = """SELECT
                b."住院天数" as DAYS
            FROM
                YB_FYJSXX_1013 a
                INNER JOIN YB_FYJSXX_1020_BCXX_3_2 b ON b."单据号" = a."单据号" 
            WHERE
                a."医疗类别" = '住院' 
                AND a."机构统筹区划代码" = '330282' 
                AND a."医保年度" = %s 
                AND a."单据号" != '                    '
            ORDER BY
                b."住院天数" 
                """ % (YEAR, )

print('read over')

df = sql.read_sql(drug_sql, connection)
df.to_pickle(os.path.join(pickle_path, 'days_%s.pkl' % YEAR))

df_list = sorted(list(df['DAYS']))
length = len(df_list)


def get_index(length_, alpha):
    return int(length_ * alpha)

print('0.05:', df_list[get_index(length, 0.05)], '0.01:', df_list[get_index(length, 0.01)])
print('0.95:', df_list[get_index(length, 0.95)], '0.99:', df_list[get_index(length, 0.99)])
print("finished.")

