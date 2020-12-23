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

YEAR = "2020"
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
                A."定点机构编码" AS ORG_ID,
                C."证件号码" AS PC_ID,
                A."姓名" AS NAME,
                C."性别" AS GENDER,
                TO_NUMBER( %s ) - TO_NUMBER( SUBSTR( C."出生日期", 1, 4 ) ) AS AGE,
                A."单据号" AS SETTLE_ID,
                A."收费项目类别" AS ITEM_TYPE,
                A."金额" AS MONEY,
                A."医保范围费用" AS LIEZHI,
                A."医保目录编码" as CATALOG_CODE,
                A."医保目录名称" as CATALOG_NAME,
                B."结算时间" AS TIME,
                D."住院天数" AS DAYS
            FROM
                YB_FYMXXX_1013 A
                INNER JOIN YB_FYJSXX_1013 B ON B."单据号" = A."单据号" 
                AND B."定点机构编码" = A."定点机构编码"
                INNER JOIN YB_CBRYXX_1013 C ON C."证件号码" = A."证件号码" 
                INNER JOIN YB_FYJSXX_1020_BCXX_3_2 D on D."单据号" = A."单据号"
            WHERE
                B."医疗类别" = '住院' 
                AND B."机构统筹区划代码" = '330282' 
                AND B."医保年度" = %s 
                AND B."单据号" != '                    '
                AND C."参保状态" = '正常'
                and not EXISTS(
                    SELECT 1 FROM YB_TMBZBAXX_1013 E WHERE E."证件号码" = A."证件号码"
                )
                and not EXISTS(
                    SELECT 1 FROM ME."CHRONICDIS_LIST" F WHERE D."出院疾病名称" = F.AKE006 and D."出院疾病名称" is not NULL
                )""" % (YEAR, YEAR)

print('read over')

df = sql.read_sql(drug_sql, connection)
old = df[(df['AGE'] > AGE_THRESHOLD)]

print('write to pickle,step1')
df.to_pickle(os.path.join(pickle_path, 'all_data_%s.pkl' % YEAR))
print('write to pickle,step2')
old.to_pickle(os.path.join(pickle_path, 'old_data_%s.pkl' % YEAR))
print("finished.")

