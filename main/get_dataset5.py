import numpy as np
import cx_Oracle as cx
import pandas.io.sql as sql
import pandas as pd
from collections import Counter
import os
'''
住院异常数据提取 
住院类型 = 21（普通住院）
医院地区 = 330183（富阳区）
'''

print('cx oracle version:', cx.clientversion())
csv_path = '/data/xlm/project/HealthCare2/dataset/'
if os.path.exists(csv_path) is not True:
    os.mkdir(csv_path)

try:
    connection = cx.connect('me/mypassword@192.168.0.241:1521/helowin', encoding="UTF-8")
except Exception as e:
    print(str(e))

print(connection.version)
cursor = connection.cursor()
print('ready')
index = 0

drug_sql = """select  TLF_KC22.AKB020, --定点医疗机构编码
                      TLF_KC22.AKC190, --就诊流水号
                      TLF_KC21.AAC001, --个人编号
                      TLF_KC21.AAC003, --姓名
                      TLF_KC21.AAC004, --性别
                      TLF_KC21.BAE450, --年龄
                      TLF_KC22.AKE003, --收费项目种类
                      TLF_KC22.AKE006, --医院收费项目名称
                      TLF_KC22.AKA063, --收费类别
                      TLF_KC22.AKC225, --单价
                      TLF_KC22.AKC226, --数量
                      TLF_KC22.AAE019, --金额
                      TLF_KC60.AKC264, --医疗费总额
                      TLF_KC60.AKC253, --自费金额
                      TLF_KC60.BKE030, --自理费用
                      TLF_KC60.BKE060, --总报销金额
                      TLF_KC21.BKC192, --入院日期
                      TLF_KC21.BKC194  --出院日期
               from TLF_KC22 
               inner join TLF_KC60 on TLF_KC60.AKC190 = TLF_KC22.AKC190
                      and TLF_KC60.AKB020 = TLF_KC22.AKB020
               inner join TLF_KC21 on TLF_KC21.AKC190 = TLF_KC22.AKC190
                      and TLF_KC21.AKB020 = TLF_KC22.AKB020
               inner join HOSPITAL on HOSPITAL.AKB020 = TLF_KC22.AKB020
               where TLF_KC21.AKA130 = '21'
               and HOSPITAL.AAA027 = '330183'
               and substr(TLF_KC60.AKC001, 1, 4) = '2020'
           """

df = sql.read_sql(drug_sql, connection)
df.sort_values(by='AKC190', ascending=True, axis=0, inplace=True)
print("yes_in_hospital2020")
df.to_csv(os.path.join(csv_path, 'in_hospital_data2020.csv'), encoding='utf-8-sig')