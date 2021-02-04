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


if __name__ == '__main__':
    AGE_THRESHOLD = 50
    # 富阳指定的私立医院
    PRIVATE_HOSPITAL = ('9325', '9327', '9333', '9300', '9383', '9310', '4030', '9417', '9324', '9335', '9385', '9341', '9311', '9352', '9308', '9445')

    print('cx oracle version:', cx.clientversion())
    pickle_path = '/home/hezhenfeng/medical_insurance/data/dataset/pickles'
    if os.path.exists(pickle_path) is not True:
        os.mkdir(pickle_path)

    try:
        connection = cx.connect('me/mypassword@192.168.0.241:1521/helowin', encoding="UTF-8")
    except Exception as e:
        print('异常：', str(e))

    print(connection.version)
    cursor = connection.cursor()
    print('ready')

    drug_sql = """SELECT
                    TLF_KC22.AKB020_1,--定点医疗机构编码
                    TLF_KC22.AKC190,--就诊流水号
                    TLF_KC21.AAC001,--个人编号
                    TLF_KC21.AAC003,--姓名
                    TLF_KC21.AAC004,--性别
                    TLF_KC21.BAE450,--年龄
                    TLF_KC22.AKE001,--药品、项目编码
                    TLF_KC22.AKE003,--收费项目种类
                    TLF_KC22.AKE006,--医院收费项目名称
                    TLF_KC22.AKA063,--收费类别
                    TLF_KC22.AKC225,--单价
                    TLF_KC22.AKC226,--数量
                    TLF_KC22.AAE019,--金额
                    TLF_KC60.AKC264,--医疗费总额
                    TLF_KC60.AKC253,--自费金额
                    TLF_KC60.BKE030,--自理费用
                    TLF_KC60.BKE060,--总报销金额
                    TLF_KC21.BKC192,--入院日期
                    TLF_KC21.BKC194,--出院日期
                    HOSPITAL.AKB022,--定点机构类型
                    HOSPITAL.BKA938,--营利类型
                    HOSPITAL.AKB023,--定点机构分类代码
                    HOSPITAL.AKA101 --机构等级
                    
                FROM
                    TLF_KC22
                    INNER JOIN TLF_KC60 ON TLF_KC60.AKC190 = TLF_KC22.AKC190 
                    AND TLF_KC60.AKB020_1 = TLF_KC22.AKB020_1
                    INNER JOIN TLF_KC21 ON TLF_KC21.AKC190 = TLF_KC22.AKC190 
                    AND TLF_KC21.AKB020_1 = TLF_KC22.AKB020_1
                    INNER JOIN HOSPITAL ON HOSPITAL.AKB020 = TLF_KC22.AKB020_1 --hospital只用来分类，可以使用AKB020
                    
                WHERE
                    TLF_KC21.AKA130 = '21' 
                    AND HOSPITAL.AAA027 = '330183' 
                    AND HOSPITAL.AKB022 = '1' --非药店机构
                    
                    AND substr( TLF_KC60.AKC001, 1, 4 ) = '2020' 
                    AND TLF_KC21.BKC197 <> '1' --剔除中心报销
                    AND TLF_KC21.AKC131 is null -- 剔除转院
                    AND EXISTS (
                    SELECT
                        1 
                    FROM
                        TLF_KC24 
                    WHERE
                        TLF_KC24.AKB020_1 = TLF_KC22.AKB020_1 
                        AND TLF_KC24.AKC190 = TLF_KC22.AKC190 
                        AND NVL( TLF_KC24.BKC380, '0' ) = '0' 
                    ) 
                    AND NOT EXISTS (  --剔除慢性病
                        SELECT
                            1 
                        FROM
                            CHRONICDIS_LIST L 
                        WHERE
                        L.AKE001 = TLF_KC21.AKC193 
                        OR L.AKE001 = TLF_KC21.AKC196)
                    AND NOT EXISTS ( --剔除18年到20年存在规定病种的人 
                        SELECT 1
                        FROM
                            TLF_KC21 a
                            INNER JOIN TLF_KC60 b ON b.AKC190 = a.AKC190 
                            AND b.AKB020_1 = a.AKB020_1
                            INNER JOIN HOSPITAL c ON c.AKB020 = a.AKB020_1 
                        WHERE
                            a.AKA130 = '14' 
                            AND c.AAA027 = '330183' 
                            AND c.AKB022 = '1' --非药店机构
                            AND substr( b.AKC001, 1, 4 ) between '2018' and '2020'
                            AND ((c.bka938 = 1 and substr(c.akb023, 1, 1) = '1' and substr(c.aka101, 1, 1) >= '2')  --公立大于等于2级，
                                 OR (substr(c.bka938, 1, 1) > '1' and substr(c.akb023, 1, 1) = '1' and substr(c.aka101, 1, 1) >= '3'))  -- 私立，大于等于3级
                            AND TLF_KC21.AAC001 = a.AAC001 
                    )
               """
    df = sql.read_sql(drug_sql, connection)
    print('read over')

    df.sort_values(by='AKC190', ascending=True, axis=0, inplace=True)
    public = df[(df['BKA938'] == '1') & (df['AKB023'].astype('str').str.startswith('1'))]  # 公立医院
    community = df[(df['BKA938'] == '1') & (df['AKB023'].astype('str').str.startswith('2'))]  # 社区医院
    private = df[(df['BKA938'] > '1') & (df['AKB023'].astype('str').str.startswith('1'))]  # 民营医院
    private_other = df[(df['BKA938'] > '1') & ~(df['AKB023'].astype('str').str.startswith('1')) & ~(df['AKB023'].astype('str').str.startswith('2'))]  # 民营其他医院
    print('ready, write to pickle,step1')
    public.to_pickle(os.path.join(pickle_path, 'public.pkl'))
    community.to_pickle(os.path.join(pickle_path, 'community.pkl'))
    private.to_pickle(os.path.join(pickle_path, 'private.pkl'))
    private_other.to_pickle(os.path.join(pickle_path, 'private_other.pkl'))

    # 筛选老年群体(当前只考虑私立)
    private_old = private[(private['AKB020_1']).isin(PRIVATE_HOSPITAL) & (private['BAE450'] > AGE_THRESHOLD)]
    print('ready, write to pickle,step2')
    private_old.to_pickle(os.path.join(pickle_path, 'private_old.pkl'))

    print("finished.")

