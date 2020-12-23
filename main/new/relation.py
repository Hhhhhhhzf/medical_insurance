import pandas as pd
import numpy as np
import time
import os
from sqlalchemy import create_engine, types

# 路径
PICKLE_PATH = '../../data/dataset/new_pickles/relation/'
OUTPUT_PATH = '../../data/output/'
if not os.path.exists(PICKLE_PATH) or not os.path.exists(OUTPUT_PATH):
    os.mkdir(PICKLE_PATH)
    os.mkdir(OUTPUT_PATH)
file_mode = ('', '_fuyang')
mode = 1


def process_org(org_data, org_type, index):
    records = []
    for pid, group in org_data.groupby(by='AAC001'):
        for org_id, org_group in group.groupby(by='AKB020_1'):
            org_name = org_group.iloc[0]['AKB021']
            settle_id_set = set()
            liezhi_fee = 0
            for i in range(len(org_group)):
                row = org_group.iloc[i]
                settle_id = row['AKC190']
                if settle_id not in settle_id_set:
                    settle_id_set.add(settle_id)
                liezhi_fee += float(row['LIEZHI'])
            records.append([index[0], pid, org_id, org_name, org_type, len(settle_id_set), liezhi_fee])
            index[0] += 1
    return records


def process_treatment_or_drug(data, relation_type, index, top_ten=False):
    records = []
    for pid, group in data.groupby(by='AAC001'):
        code_name = {}
        item_count = {}
        item_liezhi = {}
        for i in range(len(group)):
            row = group.iloc[i]
            item_code, item_name = row['AKE001'], row['AKE006']
            if item_code not in code_name:
                code_name[item_code] = item_name
            if item_code not in item_count:
                item_count[item_code] = 0
            if item_code not in item_liezhi:
                item_liezhi[item_code] = 0
            item_liezhi[item_code] += float(row['LIEZHI'])
            item_count[item_code] += float(row['AKC226'])
        sorted_list = sorted(item_count.items(), key=lambda item: item[1], reverse=True)  # 按数量进行由大到小的排序
        if top_ten and len(sorted_list) > 10:  # 当是药物的时候且药物数量大于10个
            sorted_list = sorted_list[: 10]
        for dict_value in sorted_list:
            key, value = dict_value[0], dict_value[1]
            records.append([index[0], pid, key, code_name[key], relation_type, value, item_liezhi[key]])
            index[0] += 1
    return records


def main():
    index = [1926582]
    # index = [0]
    output = []
    header = ['GUID', 'OPCODE', 'CODE', 'NAME', 'TYPE', 'COUNT', 'MONEY']
    df = pd.read_pickle(os.path.join(PICKLE_PATH, 'data'+file_mode[mode]+'.pkl'))

    # step 1: 按机构来处理
    drugstore = df[(df['AKB022'] == '2')]
    public = df[(df['AKB022'] == '1') & (df['BKA938'] == '1')]  # & (df['AKB023'].astype('str').str.startswith('1'))]
    private = df[(df['AKB022'] == '1') & (df['BKA938'] > '1')]  # & (df['AKB023'].astype('str').str.startswith('1'))]
    output += process_org(drugstore, '药店', index)
    output += process_org(public, '公立机构', index)
    output += process_org(private, '民营医院', index)
    print('step 1 finished!')

    # step 2: 按群体就医来（暂时先空）
    print('step 2 finished!')

    # step 3: 按中医治疗、物理治疗
    chinese = df[(df['AKE001'].str.startswith('f4'))]
    physical = df[(df['AKE001'].str.startswith('f3401'))]
    output += process_treatment_or_drug(chinese, '中医治疗', index)
    output += process_treatment_or_drug(physical, '物理治疗', index)
    print('step 3 finished!')

    # step 4: 按西药、中成药、中草药来
    western_drug = df[(df['AKA063'] == '11')]
    zhongcheng_drug = df[(df['AKA063'] == '12')]
    chinese_drug = df[(df['AKA063'] == '13')]
    output += process_treatment_or_drug(western_drug, '西药', index, True)
    output += process_treatment_or_drug(zhongcheng_drug, '中成药', index, True)
    output += process_treatment_or_drug(chinese_drug, '中草药', index, True)
    print('step 4 finished!')

    output = pd.DataFrame(output, columns=header)
    return output


def to_oracle(data):
    tables = ('person_relationship', 'person_relationship_fuyang')
    conn_string = 'oracle+cx_oracle://me:mypassword@192.168.0.241:1521/helowin'
    dtype = {
        'GUID': types.VARCHAR(20),
        'OPCODE': types.VARCHAR(20),
        'CODE': types.VARCHAR(50),
        'NAME': types.VARCHAR(100),
        'TYPE': types.VARCHAR(100),
        'COUNT': types.INTEGER,
        'MONEY': types.FLOAT
    }
    engine = create_engine(conn_string, echo=False, encoding='utf-8')
    data.to_sql(tables[mode], con=engine, if_exists='append', index=False, chunksize=1000, dtype=dtype)
    print('export finished!')


if __name__ == '__main__':
    start = time.time()
    data_frame = main()
    data_frame.to_pickle(os.path.join(OUTPUT_PATH, 'PERSON_RELATIONSHIP'+file_mode[mode]+'.pkl'))
    to_oracle(data_frame)
    end = time.time()
    print('finished\ntime:', end-start)
