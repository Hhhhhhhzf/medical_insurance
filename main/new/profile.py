import pandas as pd
import numpy as np
import time
import os

# 路径
PICKLE_PATH = '../../data/dataset/new_pickles/profile/'
OUTPUT_PATH = '../../data/output/'
if not os.path.exists(PICKLE_PATH) or not os.path.exists(OUTPUT_PATH):
    os.mkdir(PICKLE_PATH)
    os.mkdir(OUTPUT_PATH)

file_mode = ('', '_fuyang')
mode = 1


def preprocess():
    person_records = dict()
    df1 = pd.read_pickle(os.path.join(PICKLE_PATH, 'first'+file_mode[mode]+'.pkl'))
    for pid, records in df1.groupby(by='AAC001'):
        line0 = records.iloc[0]
        age = line0['BAE450']
        inpatient_cnt = drug_cnt = outpatient = private_cnt = 0
        for i in range(len(records)):
            line = records.iloc[i]
            if line['AKA130'] in ('21', '26', '52'):
                inpatient_cnt += 1
            elif line['AKA130'] in ('11', '14', '15', '51'):
                outpatient += 1
            # if line['AKB022'] == '2':
            #     drug_cnt += 1
            if line['AKB022'] and line['BKA938'] and line['AKB022'] == '1' and line['BKA938'] > '1':
                private_cnt += 1
        # 0:个人编码、1:年龄、2:住院次数、3:购药次数、4:门诊次数、5:民营机构次数、6:列支费用、7:处方数量
        record = [pid, age, inpatient_cnt, drug_cnt, outpatient, private_cnt, 0, 0]
        person_records[pid] = record

    df2 = pd.read_pickle(os.path.join(PICKLE_PATH, 'second'+file_mode[mode]+'.pkl'))
    for pid, records in df2.groupby(by='AAC001'):
        keywords, prescription_set = set(), set()
        drug_cnt = prescription_cnt = 0
        for i in range(len(records)):
            line = records.iloc[i]
            keyword = line['AKB020_1'] + '_' + line['AKC190']
            if line['AKE003'] and line['AKE003'] == '1' and keyword not in keywords:
                keywords.add(keyword)
                drug_cnt += 1
            if line['AKC220'] and line['AKC220'] + '_' + keyword not in prescription_set:
                prescription_set.add(line['AKC220'] + '_' + keyword)
                prescription_cnt += 1
        person_records[pid][3] = drug_cnt
        person_records[pid][7] = prescription_cnt

    df3 = pd.read_pickle(os.path.join(PICKLE_PATH, 'third'+file_mode[mode]+'.pkl'))
    for pid, records in df3.groupby(by='AAC001'):
        liezhi_money = 0
        for i in range(len(records)):
            line = records.iloc[i]
            liezhi_money += (line['AKC264'] - line['AKC253'] - line['BKE030'])
        person_records[pid][6] = liezhi_money
    # 形成dataframe
    header = ['pid', 'age', 'in_cnt', 'drug_cnt', 'out_cnt', 'private_cnt', 'liezhi_money', 'prescription_cnt']
    final_records = []
    for pid, record in person_records.items():
        final_records.append(record)
    df = pd.DataFrame(final_records, columns=header)
    df.to_pickle(os.path.join(PICKLE_PATH, 'preprocess'+file_mode[mode]+'.pkl'))


def get_thresholds():
    df = pd.read_pickle(os.path.join(PICKLE_PATH, 'preprocess'+file_mode[mode]+'.pkl'))
    age_range = ('幼儿', '童年', '少年', '青年', '壮年', '中年', '老年')
    header = ['th_in_cnt', 'th_drug_cnt', 'th_out_cnt', 'th_private_cnt', 'th_liezhi_money', 'th_prescription_cnt']
    matrix = np.zeros((len(age_range), len(header)))
    thresholds = pd.DataFrame(matrix, columns=header)
    down_scale = [0, 4, 7, 18, 41, 49, 66]
    up_scale = [3, 6, 17, 40, 48, 65, 150]
    alpha = 0.95
    for i, value in enumerate(age_range):
        temp = df[(down_scale[i] <= df['age']) & (df['age'] <= up_scale[i])]
        for j in header:
            thresholds.at[i, j] = np.nanquantile(temp[j[3:]], alpha)
    thresholds.to_pickle(os.path.join(PICKLE_PATH, 'thresholds'+file_mode[mode]+'.pkl'))


def main():
    processed_data = pd.read_pickle(os.path.join(PICKLE_PATH, 'preprocess'+file_mode[mode]+'.pkl'))
    thresholds = pd.read_pickle(os.path.join(PICKLE_PATH, 'thresholds'+file_mode[mode]+'.pkl'))
    age_index = {0: '幼儿', 1: '童年', 2: '少年', 3: '青年', 4: '壮年', 5: '中年', 6: '老年'}
    header = ['主键', '人员编码', '年龄段', '住院次数', '购药次数', '门诊次数', '民营机构次数', '列支费用', '处方数量',
              '住院次数阈值', '购药次数阈值', '门诊次数阈值', '民营机构次数阈值', '列支费用阈值', '处方数量阈值']
    output = []
    for i in range(len(processed_data)):
        row = processed_data.iloc[i]
        down_scale = [0, 4, 7, 18, 41, 49, 66]
        up_scale = [3, 6, 17, 40, 48, 65, 150]
        index = 0
        for j in range(len(down_scale)):
            if down_scale[j] <= row['age'] <= up_scale[j]:
                index = j
                break
        row['age'] = age_index[index]
        threshold = list(thresholds.iloc[index])
        record = [i] + list(row)
        for value in threshold:
            record.append(value)
        output.append(record)
    output = pd.DataFrame(output, columns=header)
    output.to_excel(os.path.join(OUTPUT_PATH, 'PERSON_DETAIL'+file_mode[mode]+'.xlsx'), index=False)


if __name__ == '__main__':
    start = time.time()
    preprocess()
    print('step1 finished')
    get_thresholds()
    print('step2 finished')
    main()
    end = time.time()
    print('finished\ntime:', end-start)