import pandas as pd
import os
import numpy as np
import time

# 路径
PICKLE_PATH = '../../data/dataset/new_pickles/drug/'
OUTPUT_PATH = '../../data/output/'
if not os.path.exists(PICKLE_PATH) or not os.path.exists(OUTPUT_PATH):
    os.mkdir(PICKLE_PATH)
    os.mkdir(OUTPUT_PATH)
file_mode = ('', '_fuyang')
mode = 0


def main():
    df = pd.read_pickle(os.path.join(PICKLE_PATH, 'data'+file_mode[mode]+'.pkl'))
    header = ['主键', '人员编码', '中草药列支费用', '中成药列支费用', '西药列支费用']
    output = []
    index = 0
    for pid, records in df.groupby(by='AAC001'):
        western_drug = zhongcheng_drug = zhongcao_drug = 0
        for i in range(len(records)):
            row = records.iloc[i]
            liezhi_money = float(row['AAE019'] - row['AKC228'] - row['AKE051'])
            if row['AKA063'] == '11':  # 西药
                western_drug += liezhi_money
            if row['AKA063'] == '12':  # 中成药
                zhongcheng_drug += liezhi_money
            if row['AKA063'] == '13':  # 中草药
                zhongcao_drug += liezhi_money
        output.append([index, pid, zhongcao_drug, zhongcheng_drug, western_drug])
        index += 1
    output = pd.DataFrame(output, columns=header)
    output.to_excel(os.path.join(OUTPUT_PATH, 'PERSON_DRUGS'+file_mode[mode]+'.xlsx'), index=False)


if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print('finished\ntime:', end-start)
