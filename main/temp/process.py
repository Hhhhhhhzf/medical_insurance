import pandas as pd
import os
import numpy as np
import time

data_path = '/home/hezhenfeng/medical_insurance/data/dataset/ages'
years = ('2018', '2019', '2020')
log_file = 'log.txt'
# pc_id, age, in_dcode, in_dname, out_dcode, out_dname


def get_index(length_, alpha):
    return int(length_ * alpha)


def is_integer(value):
    try:
        int(value)
        return True
    except ValueError:
        return False


def write_log(content):
    f = open(log_file, mode='a')
    f.write(content+'\n')
    f.close()


if __name__ == '__main__':
    start = time.time()
    header = ['code', 'disease_name', 'min_age', 'max_age', '0.01_age', '0.99_age', '0.05_age', '0.95_age']
    output = []
    code_name, code_ages, code_pcid = dict(), dict(), dict()
    for year in years:
        print(year, 'start')
        write_log(str(year)+' start')
        data = pd.read_pickle(os.path.join(data_path, '%s_data.pkl' % year))
        for i in range(len(data)):
            row = data.iloc[i]
            pc_id, age, in_dcode, in_dname, out_dcode, out_dname = row['PC_ID'], row['AGE'], row['IN_DCODE'], \
                                                                   row['IN_DNAME'], row['OUT_DCODE'], row['OUT_DNAME']
            if is_integer(age):
                age = int(age)
                # 疾病编码 映射到 名称
                if in_dcode not in code_name:
                    code_name[in_dcode] = in_dname
                if out_dcode not in code_name:
                    code_name[out_dcode] = out_dname
                # 疾病编码 映射到 年龄
                if in_dcode not in code_ages:
                    code_ages[in_dcode] = []
                if out_dcode not in code_ages:
                    code_ages[out_dcode] = []
                # 疾病编码 映射到 个人编码【集合】
                if in_dcode not in code_pcid:
                    code_pcid[in_dcode] = set()
                if out_dcode not in code_pcid:
                    code_pcid[out_dcode] = set()
                #
                if pc_id not in code_pcid[in_dcode]:
                    code_ages[in_dcode].append(age)
                    code_pcid[in_dcode].add(pc_id)
                if pc_id not in code_pcid[out_dcode]:
                    code_ages[out_dcode].append(age)
                    code_pcid[out_dcode].add(pc_id)
        end = time.time()
        write_log('this step cost time is ' + str((end - start) / 60) + ' min')
    for code, ages in code_ages.items():
        ages.sort()
        length = len(ages)
        output.append([code, code_name[code], ages[0], ages[length-1], ages[get_index(length, 0.01)],
                       ages[get_index(length, 0.99)], ages[get_index(length, 0.05)], ages[get_index(length, 0.95)]])
    df = pd.DataFrame(output, columns=header)
    df.to_pickle(os.path.join(data_path, 'ans_1.pkl'))
    df.to_excel(os.path.join(data_path, '结果_1.xlsx'), encoding='gbk', index=False)
    end = time.time()
    print('cost time is ', (end - start)/60)
    write_log('cost time is '+str((end - start)/60) + ' min')
    print('finished')
    write_log('finished')
