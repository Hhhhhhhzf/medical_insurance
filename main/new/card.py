import pandas as pd
import numpy as np
import datetime
import time
import os
from sqlalchemy import create_engine, types

# 路径
PICKLE_PATH = '../../data/dataset/new_pickles/card/'
OUTPUT_PATH = '../../data/output/'
if not os.path.exists(PICKLE_PATH) or not os.path.exists(OUTPUT_PATH):
    os.mkdir(PICKLE_PATH)
    os.mkdir(OUTPUT_PATH)
file_mode = ('', '_fuyang')
mode = 1


def main():
    df = pd.read_pickle(os.path.join(PICKLE_PATH, 'data'+file_mode[mode]+'.pkl'))
    records = [[], [], [], [], [], [], []]
    index = 0
    for pid, group in df.groupby(by='AAC001'):
        keyword_set = set()
        weeks_liezhi = [{}, {}, {}, {}, {}, {}, {}]  # 每星期的时间产生的列支
        weeks_count = [{}, {}, {}, {}, {}, {}, {}]  # 每星期的时间产生的刷卡次数
        for i in range(len(group)):
            row = group.iloc[i]
            liezhi = float(row['LIEZHI'])
            date_time = row['AKC002'][:12]
            weekday = datetime.datetime.strptime(date_time[0:8], '%Y%m%d').weekday()
            time_str = date_time[8:10]  # 按小时来处理
            # 列支费用
            if time_str not in weeks_liezhi[weekday]:
                weeks_liezhi[weekday][time_str] = 0
            weeks_liezhi[weekday][time_str] += liezhi
            keyword = row['AKB020_1'] + '_' + row['AKC190']
            # 次数计算
            if time_str not in weeks_count[weekday]:
                weeks_count[weekday][time_str] = 0
            if keyword not in keyword_set:
                weeks_count[weekday][time_str] += 1
                keyword_set.add(keyword)
        for i, dict_value in enumerate(weeks_liezhi):
            for key, value in dict_value.items():
                records[i].append([index, pid, i+1, key, value, weeks_count[i][key]])
                index += 1
    header = ['主键', '人员编码', '周几', '时间', '列支费用', '次数']
    sheet_name = ['星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期七']
    writer = pd.ExcelWriter(os.path.join(OUTPUT_PATH, 'PERSON_CARDTIME'+file_mode[mode]+'.xlsx'))
    for i in range(len(records)):
        output = pd.DataFrame(records[i], columns=header)
        output.to_excel(writer, sheet_name=sheet_name[i], index=False, encoding='utf-8')
    writer.save()
    return records


def to_oracle():
    TABLES = ('person_cardtime', 'person_cardtime_fuyang')
    FILES = ('PERSON_CARDTIME.xlsx', 'PERSON_CARDTIME_fuyang.xlsx')
    sheet_names = ['星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期七']
    header = ['GUID', 'opcode', 'day', 'time', 'money', 'count']
    conn_string = 'oracle+cx_oracle://me:mypassword@192.168.0.241:1521/helowin'
    dtype = {
        'GUID': types.VARCHAR(20),
        'opcode': types.VARCHAR(20),
        'day': types.VARCHAR(20),
        'time': types.VARCHAR(20),
        'money': types.FLOAT,
        'count': types.INTEGER
    }
    engine = create_engine(conn_string, echo=False, encoding='utf-8')
    for sheet_name in sheet_names:
        df = pd.read_excel(os.path.join(OUTPUT_PATH, FILES[mode]), sheet_name=sheet_name, names=header)
        df.to_sql(TABLES[mode], con=engine, if_exists='append', index=False, chunksize=1000, dtype=dtype)
    print('export finished!')


if __name__ == '__main__':
    start = time.time()
    # main()
    to_oracle()
    # df = pd.read_excel(os.path.join(OUTPUT_PATH, 'PERSON_CARDTIME_fuyang.xlsx'), sheet_name='星期一')
    end = time.time()
    print('finished\ntime:', end-start)