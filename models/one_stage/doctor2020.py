import cx_Oracle
import numpy as np
import os
import shutil
import pandas as pd
import time


os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'

def connor():
    connect=cx_Oracle.connect('me/mypassword@192.168.0.241:1521/helowin')    #连接数据库
    #c=connect.cursor()
    if connect:
        print("连接Oracle成功!")
    return connect                                              #获取cursor


def total_seconds(date):
    date_str = str(date)
    month_days_2016 = np.array([0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    month_days_2017 = np.array([0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    month_days_2018 = np.array([0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    month_days_2019 = np.array([0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    month_days_2020 = np.array([0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    y, m, d = date_str.split(' ')[0].split('-')
    hour, minute, second = date_str.split(' ')[1].split(':')

    if y == '2016':
        cnt_second = (np.sum(month_days_2016[:int(m)]) + int(d)-1)*24*60*60 + int(hour)*60*60 + int(minute)*60 + int(second)
    elif y == '2017':
        cnt_second = np.sum(month_days_2016)*24*60*60
        cnt_second += (np.sum(month_days_2017[:int(m)]) + int(d)-1)*24*60*60 + int(hour)*60*60 + int(minute)*60 + int(second)
    elif y == '2018':
        cnt_second = np.sum(month_days_2016) * 24 * 60 * 60 + np.sum(month_days_2017) * 24 * 60 * 60
        cnt_second += (np.sum(month_days_2018[:int(m)]) + int(d) - 1) * 24 * 60 * 60 + int(hour) * 60 * 60 + int(
            minute) * 60 + int(second)
    elif y == '2019':
        cnt_second = np.sum(month_days_2016) * 24 * 60 * 60 + np.sum(month_days_2017) * 24 * 60 * 60 + np.sum(month_days_2018) * 24 * 60 * 60
        cnt_second += (np.sum(month_days_2019[:int(m)]) + int(d) - 1) * 24 * 60 * 60 + int(hour) * 60 * 60 + int(
            minute) * 60 + int(second)
    elif y == '2020':
        cnt_second = np.sum(month_days_2016) * 24 * 60 * 60 + np.sum(month_days_2017) * 24 * 60 * 60 + np.sum(month_days_2018) * 24 * 60 * 60 + np.sum(month_days_2019) * 24 * 60 * 60
        cnt_second += (np.sum(month_days_2020[:int(m)]) + int(d) - 1) * 24 * 60 * 60 + int(hour) * 60 * 60 + int(
            minute) * 60 + int(second)
    else:
        raise Exception('invalid date', date_str)
    return cnt_second


def get_yichang_doctor():
    db = connor()
    cursor = db.cursor()

    doc_name = dict()
    inst_name = dict()
    doc_inst_total = dict()
    times_record = dict()
    doctor_record = dict()
    money_record = dict()
    doctor_liushui_money = dict()  # 医师流水号与其费用的映射，  解决退费金额

    cnt = 0
    # 机构编码, 机构名称, 医师编码, 医师姓名, 流水号, 总费用, 处方时间
    # cursor.execute('select AKB020_1,AKB021,BKF050,AKC273,AKC264,AKC002 from TLF_KC21 where BKC903 != 1')  # 不考虑外配处方
    sql = """SELECT
                b.AKB020_1,
                b.AKB021,
                b.BKF050,
                b.AKC273,
                a.AKC190,
                b.AKC264,
                a.AKC221 
            FROM
                TLF_KC22 a
                INNER JOIN TLF_KC21 b ON b.AKC190 = a.AKC190 
                AND b.AKB020_1 = a.AKB020_1
                INNER JOIN HOSPITAL c ON c.AKB020 = a.AKB020_1 
            WHERE
                c.AAA027 = '330183' 
                AND substr( b.AKC002, 1, 4 ) = '2020'
                AND b.BKC903 != 1"""
    cursor.execute(sql)
    while True:
        print('row: {}'.format(cnt))
        cnt += 1000
        rows = cursor.fetchmany(1000)
        if len(rows) == 0:
            break
        for row in rows:
            try:
                # cnt_second = '-'.join([row[-1][:4], row[-1][4:6], row[-1][6:8]]) + " " + ':'.join([row[-1][8:10], row[-1][10:12], row[-1][12:14]])
                cnt_second = total_seconds(row[-1])
            except:
                print(row)
                # input()
                continue

            # print(cnt_second)
            # input()

            # 忽略医师编码少于10位的数据（通常都是不合法的或者重复的编码，不可用）
            if row[2] and len(row[2])>10:
                # 记录医师的编码姓名映射、机构编码名称映射，记录医师涉及的机构
                if row[0] not in inst_name:
                    inst_name[row[0]] = row[1]
                if row[2] not in doc_name:
                    doc_name[row[2]] = row[3]
                if row[2] not in doc_inst_total:
                    doc_inst_total[row[2]] = set()
                doc_inst_total[row[2]].add(row[0])
                if row[2] not in doctor_liushui_money:
                    doctor_liushui_money[row[2]] = dict()
                # 记录医师所有记录的机构编码和结算时间
                if row[2] not in doctor_record:
                    doctor_record[row[2]] = []
                doctor_record[row[2]].append([row[0], cnt_second, row[-2], row[4]])  # 机构编码、计算时间（秒）、总费用、流水号

    # 对于每个医师，遍历其所有记录，记录其各尺度的违规记录数量
    for doc_id in doc_inst_total:
        if len(doc_inst_total[doc_id]) > 1:
            times_record[doc_id] = [0, 0, 0, 0]  # 初始数量都设为0
            money_record[doc_id] = 0
            doc_data = np.array(doctor_record[doc_id])
            doc_data = doc_data[doc_data[:, 1].argsort(), :]
            for i in range(1, len(doc_data)):
                if doc_data[i, 0] != doc_data[i-1, 0]:
                    duration = int(doc_data[i, 1]) - int(doc_data[i-1, 1])
                    if duration < 12*60*60:  # 最大时间改为半天
                        try:
                            money = float(doc_data[i, 2])
                            if doc_data[i, -1] not in doctor_liushui_money[doc_id]:
                                money_record[doc_id] += money
                                doctor_liushui_money[doc_id][doc_data[i, -1]] = [money]
                            elif money not in doctor_liushui_money[doc_id][doc_data[i, -1]]:
                                money_record[doc_id] += money
                                doctor_liushui_money[doc_id][doc_data[i, -1]].append(money)
                        except:
                            print(doc_data[i, 2])
                        times_record[doc_id][3] += 1
                        if duration < 60*60:
                            times_record[doc_id][2] += 1
                            if duration < 10 * 60:
                                times_record[doc_id][1] += 1
                                if duration < 5 * 60:
                                    times_record[doc_id][0] += 1

    cnt2 = 0
    output = []
    # 将有异常记录的医师数据导出
    for doc_id in times_record:
        if times_record[doc_id] != [0, 0, 0, 0]:
            cnt2 += 1
            row = [cnt2, '', doc_id]
            row.extend(times_record[doc_id])
            row.extend([len(doc_inst_total[doc_id]), doc_name[doc_id]])
            # row = [cnt2, '', doc_id, times_record[doc_id], len(doc_inst_total[doc_id]), doc_name[doc_id]]
            row.append(','.join(list(doc_inst_total[doc_id])))
            row.append(1)
            row.append(money_record[doc_id])

            output.append(row)

    headers = ["主键", "医师身份证号", "医师编码", "5分钟异常次数", "10分钟异常次数", "60分钟异常次数", "半天内异常次数", "异常机构数", "医师姓名", "异常机构编码", "风险率", "日异常列支总金额"]
    output = pd.DataFrame(np.array(output), columns=headers)
    output.to_excel('../data/model_shangxian/医师违规2020_final.xlsx', index=False)
    print(output.shape)

    print(cnt2, len(doc_name))


def get_doctor_mingxi():
    data = pd.read_excel('../data/model_shangxian/医师违规2020_final.xlsx', encoding='utf-8').values
    doctor_ids = dict()

    # 医师编码与主键的映射
    for row in data:
        doctor_ids[row[2]] = row[0]

    output = dict()

    db = connor()
    cursor = db.cursor()

    cnt = 0
    # 机构编码, 机构名称, 医师编码, 医师姓名, 处方号，结算时间, 流水号
    sql = """SELECT
                b.AKB020_1,
                b.AKB021,
                b.BKF050,
                b.AKC273,
                a.AKC220,
                a.AKC221,
                b.AKC190 
            FROM
                TLF_KC22 a
                INNER JOIN TLF_KC21 b ON b.AKC190 = a.AKC190 
                AND b.AKB020_1 = a.AKB020_1
                INNER JOIN HOSPITAL c ON c.AKB020 = a.AKB020_1 
            WHERE
                c.AAA027 = '330183' 
                AND substr( b.AKC002, 1, 4 ) = '2020'
                AND b.BKC903 != 1"""
    # cursor.execute('select AKB020_1,AKB021,BKF050,AKC273,AKC002,AKC190 from TLF_KC21 where BKC903 != 1')
    cursor.execute(sql)

    doctor_record = dict()
    while True:
        print('row: {}'.format(cnt))
        cnt += 1000
        rows = cursor.fetchmany(1000)
        if len(rows) == 0:
            break
        for row in rows:
            try:
                cnt_second = total_seconds(row[-2])
            except:
                print(row)
                continue
            if row[2] in doctor_ids:
                if row[2] not in doctor_record:
                    doctor_record[row[2]] = []
                doctor_record[row[2]].append([row[0], cnt_second, row[-3], row[-2], row[-1]])  # 机构编码、处方时间（秒）、处方号、处方时间、流水号

    for doc_id in doctor_record:
        doc_data = np.array(doctor_record[doc_id])
        doc_data = doc_data[doc_data[:, 1].argsort(), :]
        for i in range(1, len(doc_data)):
            if doc_data[i, 0] != doc_data[i - 1, 0]:
                duration = int(doc_data[i, 1]) - int(doc_data[i - 1, 1])
                tags = []
                if duration < 12 * 60 * 60:
                    tags.append('半天')
                    if duration < 60 * 60:
                        tags.append('60分钟')
                        if duration < 10 * 60:
                            tags.append('10分钟')
                            if duration < 5 * 60:
                                tags.append('5分钟')

                if len(tags) > 0:
                    for j in range(i - 1, i + 1):
                        if doc_data[j, -1] not in output:
                            output[doc_data[j, -1]] = []
                        time_str = str(doc_data[j, -2])
                        y, m, d = time_str.split(' ')[0].split('-')
                        hour, minute, second = time_str.split(' ')[1].split(':')
                        chufang_datetime = "{}-{}-{} {}:{}:{}".format(y, m, d, hour, minute, second)
                        output[doc_data[j, -1]].append([doctor_ids[doc_id], doc_data[j, 0], chufang_datetime, tags])  # 主键、机构编码、处方时间、异常类型
    cnt = 1
    output_data = []
    for liushui in output:
        records = output[liushui]
        for i in range(len(records)):
            record = records[i]
            for tag in record[-1]:
                output_data.append([record[0], record[1], liushui, cnt, tag, record[-2]])
                cnt += 1
    output_data = pd.DataFrame(output_data, columns=['外键', '机构编码', '就诊流水号', '主键', '类型', '处方时间'])
    output_data.to_excel('../data/model_shangxian/医师违规明细2020_final.xlsx', index=False)


if __name__ == '__main__':
    # 下面两步，分开执行
    start = time.time()
    # 获取违规医师
    # get_yichang_doctor()

    # # 获取违规医师相应明细
    get_doctor_mingxi()
    end = time.time()
    print('完成\n时间开销：', end-start)

