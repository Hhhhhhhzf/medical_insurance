import cx_Oracle
import numpy as np
import os
import shutil
import pandas as pd
import math


os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'


def connor():
    connect=cx_Oracle.connect('me/mypassword@192.168.0.241:1521/helowin')  # 连接数据库
    #c=connect.cursor()
    if connect:
        print("连接Oracle成功!")
    return connect                                              #获取cursor


def total_seconds(date_str):
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
        cnt_second = 0
        print('invalid date:', date_str)
    return cnt_second


def total_days(date_str):
    date_str = str(date_str)
    month_days_2016 = np.array([0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    month_days_2017 = np.array([0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    month_days_2018 = np.array([0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    month_days_2019 = np.array([0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    month_days_2020 = np.array([0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    y, m, d = date_str.split(' ')[0].split('-')

    if y == '2016':
        cnt_day = (np.sum(month_days_2016[:int(m)]) + int(d) - 1)
    elif y == '2017':
        cnt_day = np.sum(month_days_2016)
        cnt_day += (np.sum(month_days_2017[:int(m)]) + int(d) - 1)
    elif y == '2018':
        cnt_day = np.sum(month_days_2016) + np.sum(month_days_2017)
        cnt_day += (np.sum(month_days_2018[:int(m)]) + int(d) - 1)
    elif y == '2019':
        cnt_day = np.sum(month_days_2016) + np.sum(month_days_2017) + np.sum(
            month_days_2018)
        cnt_day += (np.sum(month_days_2019[:int(m)]) + int(d) - 1)
    elif y == '2020':
        cnt_day = np.sum(month_days_2016) + np.sum(month_days_2017) + np.sum(
            month_days_2018) + np.sum(month_days_2019)
        cnt_day += (np.sum(month_days_2020[:int(m)]) + int(d) - 1)
    else:
        cnt_day = 0
        print('invalid date:', date_str)
    return cnt_day


def filter_hospital():
    """
    筛选民营医疗机构、二级及二级以下的公立医疗机构，排除精神病、康复类专科公立医疗机构（民营医疗机构不排除）
    :return:
    """
    print('reading excel...')
    kb01 = pd.read_excel('../docs/两定机构KB01.xlsx', sheet_name='SQL Results')
    keep_hospital = set()
    inst_code = kb01['AKB020'].values
    fenlei = kb01['BKA938'].values
    jibie = kb01['AKA101'].values
    name = kb01['AKB021'].values
    for i in range(kb01.shape[0]):
        if fenlei[i] in [2, 3]:
            keep_hospital.add(str(int(inst_code[i])))
        elif fenlei[i]==1 and jibie[i] in [0, 10, 20, 21] and '精神' not in name[i] and '康复' not in name[i]:
            keep_hospital.add(str(int(inst_code[i])))

    print('kept', len(keep_hospital))
    return keep_hospital


def get_filtered_zhuyuan():
    """
    获取按照步骤1筛选数据后，剩余的住院记录的摘要信息
    :return:
    """
    db = connor()
    cursor = db.cursor()

    keep_hospital = filter_hospital()
    # 机构编码, 机构名称, 流水号, 个人编号, 医疗类别, 入院日期, 出院日期, 入院病种编码, 病种名称
    cursor.execute('select AKB020,AKB021,AKC190,AAC001,AKA130,BKC192,BKC194,AKC193,BKC231 from TLF_KC21_2019')

    cnt = 0
    all_zhuyuan = []
    inst_set = set()
    dangtian_map = dict()
    tongzhenduan_map = dict()
    while True:
        if cnt % 1000000 == 0:
            print('get zhuyaun', cnt)
        cnt += 1000
        rows = cursor.fetchmany(1000)
        if len(rows) == 0:
            break
        for row in rows:
            if str(int(row[0])) in keep_hospital and row[4] in ['21', '26', '52', '53']:
                inst_set.add(row[0])
                row = list(row)
                row.extend([0, 0, 0])  # 当天门诊次数, 其他天门诊次数, 相同诊断门诊次数
                all_zhuyuan.append(row)

                # 当天门诊映射记录
                if row[3] not in dangtian_map:
                    dangtian_map[row[3]] = dict()
                day = str(row[5]).split(' ')[0]

                if len(day) != 10:
                    print(row)
                    input()

                if day not in dangtian_map[row[3]]:
                    dangtian_map[row[3]][day] = []
                dangtian_map[row[3]][day].append(len(all_zhuyuan)-1)

                # 相同门诊映射记录
                if row[3] not in tongzhenduan_map:
                    tongzhenduan_map[row[3]] = dict()
                jibing_code = str(row[-2])[:4]
                if jibing_code not in tongzhenduan_map[row[3]]:
                    tongzhenduan_map[row[3]][jibing_code] = []
                tongzhenduan_map[row[3]][jibing_code].append(len(all_zhuyuan)-1)

    print(len(all_zhuyuan))

    # 机构编码, 机构名称, 流水号, 个人编号, 医疗类别, 入院日期, 出院日期, 入院病种编码, 病种名称
    cursor.execute('select AKB020,AKB021,AKC190,AAC001,AKA130,BKC192,BKC194,AKC193,BKC231 from TLF_KC21_2019')

    cnt = 0
    while True:
        if cnt % 1000000 == 0:
            print('count menzhen', cnt)
        cnt += 1000
        rows = cursor.fetchmany(1000)
        if len(rows) == 0:
            break
        for row in rows:
            if row[4] in ['11', '14', '15', '51', '16'] and row[3] in dangtian_map:
                # 住院当天的门诊数+1
                day = str(row[5]).split(' ')[0]
                if day in dangtian_map[row[3]]:
                    for i in dangtian_map[row[3]][day]:
                        all_zhuyuan[i][-3] += 1

                # 住院记录的其他天门诊数+1
                for cur_day in dangtian_map[row[3]]:
                    if cur_day!=day:
                        for i in dangtian_map[row[3]][cur_day]:
                            all_zhuyuan[i][-2] += 1

                # 判断是否为相同诊断
                jibing_code = str(row[-2])[:4]
                if jibing_code in tongzhenduan_map[row[3]]:
                    for i in tongzhenduan_map[row[3]][jibing_code]:
                        all_zhuyuan[i][-1] += 1

    filtered_zhuyuan = []
    for row in all_zhuyuan:
        # 筛选出只有当天的门诊记录的和无门诊记录的住院记录
        if (row[-3]!=0 and row[-2]==0) or row[-1]==0:
            filtered_zhuyuan.append(row)

    # 生成的结果中，很多重复的行，因为在kc21表中，一个流水号可能有多行，所以需要根据机构编码和流水号去个重
    unique_data = []
    set_uid = set()
    for row in filtered_zhuyuan:
        uid = str(row[0]) + '_' + row[2]
        if uid not in set_uid:
            set_uid.add(uid)
            unique_data.append(row)

    headers = ["机构编码", "机构名称", "流水号", "个人编号", "医疗类别", "入院日期", "出院日期", "入院病种编码", "病种名称", "当天门诊数", "其他天门诊数", "相同诊断门诊数"]
    unique_data = pd.DataFrame(np.array(unique_data), columns=headers)
    print(inst_set)
    print('筛选后的数据涉及医院数 {}'.format(len(inst_set)))

    print('筛选机构后，还剩 {}'.format(len(all_zhuyuan)))
    print('根据门诊筛选后，还剩 {}'.format(unique_data.shape[0]))
    unique_data.to_csv('../data/qunti_zhuyuan/filtered_zhuyuan.csv', index=False, encoding='gbk')


def get_group():
    data = pd.read_csv('../data/qunti_zhuyuan/filtered_zhuyuan.csv', encoding='gbk')
    data = data.sort_values('入院日期').values
    print(data.shape)

    yichang_record_cnt = 0
    all_group = []
    all_yichang_record = []
    i = 0
    # 利用滑动窗口
    while i < data.shape[0]:
        row = data[i]
        group = [row[3]]
        yichang_record = [i]
        check_in = [total_days(row[5])]
        check_out = [total_days(row[6])]
        j = i+1
        # 找到所有的离i行入院、出院时间在3天内，且同家医院的记录
        while j < data.shape[0] and total_days(data[j][5]) - min(check_in) <= 3:
            if data[j][0] == row[0]:
                out_day = total_days(data[j][6])
                if out_day - min(check_out) <=3 and max(check_out) - out_day <= 3:
                    if data[j][3] not in group:
                        group.append(data[j][3])
                    yichang_record.append(j)
                    check_in.append(total_days(data[j][5]))
                    check_out.append(out_day)
                    yichang_record_cnt += 1
            j += 1

        i = j
        # 筛选人数大于等于3的群体
        if len(group)>=3:
            all_group.append(group)
            all_yichang_record.append(yichang_record)

    # 查看各群体之间的交集，如果交集大于等于3则打印出来
    for i in range(len(all_group)):
        set_i = set(all_group[i])
        for j in range(i+1, len(all_group)):
            set_j = set(all_group[j])
            inter = set_i.intersection(set_j)
            if len(inter)>=3:
                print('{}-{}:'.format(i, j), inter)

    # 因为kc21中取出来的前3组，时间集中在2019年之前，kc22和kc60表中没有数据，因此去掉
    all_group = all_group[3:]
    all_yichang_record = all_yichang_record[3:]
    print('number of groups', len(all_group))

    output = []
    for i in range(len(all_group)):
        group = [str(member) for member in all_group[i]]
        yichang_record = [str(record) for record in all_yichang_record[i]]
        output.append([i+1, ' '.join(group), ' '.join(yichang_record)])

    headers = ['组号', '组内人员编号', '异常记录']  # 注意这里的'异常记录'保存的是filtered_zhuyuan_unique.csv按照入院时间排序后的下标
    output = pd.DataFrame(np.array(output), columns=headers)
    output.to_csv('../data/qunti_zhuyuan/group.csv', index=False, encoding='gbk')
    print(output.shape)
    print(yichang_record_cnt)


def get_mingxi():
    """
    获取异常记录的明细表（kc22）
    :return:
    """
    db = connor()
    cursor = db.cursor()

    records = pd.read_csv('../data/qunti_zhuyuan/filtered_zhuyuan.csv', encoding='gbk')
    records = records.sort_values('入院日期').values
    groups = pd.read_csv('../data/qunti_zhuyuan/group.csv', encoding='gbk').values

    # records_dict = dict()
    sqlDomains = None
    output = []
    for i, group in enumerate(groups):
        print(i)
        inds = [int(ind) for ind in group[2].split(' ')]
        for ind in inds:
            # 将机构号+流水号作为key
            # records_dict[str(records[ind][0]) + '_' + records[ind][2]] = row[0]
            cursor.execute("select * from TLF_KC22_2019 where AKB020={} and AKC190='{}'".format(records[ind][0], records[ind][2]))
            rows = cursor.fetchall()
            for row in rows:
                new_row = [group[0]]
                new_row.extend(row)
                output.append(new_row)

            if not sqlDomains:
                sqlDomains = cursor.description

    headers = ['序号', '群体住院组号']
    for domain in sqlDomains:
        headers.append(domain[0])

    output = pd.DataFrame(np.array(output), columns=headers[1:])
    output = output.sort_values(['群体住院组号', 'AKE010']).values
    output = np.concatenate([np.arange(1, output.shape[0]+1, dtype=np.int).reshape((-1, 1)), output], axis=1)
    output = pd.DataFrame(output, columns=headers)
    print(output.shape)
    output.to_csv('../data/qunti_zhuyuan/zhuyuan_mingxi.csv', index=False, encoding='utf-8')  # 用gbk编码会报错


def get_danju():
    """
    获取异常记录的单据表（kc60）
    :return:
    """
    db = connor()
    cursor = db.cursor()

    records = pd.read_csv('../data/qunti_zhuyuan/filtered_zhuyuan.csv', encoding='gbk')
    records = records.sort_values('入院日期').values
    groups = pd.read_csv('../data/qunti_zhuyuan/group.csv', encoding='gbk').values

    # records_dict = dict()
    sqlDomains = None
    output = []
    for i, group in enumerate(groups):
        print(i)
        inds = [int(ind) for ind in group[2].split(' ')]
        for ind in inds:
            # 将机构号+流水号作为key
            # records_dict[str(records[ind][0]) + '_' + records[ind][2]] = row[0]
            cursor.execute("select * from TLF_KC60_2019 where AKB020={} and AKC190='{}'".format(records[ind][0], records[ind][2]))
            rows = cursor.fetchall()
            for row in rows:
                new_row = [group[0]]
                new_row.extend(row)
                output.append(new_row)

            if not sqlDomains:
                sqlDomains = cursor.description

    headers = ['序号', '群体住院组号']
    for domain in sqlDomains:
        headers.append(domain[0])

    output = pd.DataFrame(np.array(output), columns=headers[1:])
    output = output.sort_values(['群体住院组号']).values
    output = np.concatenate([np.arange(1, output.shape[0]+1, dtype=np.int).reshape((-1, 1)), output], axis=1)
    output = pd.DataFrame(output, columns=headers)
    print(output.shape)
    output.to_csv('../data/qunti_zhuyuan/zhuyuan_danju.csv', index=False, encoding='gbk')


def get_person_info():
    db = connor()
    cursor = db.cursor()

    records = pd.read_csv('../data/qunti_zhuyuan/filtered_zhuyuan.csv', encoding='gbk')
    records = records.sort_values('入院日期').values
    groups = pd.read_csv('../data/qunti_zhuyuan/group.csv', encoding='gbk').values

    static_info = dict()
    for row in groups:
        for pid in row[1].split(' '):
            static_info[int(pid)] = []

    # 个人编号, 姓名, 性别, 年龄
    cursor.execute('select AAC001,AAC003,AAC004,BAE450 from TLF_KC21_2019')
    cnt = 0
    while True:
        if cnt % 1000000 == 0:
            print('get rows', cnt)
        cnt += 1000
        rows = cursor.fetchmany(1000)
        if len(rows) == 0:
            break
        for row in rows:
            if row[0] in static_info and len(static_info[row[0]])==0:
                static_info[row[0]] = [row[1], row[3], row[2]]  # 姓名,年龄,性别

    output_ind = 1  # 序号
    output = []
    for row in groups:
        group_output = []
        total_money, tongchou_money = dict(), dict()
        for pid in row[1].split(' '):
            total_money[int(pid)] = 0
            tongchou_money[int(pid)] = 0
            # 性别
            gender = '男' if static_info[int(pid)][2]=='1' else '女'

            # 记录每个人的关联门诊次数
            cnt_guanlian = 0
            for record_ind in row[2].split(' '):
                if int(records[int(record_ind)][3]) == int(pid):
                    cnt_guanlian += records[int(record_ind)][-3]

            # 序号,组号,个人编号,姓名,年龄,性别,风险等级,风险系数,群体住院次数(由前面的情况知，全为1）
            # 关联门诊次数,无门诊住院次数,关联医疗机构数(有前面的情况知，后两者全为1）
            line_data = [output_ind, row[0], int(pid), static_info[int(pid)][0], static_info[int(pid)][1], gender, 1, 1, 1]
            line_data.extend([cnt_guanlian, 1, 1])
            group_output.append(line_data)
            output_ind += 1

        # 医疗总费用, 统筹总金额
        inds = [int(ind) for ind in row[2].split(' ')]
        for ind in inds:
            # print(records[ind])
            cursor.execute(
                "select AKC264,AKE039 from TLF_KC60_2019 where AKB020={} and AKC190='{}'".format(records[ind][0],
                                                                                                 records[ind][2]))
            rows_kc60 = cursor.fetchall()
            for row_kc60 in rows_kc60:
                total_money[int(records[ind][3])] += row_kc60[0]
                tongchou_money[int(records[ind][3])] += row_kc60[1]

        for line_data in group_output:
            line_data.append(total_money[line_data[2]])
            line_data.append(tongchou_money[line_data[2]])
            # print(line_data)
        # input()

        output.extend(group_output)

    headers = ['序号', '群体住院组号', '参保人编号', '姓名', '年龄', '性别', '风险等级', '风险系数', '群体住院次数', '关联门诊次数', '无门诊住院次数', '关联医疗机构数', '医疗总费用', '统筹总金额']
    output = pd.DataFrame(np.array(output), columns=headers)
    print(output.shape)
    output.to_csv('../data/qunti_zhuyuan/zhuyuan_person_info.csv', index=False, encoding='gbk')


def get_duration(date1, date2):
    date1 = str(date1)
    date2 = str(date2)

    day1 = total_days(date1)
    day2 = total_days(date2)

    seconds1 = total_seconds(date1)
    seconds2 = total_seconds(date2)

    if day1==day2 and abs(seconds1-seconds2) > 12*60*60:
        return 1
    else:
        return abs(day1 - day2)


def get_yichang_huizong():
    # db = connor()
    # cursor = db.cursor()

    records = pd.read_csv('../data/qunti_zhuyuan/filtered_zhuyuan.csv', encoding='gbk')
    records = records.sort_values('入院日期').values
    groups = pd.read_csv('../data/qunti_zhuyuan/group.csv', encoding='gbk').values
    person_info = pd.read_csv('../data/qunti_zhuyuan/zhuyuan_person_info.csv', encoding='gbk').values
    mingxi_data = pd.read_csv('../data/qunti_zhuyuan/zhuyuan_mingxi.csv', encoding='utf-8').values

    output_ind = 1  # 序号
    output = []
    for row in groups:
        zuhao = row[0]  # 群体住院组号
        renshu = len(row[1].split(' '))  # 组内人数

        # 从异常人员信息表中获取 关联门诊次数, 无门诊住院次数, 总金额, 总统筹金额
        cnt_guanlian, cnt_wumenzhen, total_money, tongchou_money = 0, 0, 0, 0
        for person_row in person_info:
            if person_row[1] == zuhao:
                cnt_guanlian += person_row[-5]
                cnt_wumenzhen += person_row[-4]
                total_money += person_row[-2]
                tongchou_money += person_row[-1]

        # 从filtered_zhuyuan.csv中获取各组的出入院时间间隔
        record_inds = [int(ind) for ind in row[2].split(' ')]
        group_records = [records[ind] for ind in record_inds]
        in_durations, out_durations = [], []
        for i in range(len(group_records)):
            for j in range(len(group_records)):
                if group_records[i][3] != group_records[j][3]:
                    in_durations.append(get_duration(group_records[i][5], group_records[j][5]))
                    out_durations.append(get_duration(group_records[i][6], group_records[j][6]))
        in_min, in_max, in_mean = min(in_durations), max(in_durations), np.mean(in_durations)
        out_min, out_max, out_mean = min(out_durations), max(out_durations), np.mean(out_durations)

        # 从明细表中计算"住院使用项目相似度", "检验检查占比", "治疗费用占比", "中成药占比"
        item_set = set()
        jiancha_money, zhiliao_money, zhongcheng_money, total_money_mingxi = 0, 0, 0, 0
        group_mingxi = mingxi_data[mingxi_data[:, 1]==zuhao]
        n_item = len(group_mingxi)
        for mingxi_row in group_mingxi:
            item_set.add(mingxi_row[9])  # AKE001在表的第10列
            total_money_mingxi += mingxi_row[19]
            if str(mingxi_row[16]) in ['21', '22', '23', '24', '25', '26']:
                jiancha_money += mingxi_row[19]
            elif str(mingxi_row[16]) == '28':
                zhiliao_money += mingxi_row[19]
            elif str(mingxi_row[16]) == '12':
                zhiliao_money += mingxi_row[19]
        similarity = len(item_set) / n_item
        jiancha_ratio = jiancha_money / total_money_mingxi
        zhiliao_ratio = zhiliao_money / total_money_mingxi
        zhongcheng_ratio = zhongcheng_money / total_money_mingxi

        # 群体住院次数、关联机构数为1，风险系数设为1
        line_data = [output_ind, zuhao, renshu, 1, cnt_guanlian, cnt_wumenzhen, 1, in_min, in_max, in_mean, out_min, out_max, out_mean]
        line_data.extend([similarity, jiancha_ratio, zhiliao_ratio, zhongcheng_ratio, 1, total_money, tongchou_money])
        output_ind += 1
        print(line_data)
        # input()
        output.append(line_data)

    headers = ['序号', '群体住院组号', '组内人数', '群体住院次数', '关联门诊次数', '无门诊住院次数', '群体住院关联医疗机构数', '入院时间间隔最小值', '入院时间间隔最大值', '入院时间间隔平均值', '出院时间间隔最小值', '出院时间间隔最大值', '出院时间间隔平均值', '住院使用项目相似度', '检验检查占比', '治疗费用占比', '中成药费用占比', '风险系数', '医疗总费用', '统筹总金额']
    output = pd.DataFrame(np.array(output), columns=headers)
    print(output.shape)
    output.to_csv('../data/qunti_zhuyuan/zhuyuan_yichang_huizong.csv', index=False, encoding='gbk')


def get_final_output():
    yichang_huizong = pd.read_csv('../data/qunti_zhuyuan/zhuyuan_yichang_huizong.csv', encoding='gbk')
    person = pd.read_csv('../data/qunti_zhuyuan/zhuyuan_person_info.csv', encoding='gbk')
    danju = pd.read_csv('../data/qunti_zhuyuan/zhuyuan_danju.csv', encoding='gbk')
    mingxi = pd.read_csv('../data/qunti_zhuyuan/zhuyuan_mingxi.csv', encoding='utf-8')

    file_url = '../data/qunti_zhuyuan/qunti_zhuyuan_final.xlsx'  # 文件保存地址
    writer = pd.ExcelWriter(file_url)
    yichang_huizong.to_excel(writer, sheet_name='异常结果汇总表', index=False, encoding='utf-8')
    print('概览 done')
    person.to_excel(writer, sheet_name='异常人员信息表', index=False, encoding='utf-8')
    print('人员信息表 done')
    danju.to_excel(writer, sheet_name='异常人员结算单据表', index=False, encoding='utf-8')
    print('单据表 done')
    mingxi.to_excel(writer, sheet_name='异常人员结算明细表', index=False, encoding='utf-8')
    print('明细表 done')
    writer.save()


if __name__ == '__main__':

    # 1.获取筛选后的所有住院记录
    get_filtered_zhuyuan()

    # 2.计算得到所有的异常群体
    get_group()

    # 3.导出群体相关的所有异常结算记录（KC22）
    get_mingxi()

    # 4.导出群体相关的所有结算单据（KC60）
    get_danju()

    # 5.获取人员信息
    get_person_info()

    # 6.统计群体的各项指标，如群体住院次数等
    get_yichang_huizong()

    # 7.结果汇总
    get_final_output()
