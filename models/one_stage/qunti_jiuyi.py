import cx_Oracle
import numpy as np
import os
import shutil
import pandas as pd
from itertools import combinations


os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'


def connor():
    connect=cx_Oracle.connect('me/mypassword@192.168.0.241:1521/helowin')    #连接数据库
    #c=connect.cursor()
    if connect:
        print("连接Oracle成功!")
    return connect


def get_summary_2020():
    db = connor()
    cursor = db.cursor()

    cnt = 0
    print('数据获取中...')
    cursor.execute('select AKB020_1, AKB021, AKC190, AAC001, AAC003, AAC004, BAE450, AKA130, BKC197, AKC021, AKC196, BKF050, AKC273, AKC002 from TLF_KC21')

    data = []
    while True:
        # if cnt==50000:
        #     break
        if cnt % 1000000 == 0:
            print(cnt)
        cnt += 10000
        rows = cursor.fetchmany(10000)
        if len(rows) == 0:
            break
        for row in rows:
            # print(row)
            # input()
            if row[-1][:4]!='2020':
                continue

            if '药房' in row[1] or '药店' in row[1]:
                data.append(row)

    header = ['机构编号', '机构名称', '流水号', '病人id', '姓名', '性别', '年龄', '医疗类别', '报销标志', '医疗人员类别', '疾病编码', '医生编码', '医生姓名', '结算时间']
    data = pd.DataFrame(np.array(data), columns=header)
    print(data.shape)
    data.to_csv('../data/qunti_jiuyi_2020/summary_2020.csv', index=False, encoding='utf-8')


def total_seconds(date_str):
    date_str = str(date_str)
    date_str = '-'.join([date_str[:4], date_str[4:6], date_str[6:8]]) + " " + ':'.join(
        [date_str[8:10], date_str[10:12], date_str[12:14]])
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
    return cnt_second


def get_yichang_group():
    """
    计算得到各个异常群体，导出群体的基本信息，如组内人员编号
    :return:
    """
    yaodian_data = pd.read_csv('../data/qunti_jiuyi_2020/summary_2020.csv', encoding='utf-8')

    yaodian_values = yaodian_data.sort_values(['机构编号', '结算时间']).values

    print(yaodian_values.shape)
    input()

    i = 0
    all_fenzu = {}
    yaodian_name = {}
    fenzu_times = {}

    times_thresh = 5
    duration_thresh = 5 # minute

    while i<len(yaodian_values):
        yaodian_id = yaodian_values[i][0]
        yaodian_name[yaodian_id] = yaodian_values[i][1]
        patients_ind = {}
        ind_patients = {}
        j = i
        n_patient = 0
        while j<len(yaodian_values) and yaodian_values[j][0]==yaodian_id:
            if yaodian_values[j][3] not in patients_ind:
                patients_ind[yaodian_values[j][3]] = n_patient
                ind_patients[n_patient] = yaodian_values[j][3]
                n_patient += 1
            j += 1

        # patients_ind = list(patients_ind)
        # relation = np.zeros((n_patient, n_patient), dtype=np.int)
        print('i: {}, j: {}, 人数: {}'.format(i, j, n_patient))

        if n_patient>50000:
            print('too many people, we need to skip!')
            input()
            i = j
            continue

        relation = np.eye(n_patient, dtype=np.int) * times_thresh
        fa = list(range(n_patient))

        def findFa(v):
            if v==fa[v]:
                return v
            else:
                f = findFa(fa[v])
                fa[v] = f
                return f

        def Union(a, b):
            faA = findFa(a)
            faB = findFa(b)
            if faA!=faB:
                fa[faA] = faB

        k = i
        while k<j:
            inds = set()
            l = k
            cur_time = total_seconds(yaodian_values[k][-1])
            while l < j and total_seconds(yaodian_values[l][-1]) - cur_time <= duration_thresh*60:
                inds.add(patients_ind[yaodian_values[l][3]])
                l += 1

            for ind in inds:
                for ind2 in inds:
                    if ind!=ind2:
                        relation[ind, ind2] += 1
                        if relation[ind, ind2]==times_thresh and relation[ind2, ind]==times_thresh:
                            Union(ind, ind2)

            k = l

        ori_relation = relation.copy()
        ori_relation -= np.eye(n_patient, dtype=np.int) * times_thresh

        relation[relation < times_thresh] = 0
        relation[relation >= times_thresh] = 1

        n_adj = np.sum(relation, axis=1)

        groups = [[] for i1 in range(n_patient)]
        for i1 in range(len(fa)):
            # groups[fa[i1]].append(i1)
            if n_adj[i1]>=3:
                groups[findFa(i1)].append(i1)

        all_fenzu[yaodian_id] = []
        fenzu_times[yaodian_id] = []
        group_cnt = 0
        for group in groups:
            if len(group)==3:
                group_cnt += 1
                all_fenzu[yaodian_id].append([ind_patients[ind] for ind in group])
                fenzu_relation = ori_relation[np.ix_(group, group)]
                # print(fenzu_relation)
                times = np.sum(fenzu_relation) / (len(fenzu_relation)*(len(fenzu_relation)-1))
                # print(times)
                # input()
                fenzu_times[yaodian_id].append(int(times))
            if len(group)>3:
                k = 3
                max_group = []
                while True:
                    group_k = [ind for ind in group if n_adj[ind]>=k]
                    if len(group_k)<k:
                        break
                    all_combin = list(combinations(group_k, k))
                    # print('group_k {}, k {}, n combin {}'.format(len(group_k), k, len(all_combin)))
                    if len(all_combin)>1000000:
                        break
                    for ind, combin in enumerate(all_combin):
                        sub_relation = relation[np.ix_(combin, combin)]
                        if (sub_relation==np.ones((k, k), dtype=np.int)).all():
                            max_group = combin
                            break
                    if len(max_group)<k:
                        break
                    k += 1
                if len(max_group)>=3:
                    group_cnt += 1
                    all_fenzu[yaodian_id].append([ind_patients[ind] for ind in max_group])
                    fenzu_relation = ori_relation[np.ix_(max_group, max_group)]
                    times = np.sum(fenzu_relation) / (len(fenzu_relation) * (len(fenzu_relation) - 1))
                    fenzu_times[yaodian_id].append(int(times))

        i = j
        print('药店: {}, 组数: {}'.format(yaodian_id, group_cnt))
        print('')
        # input()

    print(all_fenzu)

    output = []
    for key in all_fenzu:
        for ind, yizu in enumerate(all_fenzu[key]):
            output.append([key, yaodian_name[key], ' '.join([str(pid) for pid in yizu]), len(yizu), fenzu_times[key][ind]])
    output = np.array(output)
    print(output.shape)
    output = pd.DataFrame(output)
    output.to_csv('../data/qunti_jiuyi_2020/group_2020_{}minute.csv'.format(duration_thresh), index=False, encoding='gbk')


def get_group_detail():
    """
    获取各分组的交易明细数据(kc22)
    :return:
    """
    db = connor()
    cursor = db.cursor()
    fenzu = pd.read_csv('../data/qunti_jiuyi_2020/group_2020_5minute.csv', encoding='gbk').values

    fenzu_data = [[] for i in range(len(fenzu))]
    patient_group = dict()
    for ind, fenzu_row in enumerate(fenzu):
        pids = [int(pid) for pid in fenzu_row[2].split(' ')]
        inst_code = fenzu_row[0]
        for pid in pids:
            if pid not in patient_group:
                patient_group[pid] = dict()
            patient_group[pid][inst_code] = ind

    summary_data = pd.read_csv('../data/qunti_jiuyi_2020/summary_2020.csv', encoding='utf-8').values
    print(summary_data.shape)

    kc21_part = dict()
    kc22_part = dict()
    for row in summary_data:
        if row[3] in patient_group and row[0] in patient_group[row[3]]:
            key = str(row[0]) + '_' + row[2]  # 用机构编号+流水号作为 key
            kc21_part[key] = [row[3], row[2], row[0], row[-4], row[-1]]  # 个人编号 流水号 机构编码 疾病编码 结算时间

    print(len(kc21_part))

    print('获取数据...')
    # 机构编号 流水号 项目编码 处方号 结算时间
    cursor.execute('select AKB020_1,AKC190,AKE001,AKC220,AKC002 FROM TLF_KC22 where substr(tlf_kc22.akc002,1,4)=2020')

    cnt = 0
    while True:
        if cnt % 1000000==0:
            print('row: {}'.format(cnt))
        cnt += 10000
        rows = cursor.fetchmany(10000)
        if len(rows) == 0:
            break
        for row in rows:
            key = str(row[0]) + '_' + str(row[1])
            if key in kc21_part:
                if key not in kc22_part:
                    kc22_part[key] = []
                kc22_part[key].append(row[2:4])

    for key in kc22_part:
        for item in kc22_part[key]:
            row = kc21_part[key].copy()
            row.extend(item)
            fenzu_data[patient_group[row[0]][row[2]]].append(row)

    for ind, data in enumerate(fenzu_data):
        print(ind, len(data))
        if len(data) == 0:
            continue
        data = np.stack(data, axis=0)
        # print(data.shape)
        # input()
        header = ['个人编号', '流水号', '机构编码', '疾病编码', '结算时间', '项目编码', '处方号']
        data = pd.DataFrame(data, columns=header)
        data.to_csv('../data/qunti_jiuyi_2020/group_details_5minute/group_{}.csv'.format(ind), index=False, encoding='gbk')


def get_group_statistic():
    """
    统计各群组的指标，如处方相似度、疾病数量等
    :return:
    """
    summary_data = pd.read_csv('../data/qunti_jiuyi_2020/group_2020_5minute.csv', encoding='gbk').values
    new_data = []
    for ind, row in enumerate(summary_data):
        group_data = pd.read_csv('../data/qunti_jiuyi_2020/group_details_5minute/group_{}.csv'.format(ind), encoding='gbk').values
        yaopin_dict = dict()
        chufang_set = set()
        jibing_set = set()
        n_share = 0
        for group_row in group_data:
            if group_row[-2] in yaopin_dict:
                yaopin_dict[group_row[-2]] += 1
                if yaopin_dict[group_row[-2]] == 2:
                    n_share += 1
            else:
                yaopin_dict[group_row[-2]] = 1

            chufang_set.add(group_row[-1])
            jibing_set.add(group_row[3])
        row = list(row)
        row.extend(['5 minutes', n_share/len(yaopin_dict), len(yaopin_dict), len(chufang_set), len(jibing_set), 1])
        new_data.append(row)
    new_data = np.stack(new_data, axis=0)
    header = ['机构编码', '机构名称', '群体个人编码', '组内人数', '群体就医次数', '就诊时间集中度', '处方相似度', '处方药品种类数', '处方数量', '处方疾病诊断数', '风险程度']
    new_data = pd.DataFrame(new_data, columns=header)
    new_data.to_csv('../data/qunti_jiuyi_2020/2019_5minute_statistic.csv', index=False, encoding='gbk')


def get_kc22():
    """
    导出异常群体涉及到的所有明细记录
    :return: 
    """
    db = connor()
    cursor = db.cursor()
    fenzu = pd.read_csv('../data/qunti_jiuyi_2020/group_2020_5minute.csv', encoding='gbk').values

    fenzu_data = [[] for i in range(len(fenzu))]
    patient_group = dict()
    for ind, fenzu_row in enumerate(fenzu):
        pids = [int(pid) for pid in fenzu_row[2].split(' ')]
        inst_code = fenzu_row[0]
        for pid in pids:
            if pid not in patient_group:
                patient_group[pid] = dict()
            patient_group[pid][inst_code] = ind

    summary_data = pd.read_csv('../data/qunti_jiuyi_2020/summary_2020.csv', encoding='utf-8').values
    print(summary_data.shape)

    kc21_part = dict()
    kc22_part = dict()
    for row in summary_data:
        if row[3] in patient_group and row[0] in patient_group[row[3]]:
            key = str(row[0]) + '_' + row[2]  # 用机构编号+流水号作为 key
            kc21_part[key] = [row[3], row[2], row[0], row[-4], row[-1]]  # 个人编号 流水号 机构编码 疾病编码 结算时间

    print(len(kc21_part))

    print('获取数据...')
    # 机构编号 流水号 ...
    cursor.execute('select * FROM TLF_KC22 where substr(tlf_kc22.akc002,1,4)=2020')

    cnt = 0
    while True:
        if cnt % 1000000 == 0:
            print('row: {}'.format(cnt))
        cnt += 10000
        rows = cursor.fetchmany(10000)
        if len(rows) == 0:
            break
        for row in rows:
            key = str(row[0]) + '_' + str(row[1])
            if key in kc21_part:
                if key not in kc22_part:
                    kc22_part[key] = []
                kc22_part[key].append(row)

    for key in kc22_part:
        for item in kc22_part[key]:
            row = kc21_part[key]
            fenzu_data[patient_group[row[0]][row[2]]].append(item)

    for ind, data in enumerate(fenzu_data):
        try:
            if len(data) > 0:
                data = np.stack(data, axis=0)
                print(data.shape)
                header = [str(i) for i in range(data.shape[1])]
                data = pd.DataFrame(data, columns=header)
                data = data.sort_values('11')
                try:
                    data.to_csv('../data/qunti_jiuyi_2020/2020_5minute_kc22/group_{}.csv'.format(ind), index=False,
                                encoding='utf-8')
                except:
                    data.to_csv('../data/qunti_jiuyi_2020/2020_5minute_kc22/group_{}.csv'.format(ind), index=False,
                                encoding='gbk')
                    print('convert to gbk...')
            else:
                print('no data')
        except:
            print('{}, wrong!')


def get_kc60():
    """
    导出异常群体涉及到的所有单据（kc60）
    :return:
    """
    db = connor()
    cursor = db.cursor()
    fenzu = pd.read_csv('../data/qunti_jiuyi_2020/group_2020_5minute.csv', encoding='gbk').values

    fenzu_data = [[] for i in range(len(fenzu))]
    patient_group = dict()
    for ind, fenzu_row in enumerate(fenzu):
        pids = [int(pid) for pid in fenzu_row[2].split(' ')]
        inst_code = fenzu_row[0]
        for pid in pids:
            if pid not in patient_group:
                patient_group[pid] = dict()
            patient_group[pid][inst_code] = ind

    summary_data = pd.read_csv('../data/qunti_jiuyi_2020/summary_2020.csv', encoding='utf-8').values
    print(summary_data.shape)

    kc21_part = dict()
    kc60_part = dict()
    for row in summary_data:
        if row[3] in patient_group and row[0] in patient_group[row[3]]:
            key = str(row[0]) + '_' + row[2]  # 用机构编号+流水号作为 key
            kc21_part[key] = [row[3], row[2], row[0], row[5], row[6]]  # 个人编号 流水号 机构编码 性别 年龄

    print(len(kc21_part))

    print('获取数据...')
    # 机构编号 流水号 ...
    cursor.execute('select * FROM TLF_KC60')

    cnt = 0
    while True:
        if cnt % 1000000 == 0:
            print('row: {}'.format(cnt))
        cnt += 10000
        rows = cursor.fetchmany(10000)
        if len(rows) == 0:
            break
        for row in rows:
            key = str(row[1]) + '_' + str(row[0])
            if key in kc21_part:
                if key not in kc60_part:
                    kc60_part[key] = []
                kc60_part[key].append(row)

    for key in kc60_part:
        for item in kc60_part[key]:
            row = kc21_part[key].copy()
            row.extend(item)
            fenzu_data[patient_group[row[0]][row[2]]].append(row[3:])

    for ind, data in enumerate(fenzu_data):
        try:
            if len(data) > 0:
                data = np.stack(data, axis=0)
                print(data.shape)
                header = [str(i) for i in range(data.shape[1])]
                data = pd.DataFrame(data, columns=header)
                data = data.sort_values('94')
                data.to_csv('../data/qunti_jiuyi_2020/2020_5minute_kc60/group_{}.csv'.format(ind), index=False,
                            encoding='gbk')
            else:
                print('no data')
        except:
            print('{}, wrong!')


def huizong():
    """
    将异常群体概览表、单据表(kc60)、明细表(kc22)导出
    :return:
    """
    group_info = pd.read_csv('../data/qunti_jiuyi_2020/2019_5minute_statistic.csv', encoding='gbk').values

    ind = np.array(list(range(1, len(group_info) + 1))).reshape((-1, 1))
    group_id = ind.copy()

    group_info = np.concatenate([ind, group_id, group_info[:, 3:]], axis=1)
    print(group_info[:10])
    print(group_info.shape)

    sheet1 = pd.DataFrame(group_info)

    kc60_data = []
    for i in range(len(group_info)):
        # path = './data/2019_{}minute_kc60_full/group_{}.csv'.format(duration, i)
        path = '../data/qunti_jiuyi_2020/2020_5minute_kc60/group_{}.csv'.format(i)
        one_data = pd.read_csv(path, encoding='gbk').values
        g_id = np.array([i + 1] * len(one_data)).reshape((-1, 1))
        one_data = np.concatenate([g_id, one_data], axis=1)
        kc60_data.append(one_data)
    kc60_data = np.concatenate(kc60_data, axis=0)
    print(kc60_data[:5])
    print(kc60_data.shape)

    sheet2 = pd.DataFrame(kc60_data)

    kc22_data = []
    for i in range(len(group_info)):
        # path = './data/2019_{}minute_kc22/group_{}.csv'.format(duration, i)
        path = '../data/qunti_jiuyi_2020/2020_5minute_kc22/group_{}.csv'.format(i)
        try:
            one_data = pd.read_csv(path, encoding='utf-8').values
        except:
            one_data = pd.read_csv(path, encoding='gbk').values
            print('use gbk')
        g_id = np.array([i + 1] * len(one_data)).reshape((-1, 1))
        one_data = np.concatenate([g_id, one_data], axis=1)
        kc22_data.append(one_data)
    kc22_data = np.concatenate(kc22_data, axis=0)
    # print(kc21_data[:5])
    # print(kc21_data.shape)

    sheet3 = pd.DataFrame(kc22_data)

    file_url = '../data/qunti_jiuyi_2020/2020_qunti_final_5minute.xlsx'  # 文件保存地址
    writer = pd.ExcelWriter(file_url)
    sheet1.to_excel(writer, sheet_name='群体就医分组方案概览', index=False, encoding='utf-8')
    print('概览 done')
    sheet2.to_excel(writer, sheet_name='异常人员结算单据表', index=False, encoding='utf-8')
    print('kc60 done')
    sheet3.to_excel(writer, sheet_name='异常人员结算明细表', index=False, encoding='utf-8')
    print('kc22 done')
    writer.save()


def shangxian():
    """
    将结果按照数据库格式建成概览表和明细表，用于上线
    :return:
    """
    # 导出概览表用于上线
    summary_data = pd.read_csv('../data/qunti_jiuyi_2020/summary_2020.csv', encoding='utf-8').values
    gailan_data = pd.read_excel('../data/qunti_jiuyi_2020/2020_qunti_final_5minute.xlsx',
                                sheet_name='群体就医分组方案概览').values
    danju_data = pd.read_excel('../data/qunti_jiuyi_2020/2020_qunti_final_5minute.xlsx', sheet_name='异常人员结算单据表')
    print(summary_data.shape)

    liushui2id = dict()
    id2name = dict()

    for row in summary_data:
        liushui2id[str(row[2])] = str(row[3])
        id2name[str(row[3])] = str(row[4])

    fenzu_data = pd.read_csv('../data/qunti_jiuyi_2020/group_2020_5minute.csv', encoding='gbk').values
    print(fenzu_data.shape)

    group_id = danju_data['组号']
    liezhi_money_record = danju_data['AKC264'] - danju_data['AKC253'] - danju_data['BKE030']
    liezhi_money = dict()
    for i in range(len(group_id)):
        if int(group_id[i]) not in liezhi_money:
            liezhi_money[group_id[i]] = 0
        liezhi_money[group_id[i]] += liezhi_money_record[i]

    output_data1 = []
    for ind, row in enumerate(gailan_data):
        output_row = [row[1], row[2], row[3], row[5], row[6], row[7], row[8], row[9], fenzu_data[ind][0]]
        people_id = fenzu_data[ind][2].replace(' ', ',')
        people_name = ','.join([id2name[pid] for pid in people_id.split(',')])
        output_row.extend([people_id, people_name, liezhi_money[ind+1]])
        output_data1.append(output_row)

    output_data1 = pd.DataFrame(output_data1, columns=['主键', '组内人数', '群体就医次数', '处方相似度', '处方药品种类数', '处方数量', '处方疾病诊断数', '风险程度', '机构编码逗号分隔', '人员编码逗号分隔', '人员名称逗号分隔', '异常列支总费用'])
    print(output_data1.shape)
    output_data1.to_excel('../data/qunti_jiuyi_2020/群体就医.xlsx', encoding='gbk', index=False)


    # 导出明细表用于上线
    liushui = danju_data['AKC190']
    inst_code = danju_data['AKB020_1']
    group_id = danju_data['组号']

    output = dict()

    for i in range(liushui.shape[0]):
        output[liushui[i]] = [group_id[i], inst_code[i]]

    output_data2 = []

    item_id = 1
    for code in output:
        try:
            output_data2.append([output[code][0], output[code][1], code, item_id, liushui2id[str(code)], id2name[str(liushui2id[code])]])
            item_id += 1
        except:
            print(code)

    output_data2 = pd.DataFrame(output_data2, columns=['外键', '机构编码', '就诊流水号', '主键', '人员编码', '人员名称'])
    print(output_data2.shape)
    output_data2.to_excel('../data/qunti_jiuyi_2020/群体就医2.xlsx', encoding='gbk', index=False)


if __name__ == '__main__':
    # 建议下面各步，一步一步执行

    # # 第一步,获取交易的摘要数据(KC21)
    # get_summary_2020()
    #
    # # 第二步,计算得出异常群体
    # get_yichang_group()
    #
    # # 第三步，获得各个群体的具体交易记录（KC22）
    # get_group_detail()
    #
    # # 第四步，根据各群体的具体交易记录统计其各项指标，如处方相似度等
    # get_group_statistic()
    #
    # # 第五步，导出群体相关的所有异常结算记录（KC22）
    # get_kc22()
    #
    # # 第六步，导出群体相关的所有结算单据（KC60）
    # get_kc60()
    #
    # # 第七步，将结果汇总成一个excel表格
    # huizong()

    # 第八步，将excel表格重新组织成表，用于导入数据库上线
    shangxian()

