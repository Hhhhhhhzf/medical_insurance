import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.abspath(os.path.dirname(curPath) + os.path.sep + ".")
sys.path.append(rootPath)

import pandas as pd
import numpy as np
import datetime
import time
import os
import multiprocessing

'''
住院异常
情况一：药占比低/检查费占比高/住院时间短/均次费用少
情况二：机构单一/次数多/年龄大
情况三：药占比特别高/住院时间短（剔除家庭病床）
'''

root_path = '/home/hezhenfeng/medical_insurance/data/dataset/pickles/'
output_path = '/home/hezhenfeng/medical_insurance/data/output/'
# 数据源
data_sources = ['public.pkl', 'community.pkl', 'private.pkl', 'private_other.pkl']
hospital_type = ['public', 'community', 'private', 'private_other']


def getTimeInterval(in_date, out_date):
    '''
    获取两个时间之间的间隔天数
    :param in_time:
    :param out_time:
    :return: days
    '''
    time_interval = []
    for i in range(len(in_date)):
        try:
            inn = datetime.datetime.strptime(str(in_date[i])[0:10], '%Y-%m-%d')
            out = datetime.datetime.strptime(str(out_date[i])[0:10], '%Y-%m-%d')
            time_interval.append(int((out - inn).days) + 1)
        except Exception:
            print("日期转换异常：", str(Exception), "\n----")
            print('in_date:', in_date, 'out_date', out_date)
            continue
    if len(time_interval) == 0:
        return 10  # 随便的值
    else:
        return np.mean(time_interval)


def get_parameters(key, group):
    """
    获取个人的各个异常判断参数
    :param group:
    :return:
    """
    settle_id = [str(idx) for idx in group['AKC190']]  # 交易流水
    age = list(group['BAE450'])[0]
    name = list(group['AAC003'])[0]
    item_type = list(group['AKE003'])  # 项目类别
    charge_type = list(group['AKA063'])  # 收费类别
    item_fee = [float(idx) for idx in group['AAE019']]
    org_code = tuple(set(group['AKB020_1']))

    # 入院日期和出院日期，对于每个住院，入院日期和出院日期都是一样的
    in_date = list(group['BKC192'].groupby(group['AKC190']).min())
    out_date = list(group['BKC194'].groupby(group['AKC190']).max())
    time_interval = getTimeInterval(in_date, out_date)  # 平均出入院时间间隔

    med_fee = 0.0  # 药品费
    exam_fee = 0.0  # 检查费
    total_cost = 0.0  # 总费用
    for i in range(len(settle_id)):
        if item_type[i] == '1':  # 药品
            med_fee += item_fee[i]
        if charge_type[i] in ['21', '22', '23', '24', '25', '26']:  # 检查项目
            exam_fee += item_fee[i]
        total_cost += item_fee[i]
    in_times = len(set(settle_id))  # 流水号数
    # 多次累加，精度丢失，total_cost 可能不是真实0
    if total_cost < 0.2:
        total_cost = 0
    avg_cost = total_cost / in_times  # 均次住院费用
    med_fee_p = 0.0  # 药占比
    exam_fee_p = 0.0  # 检查费占比
    if total_cost != 0:
        med_fee_p = med_fee / total_cost
        exam_fee_p = exam_fee / total_cost

    parameters = [key, name, med_fee_p, exam_fee_p, in_times, time_interval, avg_cost, age, org_code]
    return parameters


def get_thresholds():
    """
    获取不同类型的医院的相对阈值
    0，1，2，3行分别是公立、社区、民营、民营其他
    列分别为：药占比低阈值、检查费占比阈值、住院次数阈值、平均住院时间阈值、平均住院费用、药占比高阈值、年龄阈值、机构数阈值
    :return:DataFrame
    """
    try:
        thresholds = pd.read_pickle(os.path.join(root_path, 'threshold.pkl'))
        thresholds.to_csv(os.path.join(root_path, 'threshold.csv'))
        print('threshold got')
        return thresholds
    except Exception:
        print('无阈值文件。')
    header = ['th_med_p_low', 'th_exam_p', 'th_cnt', 'th_time', 'th_avg_cost', 'th_med_p_high', 'th_age', 'th_org']
    threshold_array = np.zeros((4, 8), dtype=float)
    for i in range(4):
        threshold_array[i, -2] = 60  # 年龄阈值60
        threshold_array[i, -1] = 2  # 机构数阈值2
    for i, data_source in enumerate(data_sources):
        df = pd.read_pickle(os.path.join(root_path, data_source))
        cnt = 0
        arrays = []
        for key, group in df.groupby(by='AAC001'):
            parameters = get_parameters(key, group)
            arrays.append([parameters[2], parameters[3], parameters[4], parameters[5], parameters[6], parameters[2]])
            cnt += 1
        up_index = 0.95
        down_index = 0.05
        if cnt > 0:
            arrays = np.array(arrays)
            for j in range(arrays.shape[1]):
                # up = np.quantile(arrays[:, j], 0.75)
                # down = np.quantile(arrays[:, j], 0.25)
                if j in (0, 3, 4):  # th_med_p_low 'th_time', 'th_avg_cost'
                    # threshold_array[i, j] = down - (up - down) * index
                    threshold_array[i, j] = np.quantile(arrays[:, j], down_index)
                elif j in (1, 2, 5):  # 'th_exam_p'  'th_cnt'  'th_med_p_high'
                    # threshold_array[i, j] = up + (up - down) * index
                    threshold_array[i, j] = np.quantile(arrays[:, j], up_index)
    thresholds = pd.DataFrame(threshold_array, columns=header)
    thresholds.to_pickle(os.path.join(root_path, 'threshold.pkl'))
    thresholds.to_csv(os.path.join(root_path, 'threshold.csv'))
    print('threshold got')
    return thresholds


def computation(key, group, threshold):
    parameters = get_parameters(key, group)
    name = parameters[1]
    med_fee_p = parameters[2]
    exam_fee_p = parameters[3]
    in_times = parameters[4]
    time_interval = parameters[5]
    avg_cost = parameters[6]
    age = parameters[7]
    org_code = parameters[8]

    info1 = [key, name, '', in_times, avg_cost, med_fee_p, ','.join(org_code), threshold['th_cnt'],
             threshold['th_avg_cost'], threshold['th_med_p_low'], '', exam_fee_p]
    info2 = info1.copy()
    info3 = info1.copy()
    if med_fee_p <= threshold['th_med_p_low'] and exam_fee_p >= threshold['th_exam_p'] \
            and time_interval <= threshold['th_time'] and avg_cost <= threshold['th_avg_cost']:
        info1[-2] = '体检住院'
    else:
        info1 = None
    if len(org_code) <= threshold['th_org'] and in_times > threshold['th_cnt'] and age >= threshold['th_age']:
        print('机构数：', len(org_code), '机构详情：', org_code, '阈值：', threshold['th_org'])
        info2[-2] = '医养住院'
    else:
        info2 = None
    if med_fee_p >= threshold['th_med_p_high'] and time_interval <= threshold['th_time']:
        info3[-2] = '药品住院'
    else:
        info3 = None
    return info1, info2, info3


def computation_bak(key, group, threshold):
    settle_id = [str(idx) for idx in group['AKC190']]  # 交易流水
    name = list(group['AAC003'])[0]
    age = list(group['BAE450'])[0]
    item_type = list(group['AKE003'])  # 项目类别
    charge_type = list(group['AKA063'])  # 收费类别
    item_fee = [float(idx) for idx in group['AAE019']]

    # 入院日期和出院日期，对于每个住院，入院日期和出院日期都是一样的
    in_date = list(group['BKC192'].groupby(group['AKC190']).min())
    out_date = list(group['BKC194'].groupby(group['AKC190']).max())

    # 住院机构编号
    org_code = tuple(set(group['AKB020_1']))
    total_cost = sum(list(group['AKC264'].groupby(group['AKC190']).mean()))

    time_interval = getTimeInterval(in_date, out_date)  # 平均出入院时间间隔
    med_fee = 0.0  # 药品费
    exam_fee = 0.0  # 检查费
    for i in range(len(settle_id)):
        if item_type[i] == '1':  # 药品
            med_fee += item_fee[i]
        if charge_type[i] in ['21', '22', '23', '24', '25', '26']:  # 检查项目
            exam_fee += item_fee[i]
    in_times = len(set(settle_id))  # 流水号数

    avg_cost = total_cost / in_times  # 均次住院费用
    med_fee_p = 0.0  # 药占比
    exam_fee_p = 0.0  # 检查费占比
    if total_cost != 0:
        med_fee_p = med_fee / total_cost
        exam_fee_p = exam_fee / total_cost

    info1 = [key, name, '', in_times, total_cost, med_fee_p, ','.join(org_code), threshold['th_cnt'], threshold['th_avg_cost'],
                             threshold['med_p_low'], '', exam_fee_p]
    info2 = info1.copy()
    info3 = info1.copy()

    if med_fee_p <= threshold['med_p_low'] and exam_fee_p >= threshold['exam_p'] and time_interval <= threshold['th_time']\
            and avg_cost <= threshold['th_avg_cost']:
        info1[-2] = '体检住院'
    else:
        info1 = None
    if len(org_code) <= threshold['th_org'] and in_times >= threshold['th_cnt'] and age >= threshold['th_age']:
        info2[-2] = '医养住院'
    else:
        info2 = None
    if med_fee_p >= threshold['med_p_high'] and time_interval <= threshold['th_time']:
        info3[-2] = '药品住院'
    else:
        info3 = None
    return info1, info2, info3


def analyse(df, threshold, cnt):
    '''
    analyse info of abnormal in hospital
    :param cnt:
    :param threshold:
    :param df:
    :return:
    '''
    all_info = []
    detail_info = []
    pool = multiprocessing.Pool(10)  # 多进程
    for key, group in df.groupby(by='AAC001'):
        infos = pool.apply_async(computation, (key, group, threshold)).get()
        for info in infos:
            if info is not None:
                all_info.append(info)
                org_settle = {}
                for i in range(len(group)):
                    row = group.iloc[i]
                    if row['AKB020_1'] not in org_settle:
                        org_settle[row['AKB020_1']] = {}
                    if row['AKC190'] not in org_settle[row['AKB020_1']]:
                        org_settle[row['AKB020_1']][row['AKC190']] = 1
                        detail_info.append([cnt+len(all_info)-1, row['AKB020_1'], row['AKC190']])  # 外键、机构编码、就诊流水号

    columns_index = ['就诊人', '就诊人名称', '规律住院间隔天数', '住院次数', '均次住院费用', '药占比', '机构编码逗号分隔',
                     '正常住院次数阈值', '正常住院均次费用阈值', '正常药占比低阈值', '类别', '检查比率']
    columns_index2 = ['外键', '机构编码', '就诊流水号']
    df_info = pd.DataFrame(all_info, columns=columns_index)
    detail_info = pd.DataFrame(detail_info, columns=columns_index2)
    return df_info, detail_info


def main():
    print('start')
    dfs = []
    dfs_detail = []  # 结果表集合，结果明细表集合
    # 按机构类型分别进行计算
    thresholds = get_thresholds()
    for i, data_source in enumerate(data_sources):
        df = pd.read_pickle(os.path.join(root_path, data_source))
        print("数据量：", len(df))
        df_info, df_info_detail = analyse(df, thresholds.iloc[i], len(dfs))
        dfs.append(df_info)
        dfs_detail.append(df_info_detail)

    df_info = pd.concat(dfs).reset_index(drop=True)
    df_info_detail = pd.concat(dfs_detail).reset_index(drop=True)
    df_info.to_csv(os.path.join(output_path, 'DM_WARNING_HOSPITAL.csv'), index=True, encoding='utf-8-sig')
    df_info_detail.to_csv(os.path.join(output_path, 'DM_WARNING_HOSPITAL_DETAIL.csv'), index=True, encoding='utf-8-sig')


if __name__ == '__main__':
    t1 = time.time()
    main()
    # get_thresholds()
    t2 = time.time()
    print('时间', t2 - t1)
