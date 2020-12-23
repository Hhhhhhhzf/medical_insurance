import pandas as pd
import numpy as np
import datetime
import time
import os
import math
from tqdm.auto import tqdm

'''
住院异常
情况一：药占比低/检查费占比高/排除同一【体检】
情况二：机构单一/次数多/年龄大【养老】
情况三：药占比特别高/住院时间短（剔除家庭病床）【药品】
'''

# 路径
pickle_path = '/home/hezhenfeng/medical_insurance/data/cixi_data/'
preprocess_path = '/home/hezhenfeng/medical_insurance/data/cixi_data/preprocess/pickles'
output_path = '/home/hezhenfeng/medical_insurance/data/cixi_data/output'
# if not os.path.exists(preprocess_path) and not os.path.exists(output_path):
#     os.mkdir(preprocess_path)
#     os.mkdir(output_path)
AGE_LOW_THRESHOLD, AGE_HIGH_THRESHOLD = 50, 60
DAYS_THRESHOLD = 182  # 半年
TEST_THRESHOLD = 50


def get_time_interval(date1, date2):
    """
    获取两个时间之间的间隔天数
    :param date2:
    :param date1:
    :return: days
    """
    try:
        inn = datetime.datetime.strptime(str(date1)[0:10], '%Y-%m-%d')
        out = datetime.datetime.strptime(str(date2)[0:10], '%Y-%m-%d')
        return abs(int((out - inn).days)) + 1
    except Exception:
        print("日期转换异常：")
        print('date1:', date1, 'date2', date2)
        return 10  # 10不具有特异性，返回一般的住院时间


def preprocess(file_name, year):
    """
    数据预处理，计算阈值、个人单次住院数据
    :return:DataFrame
    """
    # 阈值文件头
    threshold_header = ['th_all_count', 'th_one_liezhi_money', 'th_all_liezhi_money', 'th_one_med_p_low',
                        'th_one_med_p_high', 'th_one_exp_p', 'th_all_exp_p', 'th_avg_money', 'th_one_days',
                        'th_one_test_count']
    threshold_array = np.zeros((1, len(threshold_header)), dtype=float)
    thresholds = pd.DataFrame(threshold_array, columns=threshold_header)
    record_head = ['p_id', 'p_name', 'age', 'org_count', 'in_time', 'all_count', 'liezhi_money', 'med_p', 'exp_p',
                   'org_codes', 'settle_ids', 'avg_money', 'one_days', 'max_one_test_count', 'type', 'flag']
    # 个人单次住院数据汇总
    person_records = []
    hospital_data = pd.read_pickle(os.path.join(pickle_path, file_name))
    cnt = 0
    one_days_list = []
    all_count_list = []
    one_liezhi_money_list = []
    all_liezhi_money_list = []
    one_med_p_list = []
    one_exp_p_list = []
    all_exp_p_list = []
    avg_money_list = []
    # 按人进行分组
    for pid, p_records in hospital_data.groupby(by='PC_ID'):
        temp_records = []
        # 按个人就医机构进行分组
        time_set = set()
        all_count = all_exp_fee = all_liezhi_money = all_money = 0  # 个人总住院次数, 总检查费，总列支费、总费用（计算总检查费率）
        year_test_count = dict()  # TODO 待使用
        for org_id, org_records in p_records.groupby(by='ORG_ID'):
            # 按流水号进行分组
            for settle_id, settle_records in org_records.groupby(by='SETTLE_ID'):
                # 得到单次住院记录（有多条明细，累加）
                line0 = settle_records.iloc[0]
                one_exp_fee = one_med_fee = 0
                one_days = math.ceil(float(line0['DAYS']))
                one_money = settle_records['MONEY'].sum()
                one_liezhi_money = settle_records['LIEZHI'].sum()
                time_set.add(str(line0['TIME']))
                invalid_flag = 0
                one_test_count = dict()
                for j in range(len(settle_records)):
                    record = settle_records.iloc[j]
                    catalog_code = record['CATALOG_CODE']
                    if record['ITEM_TYPE'] in ('西药费', '中成药费', '中药饮片费'):  # 药品
                        one_med_fee += float(record['MONEY'])
                    elif record['ITEM_TYPE'] in ('检查费', ):  # 检查项目
                        one_exp_fee += float(record['MONEY'])
                    elif record['ITEM_TYPE'] in ('手术费', ):
                        invalid_flag = 1
                    elif record['ITEM_TYPE'] in ('化验费', ):
                        if one_test_count.get(catalog_code) is not None:
                            one_test_count[catalog_code] += 1
                        else:
                            one_test_count[catalog_code] = 0
                        # if year_test_count.get(catalog_code) is not None:
                        #     year_test_count[catalog_code] += 1
                        # else:
                        #     year_test_count[catalog_code] = 0
                max_one_test_count = 0
                if len(one_test_count) > 0:
                    max_one_test_count = max(one_test_count.values())
                # 单次住院有效
                all_count += 1
                all_money += one_money
                all_liezhi_money += one_liezhi_money
                all_exp_fee += one_exp_fee
                # 添加到链表
                one_liezhi_money_list.append(one_liezhi_money)
                one_med_p_list.append(one_med_fee/one_money)
                one_exp_p_list.append(one_exp_fee/one_money)
                one_days_list.append(one_days)
                avg_money_list.append(one_money/one_days)
                person_record = [pid, line0['NAME'], int(line0['AGE']), 1, str(line0['TIME']), 1, one_liezhi_money,
                                 one_med_fee/one_money, one_exp_fee/one_money, org_id, settle_id, one_money/one_days,
                                 one_days, max_one_test_count, '', invalid_flag]
                temp_records.append(person_record)
        all_count_list.append(all_count)
        all_liezhi_money_list.append(all_liezhi_money)
        all_exp_p_list.append(all_exp_fee/all_money)
        for record in temp_records:
            record[5] = all_count
            if record[-1] == 0:
                in_time = record[4]
                for temp in time_set:
                    if in_time != temp and get_time_interval(in_time, temp) > DAYS_THRESHOLD:
                        record[-1] = 1
                        break
        person_records += temp_records
    thresholds.loc[0, 'th_all_count'] = np.quantile(all_count_list, 0.99)
    thresholds.loc[0, 'th_one_liezhi_money'] = np.quantile(one_liezhi_money_list, 0.1)
    thresholds.loc[0, 'th_all_liezhi_money'] = np.quantile(all_liezhi_money_list, 0.1)
    thresholds.loc[0, 'th_one_med_p_low'] = np.quantile(one_med_p_list, 0.05)
    thresholds.loc[0, 'th_one_med_p_high'] = max(np.quantile(one_med_p_list, 0.99), 0.9)  # 药占比至少90%
    thresholds.loc[0, 'th_one_exp_p'] = max(np.quantile(one_exp_p_list, 0.99), 0.8)  # 检查占比至少80%
    thresholds.loc[0, 'th_all_exp_p'] = max(np.quantile(all_exp_p_list, 0.99), 0.8)  # 检查占比至少80%
    thresholds.loc[0, 'th_one_days'] = np.quantile(one_days_list, 0.05)
    thresholds.loc[0, 'th_avg_money'] = min(np.quantile(avg_money_list, 0.02), 300)
    thresholds.loc[0, 'th_one_test_count'] = TEST_THRESHOLD
    # 保存表格
    df_person_records = pd.DataFrame(person_records, columns=record_head)
    df_person_records.to_pickle(os.path.join(preprocess_path, 'preprocessed_data_all_%s.pkl' % year))
    thresholds.to_pickle(os.path.join(preprocess_path, 'thresholds_%s.pkl' % year))
    thresholds.to_csv(os.path.join(preprocess_path, 'thresholds_%s.csv' % year))
    print('thresholds got\npreprocessed data got')


def preprocess_old(file_name, year):
    """
    医养住院人群的数据预处理
    :return: thresholds_old.pkl
    """
    threshold_header = ['th_all_count', 'th_all_liezhi_money', 'th_all_med_p', 'th_all_exp_p',
                        'th_high_age', 'th_low_age', 'th_one_money', 'th_one_days', 'th_avg_money', 'th_one_test_count']
    threshold_array = np.zeros((1, len(threshold_header)), dtype=float)
    thresholds = pd.DataFrame(threshold_array, columns=threshold_header)
    # 个人所有住院数据汇总
    person_records = []
    record_head = ['p_id', 'p_name', 'age', 'org_count', 'all_count', 'liezhi_money', 'med_p', 'exp_p', 'org_codes',
                   'settle_ids', 'type', 'one_money', 'avg_money', 'one_days', 'max_one_test_count', 'flag']
    hospital_data = pd.read_pickle(os.path.join(pickle_path, file_name))
    cnt = 0
    all_days_list = []
    all_count_list = []
    all_liezhi_money_list = []
    all_med_p_list = []
    all_exp_p_list = []
    avg_money_list = []
    one_money_list = []
    one_days_list = []
    # 按人进行分组
    for pid, p_records in hospital_data.groupby(by='PC_ID'):
        # 按个人就医机构进行分组
        all_days = all_count = all_med_fee = all_exp_fee = all_liezhi_money = all_money = 0
        org_count = set()
        org_list = []
        settle_id_list = []
        name = ''
        age = 0
        temp_records = []
        for org_id, org_records in p_records.groupby(by='ORG_ID'):
            # 按流水号进行分组
            for settle_id, settle_records in org_records.groupby(by='SETTLE_ID'):
                line0 = settle_records.iloc[0]
                # 得到单次住院记录（有多条明细，累加）
                one_exp_fee = one_med_fee = 0
                one_days = math.ceil(float(line0['DAYS']))
                one_money = settle_records['MONEY'].sum()
                one_liezhi_money = settle_records['LIEZHI'].sum()
                invalid_flag = 0
                one_test_count = dict()
                for j in range(len(settle_records)):
                    record = settle_records.iloc[j]
                    catalog_code = record['CATALOG_CODE']
                    if record['ITEM_TYPE'] in ('西药费', '中成药费', '中药饮片费'):  # 药品
                        one_med_fee += float(record['MONEY'])
                    elif record['ITEM_TYPE'] in ('检查费',):  # 检查项目
                        one_exp_fee += float(record['MONEY'])
                    elif record['ITEM_TYPE'] in ('化验费', ):
                        if one_test_count.get(catalog_code) is not None:
                            one_test_count[catalog_code] += 1
                        else:
                            one_test_count[catalog_code] = 0
                max_one_test_count = 0
                if len(one_test_count) > 0:
                    max_one_test_count = max(one_test_count.values())
                # 住院有效则保存此次记录
                org_count.add(org_id)
                all_count += 1
                all_money += one_money
                all_liezhi_money += one_liezhi_money
                all_exp_fee += one_exp_fee
                all_med_fee += one_med_fee
                org_list.append(org_id)
                settle_id_list.append(settle_id)
                one_money_list.append(one_money)
                one_days_list.append(one_days)
                avg_money_list.append(one_money/one_days)
                person_record = [pid, line0['NAME'], int(line0['AGE']), 1, 1, one_liezhi_money,
                                 one_med_fee/one_money, one_exp_fee/one_money, org_id, settle_id, '', one_money,
                                 one_money/one_days, one_days, max_one_test_count, invalid_flag]
                temp_records.append(person_record)
        cnt += 1
        all_count_list.append(all_count)
        all_liezhi_money_list.append(all_liezhi_money)
        all_exp_p_list.append(all_exp_fee / all_money)
        all_med_p_list.append(all_med_fee / all_money)
        for record in temp_records:
            record[4] = all_count  # 更新年住院次数
        person_records += temp_records
    # 阈值计算
    thresholds.loc[0, 'th_all_count'] = np.quantile(all_count_list, 0.99)
    thresholds.loc[0, 'th_all_liezhi_money'] = np.quantile(all_liezhi_money_list, 0.1)
    thresholds.loc[0, 'th_all_med_p'] = max(np.quantile(all_med_p_list, 0.05), 0.9)  # 药占比至少90%
    thresholds.loc[0, 'th_all_exp_p'] = max(np.quantile(all_exp_p_list, 0.99), 0.8)  # 检查占比至少80%
    thresholds.loc[0, 'th_high_age'] = AGE_HIGH_THRESHOLD
    thresholds.loc[0, 'th_low_age'] = AGE_LOW_THRESHOLD
    thresholds.loc[0, 'th_one_money'] = np.quantile(one_money_list, 0.05)
    thresholds.loc[0, 'th_one_days'] = max(np.quantile(one_days_list, 0.99), 60)
    thresholds.loc[0, 'th_avg_money'] = min(np.quantile(avg_money_list, 0.02), 300)
    thresholds.loc[0, 'th_one_test_count'] = TEST_THRESHOLD
    df_person_records = pd.DataFrame(person_records, columns=record_head)
    df_person_records.to_pickle(os.path.join(preprocess_path, 'preprocessed_data_old_%s.pkl' % year))

    thresholds.to_pickle(os.path.join(preprocess_path, 'thresholds_old_%s.pkl' % year))
    thresholds.to_csv(os.path.join(preprocess_path, 'thresholds_old_%s.csv' % year))
    print('thresholds got\npreprocessed data got')


def filter_test_and_drug(examination_type, medical_type, th_abnormal_count):
    """
    排除单人年异常次数少于的的人
    :param examination_type: 体检异常初始数据
    :param medical_type: 药品异常初始数据
    :param th_abnormal_count: 体检药品异常可容忍违规次数的阈值
    :return:
    """
    abnormal_count = dict()
    for j in range(len(examination_type)):
        row = examination_type.iloc[j]
        pid = row['p_id']
        if abnormal_count.get(pid) is None:
            abnormal_count[pid] = 0
        abnormal_count[pid] += 1
    for j in range(len(medical_type)):
        row = medical_type.iloc[j]
        pid = row['p_id']
        if abnormal_count.get(pid) is None:
            abnormal_count[pid] = 0
        abnormal_count[pid] += 1
    abnormal_count_copy = abnormal_count.copy()
    for k, v in abnormal_count_copy.items():
        if v < th_abnormal_count:  # 删除小于阈值的key，
            abnormal_count.pop(k)
    examination_type = examination_type.loc[(examination_type['p_id'].isin(abnormal_count))]
    medical_type = medical_type.loc[(medical_type['p_id'].isin(abnormal_count))]
    return examination_type, medical_type


def main(year):
    print('start')
    result = []  # 结果表集合
    result_detail = []  # 结果明细表集合
    index1 = index2 = 0
    th_abnormal_count = 3  # 体检药品异常可容忍违规次数的阈值
    ['p_id', 'p_name', 'age', 'org_count', 'in_time', 'all_count', 'liezhi_money', 'med_p', 'exp_p',
     'org_codes', 'settle_ids', 'avg_money', 'one_days', 'max_one_test_count', 'type', 'flag']

    ['p_id', 'p_name', 'age', 'org_count', 'all_count', 'liezhi_money', 'med_p', 'exp_p', 'org_codes',
     'settle_ids', 'type', 'one_money', 'avg_money', 'one_days', 'max_one_test_count', 'flag']
    columns_index1 = ['主键', '就诊人', '就诊人名称', '年龄', '住院机构数量', '住院次数', '住院次数阈值', '药占比', '药占比阈值', '检查比率', '检查比率阈值',
                      '机构编码逗号分隔', '类别', '住院列支总费用', '住院列支总费用阈值', '住院时长', '住院时长阈值',
                      '日平均费用', '日平均费用阈值', '最大单项化验次数', '最大单项化验次数阈值']
    columns_index2 = ['主键', '外键', '机构编码', '就诊流水号']
    # 针对情况1、3
    thresholds = pd.read_pickle(os.path.join(preprocess_path, 'thresholds_%s.pkl' % year))
    for i, hospital_type in enumerate(['all']):
        threshold = thresholds.iloc[i]
        print('医院类型：', hospital_type)
        print('阈值：', threshold)
        person_records = pd.read_pickle(os.path.join(preprocess_path, 'preprocessed_data_all_%s.pkl' % year))
        # --------------------体检住院：住院时间短、药占比低、检查费占比高，排除半年内有其他住院记录，排除规定病种，手术记录--------------------
        examination_type = person_records[(person_records['one_days'] <= threshold['th_one_days']) &
                                          (person_records['med_p'] <= threshold['th_one_med_p_low']) &
                                          (person_records['exp_p'] >= threshold['th_one_exp_p']) &
                                          (person_records['flag'] != 1)]
        examination_type.loc[:, 'type'] = '体检住院'

        # --------------------药品住院：住院时间短、药占比特别高--------------------
        medical_type = person_records[(person_records['one_days'] <= threshold['th_one_days']) &
                                      (person_records['med_p'] >= threshold['th_one_med_p_high'])]
        medical_type.loc[:, 'type'] = '药品住院'
        # 排除单人年异常次数少于3的的人
        examination_type, medical_type = filter_test_and_drug(examination_type, medical_type, th_abnormal_count)
        # 输出
        for j in range(len(examination_type)):
            row = examination_type.iloc[j]
            result.append([index1, row['p_id'], row['p_name'], row['age'], row['org_count'], row['all_count'], threshold['th_all_count'],
                           row['med_p'], threshold['th_one_med_p_low'], row['exp_p'], threshold['th_one_exp_p'],
                           row['org_codes'], row['type'],  row['liezhi_money'], threshold['th_one_liezhi_money'],
                           row['one_days'], threshold['th_one_days'], row['avg_money'], threshold['th_avg_money'],
                           row['max_one_test_count'], threshold['th_one_test_count']])
            result_detail.append([index2, index1, row['org_codes'], row['settle_ids']])
            index1 += 1
            index2 += 1
        for j in range(len(medical_type)):
            row = medical_type.iloc[j]
            result.append([index1, row['p_id'], row['p_name'], row['age'], row['org_count'], row['all_count'],
                           threshold['th_all_count'], row['med_p'], threshold['th_one_med_p_high'],
                           row['exp_p'], threshold['th_all_exp_p'], row['org_codes'], row['type'], row['liezhi_money'],
                           threshold['th_all_liezhi_money'],
                           row['one_days'], threshold['th_one_days'], row['avg_money'], threshold['th_avg_money'],
                           row['max_one_test_count'], threshold['th_one_test_count']])
            result_detail.append([index2, index1, row['org_codes'], row['settle_ids']])
            index1 += 1
            index2 += 1

        # --------------------------过度或虚记:同一次住院中，某种化验次数过多---------------------------
        over_test_type = person_records[(person_records['max_one_test_count'] >= threshold['th_one_test_count'])]
        over_test_type.loc[:, 'type'] = '过度/需记化验'
        for j in range(len(over_test_type)):
            row = over_test_type.iloc[j]
            result.append([index1, row['p_id'], row['p_name'], row['age'], row['org_count'], row['all_count'],
                           threshold['th_all_count'], row['med_p'], threshold['th_one_med_p_high'],
                           row['exp_p'], threshold['th_all_exp_p'], row['org_codes'], row['type'], row['liezhi_money'],
                           threshold['th_all_liezhi_money'],
                           row['one_days'], threshold['th_one_days'], row['avg_money'], threshold['th_avg_money'],
                           row['max_one_test_count'], threshold['th_one_test_count']])
            result_detail.append([index2, index1, row['org_codes'], row['settle_ids']])
            index1 += 1
            index2 += 1

    # 针对医养住院
    thresholds = pd.read_pickle(os.path.join(preprocess_path, 'thresholds_old_%s.pkl' % year))
    for i, hospital_type in enumerate(['old']):
        threshold = thresholds.iloc[i]
        person_records = pd.read_pickle(os.path.join(preprocess_path, 'preprocessed_data_old_%s.pkl' % year))
        # --------------------高频住院：单次住院总费用（非列支，是总费用）低，且该人年总住院次数多，年龄大(>50)---------------
        old_type_frequent = person_records[(person_records['one_money'] <= threshold['th_one_money']) &
                                           (person_records['all_count'] > threshold['th_all_count']) &
                                           (person_records['age'] > threshold['th_low_age'])]
        old_type_frequent.loc[:, 'type'] = '高频住院'
        for j in range(len(old_type_frequent)):
            row = old_type_frequent.iloc[j]
            org_code = row['org_codes']
            settle_id = row['settle_ids']
            result.append([index1, row['p_id'], row['p_name'], row['age'], row['org_count'], row['all_count'],
                           threshold['th_all_count'], row['med_p'], threshold['th_all_med_p'], row['exp_p'],
                           threshold['th_all_exp_p'], org_code, row['type'], row['liezhi_money'], threshold['th_all_liezhi_money'],
                           row['one_days'], threshold['th_one_days'], row['avg_money'], threshold['th_avg_money'],
                           row['max_one_test_count'], threshold['th_one_test_count']])
            result_detail.append([index2, index1, org_code, settle_id])
            index2 += 1
            index1 += 1

        # --------------------医养住院：60岁以上，住院时间60天及以上，日均低，排除含有指定项目的住院--------------
        old_type = person_records[(person_records['avg_money'] <= threshold['th_avg_money']) &
                                           (person_records['one_days'] >= threshold['th_one_days']) &
                                           (person_records['age'] > threshold['th_high_age'])]
        old_type.loc[:, 'type'] = '医养住院'
        for j in range(len(old_type)):
            row = old_type.iloc[j]
            org_code = row['org_codes']
            settle_id = row['settle_ids']
            result.append([index1, row['p_id'], row['p_name'], row['age'], row['org_count'], row['all_count'],
                           threshold['th_all_count'], row['med_p'], threshold['th_all_med_p'], row['exp_p'],
                           threshold['th_all_exp_p'], org_code, row['type'], row['liezhi_money'],
                           threshold['th_all_liezhi_money'],
                           row['one_days'], threshold['th_one_days'], row['avg_money'], threshold['th_avg_money'],
                           row['max_one_test_count'], threshold['th_one_test_count']])
            result_detail.append([index2, index1, org_code, settle_id])
            index2 += 1
            index1 += 1
    output = pd.DataFrame(result, columns=columns_index1)
    output.to_excel(os.path.join(output_path, 'DM_WARNING_HOSPITAL_%s.xlsx' % year), index=False)
    output_detail = pd.DataFrame(result_detail, columns=columns_index2)
    output_detail.to_excel(os.path.join(output_path, 'DM_WARNING_HOSPITAL_DETAIL_%s.xlsx' % year), index=False)
    print('main finished')
    return output_detail


def detail_export(raw_data, output_detail):
    """
    导出明细数据
    :return:
    """
    output = []
    for i in tqdm(range(len(output_detail))):
        row = output_detail.iloc[i]
        f_id = row['外键']
        org_id, settle_id = row['机构编码'], row['就诊流水号']
        details_data = raw_data[(raw_data['SETTLE_ID'] == settle_id)]
        details_data.loc[:, '外键'] = f_id
        details_data = details_data.loc[:, ['外键'] + list(details_data.columns[:-1])]
        for j in range(len(details_data)):
            list_row = list(details_data.iloc[j])
            output.append(list_row)
    output = pd.DataFrame(output, columns=list(details_data.columns))
    print('ready export')
    write2excel(output, output_path, 'ZHUYUAN_DETAIL.xlsx')


def write2excel(data, path, file):
    data.to_excel(os.path.join(path, file), index=False)


if __name__ == '__main__':
    t1 = time.time()
    YEAR = '2020'
    file_name1 = 'all_data_%s.pkl' % YEAR
    preprocess(file_name1, YEAR)

    file_name2 = 'old_data_%s.pkl' % YEAR
    preprocess_old(file_name2, YEAR)
    output_detail = main(YEAR)
    hospital_data = pd.read_pickle(os.path.join(pickle_path, file_name1))
    detail_export(hospital_data, output_detail)
    t2 = time.time()
    print('时间', t2 - t1)
