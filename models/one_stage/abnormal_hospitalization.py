import pandas as pd
import numpy as np
import datetime
import time
import os
# from sklearn.cluster import KMeans
from tqdm.auto import tqdm

'''
住院异常
情况一：药占比低/检查费占比高/住院时间短/均次费用少【体检】
情况二：机构单一/次数多/年龄大【养老】
情况三：药占比特别高/住院时间短（剔除家庭病床）【药品】
'''

# 路径
pickle_path = '/home/hezhenfeng/medical_insurance/data/dataset/pickles/'
preprocess_path = '/home/hezhenfeng/medical_insurance/data/preprocess/pickles/'
output_path = '/home/hezhenfeng/medical_insurance/data/output/'
# 医院类型
# hospital_types = ['public', 'community', 'private', 'private_other']

AGE_LOW_THRESHOLD, AGE_HIGH_THRESHOLD = 50, 60

prescribed_df = pd.read_pickle(os.path.join(pickle_path, 'prescribed_disease.pkl'))
prescribed_disease_list = list(prescribed_df['AAC001'])

DAYS_THRESHOLD = 182  # 半年


def get_time_interval(in_date, out_date):
    """
    获取两个时间之间的间隔天数
    :param out_date:
    :param in_date:
    :return: days
    """
    try:
        inn = datetime.datetime.strptime(str(in_date)[0:10], '%Y-%m-%d')
        out = datetime.datetime.strptime(str(out_date)[0:10], '%Y-%m-%d')
        return abs(int((out - inn).days)) + 1
    except Exception:
        print("日期转换异常：")
        print('in_date:', in_date, 'out_date', out_date)
        return 10  # 10不具有特异性，返回一般的住院时间


def remove_flag(pid, item_code):
    # 有手术记录，或有规定病种记录
    if item_code.find('f32') >= 0 or item_code.find('f33') >= 0:
        return 1
    elif pid in prescribed_disease_list:
        return 1
    else:
        return 0


def old_remove_flag(item_code):
    code_list = ['f12010090100', 'f3106300100', 'f12040000801']
    if item_code.find('f32') >= 0 or item_code.find('f33') >= 0 or item_code in code_list:
        return 1
    else:
        return 0


def preprocess(hospital_types):
    """
    数据预处理，计算阈值、个人单词住院数据
    :return:DataFrame
    """
    # 阈值文件头
    threshold_header = ['th_one_days', 'th_all_count', 'th_one_liezhi_money', 'th_all_liezhi_money', 'th_one_med_p_low', 'th_one_med_p_high', 'th_one_exp_p', 'th_all_exp_p', 'th_avg_money']
    threshold_array = np.zeros((4, len(threshold_header)), dtype=float)
    thresholds = pd.DataFrame(threshold_array, columns=threshold_header)
    record_head = ['p_id', 'p_name', 'age', 'org_count', 'in_time', 'out_time', 'one_days', 'all_count', 'liezhi_money', 'med_p', 'exp_p', 'org_codes', 'settle_ids', 'type', 'avg_money', 'flag']
    for i, hospital_type in enumerate(hospital_types):
        # 个人单次住院数据汇总
        person_records = []
        hospital_data = pd.read_pickle(os.path.join(pickle_path, hospital_type + '.pkl'))
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
        bar = tqdm(hospital_data.groupby(by='AAC001'))
        for pid, p_records in bar:
            temp_records = []
            # 按个人就医机构进行分组
            time_set = set()
            all_count = all_exp_fee = all_liezhi_money = all_money = 0  # 个人总住院次数, 总检查费，总列支费、总费用（计算总检查费率）
            for org_id, org_records in p_records.groupby(by='AKB020_1'):
                # 按流水号进行分组
                for settle_id, settle_records in org_records.groupby(by='AKC190'):
                    # 得到单次住院记录（有多条明细，累加）
                    line0 = settle_records.iloc[0]
                    one_exp_fee = one_med_fee = 0
                    one_money = abs(float(line0['AKC264']))  # 总费用用来计算药占比
                    one_liezhi_money = one_money - abs(float(line0['AKC253'])) - abs(float(line0['BKE030']))  # 总费用-自费-自理
                    one_days = get_time_interval(line0['BKC192'], line0['BKC194'])
                    time_set.add(str(line0['BKC192']))
                    time_set.add(str(line0['BKC194']))
                    flag = 0
                    for j in range(len(settle_records)):
                        record = settle_records.iloc[j]
                        # 如果发生退款，则此次住院不计
                        if float(record['AKC264']) < 0:
                            one_money = -1
                            break
                        if remove_flag(pid, record['AKE001']) == 1:
                            flag = 1
                        if record['AKE003'] == '1':  # 药品
                            one_med_fee += float(record['AAE019'])
                        if record['AKA063'] in ['21', '22', '23', '24', '25', '26']:  # 检查项目
                            one_exp_fee += float(record['AAE019'])
                    # 单次住院有效
                    if one_money > 0:
                        all_count += 1
                        all_money += one_money
                        all_liezhi_money += one_liezhi_money
                        all_exp_fee += one_exp_fee
                        one_days_list.append(one_days)
                        one_liezhi_money_list.append(one_liezhi_money)
                        one_med_p_list.append(one_med_fee/one_money)
                        one_exp_p_list.append(one_exp_fee/one_money)
                        avg_money_list.append(one_money/one_days)
                        person_record = [pid, line0['AAC003'], int(line0['BAE450']), 1, str(line0['BKC192']), str(line0['BKC194']), one_days, 1, one_liezhi_money, one_med_fee/one_money, one_exp_fee/one_money,
                                         org_id, settle_id, '', one_money/one_days, flag]
                        temp_records.append(person_record)
            # 此人住院有效
            if all_count > 0:
                cnt += 1
                all_count_list.append(all_count)
                all_liezhi_money_list.append(all_liezhi_money)
                all_exp_p_list.append(all_exp_fee/all_money)
                for record in temp_records:
                    if record[-1] == 0:
                        in_time = record[4]
                        out_time = record[5]
                        for temp in time_set:
                            if in_time != temp and temp != out_time:
                                if get_time_interval(in_time, temp) < DAYS_THRESHOLD:
                                    record[-1] = 1
                                    break
                                if get_time_interval(out_time, temp) < DAYS_THRESHOLD:
                                    record[-1] = 1
                                    break
                    person_records.append(record)
        # 该类型医院有住院数据
        if cnt > 0:
            thresholds.loc[i, 'th_one_days'] = np.quantile(one_days_list, 0.05)
            thresholds.loc[i, 'th_all_count'] = np.quantile(all_count_list, 0.99)
            thresholds.loc[i, 'th_one_liezhi_money'] = np.quantile(one_liezhi_money_list, 0.1)
            thresholds.loc[i, 'th_all_liezhi_money'] = np.quantile(all_liezhi_money_list, 0.1)
            thresholds.loc[i, 'th_one_med_p_low'] = np.quantile(one_med_p_list, 0.05)
            temp = np.quantile(one_med_p_list, 0.99)
            thresholds.loc[i, 'th_one_med_p_high'] = temp if temp > 0.9 else 0.9  # 药占比至少90%
            thresholds.loc[i, 'th_one_exp_p'] = np.quantile(one_exp_p_list, 0.97)
            thresholds.loc[i, 'th_all_exp_p'] = np.quantile(all_exp_p_list, 0.95)
            thresholds.loc[i, 'th_avg_money'] = np.quantile(avg_money_list, 0.05)
        df_person_records = pd.DataFrame(person_records, columns=record_head)
        df_person_records.to_pickle(os.path.join(preprocess_path, 'preprocessed_data_' + hospital_type + '.pkl'))
    thresholds.to_pickle(os.path.join(preprocess_path, 'thresholds.pkl'))
    thresholds.to_csv(os.path.join(preprocess_path, 'thresholds.csv'))
    print('thresholds got\npreprocessed data got')


def preprocess_old(hospital_types):
    """
    医养住院人群的数据预处理
    :return: thresholds_old.pkl
    """
    threshold_header = ['th_all_days', 'th_all_count', 'th_all_liezhi_money', 'th_all_med_p', 'th_all_exp_p',
                        'th_high_age', 'th_low_age', 'th_org_count', 'th_avg_money', 'th_one_money', 'th_one_days']
    threshold_array = np.zeros((4, len(threshold_header)), dtype=float)
    thresholds = pd.DataFrame(threshold_array, columns=threshold_header)
    # 个人所有住院数据汇总
    person_records = []
    # flag==1表示包含f32 f33 f12010090100 f3106300100 f12040000801
    record_head = ['p_id', 'p_name', 'age', 'org_count', 'one_days', 'all_count', 'liezhi_money', 'med_p', 'exp_p', 'org_codes', 'settle_ids', 'type', 'avg_money', 'one_money', 'flag']
    for i, hospital_type in enumerate(hospital_types):
        hospital_data = pd.read_pickle(os.path.join(pickle_path, hospital_type + '_old.pkl'))
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
        bar = hospital_data.groupby(by='AAC001')
        for pid, p_records in bar:
            # 按个人就医机构进行分组
            all_days = all_count = all_med_fee = all_exp_fee = all_liezhi_money = all_money = 0
            org_count = set()
            org_list = []
            settle_id_list = []
            name = ''
            age = 0
            temp_records = []
            for org_id, org_records in p_records.groupby(by='AKB020_1'):
                # 按流水号进行分组
                for settle_id, settle_records in org_records.groupby(by='AKC190'):
                    line0 = settle_records.iloc[0]
                    # 得到单次住院记录（有多条明细，累加）
                    one_exp_fee = one_med_fee = 0
                    one_money = abs(float(line0['AKC264']))  # 总费用用来计算药占比
                    one_liezhi_money = one_money - abs(float(line0['AKC253'])) - abs(float(line0['BKE030']))  # 总费用-自费-自理
                    name = line0['AAC003']
                    age = int(line0['BAE450'])
                    one_days = get_time_interval(line0['BKC192'], line0['BKC194'])
                    flag = 0
                    for j in range(len(settle_records)):
                        record = settle_records.iloc[j]
                        # 如果发生退款，则此次住院不计
                        if float(record['AKC264']) < 0:
                            one_money = -1
                            break
                        if record['AKE003'] == '1':  # 药品
                            one_med_fee += float(record['AAE019'])
                        if record['AKA063'] in ['21', '22', '23', '24', '25', '26']:  # 检查项目
                            one_exp_fee += float(record['AAE019'])
                        if old_remove_flag(record['AKE001']) == 1:
                            flag = 1
                    # 本次住院有效
                    if one_money > 0:
                        # 住院有效则保存此次记录
                        org_count.add(org_id)
                        all_count += 1
                        all_money += one_money
                        all_days += one_days
                        all_liezhi_money += one_liezhi_money
                        all_exp_fee += one_exp_fee
                        all_med_fee += one_med_fee
                        org_list.append(org_id)
                        settle_id_list.append(settle_id)
                        avg_money_list.append(one_money/one_days)
                        one_money_list.append(one_money)
                        one_days_list.append(one_days)
                        person_record = [pid, name, age, 1, one_days, 1, one_liezhi_money,
                                         one_med_fee/one_money, one_exp_fee/one_money, org_id, settle_id, '', one_money/one_days, one_money, flag]
                        temp_records.append(person_record)
            # 此人住院有效
            if all_count > 0:
                cnt += 1
                all_count_list.append(all_count)
                all_days_list.append(all_days)
                all_liezhi_money_list.append(all_liezhi_money)
                all_exp_p_list.append(all_exp_fee / all_money)
                all_med_p_list.append(all_med_fee / all_money)
                for record in temp_records:
                    record[5] = all_count  # 更新年住院次数
                    person_records.append(record)
        # 该类型医院有住院数据
        if cnt > 0:
            thresholds.loc[i, 'th_all_days'] = np.quantile(all_days_list, 0.05)
            thresholds.loc[i, 'th_all_count'] = np.quantile(all_count_list, 0.99)  # 需要用到
            thresholds.loc[i, 'th_all_liezhi_money'] = np.quantile(all_liezhi_money_list, 0.05)
            thresholds.loc[i, 'th_all_med_p'] = np.quantile(all_med_p_list, 0.05)
            thresholds.loc[i, 'th_all_exp_p'] = np.quantile(all_exp_p_list, 0.95)
            thresholds.loc[i, 'th_high_age'] = AGE_HIGH_THRESHOLD
            thresholds.loc[i, 'th_low_age'] = AGE_LOW_THRESHOLD
            thresholds.loc[i, 'th_org_count'] = 2
            thresholds.loc[i, 'th_avg_money'] = min(np.quantile(avg_money_list, 0.02), 300)  # 需要用到
            thresholds.loc[i, 'th_one_money'] = np.quantile(one_money_list, 0.02)  # 需要用到
            thresholds.loc[i, 'th_one_days'] = 60  # 需要用到
        df_person_records = pd.DataFrame(person_records, columns=record_head)
        df_person_records.to_pickle(os.path.join(preprocess_path, 'preprocessed_data_' + hospital_type + '_old.pkl'))

    thresholds.to_pickle(os.path.join(preprocess_path, 'thresholds_old.pkl'))
    thresholds.to_csv(os.path.join(preprocess_path, 'thresholds_old.csv'))
    print('thresholds got\npreprocessed data got')


def main():
    print('start')
    result = []  # 结果表集合
    result_detail = []  # 结果明细表集合
    index1 = index2 = 0
    ['p_id', 'p_name', 'age', 'org_count', 'one_days', 'all_count', 'liezhi_money', 'med_p', 'exp_p', 'org_codes',
     'settle_ids', 'type', 'avg_money', 'one_money', 'flag']
    columns_index1 = ['主键', '就诊人', '就诊人名称', '年龄', '住院机构数量', '住院天数', '住院天数阈值', '总住院次数', '总住院次数阈值',
                     '住院日均次费用', '住院日均次费用阈值', '药占比', '药占比阈值', '检查比率', '检查比率阈值', '机构编码逗号分隔',
                     '类别', '住院列支总费用', '住院列支总费用阈值']
    columns_index2 = ['主键', '外键', '机构编码', '就诊流水号']
    # 针对情况1、3
    thresholds = pd.read_pickle(os.path.join(preprocess_path, 'thresholds.pkl'))
    for i, hospital_type in enumerate(['public', 'community', 'private', 'private_other']):
        threshold = thresholds.iloc[i]
        print('医院类型：', hospital_type)
        print('阈值：', threshold)
        person_records = pd.read_pickle(os.path.join(preprocess_path, 'preprocessed_data_' + hospital_type + '.pkl'))
        # 住院时间短、药占比低、检查费占比高，排除半年内有其他住院记录，排除规定病种，手术记录
        examination_type = person_records[(person_records['one_days'] <= threshold['th_one_days']) &
                                          (person_records['med_p'] <= threshold['th_one_med_p_low']) &
                                          (person_records['exp_p'] >= threshold['th_one_exp_p']) &
                                          (person_records['flag'] != 1)]
        examination_type.loc[:, 'type'] = '体检住院'
        # 住院时间相对短、药占比特别高
        medical_type = person_records[(person_records['one_days'] <= threshold['th_one_days']) &
                                      (person_records['med_p'] >= threshold['th_one_med_p_high'])]
        medical_type.loc[:, 'type'] = '药品住院'

        for j in range(len(examination_type)):
            row = examination_type.iloc[j]
            result.append([index1, row['p_id'], row['p_name'], row['age'], row['org_count'], row['one_days'],
                           threshold['th_one_days'], row['all_count'], threshold['th_all_count'], row['avg_money'],
                           threshold['th_avg_money'], row['med_p'], threshold['th_one_med_p_low'],
                           row['exp_p'], threshold['th_one_exp_p'], row['org_codes'], row['type'],  row['liezhi_money'],
                           threshold['th_one_liezhi_money']])
            result_detail.append([index2, index1, row['org_codes'], row['settle_ids']])
            index1 += 1
            index2 += 1
        for j in range(len(medical_type)):
            row = medical_type.iloc[j]
            result.append([index1, row['p_id'], row['p_name'], row['age'], row['org_count'], row['one_days'],
                           threshold['th_one_days'], row['all_count'], threshold['th_all_count'], row['avg_money'],
                           threshold['th_avg_money'], row['med_p'], threshold['th_one_med_p_high'],
                           row['exp_p'], threshold['th_all_exp_p'], row['org_codes'], row['type'], row['liezhi_money'],
                           threshold['th_all_liezhi_money']])
            result_detail.append([index2, index1, row['org_codes'], row['settle_ids']])
            index1 += 1
            index2 += 1

    # 针对医养住院
    thresholds = pd.read_pickle(os.path.join(preprocess_path, 'thresholds_old.pkl'))
    for i, hospital_type in enumerate(['private']):
        threshold = thresholds.iloc[i]
        person_records = pd.read_pickle(os.path.join(preprocess_path, 'preprocessed_data_' + hospital_type + '_old.pkl'))
        # 单次住院总费用（非列支，是总费用）低，且该人年总住院次数多，年龄大(>50)
        # 60岁以上，住院时间60天及以上，日均低，排除含有指定项目的住院
        old_type = person_records[((person_records['one_money'] <= threshold['th_one_money']) & (person_records['all_count'] > threshold['th_all_count']) & (person_records['age'] > threshold['th_low_age']))
        | ((person_records['age'] > threshold['th_high_age']) & (person_records['one_days'] >= threshold['th_one_days']) & (person_records['avg_money'] <= threshold['th_avg_money']) & (person_records['flag'] != 1))]
        old_type.loc[:, 'type'] = '医养住院'
        for j in range(len(old_type)):
            row = old_type.iloc[j]
            org_code = row['org_codes']
            settle_id = row['settle_ids']
            result.append([index1, row['p_id'], row['p_name'], row['age'], row['org_count'], row['one_days'],
                           threshold['th_one_days'], row['all_count'], threshold['th_all_count'],
                           row['avg_money'], threshold['th_avg_money'], row['med_p'],
                           threshold['th_all_med_p'], row['exp_p'], threshold['th_all_exp_p'],
                           org_code, row['type'], row['liezhi_money'], threshold['th_all_liezhi_money']])
            result_detail.append([index2, index1, org_code, settle_id])
            index2 += 1
            index1 += 1
    output = pd.DataFrame(result, columns=columns_index1)
    output.to_excel(os.path.join(output_path, 'DM_WARNING_HOSPITAL.xlsx'), index=False)
    output_detail = pd.DataFrame(result_detail, columns=columns_index2)
    output_detail.to_excel(os.path.join(output_path, 'DM_WARNING_HOSPITAL_DETAIL.xlsx'), index=False)


# def kmeans(n_class):
#     models = KMeans(n_clusters=n_class)
#     # 体检、药品住院
#
#     result = []  # 结果表集合
#     result_detail = []  # 结果明细表集合
#     index1 = index2 = 0
#
#     thresholds = pd.read_pickle(os.path.join(preprocess_path, 'thresholds.pkl'))
#     normal_type1 = normal_type2 = None
#     for i, hospital_type in enumerate(['public',]): #  'community', 'private', 'private_other']):
#         person_records = pd.read_pickle(os.path.join(preprocess_path, 'preprocessed_data_' + hospital_type + '.pkl'))
#         # models.fix(person_records.drop(lables=['p_id', 'p_name', 'org_codes', 'settle_ids', 'type']))
#         threshold = thresholds.iloc[i]
#         normal_type1 = person_records[(person_records['one_days'] > threshold['th_one_days']) |
#                                       (person_records['med_p'] > threshold['th_one_med_p_low']) |
#                                       (person_records['exp_p'] > threshold['th_one_exp_p']) |
#                                       (person_records['flag'] == 1)]
#         for j in range(len(normal_type1.loc[:100])):
#             row = normal_type1.iloc[j]
#             result.append([index1, row['p_id'], row['p_name'], row['age'], row['org_count'], row['one_days'],
#                            threshold['th_one_days'], row['all_count'], threshold['th_all_count'], row['avg_money'],
#                            threshold['th_avg_money'], row['med_p'], threshold['th_one_med_p_low'],
#                            row['exp_p'], threshold['th_one_exp_p'], row['org_codes'], row['type'], row['liezhi_money'],
#                            threshold['th_one_liezhi_money']])
#             result_detail.append([index2, index1, row['org_codes'], row['settle_ids']])
#             index1 += 1
#             index2 += 1
#     # 养老住院
#     thresholds = pd.read_pickle(os.path.join(preprocess_path, 'thresholds_old.pkl'))
#     for i, hospital_type in enumerate(['private']):
#         threshold = thresholds.iloc[i]
#         person_records = pd.read_pickle(os.path.join(preprocess_path, 'preprocessed_data_' + hospital_type + '_old.pkl'))
#
#         normal_type2 = person_records[((person_records['one_money'] > threshold['th_one_money']) |
#                                        (person_records['all_count'] < threshold['th_all_count']))
#                                       |
#                                       ((person_records['one_days'] < threshold['th_one_days']) |
#                                        (person_records['avg_money'] > threshold['th_avg_money']) |
#                                        (person_records['flag'] == 1))]
#         for j in range(len(normal_type2.loc[:100])):
#             row = normal_type2.iloc[j]
#             org_code = row['org_codes']
#             settle_id = row['settle_ids']
#             result.append([index1, row['p_id'], row['p_name'], row['age'], row['org_count'], row['one_days'],
#                            threshold['th_one_days'], row['all_count'], threshold['th_all_count'],
#                            row['avg_money'], threshold['th_avg_money'], row['med_p'],
#                            threshold['th_all_med_p'], row['exp_p'], threshold['th_all_exp_p'],
#                            org_code, row['type'], row['liezhi_money'], threshold['th_all_liezhi_money']])
#             result_detail.append([index2, index1, org_code, settle_id])
#             index2 += 1
#             index1 += 1
#     columns_index1 = ['主键', '就诊人', '就诊人名称', '年龄', '住院机构数量', '住院天数', '住院天数阈值', '总住院次数', '总住院次数阈值',
#                       '住院日均次费用', '住院日均次费用阈值', '药占比', '药占比阈值', '检查比率', '检查比率阈值', '机构编码逗号分隔',
#                       '类别', '住院列支总费用', '住院列支总费用阈值']
#     output = pd.DataFrame(result, columns=columns_index1)
#     output.to_excel(os.path.join(output_path, 'normal.xlsx'), index=False)

if __name__ == '__main__':
    t1 = time.time()
    preprocess(['public', 'community', 'private', 'private_other'])
    preprocess_old(['private'])
    main()
    t2 = time.time()
    print('时间', t2 - t1)
