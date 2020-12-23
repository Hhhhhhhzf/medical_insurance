
import pandas as pd
import numpy as np
import datetime
import time
import os
import multiprocessing

'''
住院异常
情况一：药占比低/检查费占比高/住院时间短/均次费用少【体检】
情况二：机构单一/次数多/年龄大【养老】
情况三：药占比特别高/住院时间短（剔除家庭病床）【药品】
'''

pickle_path = '/home/hezhenfeng/medical_insurance/data/dataset/pickles/'
preprocess_path = '/home/hezhenfeng/medical_insurance/data/preprocess/pickles/'
output_path = '/home/hezhenfeng/medical_insurance/data/output/'
# 数据源
hospital_types = ['public', 'community', 'private', 'private_other']


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
        return int((out - inn).days) + 1
    except Exception:
        print("日期转换异常：")
        print('in_date:', in_date, 'out_date', out_date)
        return 10  # 10不具有特异性，返回一般的住院时间


def get_thresholds():
    """
    获取不同类型的医院的相对阈值
    0，1，2，3行分别是公立、社区、民营、民营其他
    列分别为：单次住院天数阈值、总住院次数阈值、单次住院异常列支费用阈值，总住院列支费用阈值、单次药占比低阈值、单次药占比高阈值、单次检查比率阈值、总检查比率阈值
    :return:DataFrame
    """
    try:
        thresholds = pd.read_pickle(os.path.join(pickle_path, 'thresholds.pkl'))
        thresholds.to_csv(os.path.join(pickle_path, 'thresholds.csv'))
        print('threshold got')
        return thresholds
    except Exception:
        print('无阈值文件。')
    header = ['th_one_days', 'th_all_count', 'th_one_liezhi_money', 'th_all_liezhi_money', 'th_one_med_p_low', 'th_one_med_p_high', 'th_one_exp_p', 'th_all_exp_p']
    threshold_array = np.zeros((4, 8), dtype=float)
    thresholds = pd.DataFrame(threshold_array, columns=header)
    hospital_parameters = [{}, {}, {}, {}]
    for i, hospital_type in enumerate(hospital_types):
        hospital_data = pd.read_pickle(os.path.join(pickle_path, hospital_type + '.pkl'))
        cnt = 0
        one_days_list = []
        all_count_list = []
        one_liezhi_money_list = []
        all_liezhi_money_list = []
        one_med_p_list = []
        one_exp_p_list = []
        all_exp_p_list = []
        # 按人进行分组
        for pid, p_records in hospital_data.groupby(by='AAC001'):
            hospital_parameters[pid] = {}
            # 按个人就医机构进行分组
            all_count = all_exp_fee = all_liezhi_money = all_money = 0  # 个人总住院次数, 总检查费，总列支费、总费用（计算总检查费率）
            for org_id, org_records in p_records.groupby(by='AKB020_1'):
                # 按流水号进行分组
                for settle_id, settle_records in org_records.groupby(by='AKC190'):
                    line0 = settle_records.iloc[0]
                    # 得到单次住院记录（有多条明细，累加）
                    # 天数、次数
                    one_exp_fee = one_med_fee = 0
                    one_money = abs(float(line0['AKC264']))  # 总费用用来计算药占比
                    one_liezhi_money = one_money - abs(float(line0['AKC253'])) - abs(float(line0['BKE030']))# 总费用-自费-自理
                    one_days = get_time_interval(line0['BKC192'], line0['BKC194'])
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
                    if one_money > 0:
                        all_count += 1
                        all_money += one_money
                        all_liezhi_money += one_liezhi_money
                        all_exp_fee += one_exp_fee
                        one_days_list.append(one_days)
                        one_liezhi_money_list.append(one_liezhi_money)
                        one_med_p_list.append(one_med_fee/one_money)
                        one_exp_p_list.append(one_exp_fee/one_money)
                        hospital_parameters[pid][org_id+'_'+settle_id] = [one_days, one_liezhi_money, one_med_fee/one_money, one_exp_fee/one_money]

            if all_money > 0:
                cnt += 1
                all_count_list.append(all_count)
                all_liezhi_money_list.append(all_liezhi_money)
                all_exp_p_list.append(all_exp_fee/all_money)

        if cnt > 0:
            thresholds.loc[i, 'th_one_days'] = np.quantile(one_days_list, 0.05)
            thresholds.loc[i, 'th_all_count'] = np.quantile(all_count_list, 0.99)
            thresholds.loc[i, 'th_one_liezhi_money'] = np.quantile(one_liezhi_money_list, 0.1)
            thresholds.loc[i, 'th_all_liezhi_money'] = np.quantile(all_liezhi_money_list, 0.1)
            thresholds.loc[i, 'th_one_med_p_low'] = np.quantile(one_med_p_list, 0.05)
            thresholds.loc[i, 'th_one_med_p_high'] = np.quantile(one_med_p_list, 0.95)
            thresholds.loc[i, 'th_one_exp_p'] = np.quantile(one_exp_p_list, 0.95)
            thresholds.loc[i, 'th_all_exp_p'] = np.quantile(all_exp_p_list, 0.95)

    thresholds.to_pickle(os.path.join(pickle_path, 'thresholds.pkl'))
    thresholds.to_csv(os.path.join(pickle_path, 'thresholds.csv'))
    print('thresholds got')
    return hospital_parameters


def get_thresholds_60():
    """
    60岁以上人群的阈值获取
    :return: thresholds_60.pkl
    """
    try:
        thresholds = pd.read_pickle(os.path.join(pickle_path, 'thresholds_60.pkl'))
        print('thresholds got')
    except Exception:
        print('无阈值文件，开始创建文件')
        header = ['th_all_days', 'th_all_count', 'th_all_liezhi_money', 'th_all_med_p', 'th_all_exp_p', 'th_age', 'th_org_count']
        threshold_array = np.zeros((4, 7), dtype=float)
        for i in range(4):
            threshold_array[i, -1] = 2
            threshold_array[i, -2] = 60
        thresholds = pd.DataFrame(threshold_array, columns=header)
    hospital_parameters = [{}, {}, {}, {}]
    personal_org_settle = [{}, {}, {}, {}]
    for i, hospital_type in enumerate(hospital_types):
        hospital_data = pd.read_pickle(os.path.join(pickle_path, hospital_type + '.pkl'))
        cnt = 0
        all_days_list = []
        all_count_list = []
        all_liezhi_money_list = []
        all_med_p_list = []
        all_exp_p_list = []
        # 按人进行分组
        for pid, p_records in hospital_data.groupby(by='AAC001'):
            # 字典记录个人参数
            hospital_parameters[i][pid] = {}
            # 列表记录就诊的医院
            personal_org_settle[i][pid] = []
            # 按个人就医机构进行分组
            all_days = all_count = all_med_fee = all_exp_fee = all_liezhi_money = all_money = 0
            org_count = set()
            for org_id, org_records in p_records.groupby(by='AKB020_1'):
                # 按流水号进行分组
                org_count.add(org_id)
                for settle_id, settle_records in org_records.groupby(by='AKC190'):
                    line0 = settle_records.iloc[0]
                    # 得到单次住院记录（有多条明细，累加）
                    one_exp_fee = one_med_fee = 0
                    one_money = abs(float(line0['AKC264']))  # 总费用用来计算药占比
                    one_liezhi_money = one_money - abs(float(line0['AKC253'])) - abs(float(line0['BKE030']))  # 总费用-自费-自理
                    one_days = get_time_interval(line0['BKC192'], line0['BKC194'])
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
                    # 本次住院有效
                    if one_money > 0:
                        # 住院有效则保存此次记录
                        personal_org_settle[i][pid].append(org_id + '_' + settle_id)
                        all_count += 1
                        all_money += one_money
                        all_days += one_days
                        all_liezhi_money += one_liezhi_money
                        all_exp_fee += one_exp_fee
                        all_med_fee += one_med_fee
            # 此人住院有效
            if all_count > 0:
                cnt += 1
                all_count_list.append(all_count)
                all_days_list.append(all_days)
                all_liezhi_money_list.append(all_liezhi_money)
                all_exp_p_list.append(all_exp_fee / all_money)
                all_med_p_list.append(all_med_fee / all_money)
                hospital_parameters[i][pid]['org_count'] = len(org_count)
                hospital_parameters[i][pid]['all_count'] = all_count
        # 该类型医院有住院数据
        if cnt > 0:
            thresholds.loc[i, 'th_all_days'] = np.quantile(all_days_list, 0.05)
            thresholds.loc[i, 'th_all_count'] = np.quantile(all_count_list, 0.99)
            thresholds.loc[i, 'th_all_liezhi_money'] = np.quantile(all_liezhi_money_list, 0.1)
            thresholds.loc[i, 'th_all_med_p'] = np.quantile(all_med_p_list, 0.95)
            thresholds.loc[i, 'th_all_exp_p'] = np.quantile(all_exp_p_list, 0.95)

    thresholds.to_pickle(os.path.join(pickle_path, 'thresholds_60.pkl'))
    thresholds.to_csv(os.path.join(pickle_path, 'thresholds_60.csv'))
    print('thresholds got')
    return hospital_parameters, personal_org_settle



def preprocess_60():
    """
    60岁以上人群的数据预处理
    :return: thresholds_60.pkl
    """
    threshold_header = ['th_all_days', 'th_all_count', 'th_all_liezhi_money', 'th_all_med_p', 'th_all_exp_p', 'th_age', 'th_org_count']
    threshold_array = np.zeros((4, 7), dtype=float)
    thresholds = pd.DataFrame(threshold_array, columns=threshold_header)
    # 个人所有住院数据汇总
    person_records = []
    record_head = ['p_id', 'p_name', 'age', 'org_count', 'all_days', 'all_count', 'liezhi_money', 'med_p', 'exp_p', 'org_codes', 'settle_ids', 'type']
    for i, hospital_type in enumerate(hospital_types):
        hospital_data = pd.read_pickle(os.path.join(pickle_path, hospital_type + '_60.pkl'))
        cnt = 0
        all_days_list = []
        all_count_list = []
        all_liezhi_money_list = []
        all_med_p_list = []
        all_exp_p_list = []
        # 按人进行分组
        for pid, p_records in hospital_data.groupby(by='AAC001'):
            # 按个人就医机构进行分组
            all_days = all_count = all_med_fee = all_exp_fee = all_liezhi_money = all_money = 0
            org_count = set()
            org_list = []
            settle_id_list = []
            name = ''
            age = 0
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
            # 此人住院有效
            if all_count > 0:
                cnt += 1
                all_count_list.append(all_count)
                all_days_list.append(all_days)
                all_liezhi_money_list.append(all_liezhi_money)
                all_exp_p_list.append(all_exp_fee / all_money)
                all_med_p_list.append(all_med_fee / all_money)
                person_record = [pid, name, age, len(org_count), all_days, all_count, all_liezhi_money,
                                 all_med_fee/all_money, all_exp_fee/all_money, org_list, settle_id_list, '']
                person_records.append(person_record)
        # 该类型医院有住院数据
        if cnt > 0:
            thresholds.loc[i, 'th_all_days'] = np.quantile(all_days_list, 0.05)
            thresholds.loc[i, 'th_all_count'] = np.quantile(all_count_list, 0.99)
            thresholds.loc[i, 'th_all_liezhi_money'] = np.quantile(all_liezhi_money_list, 0.1)
            thresholds.loc[i, 'th_all_med_p'] = np.quantile(all_med_p_list, 0.05)
            thresholds.loc[i, 'th_all_exp_p'] = np.quantile(all_exp_p_list, 0.95)
            thresholds.loc[i, 'th_age'] = 60
            thresholds.loc[i, 'th_org_count'] = 2
        df_person_records = pd.DataFrame(person_records, columns=record_head)
        df_person_records.to_pickle(os.path.join(preprocess_path, 'preprocessed_data_' + hospital_type + '_60.pkl'))

    thresholds.to_pickle(os.path.join(preprocess_path, 'thresholds_60.pkl'))
    thresholds.to_csv(os.path.join(preprocess_path, 'thresholds_60.csv'))
    print('thresholds got\npreprocessed data got')



def main():
    print('start')
    dfs = []  # 结果表集合
    dfs_detail = []  # 结果明细表集合
    # 按机构类型分别进行计算
    # hospital_parameters = get_thresholds()
    # thresholds = pd.read_pickle(os.path.join(pickle_path, 'thresholds.pkl'))
    # for i, hospital_type in enumerate(hospital_types):
    #     hospital_parameter = hospital_parameters[i]
    #     for pid, parameter in hospital_parameter.items():
    #         for org_settle, line in parameter.items():
    #             # 住院时间短、列支费用少、药占比低、检查费占比高
    #             if line[0] <= thresholds[i]['th_one_days'] and line[1] <= thresholds[i]['th_one_liezhi_money'] and line[2] <=  thresholds[i]['th_one_med_p_low'] and line[3] >= thresholds[i]['th_one_exp_p']:
    #                 dfs_detail.append([org_settle.split('_')[0], org_settle.split('_')[1]])
    #             # 住院时间相对短、药占比特别高
    #             if line[0] <= thresholds[i]['th_one_days'] and line[2] >= thresholds[i]['th_one_med_p_high']:
    #                 dfs_detail.append([org_settle.split('_')[0], org_settle.split('_')[1]])


    # 针对情况2，60以上人群
    hospital_parameters, personal_org_settle = get_thresholds_60()
    thresholds = pd.read_pickle(os.path.join(pickle_path, 'thresholds_60.pkl'))
    for i, hospital_type in enumerate(hospital_types):
        hospital_parameter = hospital_parameters[i]
        for pid, parameter in hospital_parameter.items():
            # 年龄大、次数多、机构单一
            if parameter['org_count'] <= thresholds.loc[i, 'th_org_count'] and parameter['all_count'] >= thresholds.loc[i, 'th_all_count']:
                for org_settle in personal_org_settle[i][pid]:
                    dfs_detail.append([org_settle.split('_')[0], org_settle.split('_')[1]])

    # result_info = pd.concat(dfs).reset_index(drop=True)
    result_info_detail = pd.DataFrame(dfs_detail, columns=['机构编码', '就诊流水号'])
    result_info_detail.to_excel(os.path.join(output_path, 'DM_WARNING_HOSPITAL_DETAIL.xlsx'), index=False)
    # result_info.to_csv(os.path.join(output_path, 'DM_WARNING_HOSPITAL.csv'), index=True, encoding='utf-8-sig')



if __name__ == '__main__':
    t1 = time.time()
    preprocess_60()
    t2 = time.time()
    print('时间', t2 - t1)
