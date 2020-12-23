import pandas as pd
import os
from tqdm.auto import tqdm
import datetime


def read_data(path_, file_name_):
    """
    读取数据
    :param path_:pickle的路径
    :param file_name_:文件的名称
    :return: 生数据
    """
    data_ = pd.read_pickle(os.path.join(path_, file_name_))
    return data_


def time2day(time):
    """
    时间转化成天
    :param time: %Y-%m-%d 之类的格式
    :return:
    """
    try:
        day = datetime.datetime.strptime(str(time)[0:10], '%Y-%m-%d')
        return day.day
    except:
        print('时间转换异常', time)
        return None


def is_abnormal(status):
    """
    判断当前单据是否异常
    :param status:
    :return:
    """
    for i in range(1, len(status)):
        if status[i] == 1:
            return True
    return False


def fetch_explore_method():
    """
    获取探查术服务编号到省颁医保编码的映射，探查术服务编码的集合
    :return: dict():服务编号->省颁医保编码 ; set():探查术ID
    """
    path = '../../data/cixi_data/lib/'
    file1, file2 = '各类手术.xlsx', '各类探查术.xlsx'
    file1 = pd.read_excel(os.path.join(path, file1))
    file2 = pd.read_excel(os.path.join(path, file2))
    id2code = dict(zip(file1['服务编号'], file1['省颁医保编码(转入结算)']))
    id2code.update(dict(zip(file2['服务编号'], file2['省颁医保编码(转入结算)'])))
    return id2code, set(file2['服务编号'])


def fetch_table():
    """
    获取探查术的前五位编码的字典
    :return:
    """
    path = '../../data/cixi_data/lib/'
    file1 = '各类探查术.xlsx'
    file1 = pd.read_excel(os.path.join(path, file1))
    ans = dict()
    for code in file1['省颁医保编码(转入结算)']:
        ans[code[:5]] = [0, 0]  # 第一个0表示是否有探查术，第二个0表示是否有一般手术
    return ans


def main(data_):
    """
    处理的主程序
    :param data_:生数据
    :return:
    """
    id2code, explore_ids = fetch_explore_method()
    code_table = fetch_table()
    group_data_ = tqdm(data_.groupby(by='PC_ID'))
    main_index = detail_index = 0
    main_data, detail_data = [], []
    for pc_id, pc_records in group_data_:
        for settle_id, settle_records in pc_records.groupby(by='SETTLE_ID'):
            copy_code_table = code_table.copy()
            abnormal_type = [0 for i in range(error_type+1)]
            record0 = settle_records.iloc[0]
            error11, error12, error31, error32 = set(), set(), set(), set()
            error4, error5 = set(), set()
            for i in range(len(settle_records)):
                record = settle_records.iloc[i]
                catalog_code, med_type = record['CATALOG_CODE'], record['MED_TYPE']
                happen_time = time2day(record['HAPPEN_TIME'])
                # 根尖诱导成形术   6类异常
                if catalog_code in ('1000007238', ):
                    if int(record0['AGE']) >= 19:
                        abnormal_type[6] = 1
                if med_type == '住院':
                    # 骨髓活检术, 骨髓穿刺术   1类异常
                    if catalog_code in ('1000007367', '1000007366') and happen_time is not None:
                        if catalog_code == '1000007367':
                            error11.add(happen_time)
                            # temp等于另外一个集合
                            temp_error = error12
                        else:
                            error12.add(happen_time)
                            # temp等于另外一个集合
                            temp_error = error11
                        for t in temp_error:
                            if abs(t - happen_time) <= 2:  # TODO 参数化
                                abnormal_type[1] = 1
                                break

                    # 探查术与其他手术  2类异常
                    elif id2code.get(catalog_code) is not None:
                        code = id2code[catalog_code]
                        if catalog_code in explore_ids:
                            copy_code_table[code[:5]][0] = 1
                        else:
                            copy_code_table[code[:5]][1] = 1
                        if 1 == copy_code_table[code[:5]][0] == copy_code_table[code[:5]][1]:
                            abnormal_type[2] = 1

                    # 关节清理术,关节滑膜切除术  3类异常
                    elif catalog_code in ('1000013126', '1000009658', '1000009659', '1000009660') and happen_time is not None:
                        if catalog_code == '1000013126':
                            error31.add(happen_time)
                            # temp等于另外一个集合
                            temp_error = error32
                        else:
                            error32.add(happen_time)
                            # temp等于另外一个集合
                            temp_error = error31
                        for t in temp_error:
                            if abs(t - happen_time) <= 0:  # TODO 参数化
                                abnormal_type[3] = 1
                                break

                else:
                    # 分根术,复杂牙拔出术,骨性埋藏阻生牙拔出术,阻生牙拔出术   4类异常
                    if catalog_code in ('1000008446', '1000008428', '1000483517', '1000008429'):
                        if catalog_code == '1000008446':
                            error4.add(1)
                        else:
                            error4.add(2)
                        if len(error4) > 1:
                            abnormal_type[4] = 1
                    #  5类异常
                    elif catalog_code in ('1000008441', '1000483517', '1000008429', '1000008432', '1000485283',
                                          '1000485284', '1000008444', '1000008445', '1000008447'):
                        if catalog_code == '1000008441':
                            error5.add(1)
                        else:
                            error5.add(2)
                        if len(error5) > 1:
                            abnormal_type[5] = 1
            # 存在异常
            if is_abnormal(abnormal_type):
                main_data.append([main_index, record0['PC_ID'], record0['ORG_ID'], record0['ORG_NAME'], record0['NAME'],
                                  record0['AGE'], record0['SETTLE_ID'], sum(settle_records['MONEY']),
                                  sum(settle_records['LIEZHI'],
                                      ','.join([str(i) for i in range(1, error_type+1) if abnormal_type[i] == 1]))])
                for i in range(len(settle_records)):
                    record = settle_records.iloc[i]
                    detail_data.append([detail_index, main_index, record['DETAIL_ID'], record['ITEM_TYPE'],
                                        record['FEE_TYPE'], record['MONEY'], record['LIEZHI'], record['CATALOG_CODE'],
                                        record['CATALOG_NAME'], record['HAPPEN_TIME'], record['SETTLE_TIME'],
                                        record['MED_TYPE']])
                    detail_index += 1
                main_index += 1
    main_header = ['主键', '病人身份证号', '机构编码', '机构名称', '姓名', '年龄', '单据号', '总金额', '总列支', '异常类型']
    detail_header = ['主键', '外键', '单据明细号', '收费项目类别', '费用类别', '金额', '医保范围费用', '医保目录编码', '医保目录名称', '费用发生时间', '结算时间', '医疗类别']
    main_data = pd.DataFrame(main_data, columns=main_header)
    detail_data = pd.DataFrame(detail_data, columns=detail_header)
    return main_data, detail_data


def write2excel(data, path, file):
    data.to_excel(os.path.join(path, file), index=False)


if __name__ == '__main__':
    error_type = 5
    YEAR = "2020"
    pickle_path = '/home/hezhenfeng/medical_insurance/data/cixi_data/'
    output_path = '/home/hezhenfeng/medical_insurance/data/cixi_data/output/abnormal_charge/'
    file_name = 'abnormal_charge_%s.pkl' % YEAR
    raw_data = read_data(pickle_path, file_name)
    main_data_, detail_data_ = main(raw_data)
    write2excel(main_data_, output_path, '主表.xlsx')
    write2excel(detail_data_, output_path, '明细表.xlsx')
    print('finished')

