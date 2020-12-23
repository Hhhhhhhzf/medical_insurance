import os
import sys
# current_path = os.path.abspath(os.path.dirname(__file__))
# while str(current_path) != '/':
#     current_path = os.path.split(current_path)[0]
#     sys.path.append(current_path)
#     print(current_path, 'is appended')
import os
from utils.preprocess import fetch_data, read_pickle
from utils.fp_growth import find_frequent_itemsets
from tqdm.auto import tqdm
import multiprocessing
from collections import defaultdict
import time


sql_line = """SELECT
                a.AKB020_1 AS ORG_ID,
                h.AKB021 AS ORG_NAME,
                a.AKC190 AS SETTLE_ID,
                a.AKE001 AS ITEM_CODE,
                a.ake002 AS ITEM_NAME,
                a.AAE019 - a.AKC228 - a.AKE051 as LIEZHI
            
            FROM
                tlf_kc22 a
                INNER JOIN HOSPITAL h ON a.AKB020_1 = h.AKB020
                INNER JOIN tlf_kc24 b ON b.AKB020_1 = a.AKB020_1 
                AND b.AKC190 = a.AKC190 
            WHERE
                a.ake003 = '1' --药品记录
                
                AND h.AAA027 = '330183' -- 富阳地区
                
                AND substr( b.AKC001, 1, 4 ) = '2020' --时间2020
                
                AND NVL( b.BKC380, '0' ) = '0' --剔除正负交易
                
                AND NOT ( SUBSTR( h.AKB023, 1, 1 ) = '1' AND h.bka938 = '1' ) -- 非公立医院
                
                AND a.BKC125 = '0'--非全额自费
                
                AND a.AAE019 - a.AKC228 - a.AKE051 != 0--医保报销的药
                
                and a.AKA063 != '13' --去除草药
                """


def fetch_transactions(org_data):
    """
    :param org_data:
    :return: [[], []], {}, {}
    """
    temp = org_data.sort_values(by='SETTLE_ID')
    c_settle = None
    transactions, item_codes = [], set()
    item_names = {}
    for i in range(len(temp)):
        row = temp.iloc[i]
        item_names[row['ITEM_CODE']] = row['ITEM_NAME']
        settle_id = row['SETTLE_ID']
        if settle_id != c_settle:
            c_settle = settle_id
            transactions.append(list(item_codes))
            item_codes = set()
        item_codes.add(row['ITEM_CODE'])
    if len(item_codes) != 0:
        transactions.append(list(item_codes))
    return transactions, item_names


def confidence(a_num, b_num, co_num):
    return max(co_num/a_num, co_num/b_num)


def preprocess(data, min_support=0.01, min_transaction=100, item_size=2):
    """
    :param data:
    :param min_support:
    :param min_transaction:
    :param item_size:
    :return:org： 机构信息 ans：中间结果 id_name_：药品id到name的映射    distribution_：药品组合在机构间的分布   ans_item_sets_：每个机构的所有频繁项集
    """
    org, ans, id_name_, distribution_, ans_item_sets_ = [], [], {}, defaultdict(lambda: set()), []
    bar = tqdm(data.groupby(by='ORG_ID'))
    for org_id, org_data in bar:
        # if len(org_data) > 2000:
        #     continue
        line0 = org_data.iloc[0]
        bar.set_description('Data size :%d' % len(org_data))
        # 每个机构的每个单据内的项目以及项目的编号到项目名称的映射
        transactions, item_names = fetch_transactions(org_data)
        bar.set_description('Data size :%d' % len(org_data) + '   transaction size :%d' % len(transactions))
        id_name_.update(item_names)
        if len(transactions) >= min_transaction:
            itemsets = find_frequent_itemsets(transactions, minimum_support=int(len(transactions)*min_support), include_support=True)
            temp_ans, temp = [], []
            for itemset, support in itemsets:
                itemset.sort()
                if len(itemset) >= item_size:
                    temp_ans.append((itemset, [item_names[item] for item in itemset], support))
                    temp.append(tuple(itemset))
                    distribution_[tuple(itemset)].add(line0['ORG_ID'])
            if len(temp_ans) > 0:
                ans_item_sets_.append(temp)
                org.append([line0['ORG_ID'], line0['ORG_NAME']])
                ans.append(temp_ans)
                print('\n机构编码：%s  机构名称：%s' % (line0['ORG_ID'], line0['ORG_NAME'], ))
                print(temp_ans, '\n')
    return org, ans, id_name_, distribution_, ans_item_sets_


if __name__ == '__main__':
    start_time = time.time()
    pickle_path = '../../data/two_stage/data/'
    data_file = 'data.pkl'
    support = 0.05
    df = fetch_data(sql_line, pickle_path, data_file, from_db=False)
    print(df.shape)
    _, ans, id_name, distribution, _ = preprocess(df)

    pool = multiprocessing.Pool(10)  # 多进程
    org, _, _, _, ans_item_sets = preprocess(df, support)
    final_ans = []
    for i, item_sets in enumerate(ans_item_sets):
        org_ans = []
        for item_set in item_sets:
            if len(distribution[item_set]) == 1:
                org_ans.append(item_set)
        final_ans.append(org_ans)
    with open(os.path.join(pickle_path, 'all_ans_%f.txt' % support), 'w') as f:
        for i, line in enumerate(final_ans):
            f.write('机构编码：%s  机构名称：%s\n' % (org[i][0], org[i][1],))
            for item_codes in line:
                for j, item_code in enumerate(item_codes):
                    if j == 0:
                        f.write('(')
                    f.write(str(item_code) + ': ' + id_name[item_code] + ', ')
                    if j == len(item_codes)-1:
                        f.write(')   ')
            f.write('\n\n')
    # with open(os.path.join(pickle_path, 'all_ans_%f.txt' % support), 'w') as f:
    #     for i, line in enumerate(ans):
    #         f.write('机构编码：%s  机构名称：%s\n' % (org[i][0], org[i][1],))
    #         f.write(str(line)+'\n\n')
    end_time = time.time()
    print('finish')
    print('time cost :', (end_time-start_time)/60)

