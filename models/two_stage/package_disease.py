import os
import sys
current_path = os.path.abspath(os.path.dirname(__file__))
while str(current_path) != '/':
    current_path = os.path.split(current_path)[0]
    sys.path.append(current_path)
    print(current_path, 'is appended')
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
                a.AAC001 AS PC_ID,
                a.BKC231 AS IN_NAME,
                a.BKC232 AS OUT_NAME
            FROM
                tlf_kc21 a
                INNER JOIN HOSPITAL h ON a.AKB020_1 = h.AKB020
                INNER JOIN tlf_kc24 b ON b.AKB020_1 = a.AKB020_1 
                AND b.AKC190 = a.AKC190 
            WHERE

                h.AAA027 = '330183' -- 富阳地区

                AND substr( b.AKC001, 1, 4 ) = '2020' --时间2020

                AND NVL( b.BKC380, '0' ) = '0' --剔除正负交易
                """


def fetch_transactions(org_data):
    """
    获取事务，只用到out_name
    :param org_data:
    :return: [[], []], {}, {}
    """
    temp = org_data.sort_values(by='PC_ID')
    c_pc = None
    transactions, in_name, out_name = [], set(), set()
    for i in range(len(temp)):
        row = temp.iloc[i]
        pc_id = row['PC_ID']
        if pc_id != c_pc:
            c_pc = pc_id
            transactions.append(list(out_name))
            out_name = set()
        out_name.add(row['OUT_NAME'])
    if len(out_name) != 0:
        transactions.append(list(out_name))
    return transactions


def confidence(a_num, b_num, co_num):
    return max(co_num / a_num, co_num / b_num)


def preprocess(data, min_support=0.01, min_transaction=100, item_size=2):
    """
    :param data:
    :param min_support:
    :param min_transaction:
    :param item_size:
    :return:org： 机构信息    ans：中间结果[[(,),(,)], ...]     distribution_：药品组合在机构间的分布   ans_item_sets_：每个机构的所有频繁项集
    """
    org, ans, distribution_, ans_item_sets_ = [], [], defaultdict(lambda: set()), []
    bar = tqdm(data.groupby(by='ORG_ID'))
    for org_id, org_data in bar:
        # if len(org_data) > 2000:
        #     continue
        line0 = org_data.iloc[0]
        bar.set_description('Data size :%d' % len(org_data))
        # 每个机构的每个单据内的项目以及项目的编号到项目名称的映射
        transactions = fetch_transactions(org_data)
        bar.set_description('Data size :%d' % len(org_data) + '   transaction size :%d' % len(transactions))
        if len(transactions) >= min_transaction:
            itemsets = find_frequent_itemsets(transactions, minimum_support=int(len(transactions) * min_support),
                                              include_support=True)
            temp_ans, temp = [], []
            for itemset, support in itemsets:
                itemset.sort()
                if len(itemset) >= item_size:
                    temp_ans.append((itemset, support))
                    temp.append(tuple(itemset))
                    distribution_[tuple(itemset)].add(line0['ORG_ID'])
            if len(temp_ans) > 0:
                ans_item_sets_.append(temp)
                org.append([line0['ORG_ID'], line0['ORG_NAME']])
                ans.append(temp_ans)
                print('\n机构编码：%s  机构名称：%s' % (line0['ORG_ID'], line0['ORG_NAME'],))
                print(temp_ans, '\n')
    return org, ans, distribution_, ans_item_sets_


def preprocess_all_org(data, min_support=0.01, item_size=2):
    """
    获取所有机构的疾病组合
    :param data:
    :param min_support:
    :param item_size:
    :return:  ans_：中间结果(,),(,), ..
    """
    ans_ = []
    transactions = fetch_transactions(data)
    itemsets = find_frequent_itemsets(transactions, minimum_support=int(len(transactions) * min_support),
                                      include_support=True)
    for itemset, support_ in itemsets:
        itemset.sort()
        if len(itemset) >= item_size:
            print(str((itemset, support_)))
            ans_.append((itemset, support_))
    return ans_


if __name__ == '__main__':
    start_time = time.time()
    pickle_path = '../../data/two_stage/data/'
    data_file = 'package_disease_data.pkl'
    single = False
    df = fetch_data(sql_line, pickle_path, data_file, from_db=False)
    print('数据集尺寸', df.shape)

    # 单机构
    if single:
        support = 0.10
        org, ans, _, ans_item_sets = preprocess(df, support)
        with open(os.path.join(pickle_path, 'package_disease_ans_%.2f.txt' % support), 'w') as f:
            for i, org_ans in enumerate(ans):
                f.write('机构编码：%s  机构名称：%s\n' % (org[i][0], org[i][1],))
                for item_set, count in org_ans:
                    f.write(str(item_set) + ':')
                    f.write(str(count) + '    ')
                f.write('\n\n')
    # 所有机构
    else:
        support = 0.10
        ans = preprocess_all_org(df, min_support=support)
        with open(os.path.join(pickle_path, 'package_disease_ans_all_%.2f.txt' % support), 'w') as f:
            for itemset_count in sorted(ans, key=lambda x: x[1], reverse=True):
                f.write(str(itemset_count) + '\n')
    end_time = time.time()
    print('finish')
    print('time cost : %.2f min' % ((end_time - start_time) / 60, ))

