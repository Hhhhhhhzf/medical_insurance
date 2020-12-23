from sklearn.cluster import KMeans
import pandas as pd
import os
import matplotlib.pyplot as plt

def fetch_data():
    path = '../../data/output/'
    df = pd.read_excel(os.path.join(path, 'DM_WARNING_HOSPITAL.xlsx'))
    res = df.drop(labels=['类别', '就诊人', '就诊人名称', '住院天数阈值', '总住院次数阈值', '住院日均次费用阈值', '药占比阈值', '检查比率阈值', '机构编码逗号分隔', '住院列支总费用阈值'],
            axis=1, inplace=True)
    labels = df['类别']
    return res, labels


if __name__ == '__main__':
    path = '../../data/output/'
    df = pd.read_excel(os.path.join(path, 'DM_WARNING_HOSPITAL.xlsx'))
    df.drop(labels=['类别', '就诊人', '就诊人名称', '住院天数阈值', '总住院次数阈值', '住院日均次费用阈值', '药占比阈值', '检查比率阈值', '机构编码逗号分隔', '住院列支总费用阈值'], axis=1, inplace=True)
    km = KMeans(init='k-means++', n_clusters=3)
    km.fit(df)
    for i, value in enumerate(km.labels_):
        print(i, '行, 数据为：', value)
    # print(km)
    # print(df)
