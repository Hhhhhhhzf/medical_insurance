"""
工具类
"""
import pandas as pd
import os


def write_to_excel(path, file_name, data: pd.DataFrame):
    try:
        data.to_excel(os.path.join(path, file_name))
        print('write file  %s successful' % (file_name, ))
    except:
        print('write file  %s error' % (file_name, ))

