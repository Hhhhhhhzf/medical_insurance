import cx_Oracle as cx
import pandas.io.sql as sql
import pandas as pd
import os


def fetch_data(sql_line, path, file, from_db=False):
    if os.path.exists(os.path.join(path, file)) and not from_db:
        df = pd.read_pickle(os.path.join(path, file))
    else:
        try:
            connection_ = cx.Connection('me/mypassword@192.168.0.241:1521/helowin', encoding="UTF-8")
        except ValueError as e:
            print(e)
            return None
        df = sql.read_sql(sql_line, connection_)
        df.to_pickle(os.path.join(path, file))
    print('read over')
    return df


def read_pickle(path, file):
    return pd.read_pickle(os.path.join(path, file))

