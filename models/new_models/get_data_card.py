import cx_Oracle as cx
import pandas.io.sql as sql
import time
import os

PICKLE_PATH = '../../data/dataset/new_pickles/card/'

sql1 = """SELECT
            B.AAC001,--个人编码
            A.AKC190,--就诊流水号
            A.AKB020_1,--机构编码
            A.AKC264 - A.AKC253 - A.BKE030 AS liezhi,--列支费用
            B.AKC002 --结算流水号
        FROM
            TLF_KC60 A
            INNER JOIN TLF_KC21 B ON A.AKC190 = B.AKC190 
            AND A.AKB020_1 = b.AKB020_1 
            inner join HOSPITAL H on H.AKB020 = A.AKB020_1
        WHERE
            substr( A.AKC001, 1, 4 ) = '2020' 
            AND EXISTS (
            SELECT
                1 
            FROM
                TLF_KC24 C 
            WHERE
                C.AKB020_1 = A.AKB020_1 
                AND C.AKC190 = A.AKC190 
            AND NVL( C.BKC380, '0' ) = '0' 
            )
            and H.AAA027 = '330183' ---富阳地区
            """


def get_connection():
    print('connect ready...')
    try:
        my_connection = cx.connect('me/mypassword@192.168.0.241:1521/helowin', encoding="UTF-8")
        print('successfully connect!')
        return my_connection
    except Exception as e:
        print('connect fail!', e)
        return None


if __name__ == '__main__':
    print('start')
    start = time.time()
    if not os.path.exists(PICKLE_PATH):
        os.mkdir(PICKLE_PATH)
    connection = get_connection()
    df = sql.read_sql(sql1, connection)
    df.to_pickle(os.path.join(PICKLE_PATH, 'data_fuyang.pkl'))
    end = time.time()
    print('finished, total time is ', end - start)
