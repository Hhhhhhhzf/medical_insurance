import cx_Oracle as cx
import pandas.io.sql as sql
import time
import os

PICKLE_PATH = '../../data/dataset/new_pickles/drug/'

sql1 = """SELECT
            B.AAC001,--个人编码
            A.AKA063,--收费类别
            A.AAE019,--金额
            A.AKC228,--自理金额
            A.AKE051 --自费金额
            
        FROM
            TLF_KC22 A
            INNER JOIN TLF_KC21 B ON A.AKC190 = B.AKC190 
            AND A.AKB020_1 = B.AKB020_1 
            inner join HOSPITAL H on H.AKB020 = A.AKB020_1
        WHERE
            EXISTS (
            SELECT
                1 
            FROM
                TLF_KC60 C 
            WHERE
                C.AKC190 = A.AKC190 
                AND C.AKB020_1 = A.AKB020_1 
                AND substr( C.AKC001, 1, 4 ) = '2020' 
            ) 
            AND NOT EXISTS (
            SELECT
                1 
            FROM
                HOSPITAL H 
            WHERE
                H.akb022 = '1' 
                AND H.bka938 = '1' 
                AND SUBSTR( H.akb023, 1, 1 ) = '1' 
                AND A.AKB020_1 = H.AKB020 
            ) 
            AND EXISTS (
            SELECT
                1 
            FROM
                TLF_KC24 
            WHERE
                TLF_KC24.AKB020_1 = B.AKB020_1 
                AND TLF_KC24.AKC190 = B.AKC190 
                AND NVL( TLF_KC24.BKC380, '0' ) = '0' 
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
