import cx_Oracle as cx
import pandas.io.sql as sql
import time
import os

PICKLE_PATH = '../../data/dataset/new_pickles/relation/'

sql1 = """SELECT
            B.AAC001,-- 个人编码
            B.AKB020_1,--org_code
            B.AKB021,--org_name
            A.AKC190,--settle_id
            A.AKE001,--item_code
            A.AKE006,--item name
            A.AKA063,--type（西药：11、中成药：12、中草药：13）
            A.AKC226,--item_count
            A.AAE019 - A.AKC228 - A.AKE051 AS LIEZHI,--liezhi_fee
            H.AKB022,--
            H.BKA938,--
            H.AKB023
        FROM
            TLF_KC22 A
            INNER JOIN TLF_KC21 B ON A.AKC190 = B.AKC190 
            AND A.AKB020_1 = B.AKB020_1
            INNER JOIN TLF_KC60 D ON A.AKC190 = D.AKC190 
            AND A.AKB020_1 = D.AKB020_1 
            INNER JOIN HOSPITAL H on A.AKB020_1 = H.AKB020
        WHERE
            substr( D.AKC001, 1, 4 ) = '2020' 
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
            and A.AKC226 > 0
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
