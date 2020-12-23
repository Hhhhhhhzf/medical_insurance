import cx_Oracle as cx
import pandas.io.sql as sql
import os

PICKLE_PATH = '../../data/dataset/new_pickles/profile'
FILES_NAME = ('profile', 'relation', 'drug', 'card')

# 住院次数、门诊次数、民营机构次数、购药次数（药店）
sql1 = '''SELECT
                A.AAC001,--"个人编码"
                A.BAE450,--年龄
                A.AKA130,--医疗类别
                H.AKB022,--定点机构类型
                H.BKA938--营利类型
            FROM
                TLF_KC21 A
                inner join HOSPITAL H on H.AKB020 = A.AKB020_1
            WHERE
                EXISTS (
                    SELECT
                        1 
                    FROM
                        TLF_KC24 B 
                    WHERE
                        B.AKC190 = A.AKC190 
                        AND B.AKB020_1 = A.AKB020_1 
                    AND NVL( B.BKC380, '0' ) = '0' 
                )
                and substr( A.AKC002, 1, 4 ) = '2020'
                and H.AAA027 = '330183' ---富阳地区
                '''
sql2 = '''SELECT
            A.AAC001,--人员编码
            A.AKB020_1,--机构编码
            A.AKC190,--就诊流水号
            B.AKC220,--处方号
            B.AKE003 --收费项目种类
        FROM
            tlf_KC21 A
            inner join HOSPITAL H on H.AKB020 = A.AKB020_1
            INNER JOIN tlf_KC22 B ON B.AKC190 = A.AKC190 
            AND B.AKB020_1 = A.AKB020_1 
        WHERE
            EXISTS (
                SELECT
                    1 
                FROM
                    TLF_KC24 C 
                WHERE
                    C.AKC190 = A.AKC190 
                    AND C.AKB020_1 = A.AKB020_1 
                AND NVL( C.BKC380, '0' ) = '0' 
            )
            and substr( A.AKC002, 1, 4 ) = '2020'
            and H.AAA027 = '330183' ---富阳地区
            '''
sql3 = '''SELECT
                A.AAC001,--人员编码
                A.AKB020_1,--机构编码
                A.AKC190,--就诊流水号
                B.AKC264, --医疗费总额
                B.AKC253, --自费金额
                B.BKE030 --自理费用
            FROM
                tlF_KC21 A
                inner join HOSPITAL H on H.AKB020 = A.AKB020_1
                INNER JOIN tlf_KC60 B ON B.AKC190 = A.AKC190 
                AND B.AKB020_1 = A.AKB020_1 
            WHERE
                EXISTS (
                    SELECT
                        1 
                    FROM
                        TLF_KC24 C
                    WHERE
                        C.AKC190 = A.AKC190 
                        AND C.AKB020_1 = A.AKB020_1 
                    AND NVL( C.BKC380, '0' ) = '0' 
                )
                and substr( A.AKC002, 1, 4 ) = '2020'
                and H.AAA027 = '330183' ---富阳地区
                '''


def get_connection():
    try:
        my_connection = cx.connect('me/mypassword@192.168.0.241:1521/helowin', encoding="UTF-8")
        print(my_connection.version)
        print('ready')
        return my_connection
    except Exception as e:
        print('异常：', str(e))
        return None


if __name__ == '__main__':
    connection = get_connection()
    df1 = sql.read_sql(sql1, connection)
    df2 = sql.read_sql(sql2, connection)
    df3 = sql.read_sql(sql3, connection)
    print('read over')
    df1.to_pickle(os.path.join(PICKLE_PATH, 'first_fuyang.pkl'))
    df2.to_pickle(os.path.join(PICKLE_PATH, 'second_fuyang.pkl'))
    df3.to_pickle(os.path.join(PICKLE_PATH, 'third_fuyang.pkl'))
    print('finished')
