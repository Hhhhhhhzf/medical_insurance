import pandas.io.sql as sql
import pandas as pd
import os
from sqlalchemy import create_engine, types
import time

OUTPUT_PATH = '../../data/output/'
FILES = ('PERSON_CARDTIME.xlsx', 'PERSON_CARDTIME_fuyang.xlsx', 'PERSON_RELATIONSHIP.xlsx')
TABLES = ('person_cardtime', 'person_cardtime_fuyang', 'person_relationship')
conn_string='oracle+cx_oracle://me:mypassword@192.168.0.241:1521/helowin'
# sheet_names = ['星期一', '星期二', '星期三', '星期四', '星期五', '星期六', '星期七']
sheet_names = ['药店', '公立机构', '民营医院', '中医治疗', '物理治疗']
# header = ['GUID', 'opcode', 'day', 'time', 'money', 'count']
header = ['GUID', 'OPCODE', 'CODE', 'NAME', 'TYPE', 'COUNT', 'MONEY']
# dtype = {
#     'GUID': types.VARCHAR(20),
#     'opcode': types.VARCHAR(20),
#     'day': types.VARCHAR(20),
#     'time': types.VARCHAR(20),
#     'money': types.FLOAT,
#     'count': types.INTEGER
# }
dtype = {
    'GUID': types.VARCHAR(20),
    'OPCODE': types.VARCHAR(20),
    'CODE': types.VARCHAR(50),
    'NAME': types.VARCHAR(100),
    'TYPE': types.VARCHAR(100),
    'COUNT': types.INTEGER,
    'MONEY': types.FLOAT
}


if __name__ == '__main__':
    MODE = 2
    start = time.time()
    engine = create_engine(conn_string, echo=False, encoding='utf-8')
    for sheet_name in sheet_names:
        df = pd.read_excel(os.path.join(OUTPUT_PATH, FILES[MODE]), sheet_name=sheet_name, names=header)
        df.to_sql(TABLES[MODE], con=engine, if_exists='append', index=False, chunksize=1000, dtype=dtype)
    end = time.time()
    print('finished!\ntime is ', (end-start)/60, 'min')

