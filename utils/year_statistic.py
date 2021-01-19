"""
年终数据统计
"""
import os
import sys
current_path = os.path.abspath(os.path.dirname(__file__))
while str(current_path) != '/':
    current_path = os.path.split(current_path)[0]
    sys.path.append(current_path)
    print(current_path, 'is appended')
import pandas as pd
import pandas.io.sql as sql
import cx_Oracle as cx
import numpy as np
from tqdm.auto import tqdm
from utils.util import write_to_excel

pulic_sql = """
        SELECT sum(z.cnt) as cnt FROM (SELECT
         a.AKB020_1, COUNT(DISTINCT a.akc190) as cnt
        FROM
         tlf_kc21 a
         INNER JOIN HOSPITAL h ON a.AKB020_1 = h.AKB020
         INNER JOIN tlf_kc24 b ON b.AKB020_1 = a.AKB020_1 
         AND b.AKC190 = a.AKC190 
        WHERE
         h.AAA027 = '330183' -- 富阳地区
         
         AND substr( b.AKC001, 1, 4 ) = '2020' --时间2020
         
         AND NVL( b.BKC380, '0' ) = '0' --剔除正负交易
         
         AND a.BAE450 BETWEEN %d and %d ---各年龄段人数
         
        --  and a.AKA130 = '11' --21:住院  11：普通门诊   
         
        --  and h.akb022 = '2' --- 2:药店
        
                and h.akb022 = '1' and  h.bka938 = '1' and SUBSTR(h.AKB023, 1, 1) = '1' -- 公立机构
                
        -- 		and h.akb022 = '1' and  h.bka938 = '1' and SUBSTR(h.AKB023, 1, 1) = '2' -- 社区机构
         
        --  and h.akb022 = '1' and h.bka938 > '1' and SUBSTR(h.AKB023, 1, 1) = '1'  --民营机构 
                
         GROUP BY a.AKB020_1) z
"""

community_sql = """SELECT sum(z.cnt) as cnt FROM (SELECT
                     a.AKB020_1, COUNT(DISTINCT a.akc190) as cnt
                    FROM
                     tlf_kc21 a
                     INNER JOIN HOSPITAL h ON a.AKB020_1 = h.AKB020
                     INNER JOIN tlf_kc24 b ON b.AKB020_1 = a.AKB020_1 
                     AND b.AKC190 = a.AKC190 
                    WHERE
                     h.AAA027 = '330183' -- 富阳地区
                     
                     AND substr( b.AKC001, 1, 4 ) = '2020' --时间2020
                     
                     AND NVL( b.BKC380, '0' ) = '0' --剔除正负交易
                     
                     AND a.BAE450 BETWEEN %d and %d ---各年龄段人数
                     
                    --  and a.AKA130 = '11' --21:住院  11：普通门诊   
                     
                    --  and h.akb022 = '2' --- 2:药店
                    
                    -- 		and h.akb022 = '1' and  h.bka938 = '1' and SUBSTR(h.AKB023, 1, 1) = '1' -- 公立机构
                            
                            and h.akb022 = '1' and  h.bka938 = '1' and SUBSTR(h.AKB023, 1, 1) = '2' -- 社区机构
                     
                    --  and h.akb022 = '1' and h.bka938 > '1' and SUBSTR(h.AKB023, 1, 1) = '1'  --民营机构 
                            
                     GROUP BY a.AKB020_1) z
"""

private_sql = """SELECT sum(z.cnt) as cnt FROM (SELECT
                 a.AKB020_1, COUNT(DISTINCT a.akc190) as cnt
                FROM
                 tlf_kc21 a
                 INNER JOIN HOSPITAL h ON a.AKB020_1 = h.AKB020
                 INNER JOIN tlf_kc24 b ON b.AKB020_1 = a.AKB020_1 
                 AND b.AKC190 = a.AKC190 
                WHERE
                 h.AAA027 = '330183' -- 富阳地区
                 
                 AND substr( b.AKC001, 1, 4 ) = '2020' --时间2020
                 
                 AND NVL( b.BKC380, '0' ) = '0' --剔除正负交易
                 
                 AND a.BAE450 BETWEEN %d and %d ---各年龄段人数
                 
                --  and a.AKA130 = '11' --21:住院  11：普通门诊   
                 
                --  and h.akb022 = '2' --- 2:药店
                
                -- 		and h.akb022 = '1' and  h.bka938 = '1' and SUBSTR(h.AKB023, 1, 1) = '1' -- 公立机构
                        
                -- 		and h.akb022 = '1' and  h.bka938 = '1' and SUBSTR(h.AKB023, 1, 1) = '2' -- 社区机构
                 
                 and h.akb022 = '1' and h.bka938 > '1' and SUBSTR(h.AKB023, 1, 1) = '1'  --民营机构 
                        
                 GROUP BY a.AKB020_1) z
"""


money_sql = """
SELECT
 sum(a.AKC264) as MONEY
FROM
 tlf_kc60 a
 INNER JOIN TLF_KC21 c ON c.AKC190 = a.AKC190 
 AND c.AKB020_1 = a.AKB020_1
 INNER JOIN HOSPITAL h ON h.AKB020 = a.AKB020_1 
WHERE
 
 h.AAA027 = '330183' --富阳地区
 
 AND substr( a.AKC001, 1, 4 ) = '2020' --2020年数据
 
 AND c.BAE450 BETWEEN %d and %d ---各年龄段人数

 AND EXISTS (
 SELECT
  1 
 FROM
  TLF_KC24 b 
 WHERE
  b.AKB020_1 = a.AKB020_1 
  AND b.AKC190 = a.AKC190 
 AND NVL( b.BKC380, '0' ) = '0' 
 ) --剔除正负样本;
"""


liezhi_sql = """
SELECT
 sum(a.AKC264 - a.AKC253 - a.BKE030) as LIEZHI
FROM
 tlf_kc60 a
 INNER JOIN TLF_KC21 c ON c.AKC190 = a.AKC190 
 AND c.AKB020_1 = a.AKB020_1
 INNER JOIN HOSPITAL h ON h.AKB020 = a.AKB020_1 
WHERE
 
 h.AAA027 = '330183' --富阳地区
 
 AND substr( a.AKC001, 1, 4 ) = '2020' --2020年数据
 
 AND c.BAE450 BETWEEN %d and %d ---各年龄段人数

 AND EXISTS (
 SELECT
  1 
 FROM
  TLF_KC24 b 
 WHERE
  b.AKB020_1 = a.AKB020_1 
  AND b.AKC190 = a.AKC190 
 AND NVL( b.BKC380, '0' ) = '0' 
 ) --剔除正负样本;
"""


if __name__ == '__main__':
    connection = cx.connect('me/mypassword@192.168.0.241:1521/helowin', encoding="UTF-8")
    print('连接成功：', connection)
    down_years = [0, 7, 18, 41, 49, 66]  # 各阶段年段的下限
    up_years = [6, 17, 40, 48, 65, 150]  # 各阶段年龄的上限
    types = 'money'
    if types == 'count':
        header = ['公立', '社区', '私立']
        outputs = pd.DataFrame(np.zeros((len(down_years), len(header))), columns=header)
        for i in tqdm(range(len(down_years))):
            down_year, up_year = down_years[i], up_years[i]
            public_ans = sql.read_sql(pulic_sql % (down_year, up_year), connection)
            community_ans = sql.read_sql(community_sql % (down_year, up_year), connection)
            private_ans = sql.read_sql(private_sql % (down_year, up_year), connection)
            outputs.loc[i, '公立'] = public_ans.loc[0, 'CNT']
            outputs.loc[i, '社区'] = community_ans.loc[0, 'CNT']
            outputs.loc[i, '私立'] = private_ans.loc[0, 'CNT']
    else:
        header = ['总金额', '医保开销']
        outputs = pd.DataFrame(np.zeros((len(down_years), len(header))), columns=header)
        for i in tqdm(range(len(down_years))):
            down_year, up_year = down_years[i], up_years[i]
            money_ans = sql.read_sql(money_sql % (down_year, up_year), connection)
            liezhi_ans = sql.read_sql(liezhi_sql % (down_year, up_year), connection)

            outputs.loc[i, '总金额'] = money_ans.loc[0, 'MONEY']
            outputs.loc[i, '医保开销'] = liezhi_ans.loc[0, 'LIEZHI']
    print(outputs)
    write_to_excel('', 'year_statistic.xlsx', outputs)

