import pandas as pd
import os
import cx_Oracle as cx
import pandas.io.sql as sql
import time
import math

start = time.time()
print('start!!')

sql_line = """SELECT a."住院天数" as DAYS FROM YB_FYJSXX_1020_BCXX_3_2 a WHERE a."单据号" = '%s'"""
output_path = '/home/hezhenfeng/medical_insurance/data/cixi_data/output'
final_path = '/home/hezhenfeng/medical_insurance/data/cixi_data/output/final/'
files_all = ('DM_WARNING_HOSPITAL_2018.xlsx', 'DM_WARNING_HOSPITAL_2019.xlsx', 'DM_WARNING_HOSPITAL_2020.xlsx')
files_detail = ('DM_WARNING_HOSPITAL_DETAIL_2018.xlsx', 'DM_WARNING_HOSPITAL_DETAIL_2019.xlsx', 'DM_WARNING_HOSPITAL_DETAIL_2020.xlsx')
connection = cx.connect('YBOLTP/mypassword@192.168.0.241:1521/helowin', encoding="UTF-8")
cursor = connection.cursor()
print('connected!!!')
for i in range(len(files_all)):
    try:
        df_detail = pd.read_excel(os.path.join(output_path, files_detail[i]))
        df_all = pd.read_excel(os.path.join(output_path, files_all[i]))
        df_all['住院天数'] = 0
        df_all['住院天数阈值'] = 2
        length = len(df_detail)
        for j in range(length):
            row_detail, row_all = df_detail.loc[j], df_all.loc[j]
            org_id, settle_id, types = row_detail['机构编码'], row_detail['就诊流水号'], row_all['类别']
            # t_df = sql.read_sql(sql_line % settle_id, connection)
            cursor.execute(sql_line % settle_id)
            rows = cursor.fetchmany(1)
            for line in rows:
                _ = math.ceil(float(line[0]))
                print('days:', _)
                df_all.loc[j, '住院天数'] = _
                if _ > 2 and types != '医养住院':
                    df_detail.drop(j, inplace=True)
                    df_all.drop(j, inplace=True)

            # if float(t_df.loc[0, 'DAYS']) < 2:
            #     df_detail.drop(j, inplace=True)
            #     df_all.drop(j, inplace=True)
            # pass
        print('read_over')
        # df_all.reset_index()
        # df_detail.reset_index()
    except Exception as e:
        print('发生错误', e)
    else:
        df_detail.to_excel(os.path.join(final_path, files_detail[i]))
        df_all.to_excel(os.path.join(final_path, files_all[i]))
print('finished!!!')
end = time.time()

print('time cost is:', end-start)
