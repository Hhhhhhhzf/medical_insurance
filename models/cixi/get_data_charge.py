import cx_Oracle as cx
import pandas.io.sql as sql
import os

YEAR = "2020"
pickle_path = '/home/hezhenfeng/medical_insurance/data/cixi_data/'
file_name = 'abnormal_charge_%s.pkl' % YEAR

if os.path.exists(pickle_path) is not True:
    os.mkdir(pickle_path)

try:
    connection = cx.connect('YBOLTP/mypassword@192.168.0.241:1521/helowin', encoding="UTF-8")
except Exception as e:
    print('异常：', str(e))
print('ready')

drug_sql = """SELECT
                A."定点机构编码" AS ORG_ID,
                A."定点机构名称" AS ORG_NAME,
                C."证件号码" AS PC_ID,
                A."姓名" AS NAME,
                C."性别" AS GENDER,
                TO_NUMBER(B."医保年度") - TO_NUMBER(
                SUBSTR( C."出生日期", 1, 4 )) AS AGE,
                A."单据号" AS SETTLE_ID,
                A."单据明细号" AS DETAIL_ID,
                A."收费项目类别" AS ITEM_TYPE,
                A."费用类别" AS FEE_TYPE,
                A."金额" AS MONEY,
                A."医保范围费用" AS LIEZHI,
                A."医保目录编码" as CATALOG_CODE,
                A."医保目录名称" as CATALOG_NAME,
                A."费用发生时间" AS HAPPEN_TIME,
                A."结算时间" AS SETTLE_TIME,
                B."医疗类别" AS MED_TYPE
            FROM
                YB_FYMXXX_1013 A
                INNER JOIN YB_FYJSXX_1013 B ON B."单据号" = A."单据号" 
                AND B."定点机构编码" = A."定点机构编码"
                INNER JOIN YB_CBRYXX_1013 C ON C."证件号码" = A."证件号码" 
            WHERE
                B."医疗类别" = '住院' or B."医疗类别" = '门诊'
                AND B."机构统筹区划代码" = '330282' 
                AND B."医保年度" = %s
                AND B."单据号" != '                    '
                """ % (YEAR, )

df = sql.read_sql(drug_sql, connection)
print('read over')
print('write to pickle,step1')
df.to_pickle(os.path.join(pickle_path, file_name))
print("finished.")

