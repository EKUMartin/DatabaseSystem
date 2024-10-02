import pymysql
import pandas as pd
import numpy as np
import math




# pd.read_csv

conn = pymysql.connect(host="localhost", user="root", password="emfdjdhkTsp1!", db="databasesystem926", charset="utf8",port=33064)

curs = conn.cursor()    


# department 테이블에 데이터 insert
# sql = "insert into department values(%s, %s, %s)"  # deptno, deptname, floor
# curs.execute(sql, (1, "IT부", 3))

# employee 테이블에 데이터 insert
# sql = "insert into employee values(%s, %s, %s, %s, %s, %s)"  # empno, empname, title, manager, salary, dno
# curs.execute(sql, (1, "홍길동", "manager", None, 5000000, 1))
# conn.commit()

####################################################################
#
# Department_Table.csv를 pd.read_csv를 이용하여 읽어와서 department 테이블에 데이터 insert
# Employee_Table.csv를 pd.read_csv를 이용하여 읽어와서 employee 테이블에 데이터 insert
#
####################################################################
# code 작성 #
sql="insert into DEPARTMENT values(%s,%s,%s)"# empno,emp
df=pd.read_csv('./Department_Table.csv')
# df = df.where(pd.notnull(df), None)
df = df.replace({np.nan: None})
for index,row in df.iterrows():
    tu=(row['DEPTNO'],row["DEPTNAME"],row["FLOOR"])
    curs.execute(sql,tu)
# for idx, tp in df.iterrows():
#     curs.execute(sql, (1,tp["DEPTNO"], tp["DEPTNAME"],tp["FLOOR"]))
# for idx, tp in df.iterrows():
#     curs.execute(sql,(1,idx,tp["DEPTNAME"], None, 500000, 1))#cursor.execute(operation, params=None, multi=False)

conn.commit()

sql="insert into EMPLOYEE values(%s,%s,%s,%s,%s,%s)"# empno,emp
df=pd.read_csv('./Employee_Table.csv')
df = df.replace({np.nan: None})
# df = df.where(pd.notnull(df), None)
# for index,row in df.iterrows():
#     tu=(row['EMPNO'],row["EMPNAME"],row["TITLE"],row["MANAGER"],row["SALARY"],row["DNO"])
    # curs.execute(sql,tu)
for idx,tp in df.iterrows():
    curs.execute(sql,(tp['EMPNO'],tp["EMPNAME"],tp["TITLE"],tp["MANAGER"],tp["SALARY"],tp["DNO"]))
conn.commit()


