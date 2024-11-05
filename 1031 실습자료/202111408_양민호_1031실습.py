import pymysql
import pandas as pd

conn = pymysql.connect(host="localhost", user="root", password="emfdjdhkTsp1!", db="forfun", charset="utf8",port=33064)
curs = conn.cursor()  
sql="insert into DEPARTMENT values(%s,%s,%s,%s)"# empno,emp
df=pd.read_csv('./department.csv')
# df = df.where(pd.notnull(df), None)
for index,row in df.iterrows():
    tu=(row['dept_id'],row["dept_name"],row["office_location"],row["tel"])
    curs.execute(sql,tu)
    
conn.commit()

sql="insert into professor values(%s,%s,%s,%s,%s,%s)"# empno,emp
df=pd.read_csv('./professor.csv')
# df = df.where(pd.notnull(df), None)
for index,row in df.iterrows():
    tu=(row['prof_id'],row["prof_name"],row["dept_id"],row["email"],row["position"],row["hire_date"])
    curs.execute(sql,tu)
conn.commit()
sql="insert into Course values(%s,%s,%s,%s,%s,%s)"# empno,emp
df=pd.read_csv('./course.csv')
# df = df.where(pd.notnull(df), None)
for index,row in df.iterrows():
    tu=(row['course_id'],row["course_name"],row["credits"],row["prof_id"],row["max_students"],row["course_room"])
    curs.execute(sql,tu)
conn.commit()
sql="insert into enrollment values(%s,%s,%s,%s,%s,%s)"# empno,emp
df=pd.read_csv('./enrollment.csv')
# df = df.where(pd.notnull(df), None)
for index,row in df.iterrows():
    tu=(row['enrollment_id'],row["student_id"],row["course_id"],row["semester"],row["grade"],row["enroll_date"])
    curs.execute(sql,tu)
conn.commit()
