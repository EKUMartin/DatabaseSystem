import pymysql
import pandas as pd

conn = pymysql.connect(host="localhost", user="root", password="emfdjdhkTsp1!", db="databasesystem926", charset="utf8",port=33064)
curs = conn.cursor()  
sql1="select TITLE, SALARY from databasesystem926.employee where MANAGER=1 and SALARY>=50000 "
curs.execute(sql1)
data = curs.fetchall() 
df  = pd.DataFrame(data) 

df.to_csv('./exercise1.csv')

sql2="select t1.DEPTNAME, t2.EMPNAME,t1.FLOOR from databasesystem926.department t1, databasesystem926.employee t2  where t2.MANAGER=1 and t1.DEPTNO=t2.DNO and t1.FLOOR>=2"
# sql2="select * from FROM databasesystem926.employee as emp inner join databasesystem926.department as dep on emp.DNO=dep.DEPTNO where dep.floor>=2 and emp.MANAGER=1"
curs.execute(sql2)
data = curs.fetchall() 
df = pd.DataFrame(data) 
df.to_csv('./exercise2.csv')