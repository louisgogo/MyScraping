#encoding: utf-8
'''
Created on 2016年12月26日

@author: louis
'''
import pymysql
import csv 

conn=pymysql.connect(host='127.0.0.1',port=3306,user='root',passwd='888888',db='mysql',charset='utf8')
cur=conn.cursor()
selection=input('是否清空数据库（Y/N）')
if selection=='Y':
    try:
        cur.execute("DROP DATABASE bank")
    except Exception as e:
        print("数据库清空发生错误：",e)
else:
    pass
try:
    cur.execute('CREATE DATABASE bank')
except (pymysql.err.InternalError,pymysql.err.ProgrammingError):
    print('数据库已经存在')
cur.execute('USE bank')
#建立数据库表格
try:
    cur.execute('CREATE TABLE bank_detail (bank_date date,bank_Name VARCHAR(200) ,bank_expense VARCHAR(200),bank_income VARCHAR(200),bank_account VARCHAR(300),bank_people VARCHAR(200),bank_remark VARCHAR(200))')
except (AttributeError,pymysql.err.InternalError):
    print('TABLE已经存在')

data=[]
with open(r'D:\我的坚果云\Python\bank.csv','r') as f:
    reader=csv.reader(f) 
    headings=next(reader)
    print(headings)
    for r in reader:
        if r[0]!='':
            r[5]=r[5].replace('\u3000','')
            r[6]=r[6].replace('\u3000','')
            r[2]=r[2].replace(',','')
            r[3]=r[3].replace(',','')
            bank_date,bank_Name,bank_expense,bank_income,bank_account,bank_people,bank_remark=r
            print(bank_date,bank_Name,bank_expense,bank_income,bank_account,bank_people,bank_remark)
            cur.execute("insert into bank_detail values('%s','%s','%s','%s','%s','%s','%s')"%(bank_date,bank_Name,bank_expense,bank_income,bank_account,bank_people,bank_remark))
    conn.commit()
cur.execute("create table bank_beifeng (select * from bank_detail where bank_Name='农业银行（北风）')")
cur.execute("create table bank_shangmao (select * from bank_detail where bank_Name='农业银行（商贸）')")
cur.execute("create table bank_mingsheng (select * from bank_detail where bank_Name='民生银行（个人）')")
cur.execute("create table bank_nongye (select * from bank_detail where bank_Name='农业银行（个人）')")
cur.execute("create table bank_chuxiong (select * from bank_detail where bank_Name='农业银行（对公）')")
cur.execute("create table bank_songsong (select * from bank_detail where bank_Name='农业银行（宋松）')")
for i in ('bank_beifeng','bank_shangmao','bank_mingsheng','bank_nongye','bank_chuxiong','bank_songsong'):
    cur.execute("select round(sum(bank_income)-sum(bank_expense),2) from %s" %i)
    num=cur.fetchall()
    cur.execute("select bank_date from %s order by bank_date desc" %i)
    result=cur.fetchall()
    print(i,num,result[0])