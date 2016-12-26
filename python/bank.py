#encoding: utf-8
'''
Created on 2016年12月26日

@author: louis
'''
import pymysql
import csv 
import re
conn=pymysql.connect(host='127.0.0.1',port=3306,user='root',passwd='888888',db='mysql',charset='utf8')
cur=conn.cursor()

count=0
data=[]
pattern=re.compile("\\u*")
with open(r'D:\我的坚果云\Python\bank.csv','r') as f:
    reader=csv.reader(f)
    headings=next(reader)
    print(headings)
    for r in reader:
        if r[0]!='':
            r[5]=r[5].re.sub(pattern,'')
            r[6]=r[6].re.sub(pattern,'')
            data.append(r)
            count+=1
    print(count)
    print(data)