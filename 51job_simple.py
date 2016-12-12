#encoding: utf-8
'''
Created on 2016年12月1日

@author: louis
'''

from bs4 import BeautifulSoup
from math import radians, cos, sin, asin, sqrt
from urllib.request import urlopen
from urllib.parse import quote
import pymysql
import socket
import re
import datetime
import json
import csv
import codecs
from collections import namedtuple
from collections import defaultdict

job_list=[]
link_Error=set()
relink_Error=set()
data=()
timeout = 20   
socket.setdefaulttimeout(timeout)#这里对整个socket层设置超时时间。后续文件中如果再使用到socket，不必再设置 
jobarea='090200'#提供基本参数，广东030000，四川090000，省会编码是0200
keyword='策划'
keyword=quote(keyword)
pageno=1
job_Info=namedtuple('job_Info',['性质',"发布","薪资","地区","规模","招聘"])
job_prototype=job_Info("","","","","","")
jobList_url='http://m.51job.com/search/joblist.php?jobarea=%s&keyword=%s&pageno=%s'%(jobarea,keyword,pageno)

conn=pymysql.connect(host='127.0.0.1',port=3306,user='root',passwd='888888',db='mysql',charset='utf8')
cur=conn.cursor()

selection=input('是否清空数据库（Y/N）')
if selection=='Y':
    cur.execute("DROP DATABASE job_CD")
else:
    pass
try:
    cur.execute('CREATE DATABASE job_CD')
except (pymysql.err.InternalError,pymysql.err.ProgrammingError):
    print('数据库已经存在')
cur.execute('USE job_CD')
#建立数据库表格
try:
    cur.execute('CREATE TABLE work (row_Id BIGINT(10) NOT NULL AUTO_INCREMENT,job_Id VARCHAR(200) NOT NULL,job_Name VARCHAR(200) ,job_Link VARCHAR(600),job_Wage VARCHAR(300),job_AverWage VARCHAR(200),company_Id VARCHAR(200),company_Name VARCHAR(200),company_Link VARCHAR(600),company_Nature VARCHAR(200),company_Scale VARCHAR(200),company_Area VARCHAR(400),company_Address VARCHAR(500),job_PeopleNum VARCHAR(400),job_Issue date,job_Article TEXT(20000),created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY (row_Id,job_Id))')
    cur.execute("CREATE TABLE workindex (row_Id BIGINT(10) NOT NULL AUTO_INCREMENT,job_Id VARCHAR(200) NOT NULL,job_Link VARCHAR(600),created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY (row_Id,job_Id))")
except (AttributeError,pymysql.err.InternalError):
    print('TABLE已经存在')
    
#读取工作界面，并获取工作编号
def job_Reader(jobarea,keyword,pageno):
    while True:
        jobList_url='http://m.51job.com/search/joblist.php?jobarea=%s&keyword=%s&pageno=%s'%(jobarea,keyword,pageno)
        while True:
            try:
                html=urlopen(jobList_url)
                BsObj=BeautifulSoup(html,'html.parser')
                html.close()
                try:
                    if BsObj.find("p",{"class":"no_record"}).get_text()=="暂无搜索记录":
                        print("全部记录搜索完毕,现在导入数据库")
                        sql="INSERT INTO workindex(job_Link,job_Id) VALUES(%s,%s)"
                        n=cur.executemany(sql,job_list)
                        print("导入完毕，共生成记录:",n)
                        conn.commit()   
                        return
                except:
                    pass
                jobLinks=BsObj.find("div",{'class':'jblist'}).findAll('a')
            except Exception as e:
                print("网页读取有问题，重新载入",e)
            else:
                break
        for i in jobLinks:
            job_Link=i.attrs["href"]
            job_Id=re.search(re.compile("jobid=([0-9]+)$"), job_Link)
            job_Id=job_Id.group(1)
            data=(job_Link,job_Id)
            job_list.append(data)
        print("已经爬完的页数为：",pageno)
        pageno+=1
        
#获取工作明细        
def job_Detial(link):
    while True:
        try:
            html=urlopen(link)
            BsObj=BeautifulSoup(html,"html.parser")
            html.close()
        except Exception as e:
            print("工作链接读取出现问题：",e)
            print("正在进行重新读取")
        else:
            break
    #获得工作相关信息
    job_Name=BsObj.find("p",{"class":'xtit'}).get_text()
    company_Name=BsObj.find("a",{"class":'xqa'}).get_text()
    job_Article=BsObj.find('article').get_text()
    #对工作内容进行格式化
    job_Article=re.sub(re.compile("[(%)*(' ')*(\t)*(\r)*]"),"",job_Article)
    company_Link=BsObj.find('div',{"class":"xq"}).find("a").attrs["href"]
    company_Id=re.search(re.compile("coid=([0-9]+)$"), company_Link)
    company_Id=company_Id.group(1)
    job_Information=BsObj.find("div",{"class":'xqd'}).findAll("label")
    d=defaultdict(dict)
    for i in job_Information:
        d[i.get_text()[0:2]]=i.get_text()[2:]
    print(d)
    dict_to_job(d)
    print(job_prototype)
    (company_Nature,job_Issue,job_Wage,company_Area,company_Scale,job_PeopleNum)=job_prototype
    #记录地址信息
    try:
        html=urlopen(company_Link)
        BsObj_Add=BeautifulSoup(html,"html.parser")
        company_Address=BsObj_Add.find("div",{"class":"area dicons_before"}).get_text()
        html.close()
    except Exception as e:
        print('未记录到地址信息',e)
        try:
            company_Address=BsObj.find("div",{"class":"area dicons_before"}).get_text()
        except Exception as e:
            print("无法记录地址信息，错误原因:",e)
            company_Address=""
    sql="INSERT INTO work(job_Id,job_Name,job_Link,job_Wage,company_Id,company_Name,company_Link,company_Nature,company_Scale,company_Area,company_Address,job_PeopleNum,job_Issue,job_Article) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    data=(job_Id,job_Name,job_Link,job_Wage,company_Id,company_Name,company_Link,company_Nature,company_Scale,company_Area,company_Address,job_PeopleNum,job_Issue,job_Article)
    cur.execute(sql,data)
    conn.commit()
    #job_list.append(data)

def dict_to_job(s):
    print("s")
    return job_prototype._replace(**s)

#计算工资的平均数
def wage_Average(wage):
    try:
        li=re.findall(re.compile("[0-9]+"),wage)  
        a=0
        for i in range(0,len(li)):
            a=int(li[i])+a
        if wage.find('万')>0:
            a=a*10000
        if wage.find('年')>0:
            a=a/12
        avg=a/len(li)
    except Exception:
        avg=""    
    return avg

#将计算的工资平均数插入表格中
def job_AverWage():
    cur.execute("SELECT row_Id,job_Wage FROM work WHERE job_AverWage is null or job_AverWage=''")
    result=cur.fetchall()
    for w in result:
        (row_Id,job_Wage)=w
        job_Wage=wage_Average(job_Wage)
        try:
            job_Wage=round(job_Wage,2)
        except Exception as e:
            print("工资数为空值",e)
            job_Wage=""
        cur.execute("UPDATE work SET job_AverWage=%s WHERE row_Id=%s",(job_Wage,row_Id))
        conn.commit()    
    print('工资平均数计算完毕')  
    
#利用百度地图的API获取工作地址的经纬度
def getAddress(address,city):
    address=quote(address)
    city=quote(city)
    html='http://api.map.baidu.com/geocoder/v2/?address=%s&city=%s&output=json&ak=vnHoeEGIBLAis3oHv2VYXQEAouvFbq1b'%(address,city)
    while True:
        try:
            response=urlopen(html).read()
        except Exception as e:
            print("异常原因：",e)
        else:
            break
    responseJson=json.loads(response.decode('utf-8'))
    lng=responseJson.get('result').get('location').get('lng')
    lat=responseJson.get('result').get('location').get('lat')
    return lng,lat

def haversine(lon1, lat1, lon2, lat2): # 经度1，纬度1，经度2，纬度2 （十进制度数）  
    """ 
    Calculate the great circle distance between two points  
    on the earth (specified in decimal degrees) 
    """  
    # 将十进制度数转化为弧度  
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])  
    # haversine公式  
    dlon = lon2 - lon1   
    dlat = lat2 - lat1   
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2  
    c = 2 * asin(sqrt(a))   
    r = 6371 # 地球平均半径，单位为公里  
    return c * r *1000

#获取工作地点的坐标信息：
def coordinate():
    cur.execute("DROP TABLE if exists company")
    cur.execute("CREATE table company(select company_Id,company_Name,company_Scale,company_Area,company_Address FROM work where company_Scale not in ('50-150人','少于50人') GROUP BY company_id,company_Address)")
    cur.execute("ALTER TABLE company ADD COLUMN(company_x VARCHAR(300),company_y VARCHAR(300),company_Distance VARCHAR(300))")
    cur.execute("SELECT company_Id,company_Area,company_Address FROM company WHERE company_x is null")
    result=cur.fetchall()
    baidu_count=1
    for i in result:
        count=0
        (company_Id,company_Area,company_Address)=i
        city=company_Area[0:2]
        print(city+","+company_Address)
        while True:
            baidu_count+=1
            if baidu_count==5900:
                print("百度地图使用次数到上限，退出程序")
                break
            try:
                count+=1
                if count==2:
                    print("无法查到该地址信息，跳过该条信息继续...")
                    break
                if len(company_Address)<4:
                    (lng,lat)=getAddress(company_Address,city)
                    print("地址不准确，按照公司名称查找坐标")
                else:
                    (lng,lat)=getAddress(company_Address,city)
                    print(lng,lat)
            except AttributeError:
                print('对象丢失，重新寻找')
                company_Address=''
            else:
                break
        try:
            cur.execute('UPDATE company SET company_x={0},company_y={1} WHERE company_Id={2}'.format(str(lng),str(lat),company_Id))
        except Exception as e:
            print("错误原因：",e)
            cur.execute('UPDATE company SET company_x={0},company_y={1} WHERE company_Id={1}'.format("","",company_Id))
        conn.commit()

#获取工作信息，计算家和该工作地点的直线距离
def distance(homeAddress,homeCity):
    (lng,lat)=getAddress(homeAddress,homeCity)
    lon2=lng
    lat2=lat
    cur.execute("SELECT company_Id,company_x,company_y from company WHERE company_distance is null or company_distance='' and company_x is not null")
    result=cur.fetchall()
    for i in result:
        try:
            (company_Id,company_x,company_y)=i
            lon1=eval(company_x)
            lat1=eval(company_y)
            cur.execute('UPDATE company SET company_Distance=%s WHERE company_Id=%s'%(round(haversine(lon1, lat1, lon2, lat2),2),company_Id))
            print(str(round(haversine(lon1, lat1, lon2, lat2),2))+'米')
        except Exception as e:
            print("错误原因：",e)
            cur.execute('UPDATE company SET company_Distance=%s WHERE company_Id=%s'%("",company_Id))
    conn.commit()
  

if input("开始进行工作链接的采集(Y/N)?")=="Y":
    job_Reader(jobarea,keyword,pageno)

if input("开始进行工作数据采集(Y/N)?")=="Y":
    try:
        cur.execute("SELECT job_Link,job_Id FROM workindex")
        job_Links=cur.fetchall()
        pages=len(job_Links)
        page=0
        for link in job_Links:
            page+=1
            (job_Link,job_Id)=link
            try:
                print("剩余未采集的工作信息的数量：",pages-page)
                job_Detial(job_Link)
                #if page % 500==0 or page==pages:
                    #sql="INSERT INTO work(job_Id,job_Name,job_Link,job_Wage,company_Id,company_Name,company_Link,company_Nature,company_Scale,company_Area,company_Address,job_PeopleNum,job_Issue,job_Article) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    #n=cur.executemany(sql,job_list)
                    #conn.commit()
                    #print("已完成",n,"条工作记录的导入")
                    #job_list.clear()
            except AttributeError as e:
                print("错误原因：",e)
                print('未保存的工作信息的链接是：',job_Link)
                link_Error.add(job_Link)
    finally:
        count=0
        while len(link_Error)!=0:
            count+=1
            relink_Error.update(link_Error)
            print("需要重新采集的错误日志:",relink_Error)
            link_Error.clear()
            for j in relink_Error:
                try:
                    job_Detial(j)
                except Exception as e:
                    print('重新采集不成功，计入错误文档，错误原因：',e)
                    link_Error.add(j)
            if count==4:
                print('仍未采集的记录数量：',len(link_Error))
                break 
        with open('error.txt','wt')as f:
            f.write('本次程序运行的日期：%s'%str(datetime.date.today()))
            f.write('\n')
            for j in link_Error:
                f.write('未进行采集的工作链接:%s'%str(j))
                f.write('\n')

if input("是否计算平均工资(Y/N)?")=="Y":
    job_AverWage()
    print('平均工资计算完毕')   

if input("是否计算工作地点的坐标(Y/N)?")=="Y":   
    coordinate()
    print('工作直线距离计算完毕')  
    
if input("是否计算工作直线距离(Y/N)?")=="Y":   
    distance('锦江区东风路4号一栋一单元', '成都')
    print('工作直线距离计算完毕')

if input("是否生成结果数据表(Y/N)?")=="Y":
    cur.execute("DROP TABLE if exists job_Detail")
    cur.execute("create table job_Detail (select w.job_Name,w.job_Wage,w.job_AverWage,w.company_Name,w.company_Nature,w.company_Scale,w.company_Address,c.company_Distance,w.job_PeopleNum,w.job_Issue,w.job_Article,w.job_Link from company c left join work w on c.company_Id=w.company_Id where job_AverWage>=7000)")
    cur.execute("select job_Name,job_Wage,job_AverWage,company_Name,company_Nature,company_Scale,company_Address,company_Distance,job_PeopleNum,job_Issue,left(job_Article,200),job_Link from job_Detail")
    result=cur.fetchall() 
    cur.execute("select COLUMN_NAME from INFORMATION_SCHEMA.Columns where table_name='job_Detail' and table_schema='job_cd'")
    title=cur.fetchall() 
    with codecs.open("job_Detail.csv","w",encoding="UTF-8") as f:
        f_csv=csv.writer(f) 
        f_csv.writerow(title)
        f_csv.writerows(result)
        print("文件生成完毕")
        
      
                
        