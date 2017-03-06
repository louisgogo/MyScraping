# encoding: utf-8
'''
Created on 2016年12月1日

@author: louis
'''

from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.parse import quote
import pymysql
import socket
import re
import datetime
import json
import csv
import codecs
import time
import heapq
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import os

# 初始设置
timeout = 20
socket.setdefaulttimeout(timeout)  # 这里对整个socket层设置超时时间。后续文件中如果再使用到socket，不必再设置

# 读取工作界面，并获取工作编号


class job:

    def __init__(self, jobarea, keyword, income):
        self.jobarea = jobarea
        self.keyword = keyword
        self.income = income
        self.pageno = 1
        self.job_list = []
        self.data = ()

    def job_Reader(self):
        # 获取工作列表
        keyword_q = quote(self.keyword)
        repile1 = re.compile('(.*?元/.+?)')
        repile2 = re.compile("job/(.+?)/")
        while True:
            try:
                jobList_url = 'http://m.zhaopin.com/%s/?keyword=%s&pageindex=%s' % (
                    self.jobarea, keyword_q, self.pageno)
                html = urlopen(jobList_url)
                BsObj = BeautifulSoup(html, 'html.parser')
                html.close()
                lastpageno = BsObj.findAll("a", {'rel': 'nofollow'})
                lastpageno = lastpageno[2].attrs['href']
                print(lastpageno)
                lastpageno = re.search(re.compile(
                    "pageindex=(.*?)&"), lastpageno).group(1)
                lastpageno = int(lastpageno) - 800
                print(lastpageno)
            except Exception as e:
                print("网页读取，重新载入", e)
            else:
                break
        while True:
            jobList_url = 'http://m.zhaopin.com/%s/?keyword=%s&pageindex=%s' % (
                self.jobarea, keyword_q, self.pageno
            )
            while True:
                try:
                    html = urlopen(jobList_url)
                    BsObj = BeautifulSoup(html, 'html.parser')
                    html.close()
                    try:
                        if self.pageno == lastpageno:
                            print("全部记录搜索完毕,现在导入数据库")
                            print(jobList_url)
                            sql = "INSERT INTO workindex(job_Link,job_Id) VALUES(%s,%s)"
                            n = cur.executemany(sql, self.job_list)
                            print("导入完毕，共生成记录:", n)
                            conn.commit()
                            return
                    except:
                        pass
                    jobLinks = BsObj.find(
                        "div", {'class': 'r_searchlist positiolist'}).findAll('a')
                except Exception as e:
                    print("有问题，重新载入", e)
                else:
                    break
            for i in jobLinks:
                if self.keyword in i.get_text():
                    try:
                        if wage_Average(re.search(repile1, i.get_text()).group(1)) >= self.income:
                            job_Link = i.attrs["href"]
                            job_Id = re.search(repile2, job_Link)
                            job_Id = job_Id.group(1)
                            job_Link = 'http://m.zhaopin.com' + job_Link
                            self.data = (job_Link, job_Id)
                            self.job_list.append(self.data)
                            print(job_Link, job_Id)
                    except Exception as e:
                        print(e)
                        print(i.get_text())
                        job_Link = i.attrs["href"]
                        job_Id = re.search(repile2, job_Link)
                        job_Id = job_Id.group(1)
                        job_Link = 'http://m.zhaopin.com' + job_Link
                        self.data = (job_Link, job_Id)
                        self.job_list.append(self.data)
                        print(job_Link, job_Id)
            print("已经爬完的页数为：", self.pageno)
            self.pageno += 1

# 获取工作明细


def job_Detial(link):
    start = time.clock()
    link = link[0]
    job_Id = re.search(re.compile("job/(.+)/$"), link).group(1)
    while True:
        try:
            html = urlopen(link)
            BsObj = BeautifulSoup(html, "html.parser")
            html.close()
        except Exception as e:
            print("工作链接读取出现问题，跳过该链接，继续读取", e)
            return
        else:
            break
    # 获得工作相关信息
    job_Name = BsObj.find("div", {"class": 'r_jobdetails'}).h1.get_text()
    company_Name = BsObj.find("div", {"class": 'r_jobdetails'}).h2.get_text()
    job_Article = BsObj.find('article').get_text()
    # 对工作内容进行格式化
    job_Article = re.sub(re.compile(
        "[\u4e00-\u9fa5]|[\（\）\《\》\——\；\，\。\“\”\<\>\！]"), "", job_Article)
    company_Link = 'http://m.zhaopin.com' + \
        BsObj.find('div', {"class": "r_jobdetails"}).find("a").attrs["href"]
    company_Id = re.search(re.compile("company/(.+)/$"), company_Link).group(1)
    print(company_Link)
    # 记录工作的基本信息，包括'类型', "薪水", "学历", "经验", "城市", "人数", "日期"
    job_Information = BsObj.find("div", {"class": 'wrap'}).get_text()
    job_Wage = re.search(re.compile(
        "薪水(.*)"), job_Information).group(1)
    company_Area = re.search(re.compile(
        "城市(.*)"), job_Information).group(1)
    job_PeopleNum = re.search(re.compile(
        "人数(.*)"), job_Information).group(1)
    job_Issue = str(datetime.datetime.now().year) + '-' + re.search(re.compile(
        "日期(.*)"), job_Information).group(1)
    print(job_Wage, job_PeopleNum, job_Issue, company_Area)
    # 记录地址信息
    while True:
        try:
            html = urlopen(company_Link)
            BsObj_Add = BeautifulSoup(html, "html.parser")
            job_Information = BsObj_Add.find(
                "ul", {"class": "companyshadow"}).get_text()
            company_Address = re.search(re.compile(
                "公司地址(.*)"), job_Information).group(1)
            company_Address = re.sub(re.compile(
                "[^\u4e00-\u9fa5]"), "", company_Address)
            company_Nature = re.search(re.compile(
                "企业性质(.*)"), job_Information).group(1)
            company_Scale = re.search(re.compile(
                "规模(.*)"), job_Information).group(1)
            html.close()
        except Exception as e:
            print('未记录到地址信息', e)
        else:
            break
    sql = "INSERT INTO work(job_Id,job_Name,job_Link,job_Wage,company_Id,company_Name,company_Link,company_Nature,company_Scale,company_Area,company_Address,job_PeopleNum,job_Issue,job_Article) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    data = (job_Id, job_Name, link, job_Wage, company_Id, company_Name, company_Link, company_Nature,
            company_Scale, company_Area, company_Address, job_PeopleNum, job_Issue, job_Article)
    cur.execute(sql, data)
    end = time.clock()
    print("read: %f s" % (end - start))

# 计算工资的平均数


def wage_Average(wage):
    try:
        li = re.findall(re.compile('([0-9]\d*\.?\d*)'), wage)
        a = 0
        for i in li:
            a = float(i) + a
        if wage.find('万') > 0:
            a = a * 10000
        if wage.find('千') > 0:
            a = a * 1000
        if wage.find('年') > 0:
            a = a / 12
        if wage.find('/天') > 0:
            a = a * 20
        avg = round(a / len(li), 2)
    except Exception:
        avg = ""
    return avg

# 将计算的工资平均数插入表格中


def job_AverWage():
    cur.execute(
        "SELECT row_Id,job_Wage FROM work WHERE job_AverWage is null or job_AverWage=''")
    result = cur.fetchall()
    count = 0
    for w in result:
        count += 1
        (row_Id, job_Wage) = w
        job_Wage = wage_Average(job_Wage)
        try:
            job_Wage = round(job_Wage, 2)
        except Exception as e:
            print("工资数为空值", e)
            job_Wage = ""
        cur.execute(
            "UPDATE work SET job_AverWage=%s WHERE row_Id=%s", (job_Wage, row_Id))
        print("已经计算完成的数量:", count)
    conn.commit()
    print('工资平均数计算完毕')

# 利用百度地图的API获取工作地址的经纬度


def getAddress(address, city):
    address = quote(address)
    city = quote(city)
    ak = 'VNZwxcMAU5gFt3VeKL5p28EsBg4vvEsw'
    html = 'http://api.map.baidu.com/geocoder/v2/?address=%s&city=%s&output=json&ak=%s' % (
        address, city, ak)
    while True:
        try:
            u = urlopen(html).read()
        except Exception as e:
            print("异常原因：", e)
        else:
            break
    resp = json.loads(u.decode('utf-8'))
    lng = resp.get('result').get('location').get('lng')
    lat = resp.get('result').get('location').get('lat')
    return lng, lat

# 利用百度地图获取两个坐标之间的距离和时间


def getDistance_and_Duration(lon1, lat1, lon2, lat2):
    ak = 'VNZwxcMAU5gFt3VeKL5p28EsBg4vvEsw'
    html = 'http://api.map.baidu.com/direction/v2/transit?origin=%s,%s&destination=%s,%s&output=json&ak=%s' % (
        lat1, lon1, lat2, lon2, ak)
    while True:
        try:
            u = urlopen(html).read()
        except Exception as e:
            print("异常原因：", e)
        else:
            break
    resp = json.loads(u.decode('utf-8'))
    try:
        if resp.get('result').get('routes') == []:
            distance = resp.get('result').get('taxi').get("distance")
            duration = resp.get('result').get('taxi').get("duration")
            company_Traffic = "的士"
        else:
            heap = []
            heapq.heapify(heap)
            for i in resp.get('result').get('routes'):
                distance = i.get("distance")
                duration = i.get("duration")
                a = (duration, distance)
                heapq.heappush(heap, a)
            (duration, distance) = heap[0]
            company_Traffic = "公共交通"
    except Exception as e:
        print("出现错误，原因为：", e)
        distance = ''
        duration = ''
        company_Traffic = ''
    return (distance, duration, company_Traffic)

# 获取工作地点的坐标信息：


def coordinate():
    cur.execute("DROP TABLE if exists company")
    cur.execute("CREATE table company(select company_Id,company_Name,company_Scale,company_Area,company_Address FROM work where company_Scale not in ('50-150人','少于50人') GROUP BY company_id,company_Address)")
    cur.execute("ALTER TABLE company ADD COLUMN(company_x VARCHAR(300),company_y VARCHAR(300),company_Distance VARCHAR(300),company_Duration VARCHAR(300),company_Traffic VARCHAR(300))")
    cur.execute("ALTER TABLE company ADD primary key(company_Id)")
    cur.execute(
        "SELECT company_Id,company_Area,company_Address FROM company WHERE company_x is null")
    result = cur.fetchall()
    for i in result:
        count = 0
        (company_Id, company_Area, company_Address) = i
        city = company_Area[0:2]
        while True:
            try:
                count += 1
                if count == 2:
                    print("无法查到该地址信息，跳过该条信息继续...")
                    break
                if len(company_Address) < 4:
                    (lng, lat) = getAddress(company_Address, city)
                    print("地址不准确，按照公司名称查找坐标")
                else:
                    (lng, lat) = getAddress(company_Address, city)
            except AttributeError:
                print('对象丢失，重新寻找')
                company_Address = ''
            else:
                break
        try:
            print(str(lng), str(lat), company_Id)
            cur.execute("UPDATE company SET company_x='{0}',company_y='{1}' WHERE company_Id='{2}'".format(
                str(lng), str(lat), company_Id))
        except Exception as e:
            print("错误原因：", e)
            cur.execute("UPDATE company SET company_x='{0}',company_y='{1}' WHERE company_Id='{2}'".format(
                "", "", company_Id))
    conn.commit()

# 获取工作信息，计算家和该工作地点的直线距离


def distance(homeAddress, homeCity):
    (lng, lat) = getAddress(homeAddress, homeCity)
    lon1 = round(lng, 6)
    lat1 = round(lat, 6)
    cur.execute("SELECT company_Id,company_x,company_y from company WHERE company_Distance is null or company_Distance='' and company_x is not null")
    result = cur.fetchall()
    for i in result:
        (company_Id, company_x, company_y) = i
        lon2 = round(eval(company_x), 6)
        lat2 = round(eval(company_y), 6)
        company_Distance, company_Duration, company_Traffic = getDistance_and_Duration(
            lon1, lat1, lon2, lat2)
        print(company_Distance, company_Duration, company_Traffic)
        cur.execute("UPDATE company SET company_Distance='{0}',company_Duration='{1}',company_Traffic='{2}' WHERE company_Id='{3}'".format(
            company_Distance, company_Duration, company_Traffic, company_Id))
    conn.commit()

# 发送邮件的模块


def send_email(SMTP_host, from_account, from_passwd, to_account, subject, content):

    email_client = smtplib.SMTP_SSL(SMTP_host, '465')
    email_client.login(from_account, from_passwd)
    # create msg
    msg = MIMEMultipart()
    msg['Subject'] = Header(subject, 'utf-8')  # subject
    msg['From'] = from_account
    msg['To'] = to_account
    msg.attach(MIMEText(content, 'plain', 'utf-8'))
    with open('job_Detail.csv', 'rb') as f:
        mime = MIMEText(f.read(), 'base64', 'utf_8')
        mime["Content-Type"] = 'application/octet-stream'
        mime["Content-Disposition"] = 'attachment; filename={0}job_Detail.csv'.format(
            datetime.date.today())
        msg.attach(mime)
    email_client.set_debuglevel(1)
    email_client.sendmail(from_account, to_account, msg.as_string())
    email_client.quit()


def run(jobarea, homeAddress, homeCity, email, income, subject, *args):
    for i in args:
        print(i)
        work = job(jobarea, i, income)
        work.job_Reader()
    cur.execute(
        "create table test (select * from workindex group by job_Id order by row_id)")
    cur.execute("drop table workindex")
    cur.execute("create table workindex (select * from test)")
    cur.execute("drop table test")
    conn.commit()

    try:
        link_Error = set()
        relink_Error = set()
        cur.execute("SELECT job_Link FROM workindex")
        job_Links = cur.fetchall()
        pages = len(job_Links)
        page = 0
        for link in job_Links:
            page += 1
            try:
                print("剩余未采集的工作信息的数量：", pages - page)
                job_Detial(link)
            except AttributeError as e:
                print("错误原因：", e)
                print('未保存的工作信息的链接是：', link)
                link_Error.add(link)
    finally:
        conn.commit()
        count = 0
        while len(link_Error) != 0:
            count += 1
            relink_Error.update(link_Error)
            print("需要重新采集的错误日志:", relink_Error)
            link_Error.clear()
            for j in relink_Error:
                try:
                    job_Detial(j)
                except Exception as e:
                    print('重新采集不成功，计入错误文档，错误原因：', e)
                    link_Error.add(j)
            if count == 4:
                print('仍未采集的记录数量：', len(link_Error))
                break
        with open('error.txt', 'wt')as f:
            f.write('本次程序运行的日期：%s' % str(datetime.date.today()))
            f.write('\n')
            for j in link_Error:
                f.write('未进行采集的工作链接:%s' % str(j))
                f.write('\n')

    job_AverWage()
    print('平均工资计算完毕')

    coordinate()
    print('工作地点的坐标计算完毕')

    distance(homeAddress, homeCity)
    print('工作直线距离计算完毕')

    date_time = []
    for i in range(2):
        a = datetime.date.today() - datetime.timedelta(days=i)
        a = str(a)
        date_time.append(a)
    date_time = tuple(date_time)
    print(date_time)
    cur.execute("DROP TABLE if exists job_Detail")
    cur.execute("create table job_Detail (select w.job_Name,w.job_Wage,w.job_AverWage,w.company_Name,w.company_Nature,w.company_Scale,w.company_Address,c.company_Distance,c.company_Duration,c.company_Traffic,w.job_PeopleNum,w.job_Issue,w.job_Article,w.job_Link from company c left join work w on c.company_Id=w.company_Id )")
    cur.execute(
        "select job_Name,job_Wage,job_AverWage,company_Name,company_Nature,company_Scale,company_Address,company_Distance,company_Duration,company_Traffic,job_PeopleNum,job_Issue,left(job_Article,300),job_Link from job_Detail where (company_Duration<=3000 or company_Duration='') and (job_Issue in {0})".format(date_time))
    result = cur.fetchall()
    cur.execute(
        "select COLUMN_NAME from INFORMATION_SCHEMA.Columns where table_name='job_Detail' and table_schema='job_cd'")
    title = cur.fetchall()
    with codecs.open("job_Detail.csv", "w", encoding="utf_8_sig") as f:
        f_csv = csv.writer(f)
        f_csv.writerow(title)
        f_csv.writerows(result)
        print("文件生成完毕")
    while True:
        try:
            send_email('smtp.qq.com', '272861776@qq.com', 'xjsdroroibjacaej',
                       email, subject, '今天的工作邮件，请查收，最爱你的贝贝')
        except Exception as e:
            print('发生邮件错误，错误原因为:', e)
        else:
            print("邮件发送成功")
            break
    while True:
        try:
            send_email('smtp.qq.com', '272861776@qq.com', 'xjsdroroibjacaej',
                       'louse12345@163.com', subject, '工作邮件的备份资料')
        except Exception as e:
            print('发生邮件错误，错误原因为:', e)
        else:
            print("邮件发送成功")
            break

# 数据库设置
conn = pymysql.connect(host='127.0.0.1', port=3306,
                       user='root', passwd='888888', db='mysql', charset='utf8')
cur = conn.cursor()


def store():
    try:
        cur.execute("DROP DATABASE job_CD")
        cur.execute('CREATE DATABASE job_CD')
    except Exception as e:
        cur.execute('CREATE DATABASE job_CD')
    cur.execute('USE job_CD')
    # 建立数据库表格
    try:
        cur.execute('CREATE TABLE work (row_Id BIGINT(10) NOT NULL AUTO_INCREMENT,job_Id VARCHAR(200) NOT NULL,job_Name VARCHAR(200) ,job_Link VARCHAR(600),job_Wage VARCHAR(300),job_AverWage VARCHAR(200),company_Id VARCHAR(200),company_Name VARCHAR(200),company_Link VARCHAR(600),company_Nature VARCHAR(200),company_Scale VARCHAR(200),company_Area VARCHAR(400),company_Address VARCHAR(500),job_PeopleNum VARCHAR(400),job_Issue date,job_Article TEXT(20000),created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY (row_Id,job_Id))')
        cur.execute("CREATE TABLE workindex (row_Id BIGINT(10) NOT NULL AUTO_INCREMENT,job_Id VARCHAR(200) NOT NULL,job_Link VARCHAR(600),created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY (row_Id,job_Id))")
        print("数据库建立完毕")
    except (AttributeError, pymysql.err.InternalError):
        print('TABLE已经存在')

store()
jobarea = 'chengdu-801'  # 提供基本参数，成都chengdu-801
keyword1 = "策划"
keyword2 = "运营"
keyword3 = "品牌"
homeAddress = '锦江区东风路4号一栋一单元'
homeCity = "成都"
email = 'larkjoe@126.com'
income = int('6000')
subject = "宝宝鸡-{0}的工作记录，请查收".format(datetime.date.today())

run(jobarea, homeAddress, homeCity, email,
    income, subject, keyword1, keyword2, keyword3)

command = 'shutdown -s -t 60'
os.system(command)

# store()
# jobarea = '040000'  # 提供基本参数，广东030000，四川090000，深圳040000，省会编码是0200
# keyword1 = "审计"
# keyword2 = "财务"
# keyword3 = "会计"
# homeAddress = '福田区竹子林三路竹盛花园'
# homeCity = "深圳"
# email = 'louse12345@163.com'
# income = int('8000')
# subject = "肥肥-{0}的工作记录，请查收".format(datetime.date.today())

# run(jobarea, homeAddress, homeCity, email,
#     income, subject, keyword1, keyword2, keyword3)
