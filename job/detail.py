from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.parse import quote
from dataBase import connection
from itools import wage_Average
import re
import time
from collections import namedtuple
from collections import defaultdict

conn = connection()
cur = conn.cursor()
job_Info = namedtuple('job_Info', ['性质', "发布", "薪资", "地区", "规模", "招聘"])
job_prototype = job_Info("", "", "", "", "", "")


class job:

    def __init__(self, jobarea, keyword, homeAddress, homeCity, income):
        self.jobarea = jobarea
        self.keyword = keyword
        self.homeAddress = homeAddress
        self.homeCity = homeCity
        self.income = income
        self.pageno = 1
        self.job_list = []
        self.data = ()

    def job_Reader(self):
        # 获取工作列表
        keyword_q = quote(self.keyword)
        while True:
            jobList_url = 'http://m.51job.com/search/joblist.php?jobarea=%s&keyword=%s&pageno=%s' % (
                self.jobarea, keyword_q, self.pageno
            )
            while True:
                try:
                    html = urlopen(jobList_url)
                    BsObj = BeautifulSoup(html, 'html.parser')
                    html.close()
                    try:
                        if BsObj.find("p", {"class": "no_record"}).get_text() == "暂无搜索记录":
                            print("全部记录搜索完毕,现在导入数据库")
                            sql = "INSERT INTO workindex(job_Link,job_Id) VALUES(%s,%s)"
                            n = cur.executemany(sql, self.job_list)
                            print("导入完毕，共生成记录:", n)
                            conn.commit()
                            return
                    except:
                        pass
                    jobLinks = BsObj.find(
                        "div", {'class': 'jblist'}).findAll('a')
                except Exception as e:
                    print(" 有问题，重新载入", e)
                else:
                    break
            for i in jobLinks:
                if self.keyword in i.h3.get_text():
                    try:
                        if wage_Average(i.em.get_text()) >= self.income:
                            job_Link = i.attrs["href"]
                            job_Id = re.search(re.compile(
                                "jobid=([0-9]+)$"), job_Link)
                            job_Id = job_Id.group(1)
                            self.data = (job_Link, job_Id)
                            self.job_list.append(self.data)
                    except TypeError:
                        job_Link = i.attrs["href"]
                        job_Id = re.search(re.compile(
                            "jobid=([0-9]+)$"), job_Link)
                        job_Id = job_Id.group(1)
                        self.data = (job_Link, job_Id)
                        self.job_list.append(self.data)
            print("已经爬完的页数为：", self.pageno)
            self.pageno += 1

# 获取工作明细


def job_Detial(link):
    start = time.clock()
    link = link[0]
    job_Id = re.search(re.compile("jobid=([0-9]+)$"), link).group(1)
    while True:
        try:
            html = urlopen(link)
            BsObj = BeautifulSoup(html, "html.parser")
            html.close()
        except Exception as e:
            print("工作链接读取出现问题：", e)
            print("正在进行重新读取")
        else:
            break
    # 获得工作相关信息
    job_Name = BsObj.find("p", {"class": 'xtit'}).get_text()
    company_Name = BsObj.find("a", {"class": 'xqa'}).get_text()
    job_Article = BsObj.find('article').get_text()
    # 对工作内容进行格式化
    job_Article = re.sub(re.compile(
        "^[\u4e00-\u9fa5]|^[\（\）\《\》\——\；\，\。\“\”\<\>\！]"), "", job_Article)
    company_Link = BsObj.find('div', {"class": "xq"}).find("a").attrs["href"]
    company_Id = re.search(re.compile("coid=([0-9]+)$"), company_Link)
    company_Id = company_Id.group(1)
    # 记录工作的基本信息，包括性质,发布,薪资,地区,规模,招聘
    job_Information = BsObj.find("div", {"class": 'xqd'}).findAll("label")
    d = defaultdict(dict)
    for i in job_Information:
        d[i.get_text()[0:2]] = i.get_text()[2:]
    job_Information = dict_to_job(d)
    (company_Nature, job_Issue, job_Wage, company_Area,
     company_Scale, job_PeopleNum) = job_Information
    # 记录地址信息
    try:
        html = urlopen(company_Link)
        BsObj_Add = BeautifulSoup(html, "html.parser")
        company_Address = BsObj_Add.find(
            "div", {"class": "area dicons_before"}).get_text()
        html.close()
    except Exception as e:
        print('未记录到地址信息', e)
        try:
            company_Address = BsObj.find(
                "div", {"class": "area dicons_before"}).get_text()
        except Exception as e:
            print("无法记录地址信息，错误原因:", e)
            company_Address = ""
    sql = "INSERT INTO work(job_Id,job_Name,job_Link,job_Wage,company_Id,company_Name,company_Link,company_Nature,company_Scale,company_Area,company_Address,job_PeopleNum,job_Issue,job_Article) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    data = (job_Id, job_Name, link, job_Wage, company_Id, company_Name, company_Link, company_Nature,
            company_Scale, company_Area, company_Address, job_PeopleNum, job_Issue, job_Article)
    cur.execute(sql, data)
    end = time.clock()
    print("read: %f s" % (end - start))

    # 将工作信息进行替代


def dict_to_job(s):
    return job_prototype._replace(**s)

if __name__ == '__main__':
    link = ('http://m.51job.com/search/jobdetail.php?jobtype=0&jobid=86932592',)
    job_Detial(link)
    jobarea = '090200'  # 提供基本参数，广东030000，四川090000，省会编码是0200
    keyword = "策划"
    homeAddress = '锦江区东风路4号一栋一单元'
    homeCity = "成都"
    income = int('6000')
    work = job(jobarea, keyword, homeAddress, homeCity, income)
    work.job_Reader()
