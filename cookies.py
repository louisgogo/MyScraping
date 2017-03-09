import requests
from bs4 import BeautifulSoup
import pymysql

# conn = pymysql.connect(host='127.0.0.1', port=3306,
#                       user='root', passwd='888888', db='mysql', charset='utf8')
#cur = conn.cursor()
#cur.execute('use job_CD')
# cur.execute(
#    "select job_Name,job_Wage,job_AverWage,company_Name,company_Nature,company_Scale,company_Address,company_Distance,company_Duration,company_Traffic,job_PeopleNum,job_Issue,left(job_Article,300),job_Link from job_Detail where (company_Duration<=3000 or company_Duration='') ")
#result = cur.fetchall()


def job_if(jobList_url, headers, cookies):
    s = requests.session()
    html = s.get(jobList_url, headers=headers,
                 cookies=cookies, allow_redirects=False)
    html.encoding = 'utf-8'
    BsObj = BeautifulSoup(html.text, 'html.parser')
    job_Al = BsObj.find("div", {'class': "bb"}).p.span.get_text()
    print(job_Al)

with open('cookies1.txt', 'r') as f:
    cookies = {}
    for line in f.read().split(';'):
        print(line)
        name, value = line.strip().split('=', 1)  # 1代表只分割一次
        cookies[name] = value
    print(cookies)

headers = {
    'Host': 'm.51job.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    'Accept': 'text/css,*/*;q=0.1',
    'Connection': 'keep-alive'
}

#result = list(result)
#print('总结果数：', len(result))
# for i in result:
#    print(i[13])
jobList_url = 'http://m.51job.com/search/jobdetail.php?jobtype=0&jobid=84639426'
job_if(jobList_url, headers, cookies)
#        result.remove(i)
#        print("成功移除一条已申请的记录", i[13])
# print(len(result))
