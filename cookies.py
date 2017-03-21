import requests
from bs4 import BeautifulSoup
import pymysql


def job_apply(jobList_url, headers, cookies):
    s = requests.session()
    html = s.get(jobList_url, headers=headers,
                 cookies=cookies, allow_redirects=False)
    html.encoding = 'utf-8'
    BsObj = BeautifulSoup(html.text, 'html.parser')
    job_Al = BsObj.find("div", {'class': "bb"}).p.span.get_text()
    print(job_Al)
    if job_Al == "申请职位":
        url = 'http://m.51job.com/ajax/search/apply.ajax.php'
        data = {'jobid': '60865207%3A0',
                'from': 'search%2Fjobdetail', 'jc': '0'}
        html = s.post(url, headers=headers, cookies=cookies, data=data)
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

jobList_url = 'http://m.51job.com/search/jobdetail.php?jobtype=0&jobid=60865207'
job_apply(jobList_url, headers, cookies)
