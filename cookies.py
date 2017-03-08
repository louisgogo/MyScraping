import requests
from bs4 import BeautifulSoup

with open('cookies.txt', 'r') as f:
    cookies = {}
    for line in f.read().split(';'):
        name, value = line.strip().split('=', 1)  # 1代表只分割一次
        cookies[name] = value
print(cookies)
jobList_url = 'http://m.51job.com/search/jobdetail.php?jobid=85082705'
headers = {
    'Host': 'm.zhaopin.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    'Accept': 'text/css,*/*;q=0.1',
    'Connection': 'keep-alive',
    'cookies': str(cookies)
}
s = requests.session()
html = s.get(jobList_url, headers=headers,
             cookies=cookies, allow_redirects=False)
print(html.encoding)
html.encoding = 'utf-8'
BsObj = BeautifulSoup(html.text, 'html.parser')
job_if = BsObj.find("div", {'class': "bb"}).p.span.get_text()
print(job_if)
