import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime
import requests

jobList_url = 'http://m.51job.com/search/jobdetail.php?jobtype=6&jobid=86367094'
headers = {
    'Host': 'm.zhaopin.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    'Accept': 'text/css,*/*;q=0.1',
    'Connection': 'keep-alive'
}
html = requests.get(jobList_url, headers=headers, allow_redirects=False)
print(html.encoding)
html.encoding = 'utf-8'
print(html.text)
data = {'username': 'larkjoe@126.com',
        'password': 'Leo19930628'}
html = requests.post(jobList_url, data=data,
                     headers=headers, allow_redirects=False)
print(html.encoding)
html.encoding = 'utf-8'
print(html.text)
#BsObj = BeautifulSoup(html.text, 'html.parser')
#job_if = BsObj.find("div", {'class': "bb"}).p.span.get_text()
# print(job_if)
