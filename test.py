import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime
jobList_url = 'http://m.zhaopin.com/job/cc226035412j90250096000/'
html = urlopen(jobList_url)
BsObj = BeautifulSoup(html, 'html.parser')
html.close()
job_Information = BsObj.find("div", {"class": 'wrap'}).get_text()
print(job_Information)
job_Wage = re.search(re.compile(
    "薪水(.*)"), job_Information).group(1)
company_Area = re.search(re.compile(
    "城市(.*)"), job_Information).group(1)
job_PeopleNum = re.search(re.compile(
    "人数(.*)"), job_Information).group(1)
job_Issue = re.search(re.compile(
    "日期(.*)"), job_Information).group(1)
print(job_Wage, job_PeopleNum, job_Issue, company_Area)
print(BsObj.find("div", {"class": 'r_jobdetails'}).h2.get_text())
print(str(datetime.datetime.now().year) + '-')
