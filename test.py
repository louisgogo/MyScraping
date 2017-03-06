import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime
jobList_url = 'http://m.zhaopin.com/job/cc226035412j90250096000/'
html = urlopen(jobList_url)
BsObj = BeautifulSoup(html, 'html.parser')
html.close()


def wage_Average(wage):
    try:
        li = re.findall(re.compile('([0-9]\d*\.?\d*)'), wage)
        a = 0
        for i in li:
            a = float(i) + a
            print(a)
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

print(wage_Average('4000-8000元/月'))
