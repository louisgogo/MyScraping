import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime

jobList_url = 'http://m.zhaopin.com/job/cc226035412j90250096000/'
html = urlopen(jobList_url)
BsObj = BeautifulSoup(html, 'html.parser')
html.close()
a=1
a+=1
print(a)
print(a==2)
