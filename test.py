import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime
import os
jobList_url = 'http://m.zhaopin.com/job/cc226035412j90250096000/'
html = urlopen(jobList_url)
BsObj = BeautifulSoup(html, 'html.parser')
html.close()
command = 'shutdown -s -t 60'
os.system(command)
