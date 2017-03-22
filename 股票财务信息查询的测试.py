import re
from urllib.request import urlopen
from bs4 import BeautifulSoup
import datetime
import requests

url = 'http://www.cninfo.com.cn/information/stock/incomestatements_.jsp?stockCode=000002'
# 可选参数包括：incomestatements，balancesheet，cashflow，financialreport，注意变更url和data中对应的参数
# http://www.cninfo.com.cn/information/stock/financialreport_.jsp?stockCode=000002
headers = {
    'Host': 'www.cninfo.com.cn',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
    'Accept': 'text/css,*/*;q=0.1',
    'Connection': 'keep-alive'
}
data = {'yyyy': '2015',
        'mm': '-12-31',
        'cwzb': 'incomestatements'
        }
html = requests.post(url, headers=headers, allow_redirects=False, data=data)
print(html.encoding)
html.encoding = 'gbk'
print(html.text)
