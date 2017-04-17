from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
import smtplib
import re
from dataBase import connection
import datetime
from bs4 import BeautifulSoup
import requests


def wage_Average(wage):
    try:
        li = re.findall(re.compile('([0-9]\d*\.?\d*)'), wage)
        a = 0
        for i in range(0, len(li)):
            a = float(li[i]) + a
        if wage.find('万') > 0:
            a = a * 10000
        if wage.find('千') > 0:
            a = a * 1000
        if wage.find('年') > 0:
            a = a / 12
        if wage.find('天') > 0:
            a = a * 20
        avg = round(a / len(li), 2)
    except Exception:
        avg = ""
    return avg

# 将计算的工资平均数插入表格中


def job_AverWage():
    conn = connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT row_Id,job_Wage FROM work WHERE job_AverWage is null or job_AverWage=''")
    result = cur.fetchall()
    count = 0
    for w in result:
        count += 1
        (row_Id, job_Wage) = w
        job_Wage = wage_Average(job_Wage)
        try:
            job_Wage = round(job_Wage, 2)
        except Exception as e:
            print("工资数为空值", e)
            job_Wage = ""
        cur.execute(
            "UPDATE work SET job_AverWage='%s' WHERE row_Id='%s'", (job_Wage, row_Id))
        print("已经计算完成的数量:", count)
    conn.commit()
    print('工资平均数计算完毕')


def send_email(SMTP_host, from_account, from_passwd, to_account, subject, content, file_name):
    email_client = smtplib.SMTP_SSL(SMTP_host, '465')
    email_client.login(from_account, from_passwd)
    # create msg
    msg = MIMEMultipart()
    msg['Subject'] = Header(subject, 'utf-8')  # subject
    msg['From'] = from_account
    msg['To'] = to_account
    msg.attach(MIMEText(content, 'plain', 'utf-8'))
    with open('E:\{0}'.format(file_name), 'rb') as f:
        mime = MIMEText(f.read(), 'base64', 'utf_8')
        mime["Content-Type"] = 'application/octet-stream'
        mime["Content-Disposition"] = 'attachment; filename={0}{1}'.format(
            datetime.date.today(), file_name)
        msg.attach(mime)
    email_client.set_debuglevel(0)
    email_client.sendmail(from_account, to_account, msg.as_string())
    email_client.quit()

# 判断筛选出来连接是否已经进行过简历的投递


def job_if(jobList_url, headers, cookies):
    s = requests.session()
    html = s.get(jobList_url, headers=headers,
                 cookies=cookies, allow_redirects=False)
    html.encoding = 'utf-8'
    BsObj = BeautifulSoup(html.text, 'html.parser')
    job_Al = BsObj.find("div", {'class': "bb"}).p.span.get_text()
    if job_Al == '已申请':
        return True
    else:
        return False


def job_apply(jobList_url, headers, cookies, jobid):
    s = requests.session()
    html = s.get(jobList_url, headers=headers,
                 cookies=cookies, allow_redirects=False)
    html.encoding = 'utf-8'
    BsObj = BeautifulSoup(html.text, 'html.parser')
    job_Al = BsObj.find("div", {'class': "bb"}).p.span.get_text()
    print(job_Al)
    if job_Al == "申请职位":
        url = 'http://m.51job.com/ajax/search/apply.ajax.php'
        data = {'jobid': '{0}%3A0'.format(jobid),
                'from': 'search%2Fjobdetail', 'jc': '0'}
        html = s.post(url, headers=headers, cookies=cookies, data=data)
        html = s.get(jobList_url, headers=headers,
                     cookies=cookies, allow_redirects=False)
        html.encoding = 'utf-8'
        BsObj = BeautifulSoup(html.text, 'html.parser')
        job_Al = BsObj.find("div", {'class': "bb"}).p.span.get_text()
        print(job_Al)


def cooky(name):
    with open(name, 'r') as f:
        cookies = {}
        for line in f.read().split(';'):
            print(line)
            name, value = line.strip().split('=', 1)  # 1代表只分割一次
            cookies[name] = value
        return cookies

if __name__ == "__main__":
    print(wage_Average('5000-9000元/月'))
    send_email('smtp.qq.com', '272861776@qq.com', 'xjsdroroibjacaej',
               'louse12345@163.com', '测试邮件', '测试邮件')
    jobList_url = "http://m.51job.com/search/jobdetail.php?jobtype=0&jobid=86165749"
    jobid = 86165749
    cookies = cooky('cookies1.txt')
    headers = {
        'Host': 'm.51job.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'Accept': 'text/css,*/*;q=0.1',
        'Connection': 'keep-alive'
    }
    print(job_if(jobList_url, headers, cookies))
    job_apply(jobList_url, headers, cookies, jobid)
