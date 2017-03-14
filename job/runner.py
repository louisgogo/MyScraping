from dataBase import connection, build
from detail import job
from detail import job_Detial
from itools import job_AverWage, job_if, cooky, send_email
import datetime
import codecs
import csv
from maps import coordinate, distance


conn = connection()
cur = conn.cursor()
cur.execute("USE job_CD")


def run(jobarea, homeAddress, homeCity, email, income, subject, *args):
    for i in args:
        print(i)
        work = job(jobarea, i, homeAddress, homeCity, income)
        work.job_Reader()
    cur.execute(
        "create table test (select * from workindex group by job_Id order by row_id)")
    cur.execute("drop table workindex")
    cur.execute("create table workindex (select * from test)")
    cur.execute("drop table test")
    conn.commit()

    cur.execute("SELECT job_Link FROM workindex")
    job_Links = cur.fetchall()
    pages = len(job_Links)
    page = 0
    for link in job_Links:
        page += 1
        try:
            print("剩余未采集的工作信息的数量：", pages - page)
            job_Detial(link)
        except AttributeError as e:
            print("错误原因：", e)
            print('未保存的工作信息的链接是：', link)

    job_AverWage()
    print('平均工资计算完毕')

    coordinate()
    print('工作地点的坐标计算完毕')

    distance(homeAddress, homeCity)
    print('工作直线距离计算完毕')

    date_time = []
    for i in range(2):
        a = datetime.date.today() - datetime.timedelta(days=i)
        a = str(a)
        date_time.append(a)
    date_time = tuple(date_time)
    print(date_time)
    cur.execute("DROP TABLE if exists job_Detail")
    cur.execute("create table job_Detail (select w.job_Name,w.job_Wage,w.job_AverWage,w.company_Name,w.company_Nature,w.company_Scale,w.company_Address,c.company_Distance,c.company_Duration,c.company_Traffic,w.job_PeopleNum,w.job_Issue,w.job_Article,w.job_Link from company c left join work w on c.company_Id=w.company_Id )")
    cur.execute(
        "select job_Name,job_Wage,job_AverWage,company_Name,company_Nature,company_Scale,company_Address,company_Distance,company_Duration,company_Traffic,job_PeopleNum,job_Issue,left(job_Article,300),job_Link from job_Detail where (company_Duration<=3000 or company_Duration='') and (job_Issue in {0})".format(date_time))
    result = cur.fetchall()
    cur.execute(
        "select COLUMN_NAME from INFORMATION_SCHEMA.Columns where table_name='job_Detail' and table_schema='job_cd'")
    title = cur.fetchall()
# 将结果中已经投递过的工作记录去掉
    result = list(result)
    print("总记录数：", len(result))
    cookies = cooky('cookies1.txt')
    print(cookies)
    headers = {
        'Host': 'm.51job.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        'Accept': 'text/css,*/*;q=0.1',
        'Connection': 'keep-alive'
    }
    for i in result:
        print(i[13])
        if job_if(i[13], headers, cookies) == True:
            result.remove(i)
            print("成功移除一条已申请的记录", i[13])
    print("去除已经重复投递后的工作数量", len(result))
    # 生成结果CSV文件
    with codecs.open("job_Detail.csv", "w", encoding="utf_8_sig") as f:
        f_csv = csv.writer(f)
        f_csv.writerow(title)
        f_csv.writerows(result)
        print("文件生成完毕")
    while True:
        try:
            send_email('smtp.qq.com', '272861776@qq.com', 'xjsdroroibjacaej',
                       email, subject, '今天的工作邮件，请查收，最爱你的贝贝')
        except Exception as e:
            print('发生邮件错误，错误原因为:', e)
        else:
            print("邮件发送成功")
            break
    while True:
        try:
            send_email('smtp.qq.com', '272861776@qq.com', 'xjsdroroibjacaej',
                       'louse12345@163.com', subject, '工作邮件的备份资料')
        except Exception as e:
            print('发生邮件错误，错误原因为:', e)
        else:
            print("邮件发送成功")
            break
    cur.close()
    conn.close()

if __name__ == "__main__":
    jobarea = '090200'  # 提供基本参数，广东030000，四川090000，省会编码是0200
    keyword1 = "策划"
    keyword2 = "运营"
    keyword3 = "品牌"
    homeAddress = '锦江区东风路4号一栋一单元'
    homeCity = "成都"
    email = 'larkjoe@126.com'
    income = int('8000')
    subject = "宝宝鸡-{0}的工作记录，请查收".format(datetime.date.today())
    build()
    run(jobarea, homeAddress, homeCity, email,
        income, subject, keyword1, keyword2, keyword3)
