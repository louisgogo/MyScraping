import csv
from dataBase import connection, build
import datetime


def data_Filling():
    date_time = []
    conn = connection()
    cur = conn.cursor()
    for i in range(2):
        a = datetime.date.today() - datetime.timedelta(days=i)
        a = str(a)
        date_time.append(a)
    date_time = tuple(date_time)
    print(date_time)
    cur.execute("USE job_CD")
    cur.execute("DROP TABLE if exists job_Detail")
    cur.execute("create table job_Detail (select w.job_Name,w.job_Wage,w.job_AverWage,w.company_Name,w.company_Nature,w.company_Scale,w.company_Address,c.company_Distance,c.company_Duration,c.company_Traffic,w.job_PeopleNum,w.job_Issue,w.job_Article,w.job_Link,w.job_Id,w.company_Id from company c left join work w on c.company_Id=w.company_Id )")
    cur.execute(
        "select job_Name,job_Wage,job_AverWage,company_Name,company_Nature,company_Scale,company_Address,company_Distance,company_Duration,company_Traffic,job_PeopleNum,job_Issue,left(job_Article,300),job_Link,job_Id,company_Id from job_Detail where (company_Duration<=2500 or company_Duration='') and (job_Issue in {0})".format(date_time))
    result = cur.fetchall()
    cur.execute(
        "select COLUMN_NAME from INFORMATION_SCHEMA.Columns where table_name='job_Detail' and table_schema='job_cd'")
    title = cur.fetchall()
    cur.close()
    conn.close()
    return result, title


def white_List():
    # 读取工作白名单
    with open("whitelist.csv", "r") as f:
        whitelist = set()
        f_csv = csv.reader(f)
        for line in f_csv:
            whitelist.add(line[0])
        whitelist = list(whitelist)
        print(whitelist)
    return whitelist


def black_List():
    # 读取工作的黑名单
    with open("blacklist.csv", "r") as f:
        blacklist = set()
        f_csv = csv.reader(f)
        for line in f_csv:
            blacklist.add(line[0])
        blacklist = list(blacklist)
        print(blacklist)
    return blacklist


if __name__ == "__main__":
    result, title = data_Filling()
    whitelist = white_List()
    blacklist = black_List()
    result = list(result)
    filling = []
    print(len(result))
    for i in result:
        for j in whitelist:
            if j in i[0]:
                filling.append(i)
                break
                print(i[0], j)
                break
    for a in filling:
        print(a[3], a[0])
        if a[15] in blacklist:
            print(a[15], a[3], a[0])
            filling.remove(a)
    for b in filling:
        print(b[3])
