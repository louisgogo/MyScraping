import pymysql


def connection():
    conn = pymysql.connect(host='127.0.0.1', port=3306,
                           user='root', passwd='888888', db='mysql', charset='utf8')
    return conn


def build():
    conn = connection()
    cur = conn.cursor()
    try:
        cur.execute("DROP DATABASE job_CD")
        cur.execute('CREATE DATABASE job_CD')
    except Exception as e:
        cur.execute('CREATE DATABASE job_CD')
    cur.execute('USE job_CD')
    # 建立数据库表格
    try:
        cur.execute('CREATE TABLE work (row_Id BIGINT(10) NOT NULL AUTO_INCREMENT,job_Id VARCHAR(200) NOT NULL,job_Name VARCHAR(200) ,job_Link VARCHAR(600),job_Wage VARCHAR(300),job_AverWage VARCHAR(200),company_Id VARCHAR(200),company_Name VARCHAR(200),company_Link VARCHAR(600),company_Nature VARCHAR(200),company_Scale VARCHAR(200),company_Area VARCHAR(400),company_Address VARCHAR(500),job_PeopleNum VARCHAR(400),job_Issue date,job_Article TEXT(20000),created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY (row_Id,job_Id))')
        cur.execute("CREATE TABLE workindex (row_Id BIGINT(10) NOT NULL AUTO_INCREMENT,job_Id VARCHAR(200) NOT NULL,job_Link VARCHAR(600),created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,PRIMARY KEY (row_Id,job_Id))")
        print("数据库建立完毕")
    except (AttributeError, pymysql.err.InternalError):
        print('TABLE已经存在')
    cur.close()
    conn.close()

if __name__ == '__main__':
    build()
