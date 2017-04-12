from dataBase import build
from detail import job
from detail import job_Detial
from itools import job_AverWage, cooky, send_email, job_apply
import datetime
import codecs
import csv
from maps import coordinate, distance
import socket
import os
from datafilling import data_Filling, white_List, black_List

timeout = 20
socket.setdefaulttimeout(timeout)  # 这里对整个socket层设置超时时间。后续文件中如果再使用到socket，不必再设置


def run(jobarea, homeAddress, homeCity, email, income, subject, *args):
    for i in args:
        print(i)
        work = job(jobarea, i, homeAddress, homeCity, income)
        job_Links = work.job_Reader()
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

    # 进行数据整理
    result, title = data_Filling()

    # 生成结果CSV文件
    with codecs.open("job_Detail.csv", "w", encoding="utf_8_sig") as f:
        f_csv = csv.writer(f)
        f_csv.writerow(title)
        f_csv.writerows(result)
        print("文件生成完毕")
    while True:
        try:
            send_email('smtp.qq.com', '272861776@qq.com', 'xjsdroroibjacaej',
                       email, subject, '今天的工作邮件，请查收，最爱你的贝贝', 'job_Detail.csv')
        except Exception as e:
            print('发生邮件错误，错误原因为:', e)
        else:
            print("邮件发送成功")
            break
    while True:
        try:
            send_email('smtp.qq.com', '272861776@qq.com', 'xjsdroroibjacaej',
                       'louse12345@163.com', subject, '工作邮件的备份资料', 'job_Detail.csv')
        except Exception as e:
            print('发生邮件错误，错误原因为:', e)
        else:
            print("邮件发送成功")
            break
# 执行数据清理工作
    whitelist = white_List()
    blacklist = black_List()
    result = list(result)
    filling = []
    for i in result:
        for j in whitelist:
            if j in i[0]:
                filling.append(i)
                print(i[0], j)
                break
    for i in filling:
        if i[15] in blacklist:
            filling.remove(i)
            print("黑名单公司：", i[0], i[3])
    dellist = list(set(result) - set(filling))
# 将筛选后删除的结果列示出来
    with codecs.open("job_Del.csv", "w", encoding="utf_8_sig") as f:
        f_csv = csv.writer(f)
        f_csv.writerows(dellist)
        print("未进行自动投递的文件生成")
# 将筛选后的结果发送到邮箱
    with codecs.open("job_Filter.csv", "w", encoding="utf_8_sig") as f:
        f_csv = csv.writer(f)
        f_csv.writerow(title)
        f_csv.writerows(filling)
        print("自动投递文件生成")
    while True:
        try:
            send_email('smtp.qq.com', '272861776@qq.com', 'xjsdroroibjacaej',
                       'louse12345@163.com', "自动投递简历的列表", '清理后的工作清单', 'job_Filter.csv')
        except Exception as e:
            print('发生邮件错误，错误原因为:', e)
        else:
            print("邮件发送成功")
            break
# 自动投递简历功能
    a = input("是否需要进行自动投递？(Y/N)")
# 再次执行数据清理工作
    whitelist = white_List()
    blacklist = black_List()
    for i in filling:
        for j in whitelist:
            if j in i[0]:
                filling.append(i)
                print(i[0], j)
                break
    for i in filling:
        if i[15] in blacklist:
            filling.remove(i)
    if a == 'Y':
        headers = {
            'Host': 'm.51job.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
            'Accept': 'text/css,*/*;q=0.1',
            'Connection': 'keep-alive'
        }
        cookies = cooky('cookies1.txt')
        for i in filling:
            print(i[13], i[14])
            job_apply(i[13], headers, cookies, i[14])
            print("已投递企业信息：", i[0], i[3])

if __name__ == "__main__":
    jobarea = '090200'  # 提供基本参数，广东030000，四川090000，省会编码是0200
    keyword1 = "策划"
    keyword2 = "运营"
    keyword3 = "品牌"
    homeAddress = '锦江区东风路4号一栋一单元'
    homeCity = "成都"
    email = 'larkjoe@126.com'
    income = int('6000')
    subject = "宝宝鸡-{0}的工作记录，请查收".format(datetime.date.today())
    close = input("是否需要执行自动关机功能？(Y/N)")
    build()
    run(jobarea, homeAddress, homeCity, email,
        income, subject, keyword1, keyword2, keyword3)
    if close == "Y":
        os.system('shutdown -s -t 200')
