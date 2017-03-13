from urllib.parse import quote
from urllib.request import urlopen
import json
from dataBase import connection
import heapq

conn = connection()
cur = conn.cursor()

# 根据地址和城市获取坐标信息


def getAddress(address, city):
    address = quote(address)
    city = quote(city)
    ak = 'VNZwxcMAU5gFt3VeKL5p28EsBg4vvEsw'
    html = 'http://api.map.baidu.com/geocoder/v2/?address=%s&city=%s&output=json&ak=%s' % (
        address, city, ak)
    while True:
        try:
            u = urlopen(html).read()
        except Exception as e:
            print("异常原因：", e)
        else:
            break
    resp = json.loads(u.decode('utf-8'))
    lng = resp.get('result').get('location').get('lng')
    lat = resp.get('result').get('location').get('lat')
    return lng, lat

# 利用百度地图获取两个坐标之间的距离和时间


def getDistance_and_Duration(lon1, lat1, lon2, lat2):
    ak = 'VNZwxcMAU5gFt3VeKL5p28EsBg4vvEsw'
    html = 'http://api.map.baidu.com/direction/v2/transit?origin=%s,%s&destination=%s,%s&output=json&ak=%s' % (
        lat1, lon1, lat2, lon2, ak)
    while True:
        try:
            u = urlopen(html).read()
        except Exception as e:
            print("异常原因：", e)
        else:
            break
    resp = json.loads(u.decode('utf-8'))
    try:
        if resp.get('result').get('routes') == []:
            distance = resp.get('result').get('taxi').get("distance")
            duration = resp.get('result').get('taxi').get("duration")
            company_Traffic = "的士"
        else:
            heap = []
            heapq.heapify(heap)
            for i in resp.get('result').get('routes'):
                distance = i.get("distance")
                duration = i.get("duration")
                a = (duration, distance)
                heapq.heappush(heap, a)
            (duration, distance) = heap[0]
            company_Traffic = "公共交通"
    except Exception as e:
        print("出现错误，原因为：", e)
        distance = ''
        duration = ''
        company_Traffic = ''
    return (distance, duration, company_Traffic)


if __name__ == "__main__":
    homeAddress, homeCity = '锦江区东风路4号一栋一单元', "成都"
    print(getAddress(homeAddress, homeCity))
    print(getDistance_and_Duration(104.09595265862312,
                                   30.659046694795734, 112.09595265862312, 35.659046694795734))
