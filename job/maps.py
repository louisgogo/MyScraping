


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

# 获取工作地点的坐标信息：


def coordinate():
    lng = ''
    lat = ''
    cur.execute("DROP TABLE if exists company")
    cur.execute("CREATE table company(select company_Id,company_Name,company_Scale,company_Area,company_Address FROM work where company_Scale not in ('50-150人','少于50人') GROUP BY company_id,company_Address)")
    cur.execute("ALTER TABLE company ADD COLUMN(company_x VARCHAR(300),company_y VARCHAR(300),company_Distance VARCHAR(300),company_Duration VARCHAR(300),company_Traffic VARCHAR(300))")
    cur.execute(
        "SELECT company_Id,company_Area,company_Address,company_Name FROM company WHERE company_x is null")
    result = cur.fetchall()
    for i in result:
        count = 0
        (company_Id, company_Area, company_Address, company_Name) = i
        city = company_Area[0:2]
        print(city + "," + company_Address)
        while True:
            try:
                count += 1
                if count == 2:
                    print("无法查到该地址信息，跳过该条信息继续...")
                    break
                if len(company_Address) < 4:
                    (lng, lat) = getAddress(company_Name, city)
                    print("地址不准确，按照公司名称查找坐标")
                else:
                    (lng, lat) = getAddress(company_Address, city)
                    print(lng, lat)
            except AttributeError:
                print('对象丢失，重新寻找')
                company_Address = ''
            else:
                break
        cur.execute("UPDATE company SET company_x='{0}',company_y='{1}' WHERE company_Id='{2}'".format(
            str(lng), str(lat), company_Id))
    conn.commit()

# 获取工作信息，计算家和该工作地点的直线距离


def distance(homeAddress, homeCity):
    (lng, lat) = getAddress(homeAddress, homeCity)
    lon1 = round(lng, 6)
    lat1 = round(lat, 6)
    cur.execute("SELECT company_Id,company_x,company_y from company WHERE company_Distance is null or company_Distance='' and company_x is not null")
    result = cur.fetchall()
    for i in result:
        (company_Id, company_x, company_y) = i
        if company_x != '' and company_y != '':
            (company_Id, company_x, company_y) = i
            lon2 = round(eval(company_x), 6)
            lat2 = round(eval(company_y), 6)
            company_Distance, company_Duration, company_Traffic = getDistance_and_Duration(
                lon1, lat1, lon2, lat2)
            print(company_Distance, company_Duration, company_Traffic)
            cur.execute("UPDATE company SET company_Distance='{0}',company_Duration='{1}',company_Traffic='{2}' WHERE company_Id='{3}'".format(
                company_Distance, company_Duration, company_Traffic, company_Id))
    conn.commit()
