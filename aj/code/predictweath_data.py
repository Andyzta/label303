# https://www.timeanddate.com/weather/@1789395/historic?month=5&year=2019
#https://www.timeanddate.com/weather/@1789395/ext

import pandas as pd
import json
import requests
import time
import datetime

import os
from bs4 import BeautifulSoup
import  re
import platform
'''
## dd/mm/yyyy格式   我选yyyy//mm//dd格式
today = time.strftime("%Y/%m/%d")
print(today)
'''

#爬取的历史日期都是分4个时间段的，但是爬取的预测天气是全天的，所以，如果要输入的预测爬虫是在今天的前三天内，要用新的写法。因为爬取网站链接完全不同，虽然是一个网站

# 由于获得天气的网站，国内已有的并没有找到和湿度相关（一些仅仅提到最大相对湿度或者最小相对湿度），目前只了解到这一个网站，获取数据也只能精确到天津，没法到西青区，而日出日落数据是获得西青区本地的

# 获取前1天或N天的日期，beforeOfDay=1：前1天；beforeOfDay=N：前N天，beforeOfDay=0,today
# def path(input_path):
#     curPath = os.path.abspath(os.path.dirname(__file__))
#     rootPath = curPath[:curPath.find("aj\\") + len("aj\\")]
#     dataPath = os.path.abspath(rootPath + input_path)
#     print(dataPath)
#     return dataPath
def path(input_path):




    curPath = os.path.abspath(os.path.dirname(__file__))
    if platform.system().lower() == 'windows':
        rootPath = curPath[:curPath.find("aj\\") + len("aj\\")]
        # print("windows")
    elif platform.system().lower() == 'linux':
        rootPath = curPath[:curPath.find("aj/") + len("aj/")]
        # print("linux")

    dataPath = os.path.abspath(rootPath + input_path)
    # print(dataPath)
    return dataPath

def getdate(beforeOfDay):
    today = datetime.datetime.now()
    # 计算偏移量

    offset = datetime.timedelta(days=-beforeOfDay)
    # 获取想要的日期的时间
    re_date = (today + offset).strftime('%Y%m%d')

    return re_date






def prehourbyhour(predict_date='hour',output_path=path('data/prehour.csv')):
    headers = {
        'User-Agent': '(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}

    # url = 'https://www.timeanddate.com/weather/china/tianjin/historic?month=' + str(month) + '&year=' + str(year)
    historycity_code=judge_district()
    url = 'https://www.timeanddate.com/weather/'+historycity_code+'/hourly'  # 实时变化的哦，根据现在的时间，如果是9.59最先显示的就是今天10点时间段，如果已经10点，时间段会更新为11点哦。

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    Data = soup.find_all('script', {'type': 'text/javascript'})
    for i in range(0,4):
        if(Data[i].get_text().lstrip()[:8]=='var data'):
            data=Data[i].get_text()
    data2 = data.replace('var data=', '')

    data2 = "".join([data2.strip().rsplit("}", 1)[0], "}"])
    date_json1 = json.loads(data2)
    dwtemp = date_json1['units']['temp']
    dwwind = date_json1['units']['wind']
    detail = date_json1['detail']


    # print(detail)
    today = datetime.datetime.now()
    # print(today)
    hour=today.strftime('%H')#获得现在几点，目前输出的10点，所以链接是从11开始的，判断从哪个位置取第二天的。
    year=today.strftime('%Y')
    month=today.strftime('%m')
    datenow=today.strftime('%d')
    if(month=='12' and datenow=='31' ):
        year=str(int(year)+1);
        month='1'
    if ((int(year) % 4 == 0 and int(year) % 100 != 0) or (int(year) % 400 == 0)):
        flag=1;
    else:
        flag=0;
    #判断月末
    if(datenow=='31'):
        month=str(int(month)+1)
    elif(datenow=='30' and (month=='4' or month=='6' or month=='9' or month=='11')):
        month = str(int(month) + 1)
    elif((datenow=='29' and month=='2') or (flag==1 and datenow=='29' and month=='2')):
        month = str(int(month) + 1)
    num = 72
    date = []
    temp = []
    templow = []
    barometer = []
    wind = []
    humidity = []
    desc = []
    #date1 = year + '/' + month + '/' + detail[0]['ds'][3:5]+' '+detail[0]['ds'][10:15]
    #print(date1)

    for i in range(24-int(hour)-1,24-int(hour)-1+num):
#爬虫获得的数据，温度和风力的单位都不同，导致值不同
        if 'ds' in detail[i].keys():
            if platform.system().lower() == 'windows':

                date1 = year + '/' + month + '/' + detail[i]['ds'][3:5] + detail[i]['ds'].split(',')[-1]

                date.append(date1)

            if platform.system().lower() == 'linux' and detail[i]['ds'].endswith("m"):

                date1day = re.findall('\s\d+\s', detail[i]['ds'])[0].strip()
                date1hour=detail[i]['ds'].split('at')[-1]

                date1 = year + '/' + month + '/' + date1day + ' ' + date1hour

            # date.append(detail[i]['ds'])
                date.append(date1)
            if platform.system().lower() == 'linux' and detail[i]['ds'].endswith("0"):
                date1 = year + '/' + month + '/' + detail[i]['ds'][3:5] + detail[i]['ds'].split(',')[-1]

                date.append(date1)



        else:
            date.append('未知')
        if 'temp' in detail[i].keys():
            if dwtemp!='°C':
                templinux='%.1f'%((5/9)*(float(detail[i]['temp'])-32))
                temp.append(str(templinux))

            elif dwtemp=='°C':
                temp.append(detail[i]['temp'])
        else:
            temp.append('未知')
        # if 'templow' in detail[i].keys():
        #     templow.append(detail[i]['templow'])
        # else:
        #     templow.append('未知')
        # if 'baro' in detail[i].keys():
        #     barometer.append(detail[i]['baro'])
        # else:
        #     barometer.append('未知')
        if 'wind' in detail[i].keys():
            if dwwind!='km/h':
                windlinux=str('%.1f'%(1.609344*(float(detail[i]['wind']))))
                wind.append(windlinux)
            elif dwwind=='km/h':
                wind.append(detail[i]['wind'])


        else:
            wind.append('未知')
        if 'hum' in detail[i].keys():
            humidity.append(detail[i]['hum'])
        else:
            humidity.append('未知')
        if 'desc' in detail[i].keys():
            desc.append(detail[i]['desc'])
        else:
            desc.append('未知')
    # print(date)
    # print(temp,wind)
    dict = {'SDATE': date,
            'temp': temp,

            'wind': wind,
            'humidity': humidity,
            'describe': desc,
            'is_holiday':is_Holiday(),
            # 'sunrise': sunrise,
            # 'sunset': sunset
            }
    dict1 = {
            'temp': temp,
            'humidity': humidity,
            }
    dataframe = pd.DataFrame(dict)
    dataframe1 = pd.DataFrame(dict1)
    dataframe1 = dataframe1.loc[0:23, ]
    #print(dataframe)

    # output_path="D:/hour.csv"
    dataframe.to_csv(output_path, sep=',',  index=None, encoding="utf_8_sig")  # 改成覆盖
# dataframe.to_csv('D:\\data1.csv', mode='a', sep=',', header=None, index=None, encoding="utf_8_sig")
    for root, dirs, files in os.walk('../data'):
        for name in files:
            if name.startswith("ycws"):  # 指定要删除的格式，
                os.remove(os.path.join(root, name))
                #print("Delete File: " + os.path.join(root, name))
    output_path1 = "../data/ycws" + datetime.datetime.strftime(datetime.date.today(), '%Y-%m-%d') + ".json"
    if os.path.exists(output_path1):
        dataframe1.to_json(output_path1)
    else:
        dataframe1.to_json(output_path1)


# 历史温度爬虫完毕
# 判断是哪个地区
historycity_code=''
def judge_district(input_common_config='/data/common_config.csv',project_code="Default"):
    #z暂时project_code只有default
    if(project_code=='Default'):
        common_config_path=path(input_common_config)
        data=pd.read_csv(common_config_path)
        # history_city_code=data['history_city_code'][data['project_code']=='Default']
        # print(history_city_code)
        # return history_city_code
        for i in range(0,data.shape[0]):
            if(data['project_code'][i]=='default'):
                historycity_code=data['history_city_code'][i]

                # print(historycity_code)
        #

    return historycity_code
dataS=[]
is_holidaylist = []
def is_Holiday():

    start_date = dataS[0]
    end_date = dataS[len(dataS) - 1]
    # set_date = datetime.datetime.strptime(start_date,"%Y-%m-%d")

    is_mondaylist = []
    date = []
    '''判断当天日期是否为节假日'''
    rest_holiday=pd.read_csv(path('data/rest_holiday.csv'))
    rest_workday = pd.read_csv(path('data/rest_workday.csv'))

    # 把调休的休息日加到这里面
    # rest_holiday = [
    #     '2018-12-31',
    #     '2019-01-01', '2019-02-04', '2019-02-05', '2019-02-06', '2019-02-07', '2019-02-08',
    #     '2019-04-05', '2019-04-29', '2019-04-30', '2019-05-01', '2019-06-07', '2019-09-13',
    #     '2019-10-01', '2019-10-02', '2019-10-03', '2019-10-04', '2019-10-07', '2019-12-30',
    #     '2019-12-31',
    #
    # ]
    # # 把调休的工作日加到这里面
    # rest_workday = [
    #     '2019-02-02', '2019-02-03', '2019-04-27', '2019-02-28', '2019-09-29', '2019-10-12',
    #     '2019-12-28', '2019-12-29',
    #
    # ]
    for i in range(0, len(dataS)):
        set_date = datetime.datetime.strptime(dataS[i], "%Y-%m-%d")
        set_date_str = set_date.strftime('%Y-%m-%d')
        #         if set_date_str>end_date:
        #             break
        # 0~6代表周一~周日
        #         set_date_str=dataS[i]
        weekday = set_date.weekday()
        if set_date_str in rest_holiday or (weekday in [5, 6] and set_date_str not in rest_workday):
            is_holiday = 1
            is_monday = 0
        else:
            is_holiday = 0
            is_monday = 1
        '''
        #这里的sql语句可根据自己的需要进行调整
        sql="INSERT INTO dmdc.t_is_holiday(`date`,is_holiday,is_monday) VALUES ('%s',%s,%s);"%(set_date_str,is_holiday,is_monday)
        #把sql语句写入sql文件
        with open('./date_is_holiday.sql','a+') as f:
            f.write(sql+'\n')

         '''

        is_holidaylist.append(is_holiday)
        is_mondaylist.append(is_monday)
    return is_holidaylist

#主体爬虫，看是爬那种
def crawler(predict_date=False,output_path=path('data/prehour.csv')):

    # if predict_date == False :
    #     # years = 2019  #
    #     #years = list(range(2019, datetime.datetime.now().year+1))
    #     years = list(range(2018, datetime.datetime.now().year ))
    #     print(years)
    #     # months = ['3', '4']  # '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    #     months = ['3','4','6']
    #     history_crawler(year=years, month=months,
    #                     output_path=output_path)
    #     print(1)
    try:
        if predict_date == 'prehour':
            predict_d = datetime.datetime.strptime(getdate(0), "%Y%m%d")
            for i in range(1, 4):
                for j in range(0, 24):
                    delta = datetime.timedelta(days=i)
                    dataS.append(str(predict_d + delta)[:10])
            prehourbyhour(predict_date='hour', output_path=path('data/prehour.csv'))
        if predict_date >= getdate(3):
        # print(2)

            predict_d = datetime.datetime.strptime(predict_date, "%Y%m%d")

            for i in range(1, 4):
                delta = datetime.timedelta(days=i)
                dataS.append(str(predict_d + delta)[:10])
            year = int(predict_date[0:4])
            month = predict_date[4:6]
            date = int(predict_date[6:8])
        # print(year,month,date)
        # print(len(dataS))
            get_weather(year=year, month=month, date1=date,
                    output_path=output_path)

        if predict_date < getdate(3):

            predict_d = datetime.datetime.strptime(predict_date, "%Y%m%d")

            for i in range(1, 4):
                for j in range(0, 4):
                    delta = datetime.timedelta(days=i)
                    dataS.append(str(predict_d + delta)[:10])

            year = int(predict_date[0:4])
            month = int(predict_date[4:6])
            date = int(predict_date[6:8])
            predicthistory(year=year, month=month, date=date,
                       output_path=output_path)
    except Exception:
        pass
    print('%s' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    print("第二天天气数据爬取完成")
    # if predict_date >= getdate(3):
    #     # print(2)
    #
    #     predict_d = datetime.datetime.strptime(predict_date, "%Y%m%d")
    #
    #     for i in range(1, 4):
    #         delta = datetime.timedelta(days=i)
    #         dataS.append(str(predict_d + delta)[:10])
    #     year = int(predict_date[0:4])
    #     month = predict_date[4:6]
    #     date = int(predict_date[6:8])
    #     # print(year,month,date)
    #     print(len(dataS))
    #     get_weather(year=year, month=month, date1=date,
    #                 output_path=output_path)
    #
    # if predict_date < getdate(3):
    #     predict_d= datetime.datetime.strptime(predict_date, "%Y%m%d")
    #
    #     for i in range(1, 4):
    #         for j in range(0,4):
    #             delta = datetime.timedelta(days=i)
    #             dataS.append(str(predict_d + delta)[:10])
    #
    #     year = int(predict_date[0:4])
    #     month = int(predict_date[4:6])
    #     date = int(predict_date[6:8])
    #     predicthistory(year=year, month=month, date=date,
    #                    output_path=output_path)
        # print(3,year,month,date)
    # else:
    #     print(3)
    #     year = int(predict_date[0:4])
    #     month = predict_date[4:6]
    #     date = int(predict_date[6:8])
    #     # print(year,month,date)
    #     get_weather(year=year, month=month, date1=date,
    #                 output_path=output_path)
    return dataS

# crawler(input_common_config_path=sys.argv[1], input_private_config_path=sys.argv[2],
#  		project_code=sys.argv[3],predict_date=sys.argv[4],output_path=sys.argv[5])
#预测和历史的处理不能同时，分开运行，注释掉另外一行

if __name__ == '__main__':



   crawler(predict_date='prehour',output_path=path('data/prehour.csv'))




