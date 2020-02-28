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
'''
## dd/mm/yyyy格式   我选yyyy//mm//dd格式
today = time.strftime("%Y/%m/%d")
print(today)
'''

#爬取的历史日期都是分4个时间段的，但是爬取的预测天气是全天的，所以，如果要输入的预测爬虫是在今天的前三天内，要用新的写法。因为爬取网站链接完全不同，虽然是一个网站

# 由于获得天气的网站，国内已有的并没有找到和湿度相关（一些仅仅提到最大相对湿度或者最小相对湿度），目前只了解到这一个网站，获取数据也只能精确到天津，没法到西青区，而日出日落数据是获得西青区本地的

# 获取前1天或N天的日期，beforeOfDay=1：前1天；beforeOfDay=N：前N天，beforeOfDay=0,today
def getdate(beforeOfDay):
    today = datetime.datetime.now()
    # 计算偏移量

    offset = datetime.timedelta(days=-beforeOfDay)
    # 获取想要的日期的时间
    re_date = (today + offset).strftime('%Y%m%d')

    return re_date


# 先不考虑打擦边球的情况，陷入沉思要考虑东西还挺多啊，该死的略略略，1到3天，我是不是还得写不一样的函数？hello，改个参数i啥的，小问题
#预测爬虫，获得想要的json数据
def get_datejson(year, month, date1):
    headers = {
        'User-Agent': '(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
    # url = 'https://www.timeanddate.com/weather/china/tianjin/historic?month=6&year=2019'
    # url='https://www.timeanddate.com/weather/china/'+str(report_country)+'/historic?month='+str(month)+'&year='+str(year)
    #url = 'https://www.timeanddate.com/weather/china/tianjin/ext'  # 当天看到的天气链接和历史有很大不同    ，到底要获得的是之前那种预测还是当天，虽然是投入使用，但是那啥测试的时候预测果然还是以前的数据诶
    historycity_code=judge_district()
    url='https://www.timeanddate.com/weather/'+historycity_code+'/ext'
    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.text, 'html.parser')
    Data = soup.find_all('script', {'type': 'text/javascript'})
    data = Data[2].get_text()

    data1 = data[10:-1]
    data2 = data1.replace(';', '')
    # print(data2)
    # print(type(data2))
    # data2 = "".join([data2.strip().rsplit("}", 1)[0], "}"])
    date_json = json.loads(data2)
    return date_json

#这个爬虫是爬取今天后3天的天气情况
# 获取天气信息
# def get_date(year, month, report_country, output_path):
def get_weather(year, month, date1, output_path):
    # date_json = get_datejson(year, month, report_country)
    date_json = get_datejson(year, month, date1)

    detail = date_json['detail']#除了日出，日落之外的数据，最高温度，最低温度，湿度，风力，气压，预测天气没有气压，考虑删掉气压

    num = 3
    date = []
    temp = []
    templow = []
    barometer = []
    wind = []
    humidity = []
    desc = []
    '''
    print(year)
    print(month)
    print(date1)
    str1=str(year)+'/'+str(int(month))+'/'+str(date1)

    print(str1)
    '''
    for i in range(0, num):

        if 'ds' in detail[i].keys():
            str1 = str(year) + '/' + str(int(month)) + '/' + str(date1 + i + 1)#修改日期格式
            # date.append(detail[i]['ds'])
            date.append(str1)
        else:
            date.append('未知')
        if 'temp' in detail[i].keys():
            temp.append(detail[i]['temp'])
        else:
            temp.append('未知')
        if 'templow' in detail[i].keys():
            templow.append(detail[i]['templow'])
        else:
            templow.append('未知')
        if 'baro' in detail[i].keys():
            barometer.append(detail[i]['baro'])
        else:
            barometer.append('未知')
        if 'wind' in detail[i].keys():
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

    #爬取日出日落时间
    # url1 = 'https://richurimo.51240.com/xiqingqu__time__2019_07__richurimo/'
    url1 = 'https://richurimo.51240.com/xiqingqu__time__' + str(year) + '_' + month + '__richurimo/'
    response1 = requests.get(url1)
    # print(response.text)
    soup = BeautifulSoup(response1.content, 'lxml')

    table = soup.find_all('table')[0]
    # print(table)
    df = pd.read_html(str(table))[1]
    # print(df)
    i = df.shape[0]

    num = date1

    sunrise = []
    sunset = []
    for i in df[1].tolist()[num + 1:num + 4]:
        sunrise.append(i)
    for m in df[3].tolist()[num + 1:num + 4]:
        sunset.append(m)

    #写成字典，一次性转换
    dict = {'date': date,
            'temp': temp,
            'templow': templow,
            'barometer': barometer,
            'wind': wind,
            'humidity': humidity,
            'describe': desc,
            'sunrise': sunrise,
            'sunset': sunset
            }
    dataframe = pd.DataFrame(dict)
    print(dataframe)
'''
    # D:/weather_report.csv这种判断的写法，主要是因为是不覆盖，接着以前写的，就有最上面的索引要不要删掉的问题
    if os.path.exists("D:/weather_report1.csv"):
        dataframe.to_csv('D:\\weather_report1.csv', mode='a', sep=',', header=None, index=None,
                         encoding="utf_8_sig")
    else:
        dataframe.to_csv('D:\\weather_report1.csv', mode='a', sep=',', index=None, encoding="utf_8_sig")
    # return (new_df_temperature_report)
'''
#爬取的今天的前三天往前走的天气数据（随便写的，总是要用，）
# 爬虫主体函数
def predicthistory(year, month, date, output_path):
    headers = {
        'User-Agent': '(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}

    #url = 'https://www.timeanddate.com/weather/china/tianjin/historic?month=' + str(month) + '&year=' + str(year)
    historycity_code=judge_district()
    url='https://www.timeanddate.com/weather/'+historycity_code+'/historic?month=' + str(month) + '&year=' + str(year)
    response = requests.get(url, headers=headers)
    j = date  # 判断后面的温度数据和日出日落数据，从哪个位置取
    soup = BeautifulSoup(response.text, 'html.parser')
    # Data=json.loads(soup.find('script', {'type': 'text/javascript'}).get_text())["detail"]
    # print(Data)
    # jd = json.loads(comments.text.strip('var data=')) #移除改var data=将其变为json数据
    Data = soup.find_all('script', {'type': 'text/javascript'})
    # print(Data[3])
    # print(Data[3].get_text())
    data = Data[3].get_text()

    # print(data.replace(';window.month=5;window.year=2019;',''))
    s = ';window.month=' + str(month) + ';window.year=' + str(year) + ';'
    data1 = data.replace(s, '')
    data2 = data1.replace('var data=', '')
    # print(data2)
    # print(type(data2))
    # date_json1= json.loads(data2)
    data2 = "".join([data2.strip().rsplit("}", 1)[0], "}"])
    date_json1 = json.loads(data2)
    # print(date_json['detail'])
    detail = date_json1['detail']
    num = 3
    date = []
    temp = []
    templow = []
    barometer = []
    wind = []
    humidity = []
    desc = []

    for i in range(j * 4, j * 4 + num * 4):

        if 'ds' in detail[i].keys():
            pattern = re.compile(r'\d+')#修改天气格式
            number = pattern.findall(detail[i]['ds'], 1, 7)
            today = "".join(number)
            time = pattern.findall(detail[i]['ds'], 15, 19)

            if time[0] == '00':
                time = '0:00'
            elif time[0] == '06':
                time = '6:00'
            elif time[0] == '12':
                time = '12:00'
            else:
                time = '18:00'

            str1 = str(year) + '/' + str(int(month)) + '/' + today+' '+time
            # date.append(detail[i]['ds'])
            date.append(str1)
            #date.append(detail[i]['ds'])
        else:
            date.append('未知')
        if 'temp' in detail[i].keys():
            temp.append(detail[i]['temp'])
        else:
            temp.append('未知')
        if 'templow' in detail[i].keys():
            templow.append(detail[i]['templow'])
        else:
            templow.append('未知')
        if 'baro' in detail[i].keys():
            barometer.append(detail[i]['baro'])
        else:
            barometer.append('未知')
        if 'wind' in detail[i].keys():
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

    # url1 = 'https://richurimo.51240.com/xiqingqu__time__2019_06__richurimo/'

    #url1 = 'https://richurimo.51240.com/xiqingqu__time__' + str(year) + '_0' + str(month) + '__richurimo/'
    if len(str(month)) == 2:
        url1 = url1 = 'https://richurimo.51240.com/xiqingqu__time__' + str(year) + '_' + str(month) + '__richurimo/'
    else:
        url1 = 'https://richurimo.51240.com/xiqingqu__time__' + str(year) + '_0' + str(month)+ '__richurimo/'
    print(url1)
    response1 = requests.get(url1)
    # print(response.text)
    soup = BeautifulSoup(response1.content, 'lxml')

    table = soup.find_all('table')[0]
    # print(table)
    df = pd.read_html(str(table))[1]
    # print(df)
    i = df.shape[0]

    sunrise = []
    sunset = []

    for i in df[1].tolist()[j + 1:j + 4]:
        for j in range(0, 4):
            sunrise.append(i)
    for m in df[3].tolist()[j + 1:j + 4]:
        for n in range(0, 4):
            sunset.append(m)

    dict = {'date': date,
            'temp': temp,
            'templow': templow,
            'barometer': barometer,
            'wind': wind,
            'humidity': humidity,
            'describe': desc,
            'sunrise': sunrise,
            'sunset': sunset}
    dataframe = pd.DataFrame(dict)
    print(dataframe)
    '''
    if os.path.exists("D:/prehistory.csv"):
        dataframe.to_csv('D:\\prehistory.csv', mode='a', sep=',', header=None, index=None,
                         encoding="utf_8_sig")
    else:
        dataframe.to_csv('D:\\prehistory.csv', mode='a', sep=',', index=None, encoding="utf_8_sig")
    '''
#def prehourbyhour(year,month,date,output_path):
def prehourbyhour():
    headers = {
        'User-Agent': '(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}

    # url = 'https://www.timeanddate.com/weather/china/tianjin/historic?month=' + str(month) + '&year=' + str(year)
    historycity_code=judge_district()
    url = 'https://www.timeanddate.com/weather/'+historycity_code+'/hourly'  # 实时变化的哦，根据现在的时间，如果是9.59最先显示的就是今天10点时间段，如果已经10点，时间段会更新为11点哦。
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    # Data=json.loads(soup.find('script', {'type': 'text/javascript'}).get_text())["detail"]
    # print(Data)
    # jd = json.loads(comments.text.strip('var data=')) #移除改var data=将其变为json数据
    Data = soup.find_all('script', {'type': 'text/javascript'})
    # print(Data[3])
    # print(Data[3].get_text())
    data = Data[2].get_text()
    #print(data)
    data2 = data.replace('var data=', '')
    # print(data2)
    # print(type(data2))
    # date_json1= json.loads(data2)
    data2 = "".join([data2.strip().rsplit("}", 1)[0], "}"])
    date_json1 = json.loads(data2)
    # print(date_json['detail'])
    detail = date_json1['detail']
    print(detail)
    today = datetime.datetime.now()
    #timenow=today.strftime('%Y%m%d%H')
    #print(timenow)
    hour=today.strftime('%H')#获得现在几点，目前输出的10点，所以链接是从11开始的，判断从哪个位置取第二天的。
    #print(hour)
    year=today.strftime('%Y')
    month=today.strftime('%m')
    datenow=today.strftime('%d')
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

        if 'ds' in detail[i].keys():
            #date=year+'/'+month+'/'+detail[i]['ds'][2:4]
            date1=year + '/' + month + '/' + detail[i]['ds'][3:5]+' '+detail[i]['ds'][10:15]
            #date.append(detail[i]['ds'])
            date.append(date1)
        else:
            date.append('未知')
        if 'temp' in detail[i].keys():
            temp.append(detail[i]['temp'])
        else:
            temp.append('未知')
        if 'templow' in detail[i].keys():
            templow.append(detail[i]['templow'])
        else:
            templow.append('未知')
        if 'baro' in detail[i].keys():
            barometer.append(detail[i]['baro'])
        else:
            barometer.append('未知')
        if 'wind' in detail[i].keys():
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
    #print(len(date),len(temp),len(templow),len(barometer),len(wind),len(humidity),len(desc))
    print(date, temp, templow)
    print(len(date), len(temp), len(templow))
    #print(month)出现的07哦
    if len(month) == 2:
        url1 = 'https://richurimo.51240.com/xiqingqu__time__' + year + '_' + month + '__richurimo/'
    else:
        url1 = 'https://richurimo.51240.com/xiqingqu__time__' + year + '_0' + month + '__richurimo/'
    # print(url1)
    response1 = requests.get(url1)
    # print(response.text)
    soup = BeautifulSoup(response1.content, 'lxml')

    table = soup.find_all('table')[0]
    # print(table)
    df = pd.read_html(str(table))[1]
    # print(df)
    i = df.shape[0]

    sunrise = []
    sunset = []
    for i in df[1].tolist()[int(datenow)+1:int(datenow)+4]:
        for j in range(0,24):
            sunrise.append(i)
    for m in df[3].tolist()[int(datenow)+1:int(datenow)+4]:
        for n in range(0,24):
            sunset.append(m)
    print(sunrise, sunset)

    dict = {'date': date,
            'temp': temp,
            'templow': templow,
            'barometer': barometer,
            'wind': wind,
            'humidity': humidity,
            'describe': desc,
            'sunrise': sunrise,
            'sunset': sunset}
    dataframe = pd.DataFrame(dict)
    print(dataframe)
    output_path="D:/hour.csv"
    if os.path.exists(output_path):
        dataframe.to_csv(output_path, mode='a', sep=',', header=None, index=None,
                         encoding="utf_8_sig")
    else:
        dataframe.to_csv(output_path, mode='a', sep=',', index=None, encoding="utf_8_sig")
# dataframe.to_csv('D:\\data1.csv', mode='a', sep=',', header=None, index=None, encoding="utf_8_sig")
# 预测爬虫结束

# 历史温度数据的爬取，这个就算是正常的历史温度爬取，上面那个预测以前的主要是算是做测试时候用
def history_crawler(year, month, output_path):
    # date_json = get_datejson(year, month, report_country)
    historycity_code=judge_district()
    # url = 'https://www.timeanddate.com/weather/china/tianjin/historic?month=6&year=2019'
    # url='https://www.timeanddate.com/weather/china/'+str(report_country)+'/historic?month='+str(month)+'&year='+str(year)
    headers = {
        'User-Agent': '(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}
    for year in year:
        for j in range(0, len(month)):
            # url = 'https://www.timeanddate.com/weather/@1789395/historic?month=' + month[j] + '&year=' + str(year)
            url = 'https://www.timeanddate.com/weather/'+historycity_code+'/historic?month=' + month[j] + '&year=' + str(year)
            # https://www.timeanddate.com/weather/@1789395/historic?month=5&year=2019
            #print(url)
            response = requests.get(url, headers=headers)

            soup = BeautifulSoup(response.text, 'html.parser')
            # Data=json.loads(soup.find('script', {'type': 'text/javascript'}).get_text())["detail"]
            # print(Data)
            # jd = json.loads(comments.text.strip('var data=')) #移除改var data=将其变为json数据
            Data = soup.find_all('script', {'type': 'text/javascript'})
            # print(Data[3])
            # print(Data[3].get_text())
            data = Data[3].get_text()

            # print(data.replace(';window.month=5;window.year=2019;',''))
            s = ';window.month=' + month[j] + ';window.year=' + str(year) + ';'
            #print(s)
            data1 = data.replace(s, '')
            data2 = data1.replace('var data=', '')
            # print(data2)
            # print(type(data2))
            # date_json1= json.loads(data2)
            data2 = "".join([data2.strip().rsplit("}", 1)[0], "}"])
            date_json1 = json.loads(data2)
            # print(date_json['detail'])
            detail = date_json1['detail']
            print(detail)
            num = len(detail)
            date = []
            temp = []
            templow = []
            barometer = []
            wind = []
            humidity = []
            desc = []
            for i in range(0, num):

                if 'ds' in detail[i].keys():
                    date.append(detail[i]['ds'])
                else:
                    date.append('未知')
                if 'temp' in detail[i].keys():
                    temp.append(detail[i]['temp'])
                else:
                    temp.append('未知')
                if 'templow' in detail[i].keys():
                    templow.append(detail[i]['templow'])
                else:
                    templow.append('未知')
                if 'baro' in detail[i].keys():
                    barometer.append(detail[i]['baro'])
                else:
                    barometer.append('未知')
                if 'wind' in detail[i].keys():
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
            print(len(date),
                  len(temp),
                  len(templow),
                  len(barometer),
                  len(wind),
                  len(humidity),
                  len(desc))
            #print(date)
            # url1 = 'https://richurimo.51240.com/xiqingqu__time__2019_06__richurimo/'
            print(len(month[j]))
            if len(month[j]) == 2:
                url1 = 'https://richurimo.51240.com/xiqingqu__time__' + str(year) + '_'+month[j] + '__richurimo/'
            else:
                url1 = 'https://richurimo.51240.com/xiqingqu__time__' + str(year) + '_0' + month[j] + '__richurimo/'
            #print(url1)
            response1 = requests.get(url1)
            # print(response.text)
            soup = BeautifulSoup(response1.content, 'lxml')

            table = soup.find_all('table')[0]
            # print(table)
            df = pd.read_html(str(table))[1]
            # print(df)
            i = df.shape[0]

            sunrise = []
            sunset = []
            for i in df[1].tolist()[1:]:
                for j in range(0, 4):
                    sunrise.append(i)
            for m in df[3].tolist()[1:]:
                for n in range(0, 4):
                    sunset.append(m)
            print(len(sunrise),len(sunset))
            dict = {'date': date,
                    'temp': temp,
                    'templow': templow,
                    'barometer': barometer,
                    'wind': wind,
                    'humidity': humidity,
                    'describe': desc,
                    'sunrise': sunrise,
                    'sunset': sunset}
            dataframe = pd.DataFrame(dict)
            print(dataframe)
        '''
        if os.path.exists("D:/history_temperature.csv"):
            dataframe.to_csv('D:\\history_temperature.csv', mode='a', sep=',', header=None, index=None,
                             encoding="utf_8_sig")
        else:
            dataframe.to_csv('D:\\history_temperature.csv', mode='a', sep=',', index=None, encoding="utf_8_sig")
        '''
# dataframe.to_csv('D:\\data1.csv', mode='a', sep=',', header=None, index=None, encoding="utf_8_sig")
def path(input_path):
    curPath = os.path.abspath(os.path.dirname(__file__))
    rootPath = curPath[:curPath.find("aj\\") + len("aj\\")]
    dataPath = os.path.abspath(rootPath + input_path)
    return dataPath

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


#主体爬虫，看是爬那种
def crawler(predict_date=False, output_path="D:/temperature.csv"):
    if predict_date == False :
        # years = 2019  #
        #years = list(range(2019, datetime.datetime.now().year+1))
        years = list(range(2018, datetime.datetime.now().year ))
        print(years)
        # months = ['3', '4']  # '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
        months = ['3','4','6']
        history_crawler(year=years, month=months,
                        output_path=output_path)
        print(1)
    if predict_date >= getdate(3):
        print(2)
        year = int(predict_date[0:4])
        month = predict_date[4:6]
        date = int(predict_date[6:8])
        # print(year,month,date)
        get_weather(year=year, month=month, date1=date,
                    output_path=output_path)

    if predict_date < getdate(3):
        year = int(predict_date[0:4])
        month = int(predict_date[4:6])
        date = int(predict_date[6:8])
        predicthistory(year=year, month=month, date=date,
                       output_path=output_path)
        print(3,year,month,date)
    # else:
    #     print(3)
    #     year = int(predict_date[0:4])
    #     month = predict_date[4:6]
    #     date = int(predict_date[6:8])
    #     # print(year,month,date)
    #     get_weather(year=year, month=month, date1=date,
    #                 output_path=output_path)


# crawler(input_common_config_path=sys.argv[1], input_private_config_path=sys.argv[2],
#  		project_code=sys.argv[3],predict_date=sys.argv[4],output_path=sys.argv[5])

if __name__ == '__main__':
    '''
	crawler(input_common_config_path="D:\\Power\\codes\\web_test\\dlwd\\common_config.csv",
			input_private_config_path="123",
			project_code=" dlwd",
			predict_date = False,
			output_path = "D:/Power/codes/web_test/dlwd/history_temperature.csv" )
	crawler(input_common_config_path="D:\\Power\\codes\\web_test\\dlwd\\common_config.csv",
			input_private_config_path="123",
			project_code="dlwd",
			predict_date = "201808",
			output_path = "D:/Power/codes/web_test/dlwd/weather_report.csv" )
    '''
    # get_datejson(2019,5)

    #crawler(predict_date=False,output_path="D:/history_temperature.csv")

    # crawler(predict_date="20190717", output_path="D:/weather_report.csv")




    # print(getdate(0))
    # crawler(predict_date=False, output_path="D:/temperature.csv")
    crawler(predict_date='20190907',output_path="D:/prehistory.csv")


    # prehourbyhour()
    # p=judge_district()
    # print(p)