#导入模块
import datetime
import requests
import os
from bs4 import BeautifulSoup
import json
import pandas as pd
import platform
#判断是否是最后一个月
iflastm = False

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

def getdata(input_path=path('data/工况.csv')):
    # data = pd.read_excel(input_path)
    data = pd.read_csv(input_path)
    dict = {'时间': data['时间'],
            '操作': data['操作'],

            }
    dataframe = pd.DataFrame(dict)

    return dataframe





day = []
month = []
year = []
dataS = []

#拼接爬虫需要的年月日列表数据
def dealYMD():
    dataframe=getdata()
    startyear = dataframe['时间'][0][:4]
    endyear = dataframe['时间'][len(dataframe['时间']) - 1][:4]

    # endyear=2022
    # print(startyear,endyear,type(startyear))

    # else:
    #     i = int(endyear) - int(startyear)
    #     for j in range(0, i + 1):
    #         year.append(int(startyear) + j)
    # print(year,type(year))

    startmonth = dataframe['时间'][0][5:7]
    endmonth = dataframe['时间'][len(dataframe['时间']) - 1][5:7]
    if (int(startyear) == int(endyear)):
        month.append(startmonth)
        # 目前只是2019年，先这么写
        for i in range(1, int(endmonth) - int(startmonth)):
            month.append(str(int(startmonth) + i))
        month.append(endmonth)
    else:
        i = int(endyear) - int(startyear)
        if (int(startmonth) == 12):
            month.append(startmonth)

        else:
            i = 12 - int(startmonth)
            for j in range(0, i + 1):
                month.append(str(int(startmonth) + j))
        if (int(endyear) - int(startyear) == 1):
            pass
        else:
            for j in range(0, int(endyear) - int(startyear) - 1):
                for num in range(1, 13):
                    month.append(str(num))

        if (int(endmonth) == 1):
            month.append(endmonth)
        else:

            for j in range(1, int(endmonth) + 1):
                month.append(str(j))
    # month=['06','7','8','09']
    # print(month,type(year))

    startdate = dataframe['时间'][0][8:10]
    enddate = dataframe['时间'][len(dataframe['时间']) - 1][8:10]
    day.append(startdate)
    for i in range(0, len(month) - 2):
        day.append('1')
    day.append(enddate)

    if (int(startyear) == int(endyear)):
        for i in range(0, int(endmonth) - int(startmonth) + 1):
            year.append(endyear)
    else:
        i = int(endyear) - int(startyear)
        if (int(startmonth) == 12):
            year.append(startyear)

        else:
            i = 12 - int(startmonth)
            for j in range(0, i + 1):
                year.append(startyear)
        if (int(endyear) - int(startyear) == 1):
            pass
        else:
            for j in range(0, int(endyear) - int(startyear) - 1):
                for num in range(1, 13):
                    year.append(str(int(startyear) + j + 1))

        if (int(endmonth) == 1):
            year.append(str(endyear))
        else:

            for j in range(1, int(endmonth) + 1):
                year.append(str(endyear))
    # print(month,len(month))
    # print(day,len(day))
    # print(year,len(year))

    for i in range(0, len(dataframe['时间'])):
        dataS.append(dataframe['时间'][i][0:10])
    # print(year,month,day,dataS,len(dataS))
    return year,month,day,dataS

#地区判断
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
temp = []
templow = []
wind = []
humidity = []
desc = []
dss=[]
#爬虫函数
def history_crawlersameY(year,month,day,flag=len(temp)):
    dataframe = getdata()
    headers = {
        'User-Agent': '(Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36'}



    historycity_code=judge_district()
    url='https://www.timeanddate.com/weather/'+historycity_code+'/historic?month='+ month + '&year=' + year
    # url = 'https://www.timeanddate.com/weather/china/tianjin/historic?month=' + month + '&year=' + year
    # print(url)
    response = requests.get(url, headers=headers)

    soup = BeautifulSoup(response.text, 'html.parser')

    Data = soup.find_all('script', {'type': 'text/javascript'})
    for i in range(0,4):
        if(Data[i].get_text().lstrip()[:8]=='var data'):
            datadetail=Data[i].get_text()

    # datadetail = Data[3].get_text()

    s = ';window.month=' + str(month) + ';window.year=' + str(year) + ';'
    data1 = datadetail.replace(s, '')
    data2 = data1.replace('var data=', '')
    data2 = "".join([data2.strip().rsplit("}", 1)[0], "}"])
    date_json1 = json.loads(data2)

    detail = date_json1['detail']
    print(detail)
    # print(detail[0]['ds'][18:19],detail[0]['ds'][16:18])
    # print(detail[0]['ds'][19:20], detail[0]['ds'][17:19])
    if(iflastm==True):
        i=0
        num = (int(day)) * 4
        if (num > len(detail)):
            num = len(detail)
    else:
        num = len(detail)
        i = (int(day) - 1) * 4

    #这里还需要判断语句
    # if last:
    #     num=(int(day[0]) - 1) * 4
    #     i=0
    j = flag
    print(iflastm, i, j,num)
    print("----------------------------")
    # while j < len(dataS):
    while j < 425:
        try:

            # ds = []

            if dataframe['时间'][j][5:7] > str(int(month)):
                # print(dataframe['时间'][j][5:7])
                break
            if i == num:
                if (j < len(dataS) and (int(dataframe['时间'][j][11:13]) == (int(ds) + 6))):
                    if 'temp' in detail[i - 1].keys():
                        temp.append(detail[i - 1]['temp'])
                    else:
                        temp.append('未知')
                    if 'templow' in detail[i - 1].keys():
                        templow.append(detail[i - 1]['templow'])
                    else:
                        templow.append('未知')

                    if 'hum' in detail[i - 1].keys():
                        humidity.append(detail[i - 1]['hum'])
                    else:
                        humidity.append('未知')
                    if 'wind' in detail[i - 1].keys():
                        wind.append(detail[i - 1]['wind'])
                    else:
                        wind.append('未知')
                    if 'desc' in detail[i - 1].keys():
                        desc.append(detail[i - 1]['desc'])
                    else:
                        desc.append('未知')
                    if 'ds' in detail[i - 1].keys():
                        dss.append(detail[i - 1]['ds'])
                    else:
                        dss.append('未知')
                    j = j + 1

                else:
                    break
            if 'ds' in detail[i].keys():

                if platform.system().lower() == 'windows':
                    if detail[i]['ds'][18:19] == ':':
                        ds = detail[i]['ds'][16:18]
                    if detail[i]['ds'][19:20] == ':':
                        ds = detail[i]['ds'][17:19]
                    if detail[i]['ds'][20:21] == ':':
                        ds = detail[i]['ds'][18:20]



                while (int(dataframe['时间'][j][11:13]) < 6) and (int(ds) == 0):

                # 说明时间段相等
                #                 print(i,j,data['SDATE'][j])

                    print(detail[i]['ds'],i,j)
                    if 'temp' in detail[i].keys():
                        temp.append(detail[i]['temp'])
                    else:
                        temp.append('未知')
                    if 'templow' in detail[i].keys():
                        templow.append(detail[i]['templow'])
                    else:
                        templow.append('未知')

                    if 'hum' in detail[i].keys():
                        humidity.append(detail[i]['hum'])
                    else:
                        humidity.append('未知')
                    if 'wind' in detail[i].keys():
                        wind.append(detail[i]['wind'])
                    else:
                        wind.append('未知')
                    if 'desc' in detail[i].keys():
                        desc.append(detail[i]['desc'])
                    else:
                        desc.append('未知')
                    if 'ds' in detail[i].keys():
                        dss.append(detail[i]['ds'])
                    else:
                        dss.append('未知')
                    j = j + 1

                while (6 <= int(dataframe['时间'][j][11:13]) < 12) and (int(ds) == 6):
                # 说明时间段相等
                    print(detail[i]['ds'],i,j)
                #                 print(i,j,data['SDATE'][j])
                    if 'temp' in detail[i].keys():
                        temp.append(detail[i]['temp'])
                    else:
                        temp.append('未知')
                    if 'templow' in detail[i].keys():
                        templow.append(detail[i]['templow'])
                    else:
                        templow.append('未知')

                    if 'hum' in detail[i].keys():
                        humidity.append(detail[i]['hum'])
                    else:
                        humidity.append('未知')
                    if 'wind' in detail[i].keys():
                        wind.append(detail[i]['wind'])
                    else:
                        wind.append('未知')
                    if 'desc' in detail[i].keys():
                        desc.append(detail[i]['desc'])
                    else:
                        desc.append('未知')
                    if 'ds' in detail[i].keys():
                        dss.append(detail[i]['ds'])
                    else:
                        dss.append('未知')
                    j = j + 1
                while (12 <= int(dataframe['时间'][j][11:13]) < 18) and (int(ds) == 12):
                # 说明时间段相等
                #                 print(i,j)
                    print(detail[i]['ds'], i, j)
                    if 'temp' in detail[i].keys():
                        temp.append(detail[i]['temp'])
                    else:
                        temp.append('未知')
                    if 'templow' in detail[i].keys():
                        templow.append(detail[i]['templow'])
                    else:
                        templow.append('未知')

                    if 'hum' in detail[i].keys():
                        humidity.append(detail[i]['hum'])
                    else:
                        humidity.append('未知')
                    if 'wind' in detail[i].keys():
                        wind.append(detail[i]['wind'])
                    else:
                        wind.append('未知')
                    if 'desc' in detail[i].keys():
                        desc.append(detail[i]['desc'])
                    else:
                        desc.append('未知')
                    if 'ds' in detail[i].keys():
                        dss.append(detail[i]['ds'])
                    else:
                        dss.append('未知')
                    j = j + 1

                while (18 <= int(dataframe['时间'][j][11:13]) <= 24) and (int(ds) == 18):
                # 说明时间段相等
                    print(detail[i]['ds'], i, j)
                    if 'temp' in detail[i].keys():
                        temp.append(detail[i]['temp'])
                    else:
                        temp.append('未知')
                    if 'templow' in detail[i].keys():
                        templow.append(detail[i]['templow'])
                    else:
                        templow.append('未知')

                    if 'hum' in detail[i].keys():
                        humidity.append(detail[i]['hum'])
                    else:
                        humidity.append('未知')
                    if 'wind' in detail[i].keys():
                     wind.append(detail[i]['wind'])
                    else:
                     wind.append('未知')
                    if 'desc' in detail[i].keys():
                        desc.append(detail[i]['desc'])
                    else:
                        desc.append('未知')
                    if 'ds' in detail[i].keys():
                        dss.append(detail[i]['ds'])
                    else:
                        dss.append('未知')
                    j = j + 1
                i = i + 1
        except Exception:
            pass


    # print(dss)
    flag=len(temp)
    print(temp)
    print(templow)
    print(wind)
    print(humidity)
    dict = {'date':dss,
            'temphigh': temp,
            'templow': templow,

            'wind': wind,
            'humidity': humidity,

            }
    dataframe2 = pd.DataFrame(dict)
    print(dataframe2,j)
    # print(len(temp),iflastm,num,day)
    return temp,templow,humidity,wind,desc,dataframe2,flag



def dl_preprocession():
    dataframe = getdata()
    dealYMD()
    for i in range(0, len(month)):
        if (i == len(month) - 1):
            iflastm = True
        # for month in month:
        #     history_crawlersameY(year=2019, month=month, day='1', flag=len(temp))
        history_crawlersameY(year=year[i], month=month[i], day=day[i], flag=len(temp))

    dict = {'时间': dataframe['时间'],
            '操作': dataframe['操作'],

            'temphigh': temp,
            'templow': templow,

            'wind': wind,
            'humidity': humidity,

            }
    dataframe = pd.DataFrame(dict)
    print(dataframe)
    # dataframe.to_csv('D:\\总电量test.csv', sep=',', header=None, index=None,encoding="utf_8_sig")#改成覆盖
    dataframe.to_csv(path('data/工况test.csv'), sep=',', index=None, encoding="utf_8_sig")  # 改成覆盖
if __name__ == '__main__':
    # # getenergy()
    # dl_preprocession()
    # print(path('总电量.xlsx'))
    # if os.path.exists("D:/总电量test.csv"):
    #     dataframe.to_csv('D:\\总电量test.csv', mode='a', sep=',', header=None, index=None,
    #                      encoding="utf_8_sig")
    # else:
    #     dataframe.to_csv('D:\\总电量test.csv', mode='a', sep=',', index=None, encoding="utf_8_sig")

    dataframe = getdata()
    dealYMD()
    print(year,month,day)
    history_crawlersameY(year=year[2], month=month[2], day=day[2], flag=250)

    # dl_preprocession()
    # print(temp)
    # print(templow)

    # [0, 3, 3, 4, 4, 4, 3, -3, -3, 4, 4, 5, 3, 4]
    # [-2, -4, -4, 3, 3, 3, -4, -5, -5, -5, -5, 3, 2, 0]
    # [3, 2, 2, 8, 8, 8, 3, 2, 2, 2, 2, 7, 4, 5]
    # [47, 53, 53, 36, 36, 36, 60, 79, 79, 65, 65, 47, 58, 81]

# [1, 1, 1, -3, -6, -6, 3, 3, 3, 3, 5, 2, -3, -3, 5, 5, 5, 5, 7, 7, 3, 3, -3, -3, 6, 6, 6, 6, 9, 9, 9, 9, 2, 2, -2, -2, 2,
#  2, 2, 4, 4, 0, 0, 1, 0, 0, 2, 2, 2, 0, 3, 3, 3, 3, 3, 5, 5, 5, 1, 1, -4, -4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 3, 1, 1, 4, 4,
#  4, 4, 5, 5, 5, 5, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 2, 2, 2, 2, 2, 0, -2, -2, -1, 0, -3, -3, -1, -1, -1, -1, 1,
#  1, -2, -2, -4, -4, 1, 1, 1, 1, 2, 2, 0, 0, -2, -2, 0, 0, 3, 0, 2, 2, 2, 2, 4, 4, 1, 1, -2, -2, -3, -3, -1, -1, -2, -2,
#  0, 4, 4, 4, 4, 4, 1, 4, 5, 5, 0, 0, 5, 5]
# [-4, -4, -4, -7, -9, -9, -9, -9, -9, -9, 2, -4, -7, -7, -6, -6, -6, -6, 3, 3, -3, -3, -6, -6, -6, -6, -6, -6, 2, 2, 2,
#  2, -2, -2, -6, -6, -5, -5, -5, 2, 2, 0, 0, 0, 0, 0, -1, 0, 0, 0, -3, -3, -3, -3, -3, 1, 1, 1, -4, -4, -6, -6, -7, -7,
#  2, 2, 2, 2, 2, 2, 2, -1, -4, -4, -3, -3, -3, -3, 3, 3, 3, 3, 0, 0, -7, -7, -7, -7, -1, -1, -3, -3, -5, -5, -6, -6, -6,
#  -6, -1, -1, -1, -4, -8, -8, -8, -5, -5, -5, -10, -10, -10, -10, -2, -2, -7, -7, -9, -9, -10, -10, -10, -10, 0, 0, -2,
#  -2, -7, -7, -8, -8, 0, -4, -6, -6, -6, -6, 1, 1, -2, -2, -5, -5, -5, -5, -3, -3, -2, -2, -2, -1, -1, -1, -1, -1, -5,
#  -5, 0, 0, -6, -6, 1, 1]
# [3, 3, 3, 3, 1, 1, 4, 4, 4, 4, 8, 2, 2, 2, 5, 5, 5, 5, 6, 6, 3, 3, 1, 1, 2, 2, 2, 2, 9, 9, 9, 9, 3, 3, 3, 3, 7, 7, 7, 9,
#  9, 6, 6, 5, 10, 10, 13, 17, 17, 15, 21, 21, 21, 21, 21, 16, 16, 16, 3, 3, 2, 2, 2, 2, 8, 8, 8, 8, 8, 8, 8, 2, 2, 2, 9,
#  9, 9, 9, 13, 13, 13, 13, 13, 13, 12, 12, 12, 12, 4, 4, 3, 3, 2, 2, 4, 4, 4, 4, 10, 10, 10, 4, 14, 14, 22, 22, 8, 8, 7,
#  7, 7, 7, 6, 6, 2, 2, 2, 2, 2, 2, 2, 2, 8, 8, 4, 4, 3, 3, 4, 4, 5, 2, 2, 2, 2, 2, 6, 6, 6, 6, 6, 6, 8, 8, 9, 9, 9, 9,
#  19, 13, 13, 13, 7, 7, 15, 17, 9, 9, 4, 4, 4, 4]
# [34, 34, 34, 63, 79, 79, 63, 63, 63, 63, 24, 44, 63, 63, 46, 46, 46, 46, 26, 26, 46, 46, 71, 71, 60, 60, 60, 60, 30, 30,
#  30, 30, 78, 78, 91, 91, 84, 84, 84, 65, 65, 95, 94, 91, 87, 87, 75, 54, 54, 46, 42, 42, 42, 42, 42, 33, 33, 33, 58, 58,
#  79, 79, 72, 72, 39, 39, 39, 39, 39, 39, 39, 55, 66, 66, 59, 59, 59, 59, 32, 32, 32, 32, 43, 43, 49, 49, 49, 49, 44, 44,
#  58, 58, 64, 64, 56, 56, 56, 56, 34, 34, 34, 51, 45, 45, 40, 30, 41, 41, 55, 55, 55, 55, 33, 33, 58, 58, 78, 78, 70, 70,
#  70, 70, 38, 38, 56, 56, 80, 80, 81, 81, 53, 74, 76, 76, 76, 76, 35, 35, 60, 60, 88, 88, 91, 91, 77, 77, 81, 81, 47, 28,
#  28, 28, 37, 37, 42, 35, 20, 20, 49, 49, 49, 49]
