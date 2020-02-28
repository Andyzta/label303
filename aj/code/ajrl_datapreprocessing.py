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

name = []
def get_filename():  # 输入路径、文件类型 例如'.csv'

    data = pd.read_csv(path('config/sbdy_config.csv'))

    for i in range(len(data['type'])):

        if (data['type'][i] == 'rl'):
            name.append(data['bh'][i])
    return name

filepath=[]
def get_filepath():

    get_filename()


    for i in range(0, len(name)):
        filepath.append(path('rl/' + name[i]+'.csv'))
    return filepath


# def getdata(input_path=path('data/总电量6_12.csv')):
#         data = pd.read_csv(input_path)
#         dict = {'时间': data['时间'],
#                 'ajdlbd.YC0053': data['ajdlbd.YC0053'],
#                 'ajdlbd.YC0865': data['ajdlbd.YC0865'],
#                 }
#         dataframe = pd.DataFrame(dict)
#         return dataframe
def getdata(input_path=path('rl/YC467.csv')):
        data = pd.read_csv(input_path)
        dict = {'时间': data['时间'],
                'energy': data['energy'],

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
    # print(detail)
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
    # print(iflastm, i, j,num)
    # print("----------------------------")
    while j < len(dataS):

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
                    # print(ds)

                if platform.system().lower() == 'linux':

                    if detail[i]['ds'].endswith("12:00 am — 6:00 am"):

                        ds = 0

                    elif detail[i]['ds'].endswith("6:00 am — 12:00 pm"):

                        ds = 6

                    elif detail[i]['ds'].endswith("12:00 pm — 6:00 pm"):

                        ds = 12

                    elif detail[i]['ds'].endswith("6:00 pm — 12:00 am"):

                        ds = 18

                if platform.system().lower() == 'linux':

                    if detail[i]['ds'].endswith("00:00 — 06:00"):

                        ds = 0

                    elif detail[i]['ds'].endswith("06:00 — 12:00"):

                        ds = 6

                    elif detail[i]['ds'].endswith("12:00 — 18:00"):

                        ds = 12

                    elif detail[i]['ds'].endswith("18:00 — 00:00"):

                        ds = 18
                while (int(dataframe['时间'][j][11:13]) < 6) and (int(ds) == 0):

                # 说明时间段相等
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

                while (6 <= int(dataframe['时间'][j][11:13]) < 12) and (int(ds) == 6):
                # 说明时间段相等

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
    dict = {'date':dss,
            'temphigh': temp,
            'templow': templow,

            'wind': wind,
            'humidity': humidity,

            }
    dataframe2 = pd.DataFrame(dict)
    # print(dataframe2,j)
    # print(len(temp),iflastm,num,day)
    return temp,templow,humidity,wind,desc,dataframe2,flag

#获得节假日数据
is_holidaylist = []
def is_Holiday():
    start_date = dataS[0]
    end_date = dataS[len(dataS) - 1]
    # set_date = datetime.datetime.strptime(start_date,"%Y-%m-%d")

    is_mondaylist = []
    date = []
    '''判断当天日期是否为节假日'''
    rest_holiday = pd.read_csv(path('data/rest_holiday.csv'))
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
    # print(is_holidaylist)
    return is_holidaylist

#将前面函数的操作聚合在一起
def rl_datapreprossion(dataframe):
    # dataframe = getdata()


    for i in range(0, len(month)):
        if (i == len(month) - 1):
            global iflastm
            iflastm = True
            # print(iflastm)
            # print(i)
        # for month in month:
        #     history_crawlersameY(year=2019, month=month, day='1', flag=len(temp))
        history_crawlersameY(year=year[i], month=month[i], day=day[i], flag=len(temp))

    is_Holiday()
    # print(len(dataframe['时间']),len(temp),len(is_holidaylist))
    dict = {'时间': dataframe['时间'][:len(temp)],

            'energy': dataframe['energy'][:len(temp)],

            'temphigh': temp,
            'templow': templow,
            'is_holiday': is_holidaylist[:len(temp)],
            'wind': wind,
            'humidity': humidity,

            }
    # 'is_holiday': is_holidaylist,
    dataframerl = pd.DataFrame(dict)
    # print(dataframe)
    return dataframerl
    # dataframe.to_csv(path('dl/总电量test2.csv'), sep=',', index=None, encoding="utf_8_sig")  # 改成覆盖
def all_dealdata():
    dealYMD()
    get_filepath()

    for i in range(0, len(filepath)):
        dataframe = getdata(filepath[i])
        # print(dataframe)
        data = rl_datapreprossion(dataframe)
        data = data.rename(columns={'时间': 'SDATE'})
        data['tempavg'] = (data['temphigh'] + data['templow']) / 2
        data['tempavg'] = data['tempavg'].astype('int')
        data['SDATE'] = pd.to_datetime(data['SDATE'])
        data = data.loc[(data['energy'] < 1500) & (data['energy'] >= 0)]
        data.index = range(data.shape[0])  # 重置索引
        data['hour'] = data['SDATE'].dt.hour
        data['dayofweek'] = data['SDATE'].dt.dayofweek+1
        data['weekofyear'] = data['SDATE'].dt.weekofyear
        data.dropna(inplace=True)
        data.to_csv(path('rl/deal' + name[i]+'.csv'), sep=',', index=None, encoding="utf_8_sig")  # 改成覆盖
        print("热量数据处理完成")


if __name__ == '__main__':
    all_dealdata()