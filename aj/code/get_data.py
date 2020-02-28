#!/usr/bin/python
# -*- coding: UTF-8 -*-
import datetime
import requests
import json
import pandas as pd
import  platform
import os
import time
from urllib.parse import urlencode
# from pyquery import PyQuery as pq

host = 'm.weibo.cn'
base_url = 'http://39.105.189.154:12998/sys/login/restful'
user_agent = 'User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1 wechatdevtools/0.7.0 MicroMessenger/6.3.9 Language/zh_CN webview/0'

headers = {
    'Host': host,
    'Referer': 'https://m.weibo.cn/u/1665372775',
    'User-Agent': user_agent
}



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

def get_userid():
    params = {
        'username': "刘涛",
        'password': '88888888'
    }
    url = base_url;
    try:
        response = requests.post(url, params)
        #print(response)
        #print(response.text)
        if response.status_code == 200:
            return response.json()
    except requests.ConnectionError as e:
        print('抓取错误', e.args)


def get_data(logintoken, stationName, pointNames, begintime, endtime, searchType, timeType):
    url = 'http://39.105.189.154:12998/statistics/getDatasForForecast/'
    params = {
        'logintoken': logintoken,
        'stationName': stationName,
        'pointNames': pointNames,
        'begintime': begintime,
        'endtime': endtime,
        'searchType': searchType,
        'timeType': timeType
    }
    try:
    
        response = requests.post(url, params)
        #print(response)
        #print(response.text)
        if response.status_code == 200:
            return response.json()
    except requests.ConnectionError as e:
        print('抓取错误', e.args)



# print(userid)
# print(userid.get('token'))

# data = get_data(userid.get('token'), 'ajdlbd', json.dumps(['YC0053']), '2019-11-08 00:00:00', '2019-11-18 00:00:00',
#                 'max', 'quarter')
date_list = []
def getBetweenDay(begin_date,end_date):

    begin_date = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)
    return date_list

begin_time = []
end_time = []
def deal_time():
    start_date = '2019-09-23 17:00:00'
    today = datetime.datetime.now()
    timenow = today.strftime('%Y-%m-%d %H:%M:%S')
    # 2019 - 09 - 2317: 15:00
    hour = today.strftime('%H')  # 获得现在几点，目前输出的10点，所以链接是从11开始的，判断从哪个位置取第二天的。
    # print(hour)
    year = today.strftime('%Y')
    month = today.strftime('%m')
    datenow = today.strftime('%d')
    minute = today.strftime('%M')

    if (int(minute) <= 15):
        minute = '00'
    elif (15 < int(minute) <= 30):
        minute = '15'
    elif (30 < int(minute) <= 45):
        minute = '30'
    elif (45 < int(minute) <= 59):
        minute = '45'
    end_date = year + '-' + month + '-' + datenow + ' ' + hour + ':' + minute + ':00'


    getBetweenDay(start_date[:10], end_date[:10])
    #print(date_list)
    i = 0
    while (i < len(date_list)):
        if(i==0):
            begin_time.append(start_date)
        else:
            begin_time.append(date_list[i] + ' 00:00:00')
        if (i + 9 >= len(date_list)):
            end_time.append(end_date)
        elif (i + 9 < len(date_list)):
            end_time.append(date_list[i + 9] + ' 23:45:00')

        i = i + 10
    return begin_time,end_time
rl_list=[]
dl_list=[]
ll_list=[]
rl_list_time = []
rl_list_value = []
dl_list_time = []
dl_list_value = []
ll_list_time = []
ll_list_value = []
rl_list_time_deal = []

dl_list_time_deal = []

ll_list_time_deal = []
rl_dict = []
dl_dict = []
ll_dict = []
def deal_data():
    deal_time()
    data = pd.read_csv(path('config/sbdy_config.csv'))

    for i in range(len(data['type'])):


        if(data['type'][i]=='rl'):
            rl_list.append(data['bh'][i])
        if (data['type'][i] == 'dl'):
            dl_list.append(data['bh'][i])
        if (data['type'][i] == 'll'):
            ll_list.append(data['bh'][i])


    if (len(rl_list) != 0):
        for i in range(0, len(rl_list)):
            rl_list_time.append([])
            rl_list_value.append([])
            rl_list_time_deal.append([])
            rl_dict.append({})

    if (len(dl_list) != 0):
        for i in range(0, len(dl_list)):
            dl_list_time.append([])
            dl_list_value.append([])
            dl_list_time_deal.append([])
            dl_dict.append({})

    if (len(ll_list) != 0):
        for i in range(0, len(ll_list)):
            ll_list_time.append([])
            ll_list_value.append([])
            ll_list_time_deal.append([])
            ll_dict.append({})
    data_YC0053=pd.read_csv(path('data/YC0053.csv'))
    data_YC0865 = pd.read_csv(path('data/YC0865.csv'))
    data_YC456 = pd.read_csv(path('data/YC456.csv'))

    for i in range(0,2192):
        dl_list_time[0].append(data_YC0053['时间'][i])
        dl_list_value[0].append(data_YC0053['energy'][i])
    # print(dl_list_time[0])
    # print(dl_list_value[0])
    for i in range(0,2192):
        dl_list_time[1].append(data_YC0865['时间'][i])
        dl_list_value[1].append(data_YC0865['energy'][i])
    # print(dl_list_time[1])
    # print(dl_list_value[1])
    for i in range(0,2238):
        ll_list_time[0].append(data_YC456['时间'][i])
        ll_list_value[0].append(data_YC456['energy'][i])
    # print(ll_list_time[0])
    # print(ll_list_value[0])
    userid = get_userid()
    # print(dl_list)
    #
    # print(json.dumps(dl_list[0]))
    # print(json.dumps(['YC0053']))
    # print(json.dumps([dl_list[0]])==(json.dumps(['YC0053'])))
    # print(len(begin_time))
    for i in range(0,len(begin_time)):
        for j in range(0,len(dl_list)):

            data = get_data(userid.get('token'), 'ajdlbd', json.dumps([dl_list[j]]),begin_time[i], end_time[i],
                'max', 'quarter')
            dict = sorted(data[0]['ajdlbd.'+dl_list[j]].items(), key=lambda asd: asd[0], reverse=False)
            # print(dict)
            for k in range(0,len(dict)):
                dl_list_time[j].append(dict[k][0])
                dl_list_value[j].append(dict[k][1])
    for i in range(0,len(begin_time)):
        for j in range(0,len(rl_list)):

            data = get_data(userid.get('token'), 'ajwldl2', json.dumps([rl_list[j]]),begin_time[i], end_time[i],
                'max', 'quarter')
            dict = sorted(data[0]['ajwldl2.'+rl_list[j]].items(), key=lambda asd: asd[0], reverse=False)
            # print(dict)
            for k in range(0,len(dict)):
                rl_list_time[j].append(dict[k][0])
                rl_list_value[j].append(dict[k][1])
    for i in range(0,len(begin_time)):
        for j in range(0,len(ll_list)):

            data = get_data(userid.get('token'), 'ajwldl2', json.dumps([ll_list[j]]),begin_time[i], end_time[i],
                'max', 'quarter')
            dict = sorted(data[0]['ajwldl2.'+ll_list[j]].items(), key=lambda asd: asd[0], reverse=False)
            # print(dict)
            for k in range(0,len(dict)):
                ll_list_time[j].append(dict[k][0])
                ll_list_value[j].append(dict[k][1])
    for i in range(0, len(dl_list)):
        dl_dict[i] = {
            '时间': dl_list_time[i],
            'energy': dl_list_value[i],
        }
    for i in range(0, len(rl_list)):
        rl_dict[i] = {
            '时间': rl_list_time[i],
            'energy': rl_list_value[i],
        }


    for i in range(0, len(ll_list)):
        ll_dict[i] = {
            '时间': ll_list_time[i],
            'energy': ll_list_value[i],
        }

    for i in range(0, len(rl_list)):
        dataframerl=pd.DataFrame(rl_dict[i])
        dataframerl['时间'] = pd.to_datetime(dataframerl['时间'])
        dataframerl=dataframerl.loc[dataframerl['时间'].dt.minute.isin([0])]
        dataframerl.index = range(dataframerl.shape[0])
        dataframerl['energy'] = dataframerl['energy'].shift(-1) - dataframerl['energy']
        dataframerl=dataframerl.dropna()
        dataframerl.to_csv(path('rl/'+rl_list[i]+'.csv'), sep=',', index=None, encoding="utf_8_sig")

    for i in range(0, len(dl_list)):
        dataframedl=pd.DataFrame(dl_dict[i])
        dataframedl['时间'] = pd.to_datetime(dataframedl['时间'])
        dataframedl=dataframedl.loc[dataframedl['时间'].dt.minute.isin([0])]
        dataframedl.index = range(dataframedl.shape[0])
        dataframedl['energy'] = dataframedl['energy'].shift(-1) - dataframedl['energy']
        dataframedl=dataframedl.dropna()
        dataframedl.to_csv(path('dl/'+dl_list[i]+'.csv'), sep=',', index=None, encoding="utf_8_sig")

    for i in range(0, len(ll_list)):
        dataframell=pd.DataFrame(ll_dict[i])
        dataframell['时间'] = pd.to_datetime(dataframell['时间'])
        dataframell=dataframell.loc[dataframell['时间'].dt.minute.isin([0])]
        dataframell.index = range(dataframell.shape[0])
        dataframell['energy'] = dataframell['energy'] * (-1)
        dataframell['energy'] = dataframell['energy'].shift(-1) - dataframell['energy']
        dataframell=dataframell.dropna()
        dataframell.to_csv(path('ll/'+ll_list[i]+'.csv'), sep=',', index=None, encoding="utf_8_sig")
        print('%s' % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        print("爬取电量冷量热量数据执行成功")

    return dl_list_value,dl_list_time,rl_list_time,rl_list_value,dl_list_time,dl_list_value ,ll_list_time ,ll_list_value,dl_dict,dl_list
if __name__ == '__main__':

   deal_data()
   # print(dl_list_time[0])
   # print(dl_list_value[0])
   # print(dl_list_time[1])
   # print(dl_list_value[1])
   # dataframedl = pd.DataFrame(dl_dict[0])
   # print(dataframedl)
   # dataframedl.to_csv(path('dl/' + dl_list[0] + 'test.csv'), sep=',', index=None, encoding="utf_8_sig")
   #  x=['YC0053','1']
   # 'ajwldl2.YC456'
   #  userid = get_userid()
   #  data = get_data(userid.get('token'), 'ajwldl2', json.dumps(['YC467']), '2019-09-23 00:00:00', '2019-09-24 00:00:00',
   #                  'max', 'quarter')
   #  dict = sorted(data[0]['ajwldl2.YC467'].items(), key=lambda asd: asd[0], reverse=False)
   #  print(dict)
   #  print(data)
    # data = get_data(userid.get('token'), 'ajdlbd', json.dumps(['YC0053']), '2019-12-29 00:00:00', '2020-01-07 13:45:00',
    #                 'max', 'quarter')
    # dict_nl=[]
    #
    #
    #     # for k, v in dict.items():
    #     #  	print(k, v)
    #     dict_nl.append(dict)
    # print(dict_nl)



    # # 得到的数据从开始日期的后15分钟开始。截止日期的后15分钟结束，实时的，强。9月23才开始，我佛辣，明明到1月6号的也有鸭
    # # 2019-06-01 00:00:00  2019-12-10 23:00:00
    # print(data);
    # # print(type(data))
    # # print(data[0])
    # # print(type(data[0]['ajdlbd.YC0053']))
    # # for k, v in data[0]['ajdlbd.YC0053'].items():
    # #  	print(k, v)
    # dict = sorted(data[0]['ajdlbd.'+x[0]].items(), key=lambda asd: asd[0], reverse=False)
    # print(dict)
    # y=['YC0053']
    # print(json.dumps(['YC0053']))
    # print(type(json.dumps(['YC0053'])))
    # y='YC0053'
    # yl=y.split()
    # print(json.dumps(yl)==(json.dumps(['YC0053'])))
    # print(json.dumps(y))
    # print(json.dumps(x[0])==(json.dumps(['YC0053'])))