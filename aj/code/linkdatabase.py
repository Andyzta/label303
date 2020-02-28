import  platform


from influxdb import InfluxDBClient
import pandas as pd
import os
import sys
# reload(sys)
# sys.setdefaultencoding('uft-*8')
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

# def path(input_path):
#     curPath = os.path.abspath(os.path.dirname(__file__))
#     rootPath = curPath[:curPath.find("aj/") + len("aj/")]
#     dataPath = os.path.abspath(rootPath + input_path)
#     return dataPath
def DBcon():

    # data=pd.read_csv("D:/hhxx2/aj/data/database_config.csv")
    data=pd.read_csv(path('data/database_config.csv'))
    InfHost=data['host'][0]
    InfPort=data['port'][0]
    InfUser = data['user'][0]
    InfPwd = data['passwd'][0]
    InfDB=data['db'][0]
    # 连接influxdb
    client = InfluxDBClient(InfHost, InfPort, InfUser, InfPwd, InfDB)
    data=pd.read_csv(path('data/unit1112.csv'))

    j=0
    for i in range(0,len(data['time'])):
        json_body = [
        {
            "measurement": "test",
            "tags": {
                            "t": str(i),
                        },
            "time": data['time'][i],

            "fields": {

                "name": data['name'][i],
                "unit":data['unit'][i],
                "value":data['value'][i],
            }
        }
        ]
        if(data['value'][i]!='none'):
            j=j+1
            client.write_points(json_body)  # 写入数据
    # print(data['time'][0], len(data['time']),j)2019/6/1 0:00 6891 6622
    # print(data)
    # print(client)
    # result = client.query('select * from students;')
    result1 = client.query('select * from test;')
    # print("Result: {0}".format(result))
    return result1
# def MysqlClose():
#     MysqlCon.close()
#     MysqlCur.close()
def getdata():
    result = DBcon()

    # 这三个列表是来获取设备代号配置文件sbdh_config中的bh这一项的
    rl_list=[]
    dl_list=[]
    ll_list=[]
    data = pd.read_csv(path('config/sbdy_config.csv'))

    for i in range(len(data['type'])):


        if(data['type'][i]=='rl'):
            rl_list.append(data['bh'][i])
        if (data['type'][i] == 'dl'):
            dl_list.append(data['bh'][i])
        if (data['type'][i] == 'll'):
            ll_list.append(data['bh'][i])

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

    # 长见识了，points使用过一次就莫得了的样子


    points = result.get_points()
    for items in points:

        for i in range(0, len(rl_list)):

            if (items['name'] == rl_list[i]):
                rl_list_time[i].append(items['time'])
                rl_list_value[i].append(items['value'])


    points = result.get_points()
    for items in points:

        for i in range(0, len(dl_list)):

            if (items['name'] == dl_list[i]):
                dl_list_time[i].append(items['time'])
                dl_list_value[i].append(items['value'])



    points = result.get_points()
    for items in points:

        for i in range(0, len(ll_list)):

            if (items['name'] == ll_list[i]):
                ll_list_time[i].append(items['time'])
                ll_list_value[i].append(items['value'])

    for i in range(0, len(rl_list_time)):
        for item in rl_list_time[i]:
            first = item[:10]
            end = item[11:19]
            item = first + ' ' + end

            rl_list_time_deal[i].append(item)

    for i in range(0, len(dl_list_time)):
        for item in dl_list_time[i]:
            first = item[:10]
            end = item[11:19]
            item = first + ' ' + end

            dl_list_time_deal[i].append(item)

    for i in range(0, len(ll_list_time)):
        for item in ll_list_time[i]:

            first = item[:10]
            end = item[11:19]
            item = first + ' ' + end
            ll_list_time_deal[i].append(item)
    print(ll_list_time)
    print(ll_list_time_deal)
    print(ll_list_value)
    for i in range(0, len(rl_list)):
        rl_dict[i] = {
            '时间': rl_list_time_deal[i],
            'energy': rl_list_value[i],
        }

    for i in range(0, len(dl_list)):
        dl_dict[i] = {
            '时间': dl_list_time_deal[i],
            'energy': dl_list_value[i],
        }

    for i in range(0, len(ll_list)):
        ll_dict[i] = {
            '时间': ll_list_time_deal[i],
            'energy': ll_list_value[i],
        }

    # for i in range(0, len(rl_list)):
    #     dataframerl=pd.DataFrame(rl_dict[i])
    #     dataframerl.to_csv(path('rl/'+rl_list[i]+'.csv'), sep=',', index=None, encoding="utf_8_sig")
    #
    # for i in range(0, len(dl_list)):
    #     dataframedl=pd.DataFrame(dl_dict[i])
    #     dataframedl.to_csv(path('dl/'+dl_list[i]+'.csv'), sep=',', index=None, encoding="utf_8_sig")
    #
    # for i in range(0, len(ll_list)):
    #     dataframell=pd.DataFrame(ll_dict[i])
    #     dataframell.to_csv(path('ll/'+ll_list[i]+'.csv'), sep=',', index=None, encoding="utf_8_sig")


    # # dataframe.to_csv('D:\\总电量test.csv', sep=',', header=None, index=None,encoding="utf_8_sig")#改成覆盖
    # dataframenl.to_csv(path('data/能量表.csv.'), sep=',', index=None, encoding="utf_8_sig")  # 改成覆盖
    # dataframedl.to_csv(path('data/总电量.csv.'), sep=',', index=None, encoding="utf_8_sig")  # 改成覆盖

if __name__ == '__main__':
    getdata()
    # result = DBcon()
    # print(result)