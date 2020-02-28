# from influxdb import InfluxDBClient
# import pandas as pd
# import sys
# # reload(sys)
# # sys.setdefaultencoding('uft-*8')
# def DBcon():
#
#     data=pd.read_csv("D:/hhxx2/aj/data/database_config.csv")
#     InfHost=data['host'][0]
#     InfPort=data['port'][0]
#     InfUser = data['user'][0]
#     InfPwd = data['passwd'][0]
#     InfDB=data['db'][0]
#     # 连接influxdb
#     client = InfluxDBClient(InfHost, InfPort, InfUser, InfPwd, InfDB)
#     data=pd.read_csv('D:/unit.csv')
#     # # # 返回连接
#     # # # return MysqlCon, MysqlCur, Influxdb_Client
#     print(data['time'][0],len(data['time']))
#     # for i in range(0,len(data['time'])):
#     #     json_body = [
#     #     {
#     #         "measurement": "bwybd_statistical",
#     #         "time": data['time'][i],
#     #
#     #         "fields": {
#     #
#     #             "name": data['name'][i],
#     #             "unit":data['unit'][i],
#     #             "value":data['value'][i],
#     #         }
#     #     }
#     #     ]
#     #     if(data['value'][i]!='none'):
#     #         client.write_points(json_body)  # 写入数据
#     # print(data)
#     # print(client)
#     # result = client.query('select * from students;')
#     result1 = client.query('select * from bwybd_statistical;')
#     # print("Result: {0}".format(result))
#     return result1
# # def MysqlClose():
# #     MysqlCon.close()
# #     MysqlCur.close()
#
#
# if __name__ == '__main__':
#     # MysqlCon, MysqlCur, Influxdb_Client, Redis_Client = DBcon()
#     result=DBcon()
#     # print(result,type(result))
#     # print("Result: {0}".format(result))
#     # points = result.get_points()
#     # time=[]
#     # for item in points:
#     #     # print(item['time'])
#     #     time.append(item['time'])
#     # print(len(time))
#     # YC0053_value=[]
#     # YC456_value=[]
#     # YC0865_value=[]
#     # for item in points:
#     # # print(item['time'])
#     #     if(item['name']=='YC0053'):
#     #
#     #         YC0053_value.append(item['value'])
#     #     if (item['name'] == 'YC456'):
#     #         YC456_value.append(item['value'])
#     #     if (item['name'] == 'YC0865'):
#     #         YC0865_value.append(item['value'])
#     # print(len(YC0053_value),len(YC456_value),len(YC0865_value))
#     #再把数据一转换
#      # print(type(result['score']))




from influxdb import InfluxDBClient
import pandas as pd
import os
import sys
# reload(sys)
# sys.setdefaultencoding('uft-*8')
def path(input_path):
    curPath = os.path.abspath(os.path.dirname(__file__))
    rootPath = curPath[:curPath.find("aj\\") + len("aj\\")]
    dataPath = os.path.abspath(rootPath + input_path)
    return dataPath
def DBcon():

    data=pd.read_csv("D:/hhxx2/aj/data/database_config.csv")
    InfHost=data['host'][0]
    InfPort=data['port'][0]
    InfUser = data['user'][0]
    InfPwd = data['passwd'][0]
    InfDB=data['db'][0]
    # 连接influxdb
    client = InfluxDBClient(InfHost, InfPort, InfUser, InfPwd, InfDB)
    # data=pd.read_csv('D:/unit.csv')

    # j=0
    # for i in range(0,len(data['time'])):
    #     json_body = [
    #     {
    #         "measurement": "test3",
    #         "tags": {
    #                         "t": str(i),
    #                     },
    #         "time": data['time'][i],
    #
    #         "fields": {
    #
    #             "name": data['name'][i],
    #             "unit":data['unit'][i],
    #             "value":data['value'][i],
    #         }
    #     }
    #     ]
    #     if(data['value'][i]!='none'):
    #         j=j+1
    #         client.write_points(json_body)  # 写入数据
    # print(data['time'][0], len(data['time']),j)2019/6/1 0:00 6891 6622
    # print(data)
    # print(client)
    # result = client.query('select * from students;')
    result1 = client.query('select * from test3;')
    # print("Result: {0}".format(result))
    return result1
# def MysqlClose():
#     MysqlCon.close()
#     MysqlCur.close()


if __name__ == '__main__':
    # MysqlCon, MysqlCur, Influxdb_Client, Redis_Client = DBcon()
    result=DBcon()
    # print(result,type(result))
    # print("Result: {0}".format(result))
    points = result.get_points()
    # time=[]
    # for item in points:
    #     # print(item['time'])
    #     time.append(item['time'])
    # print(len(time))
    #这time不能重复？？？
    # print(len(time))
    YC0053_value=[]
    YC456_value=[]
    YC0865_value=[]
    YC0053_time = []
    YC456_time = []

    points = result.get_points()
    for item in points:
    # print(item['time'])
        if(item['name']=='YC0053'):
            YC0053_time.append(item['time'])
            YC0053_value.append(item['value'])
        if (item['name'] == 'YC456'):
            YC456_time.append(item['time'])
            YC456_value.append(item['value'])
        if (item['name'] == 'YC0865'):
            YC0865_value.append(item['value'])
    # print(len(YC0053_value),len(YC456_value),len(YC0865_value))
    # print(YC0053_value)
    print(YC456_time[0])
    #处理时间格式
    time456 = []
    for item in YC456_time:
        month = item[5:7]
        day = item[8:10]
        hour = item[11:13]
        if item[5:6] == '0':
            month = item[6:7]
        if item[8:9] == '0':
            day = item[9:10]
        if item[11:12] == '0':
            hour = item[12:13]
        item = item[:4] + '/' + month + '/' + day + ' ' + hour + item[13:16]
        time456.append(item)
    print(time456)
    time0053 = []
    for item in YC0053_time:
        month = item[5:7]
        day = item[8:10]
        hour = item[11:13]
        if item[5:6] == '0':
            month = item[6:7]
        if item[8:9] == '0':
            day = item[9:10]
        if item[11:12] == '0':
            hour = item[12:13]
        item = item[:4] + '/' + month + '/' + day + ' ' + hour + item[13:16]
        time0053.append(item)
    print(time0053)
    dictnl={
        '时间':time456,
        'ajwldl2.YC456':YC456_value
    }
    dictdl={
        '时间': time0053,
        'ajwldl2.YC0053': YC0053_value,
        'ajdlbd.YC0865': YC0865_value,
    }
    dataframenl = pd.DataFrame(dictnl)
    dataframedl = pd.DataFrame(dictdl)
    # print(dataframe)
    # dataframe.to_csv('D:\\总电量test.csv', sep=',', header=None, index=None,encoding="utf_8_sig")#改成覆盖
    dataframenl.to_csv(path('data/能量表.csv.'), sep=',', index=None, encoding="utf_8_sig")  # 改成覆盖
    dataframedl.to_csv(path('data/总电量.csv.'), sep=',', index=None, encoding="utf_8_sig")  # 改成覆盖
    #有点bug，1.3长度应该相等的
    #再把数据一转换
     # print(type(result['score']))

