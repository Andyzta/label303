#!/usr/bin/python
# -*- coding：utf-8 -*-
import os
DIR = os.path.dirname(os.path.abspath(__file__))
import sys
sys.path.append(DIR)
import pandas as pd
import numpy as np
import pymysql
import datetime
import  csv
import requests
import  demjson
from os.path import *
import xgboost as xgb
from sklearn.model_selection import KFold, cross_val_score as CVS, train_test_split as TTS
from time import time
from sklearn.metrics import mean_squared_error as MSE, r2_score
import pickle
import sys
from predictweath_data import crawler as crawler
from predictweath_data import path as path
os.chdir(DIR);
# pre函数，预测函数，无返回值，只需要根据输入的路径来输出一个预测结果的json文件
def pre(input_path = "../data/prehour.csv",  model_path="../model/nlwinterhourxgboost.dat"):
    # 读取路径中的文件
    data = pd.read_csv(input_path,engine='python', encoding="utf_8_sig")
    data = data.loc[0:23, ]
    data = data.rename(columns={'temp': 'tempavg'})
    data['tempavg'] = data['tempavg'].astype('int')
    data['temphigh'] =data['tempavg']+3
    data['templow'] = data['tempavg'] - 3
    data['SDATE'] = pd.to_datetime(data['SDATE'])
    data['hour'] = data['SDATE'].dt.hour
    data['dayofweek'] = data['SDATE'].dt.dayofweek + 1
    data['weekofyear'] = data['SDATE'].dt.weekofyear
    data.dropna(inplace=True)
    dataset=pd.DataFrame({'temphigh':data['temphigh'],'templow':data['templow'],
                          'is_holiday':data['is_holiday'],'wind':data['wind'],
                          'humidity': data['humidity'],'tempavg':data['tempavg'],
                          'hour': data['hour'],'dayofweek': data['dayofweek'],
                         'weekofyear': data['weekofyear']})
    # 导入模型
    loaded_model = pickle.load(open(model_path, "rb"))
    # 做预测，直接调用接口predict
    # ypreds = loaded_model.predict(X_validation)
    yc = xgb.DMatrix(dataset,np.arange(dataset.shape[0]))
    ypreds = loaded_model.predict(yc)
    dataset['ypreds'] = ypreds
    dataset.loc[(dataset['hour'].isin([0, 1, 2, 18, 19, 20, 21, 22, 23])) & (
    dataset['dayofweek'].isin([1, 2, 3, 4, 5])), 'ypreds'] = 0
    dataset.loc[(dataset['dayofweek'].isin([1, 2, 3, 4, 5])) & (dataset['is_holiday'].isin([1])), 'ypreds'] = 0
    dataset.loc[(dataset['hour'] >= 7) & (dataset['dayofweek'].isin([7])) & (dataset['tempavg'].mean() <= 10) & (
    dataset['is_holiday'].isin([1])), 'ypreds'] = 0
    dataset.loc[(dataset['dayofweek'].isin([7])) & (dataset['tempavg'].mean() > 10) & (
    dataset['is_holiday'].isin([1])), 'ypreds'] = 0
    dataset.loc[(dataset['hour'] <= 22) & (dataset['dayofweek'].isin([6])) & (dataset['tempavg'].mean() <= 10) & (
    dataset['is_holiday'].isin([1])), 'ypreds'] = 0
    dataset.loc[(dataset['dayofweek'].isin([6])) & (dataset['tempavg'].mean() > 10) & (
    dataset['is_holiday'].isin([1])), 'ypreds'] = 0
    data = pd.DataFrame({'nl': [dataset['ypreds'].sum()]})
    '''
    data = pd.DataFrame({ 'nl':ypreds},index=data['SDATE'].tolist())
    data['SDATE'] = data.index
    data['SDATE'] =data['SDATE'].astype('str')
    data.index=data['SDATE']
    data = data.drop(['SDATE'], axis=1)
    '''
    for root, dirs, files in os.walk('../data'):
        for name in files:
            if name.startswith("ycnlwinter"):  # 指定要删除的格式，
                os.remove(os.path.join(root, name))
                print("Delete File: " + os.path.join(root, name))
    output_path="../data/ycnlwinter"+datetime.datetime.strftime(datetime.date.today(),'%Y-%m-%d')+".json"
    if os.path.exists(output_path):
        data.to_json(output_path)
    else:
        data.to_json(output_path)
        
def write2db(data,id):
    conn = pymysql.connect(host="localhost", user="root",password="pwdb1381",database="power",charset="utf8")
    cursor = conn.cursor()
    #sql = "update predict_data set data = \"%s\" , date = \"%s\" where predict_device_id = %s "%(data,datetime.datetime.now(),id)
    sql = "INSERT INTO predict_data(data,date,predict_device_id) VALUES(%s,%s,%s)"
    #print(sql)
    try:  
        cursor.execute(sql,(data,(datetime.datetime.now() + datetime.timedelta(days = 1)),id))
        #执行sql语句
        conn.commit()
    except Exception as e:  
        raise e  
    finally:  
        conn.close()  #关闭连接 

    
# pre函数，预测函数，无返回值，只需要根据输入的路径来输出一个预测结果的csv文件
def prehour(input_path = "../data/prehour.csv",  model_path="../model/nlwinterhourxgboost.dat",id =36):
    # 读取路径中的文件
    data = pd.read_csv(input_path,engine='python', encoding="utf_8_sig")
    data = data.loc[0:23, ]
    data = data.rename(columns={'temp': 'tempavg'})
    data['tempavg'] = data['tempavg'].astype('int')
    data['temphigh'] =data['tempavg']+3
    data['templow'] = data['tempavg'] - 3
    data['SDATE'] = pd.to_datetime(data['SDATE'])
    data['hour'] = data['SDATE'].dt.hour
    data['dayofweek'] = data['SDATE'].dt.dayofweek + 1
    data['weekofyear'] = data['SDATE'].dt.weekofyear
    data.dropna(inplace=True)
    dataset=pd.DataFrame({'temphigh':data['temphigh'],'templow':data['templow'],
                          'is_holiday':data['is_holiday'],'wind':data['wind'],
                          'humidity': data['humidity'],'tempavg':data['tempavg'],
                          'hour': data['hour'],'dayofweek': data['dayofweek'],
                         'weekofyear': data['weekofyear']})
    # 导入模型
    loaded_model = pickle.load(open(model_path, "rb"))
    # 做预测，直接调用接口predict
    # ypreds = loaded_model.predict(X_validation)
    yc = xgb.DMatrix(dataset,np.arange(dataset.shape[0]))
    ypreds = loaded_model.predict(yc)
    dataset['ypreds'] = ypreds
    dataset.to_csv('../data/1月4号.csv', index=None, encoding="utf_8_sig")
    dataset.loc[(dataset['hour'].isin([0, 1, 2, 18, 19, 20, 21, 22, 23])) & (
    dataset['dayofweek'].isin([1, 2, 3, 4, 5])), 'ypreds'] = 0
    dataset.loc[(dataset['dayofweek'].isin([1, 2, 3, 4, 5])) & (dataset['is_holiday'].isin([1])), 'ypreds'] = 0
    dataset.loc[(dataset['hour'] >= 7) & (dataset['dayofweek'].isin([7])) & (dataset['tempavg'].mean() <= 10) & (
    dataset['is_holiday'].isin([1])), 'ypreds'] = 0
    dataset.loc[(dataset['dayofweek'].isin([7])) & (dataset['tempavg'].mean() > 10) & (
    dataset['is_holiday'].isin([1])), 'ypreds'] = 0
    dataset.loc[(dataset['hour'] <= 22) & (dataset['dayofweek'].isin([6])) & (dataset['tempavg'].mean() <= 10) & (
    dataset['is_holiday'].isin([1])), 'ypreds'] = 0
    dataset.loc[(dataset['dayofweek'].isin([6])) & (dataset['tempavg'].mean() > 10) & (
    dataset['is_holiday'].isin([1])), 'ypreds'] = 0
    data = pd.DataFrame({'nl': dataset['ypreds']})
    '''
    data = pd.DataFrame({ 'nl':ypreds},index=data['SDATE'].tolist())
    data['SDATE'] = data.index
    data['SDATE'] =data['SDATE'].astype('str')
    data.index=data['SDATE']
    data = data.drop(['SDATE'], axis=1)
    '''
    for root, dirs, files in os.walk('../data'):
        for name in files:
            if name.startswith("ycnlhourwinter"):  # 指定要删除的格式，
                os.remove(os.path.join(root, name))
                #print("Delete File: " + os.path.join(root, name))
    output_path="../data/ycnlhourwinter"+datetime.datetime.strftime(datetime.date.today(),'%Y-%m-%d')+".json"
    energy = []
    for i in data['nl']:
        i = round(i, 2)
        energy.append(str(i))
    write2db(','.join(energy) ,id)
    print("热量预测结果成功写入数据库")
    if os.path.exists(output_path):
        data.to_json(output_path)
    else:
        data.to_json(output_path)
if __name__ == '__main__':
    crawler(predict_date='prehour', output_path=path('data/prehour.csv'))
    #pre(input_path = "../data/prehour.csv",  model_path="../model/nlwinterhourxgboost.dat")
    for root, dirs, files in os.walk('../rl'):
        for name in files:
            if name.endswith("dat"):
                id=int(name.rstrip('.dat'))
                prehour(input_path="../data/prehour.csv",model_path=os.path.join(root, name),id=id)
