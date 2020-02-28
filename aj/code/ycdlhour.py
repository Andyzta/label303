#!/usr/bin/python
# -*- coding：utf-8 -*-

import os
DIR = os.path.dirname(os.path.abspath(__file__))
import sys
sys.path.append(DIR)
import pandas as pd
import pymysql
import numpy as np
import datetime
import  csv
import requests
import  demjson
from bs4 import BeautifulSoup
import  re
import xgboost as xgb
from sklearn.model_selection import KFold, cross_val_score as CVS, train_test_split as TTS
from time import time
from sklearn.metrics import mean_squared_error as MSE, r2_score
import pickle
from predictweath_data import crawler as crawler
from predictweath_data import path as path
os.chdir(DIR);


# pre函数，预测函数，无返回值，只需要根据输入的路径来输出一个预测结果的json文件
def pre(input_path = "../data/prehour.csv", model_path="../model/dlhourxgboost.dat"):
	# 读取路径中的文件
	data = pd.read_csv(input_path,engine='python', encoding="utf_8_sig")
	data = data.loc[0:23, ]
	data = data.rename(columns={'temp': 'tempavg'})
	data['tempavg'] = data['tempavg'].astype('int')
	data['temphigh'] =data['tempavg']+3
	data['templow'] = data['tempavg'] - 3
	data['SDATE'] = pd.to_datetime(data['SDATE'])
	data['hour'] = data['SDATE'].dt.hour
	data['weekofyear'] = data['SDATE'].dt.weekofyear
	data['dayofweek'] = data['SDATE'].dt.dayofweek
	data['season'] = 1
	data.loc[data['SDATE'].dt.month.isin([7, 8]), 'season'] = 2
	data.loc[(data['SDATE'].dt.month.isin([6])) & (data['SDATE'].dt.day > 15), 'season'] = 2
	data.loc[(data['SDATE'].dt.month.isin([9])) & (data['SDATE'].dt.day < 15), 'season'] = 2
	data.loc[data['SDATE'].dt.month.isin([10]), 'season'] = 3
	data.loc[(data['SDATE'].dt.month.isin([9])) & (data['SDATE'].dt.day > 15), 'season'] = 3
	data.loc[(data['SDATE'].dt.month.isin([11])) & (data['SDATE'].dt.day < 15), 'season'] = 3
	data.loc[data['SDATE'].dt.month.isin([12, 1, 2]), 'season'] = 4
	data.loc[(data['SDATE'].dt.month.isin([11])) & (data['SDATE'].dt.day > 15), 'season'] = 4
	data.loc[(data['SDATE'].dt.month.isin([3])) & (data['SDATE'].dt.day < 11), 'season'] = 4
	data.dropna(inplace=True)
	dataset=pd.DataFrame({'temphigh':data['temphigh'],'templow':data['templow'],
						  'is_holiday':data['is_holiday'],'wind':data['wind'],
						  'humidity': data['humidity'],'tempavg':data['tempavg'],
						  'hour': data['hour'],'weekofyear':data['weekofyear'],
						  'dayofweek': data['dayofweek'],'season':data['season'],})
	# 导入模型
	loaded_model = pickle.load(open(model_path, "rb"))
	# 做预测，直接调用接口predict
	# ypreds = loaded_model.predict(X_validation)
	yc = xgb.DMatrix(dataset,np.arange(dataset.shape[0]))
	ypreds = loaded_model.predict(yc)
	data = pd.DataFrame({ 'dl':ypreds},index=data['SDATE'].tolist())
	data = data.resample('D').sum()
	data['SDATE'] = data.index
	data['SDATE'] =data['SDATE'].astype('str')
	data.index=data['SDATE']
	data = data.drop(['SDATE'], axis=1)
	for root, dirs, files in os.walk('../data'):
		for name in files:
			if name.startswith("ycdl"):  # 指定要删除的格式，
				os.remove(os.path.join(root, name))
				print("Delete File: " + os.path.join(root, name))
	output_path="../data/ycdl"+datetime.datetime.strftime(datetime.date.today(),'%Y-%m-%d')+".json"
	if os.path.exists(output_path):
		data.to_json(output_path)
	else:
		data.to_json(output_path)
		
def write2db(data,id):
	conn = pymysql.connect(host="localhost", user="root",password="pwdb1381",database="power",charset="utf8")
	cursor = conn.cursor()
	#sql = "update predict_dl set data = \"%s\" , date = \"%s\" where id = %s "%(data,(datetime.datetime.now() + datetime.timedelta(days = 1)),id)
	#sql = """INSERT INTO predict_dl(data,date,id) VALUES(data,((data,datetime.datetime.now() + datetime.timedelta(days = 1)),id)"""
	sql = "INSERT INTO predict_dl(data,date,id) VALUES(%s,%s,%s)"
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
def prehour(input_path = "../data/prehour.csv",  model_path="../model/dlhourxgboost.dat"):
	# 读取路径中的文件
	data = pd.read_csv(input_path,engine='python', encoding="utf_8_sig")
	data = data.loc[0:23,]
	data = data.rename(columns={'temp': 'tempavg'})
	data['tempavg'] = data['tempavg'].astype('int')
	data['temphigh'] =data['tempavg']+3
	data['templow'] = data['tempavg'] - 3
	data['SDATE'] = pd.to_datetime(data['SDATE'])
	data['hour'] = data['SDATE'].dt.hour
	data['weekofyear'] = data['SDATE'].dt.weekofyear
	data['dayofweek'] = data['SDATE'].dt.dayofweek
	data['season'] = 1
	data.loc[data['SDATE'].dt.month.isin([7, 8]), 'season'] = 2
	data.loc[(data['SDATE'].dt.month.isin([6])) & (data['SDATE'].dt.day > 15), 'season'] = 2
	data.loc[(data['SDATE'].dt.month.isin([9])) & (data['SDATE'].dt.day < 15), 'season'] = 2
	data.loc[data['SDATE'].dt.month.isin([10]), 'season'] = 3
	data.loc[(data['SDATE'].dt.month.isin([9])) & (data['SDATE'].dt.day > 15), 'season'] = 3
	data.loc[(data['SDATE'].dt.month.isin([11])) & (data['SDATE'].dt.day < 15), 'season'] = 3
	data.loc[data['SDATE'].dt.month.isin([12, 1, 2]), 'season'] = 4
	data.loc[(data['SDATE'].dt.month.isin([11])) & (data['SDATE'].dt.day > 15), 'season'] = 4
	data.loc[(data['SDATE'].dt.month.isin([3])) & (data['SDATE'].dt.day < 11), 'season'] = 4
	data.dropna(inplace=True)
	dataset=pd.DataFrame({'temphigh':data['temphigh'],'templow':data['templow'],
						  'is_holiday':data['is_holiday'],'wind':data['wind'],
						  'humidity': data['humidity'],'tempavg':data['tempavg'],
						  'hour': data['hour'],'weekofyear':data['weekofyear'],
						  'dayofweek': data['dayofweek'],'season':data['season'],})
	# 导入模型
	loaded_model = pickle.load(open(model_path, "rb"))
	# 做预测，直接调用接口predict
	# ypreds = loaded_model.predict(X_validation)
	yc = xgb.DMatrix(dataset,np.arange(dataset.shape[0]))
	ypreds = loaded_model.predict(yc)
	data = pd.DataFrame({'dl': ypreds})
	'''
	data = pd.DataFrame({ 'nl':ypreds},index=data['SDATE'].tolist())
	data['SDATE'] = data.index
	data['SDATE'] =data['SDATE'].astype('str')
	data.index=data['SDATE']
	data = data.drop(['SDATE'], axis=1)
	'''
	for root, dirs, files in os.walk('../data'):
		for name in files:
			if name.startswith("ycdlhour"):  # 指定要删除的格式，
				os.remove(os.path.join(root, name))
				#print("Delete File: " + os.path.join(root, name))
	output_path="../data/ycdlhour"+datetime.datetime.strftime(datetime.date.today(),'%Y-%m-%d')+".json"
	energy = []
	for i in data['dl']:
		i = round(i, 2)
		energy.append(str(i))
	write2db(','.join(energy) ,1)
	print("电量预测结果成功写入数据库")
	if os.path.exists(output_path):
		data.to_json(output_path)
	else:
		data.to_json(output_path)
if __name__ == '__main__':
	crawler(predict_date='prehour', output_path=path('data/prehour.csv'))
	#pre(input_path="../data/prehour.csv",  model_path="../model/dlhourxgboost.dat")
	prehour(input_path = "../data/prehour.csv",  model_path="../dl/dlhourxgboost.dat")