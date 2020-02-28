#!/usr/bin/python
# -*- coding：utf-8 -*-

import os
DIR = os.path.dirname(os.path.abspath(__file__))
import sys
sys.path.append(DIR)
import pandas as pd
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
import warnings
os.chdir(DIR);

# pre函数，预测函数，无返回值，只需要根据输入的路径来输出一个预测结果的csv文件
def Model_construcion(input_path = "../dl/deal电量.csv", model_path = "../dl/dlhourxgboost.dat"):
	warnings.filterwarnings("ignore")
	data = pd.read_csv(input_path, engine='python', encoding="utf_8_sig")
	data=data.loc[(data['energy'] < 400) & (data['energy'] > 30)]
	data.index = range(data.shape[0])
	data['SDATE'] = pd.to_datetime(data['SDATE'])
	data.to_csv('../data/datadl.csv')
	dataset = data.copy()
	#dataset.to_csv('../data/dataset.csv')
	#为后面以日为单位测试，加上日期做准备
	testset = dataset[995:1156]
	#testset = dataset[dataset.shape[0]-168:]
	testset.index = range(testset.shape[0])
	data = pd.DataFrame({'temphigh': data['temphigh'], 'templow': data['templow'],
						 'is_holiday': data['is_holiday'], 'wind': data['wind'],
						 'humidity': data['humidity'], 'tempavg': data['tempavg'],
						 'hour': data['hour'], 'weekofyear': data['weekofyear'],
						 'dayofweek': data['dayofweek'], 'season': data['season'],
						 'energy': data['energy']})
	# 分离数据集
	train = pd.concat([data[0:995], data[1156:]], axis=0, ignore_index=True)
	#train = data[:data.shape[0]-168]
	#test = data[data.shape[0]-168:]
	test = data[995:1156]
	#train.to_csv('../data/电量train.csv')
	#test.to_csv('../data/电量test.csv')
	X_train = train.drop(['energy'], axis=1)
	Y_train = train['energy']
	X_test = test.drop(['energy'], axis=1)
	Y_test = test['energy']
	param = {'silent': True
		, 'obj': 'reg:linear'
		, "max_depth": 8
		, "eta": 0.01
		, "gamma": 0
		, "lambda": 1
		, "alpha": 0
		, "colsample_bytree": 1
		, "colsample_bylevel": 0.4
		, "colsample_bynode": 1
		, "nfold": 5}
	num_round=1000
	#cvresult = xgb.cv(param, dfull, num_round)
	dtrain = xgb.DMatrix(X_train, Y_train)
	dtest = xgb.DMatrix(X_test, Y_test)
	bst = xgb.train(param, dtrain, num_round)
	# 保存模型
	pickle.dump(bst, open(model_path, "wb"))
	print("电量模型训练完成")
	ypreds = bst.predict(dtest)
	#print('预测1小时准确率%5.2f' % r2_score(Y_test, ypreds))
	testset.index = testset['SDATE'].tolist()
	testset = testset.drop(['SDATE'], axis=1)
	testset['ypreds'] = ypreds
	testset = testset.resample('D').sum()
	#	print('预测每天准确率%5.2f' % r2_score(testset['energy'], testset['ypreds']))
	compare = pd.DataFrame({'Y_test': Y_test, 'ypreds': ypreds})
	compare['准确率'] = 1 - abs(compare['Y_test'] - compare['ypreds']) / compare['Y_test']
	print('预测每小时准确率%5.2f' % compare['准确率'].mean())
	compare1 = pd.DataFrame({'Y_test': testset['energy'], 'ypreds': testset['ypreds']})
	compare1['准确率'] = 1 - abs(compare1['Y_test'] - compare1['ypreds']) / compare1['Y_test']
	print('预测每天准确率%5.2f' % compare1['准确率'].mean())
	print('预测测试集准确率%5.2f' % (1 - abs(compare1['Y_test'].sum() - compare1['ypreds'].sum()) / compare1['Y_test'].sum()))
	'''
	yfpreds = bst.predict(dfull)  # 传统接口predict
	dataset.index = dataset['SDATE'].tolist()
	dataset = dataset.drop(['SDATE'], axis=1)
	dataset['yfpreds'] = yfpreds
	dataset = dataset.resample('D').sum()
	print('预测每天准确率%5.2f' % r2_score(dataset['total'],dataset['yfpreds']))
	data = pd.DataFrame({'SDATE': dataset.index, 'total': dataset.total, 'yfpreds': dataset.yfpreds})
	data1 = data.copy()
	data['SDATE'] = data['SDATE'].dt.date
	data.index = range(data.shape[0])
	data1 = data1.drop(['SDATE'], axis=1)
	data1 = data1.resample('M').sum()
	'''
if __name__ == '__main__':
	Model_construcion(input_path = "../dl/deal电量.csv", model_path = "../dl/dlhourxgboost.dat")