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
def pre(input_path = "../data/总能量test.csv", output_path = "../data/winter能量训练hour.csv"):
	# 读取路径中的文件
	data = pd.read_csv(input_path,engine='python', encoding="utf_8_sig")
	data = data.rename(columns={'时间': 'SDATE','YC467':'energy'})
	data['tempavg'] = (data['temphigh'] + data['templow']) / 2
	data['tempavg'] = data['tempavg'].astype('int')
	data['SDATE'] = pd.to_datetime(data['SDATE'])
	data = data.loc[(data['energy'] < 1500) & (data['energy'] >= 0)]
	data.index = range(data.shape[0])  # 重置索引
	data['hour'] = data['SDATE'].dt.hour
	data['dayofweek'] = data['SDATE'].dt.dayofweek+1
	data['weekofyear'] = data['SDATE'].dt.weekofyear
	data.dropna(inplace=True)
	if os.path.exists(output_path):
		data.to_csv(output_path, encoding="utf_8_sig")
	else:
		data.to_csv(output_path, encoding="utf_8_sig")

if __name__ == '__main__':
    pre(input_path = "../data/总能量test1.csv", output_path = "../data/summer能量训练hour.csv")