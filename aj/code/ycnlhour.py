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
from xgboost import XGBRegressor as XGBR
import requests
import  demjson
import os
import xgboost as xgb
from sklearn.model_selection import KFold, cross_val_score as CVS, train_test_split as TTS
from time import time
from sklearn.metrics import mean_squared_error as MSE, r2_score
import pickle
import calendar
import time
from ycllhoursummer import prehour as prehoursummer
from ycrlhourwinter import prehour as prehourwinter
from predictweath_data import crawler as crawler
from predictweath_data import path as path
os.chdir(DIR);

if __name__ == '__main__':
	crawler(predict_date='prehour', output_path=path('data/prehour.csv'))
	x=time.localtime().tm_mon	
	if x in [11,12,1,2,3,4,5]:
		for root, dirs, files in os.walk('../rl'):
			for name in files:
				if name.endswith("dat"):
					id=int(name.rstrip('.dat'))
					prehourwinter(input_path="../data/prehour.csv",model_path=os.path.join(root, name),id=id)
	else:
		for root, dirs, files in os.walk('../ll'):
			for name in files:
				if name.endswith("dat"):
					id=int(name.rstrip('.dat'))
					prehourwinter(input_path="../data/prehour.csv",model_path=os.path.join(root, name),id=id)