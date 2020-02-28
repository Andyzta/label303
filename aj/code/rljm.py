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
from ajrl_datapreprocessing import all_dealdata as nl_datapreprossion
from rlwinterhourmodeling import Model_construcion as winterModel_construcion
from get_data import deal_data as get_data
os.chdir(DIR);

    
if __name__ == '__main__':
	get_data()
	nl_datapreprossion()
	for root, dirs, files in os.walk('../rl'):
		for name in files:
			if name.startswith("deal"):
				yc=name.lstrip('deal').rstrip('.csv')
				data = pd.read_csv('../config/sbdy_config.csv')
				for i in range(len(data['id'])):
					if(data['bh'][i]==yc):
						nameid=str(data['id'][i])
				winterModel_construcion(input_path=os.path.join(root, name), model_path=os.path.join(root,nameid+'.dat'))
                
                
	
