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
from ajdl_datapreprocessing import all_dealdata as dl_preprocession
from dlhourmodeling import Model_construcion as Model_construcion
from get_data import deal_data as get_data
os.chdir(DIR);

if __name__ == '__main__':
	get_data()
	dl_preprocession()
	Model_construcion(input_path = "../dl/deal电量.csv", model_path = "../dl/dlhourxgboost.dat")
