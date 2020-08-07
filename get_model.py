#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import os
import json
import re
import time
from branch_code.dadi_loader import train_model,get_time_dif,valuation,train_model1
from sklearn.externals import joblib
# from data_from_oracle import *
from outlier_processing import outlier_process_train
from data_transform import pp_yc_process,yuanchang_process

def peijian_model(handle):
    start_time_model = time.time()
    # extract=Extract()
    df=handle.get_months_data()
    df=outlier_process_train(df)
    df=pp_yc_process(df)
    ss, xgb_model, x_test, y_test=train_model(df)
    print('模型训练耗时为{}'.format(get_time_dif(start_time_model)))
    joblib.dump(ss, "pp_yc/data_ss.model")  ## 将标准化模型保存
    joblib.dump(xgb_model, "pp_yc/gbm.model")  ## 将模型保存
    y_pred = xgb_model.predict(x_test)
    valuation(y_pred, y_test)
    handle.logger.info(('get model done!'))

if __name__=='__main__':
    os.environ['CUDA_VISIBLE_DEVICES'] = "0"
    peijian_model()