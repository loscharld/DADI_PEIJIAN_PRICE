#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import numpy as np
import pandas as pd
import json
import re
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import GridSearchCV
import xgboost as xgb
import time
from datetime import timedelta
from sklearn.model_selection import train_test_split
from sklearn.linear_model.coordinate_descent import ConvergenceWarning
from sklearn.preprocessing import StandardScaler
import warnings
from data_from_oracle import args
# import tensorflow as tf
from datetime import datetime
# import lightgbm as lgb



def get_time_dif(start_time):
    """获取已使用时间"""
    end_time = time.time()
    time_dif = end_time - start_time
    return timedelta(seconds=int(round(time_dif)))

def province_transform(x):
    x = str(x)
    if x.startswith('6501'):
        con = '新疆'
    elif x.startswith('2102'):
        con = '大连'
    elif x.startswith('4501'):
        con = '广西'
    elif x.startswith('3502'):
        con = '厦门'
    elif x.startswith('3401'):
        con = '安徽'
    elif x.startswith('4601'):
        con = '海南'
    elif x.startswith('2201'):
        con = '吉林'
    elif x.startswith('3702'):
        con = '青岛'
    elif x.startswith('6101'):
        con = '陕西'
    elif x.startswith('3101'):
        con = '上海'
    elif x.startswith('5001'):
        con = '重庆'
    elif x.startswith('3501'):
        con = '福建'
    elif x.startswith('6201'):
        con = '甘肃'
    elif x.startswith('2101'):
        con = '辽宁'
    elif x.startswith('1501'):
        con = '内蒙古'
    elif x.startswith('3302'):
        con = '宁波'
    elif x.startswith('6401'):
        con = '宁夏'
    elif x.startswith('4403'):
        con = '深圳'
    elif x.startswith('5101'):
        con = '四川'
    elif x.startswith('1201'):
        con = '天津'
    elif x.startswith('3301'):
        con = '浙江'
    elif x.startswith('1301'):
        con = '河北'
    elif x.startswith('4101'):
        con = '河南'
    elif x.startswith('3201'):
        con = '江苏'
    elif x.startswith('3601'):
        con = '江西'
    elif x.startswith('3701'):
        con = '山东'
    elif x.startswith('5201'):
        con = '贵州'
    elif x.startswith('1401'):
        con = '山西'
    elif x.startswith('5401'):
        con = '西藏'
    elif x.startswith('5301'):
        con = '云南'
    elif x.startswith('4401'):
        con = '广东'
    elif x.startswith('2301'):
        con = '黑龙江'
    elif x.startswith('4201'):
        con = '湖北'
    elif x.startswith('1101'):
        con = '北京'
    elif x.startswith('4301'):
        con = '湖南'
    elif x.startswith('6301'):
        con = '青海'
    else:
        con = np.nan
    return con

def paint_trainsform(x):
    x=str(x)
    if '全喷' in x:
        con=3
    elif '半喷' in x:
        con=2
    elif '抛光' in x:
        con=1
    else:
        con=3
    return con


# 箱线图异常值检测,返回异常值(统计数据)
def detectoutliers_stc(price):
    outlier_list_col = []
        # 1st quartile (25%)
    Q1 = np.percentile(price, 25)
    # 3rd quartile (75%)
    Q3 = np.percentile(price,75)
    IQR = Q3 - Q1
    outlier_step = 1.5 * IQR
    try:
        for i in price:
            if ((i < Q1 - outlier_step) | (i > Q3 + outlier_step )):
                outlier_list_col.append(i)
    except Exception as e:
        print(e)
    if len(outlier_list_col) >0:
        return outlier_list_col

# 箱线图异常值检测,返回异常值(训练模型)
def detectoutliers_train(price):
    outlier_list_col = []
        # 1st quartile (25%)
    Q1 = np.percentile(price, 25)
    # 3rd quartile (75%)
    Q3 = np.percentile(price,75)
    IQR = Q3 - Q1
    outlier_step = 1.5* IQR
    up_limit=Q3+outlier_step
    low_limit=Q1-outlier_step
    try:
        for i in price:
            if ((i < low_limit) | (i > up_limit)):
                outlier_list_col.append(i)
    except Exception as e:
        print(e)
    if len(outlier_list_col) >0 :
        return outlier_list_col

def get_upper(x):
    try:
        con = x.upper()
    except:
        con = x
    return con


def valuation(prediction, label):
    result = np.sqrt(mean_squared_error(prediction, label))
    print('RMSE误差是：{}'.format(result))

def paint_trainsform1(x):
    x=str(x)
    if '全喷' in  x:
        con='全喷'
    elif '半喷' in x:
        con='半喷'
    elif '抛光' in x:
        con='抛光'
    else:
        con='全喷'
    return con

def banjin_transform(x):
    x=str(x)
    if '(大)' in x:
        con=3
    elif '(中)' in x:
        con=2
    elif '(小)' in x:
        con=1
    else:
        con=np.nan
    return con

def banjin_transform1(x):
    x=str(x)
    if '(大)' in x:
        con='整形修复(大)'
    elif '(中)' in x:
        con='整形修复(中)'
    elif '(小)' in x:
        con='整形修复(小)'
    else:
        con=np.nan
    return con

def chaizhuang_transform(x):
    x=str(x)
    if '(含附件)' in x:
        con=2
    else:
        con=1
    return con

def chaizhuang_transform1(x):
    x=str(x)
    if '(含附件)' in x:
        con='含附件'
    else:
        con='不含附件'
    return con

def jixiu_transform(x):
    x=str(x)
    if '(含调整)' in x:
        con='(含调整)'
    elif '(不含调整)' in x:
        con='(不含调整)'
    else:
        con='修复'
    return con

def train_model1(df):
    ## 拦截异常
    warnings.filterwarnings(action='ignore', category=ConvergenceWarning)
    y = np.log(df['UNITPRICE'])
    x = df.drop('UNITPRICE', axis=1, inplace=False)
    # 数据的分割，
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=14)
    # 标准化
    ss = StandardScaler(with_mean=True, with_std=True)
    x_train = ss.fit_transform(x_train)
    x_test = ss.transform(x_test)

    class grid():
        def __init__(self, model):
            self.model = model
        def grid_get(self, X, y, param_grid):
            grid_search = GridSearchCV(self.model, param_grid, cv=5, scoring="neg_mean_squared_error")
            grid_search.fit(X, y)
            print(grid_search.best_params_, np.sqrt(-grid_search.best_score_))
            #         grid_search.cv_results_['mean_test_score'] = np.sqrt(-grid_search.cv_results_['mean_test_score'])
            #         print(pd.DataFrame(grid_search.cv_results_)[['params','mean_test_score','std_test_score']])
            return grid_search

    gbm_model = grid(lgb.LGBMRegressor(objective='regression',metric='rmse')).grid_get(x_train, y_train,{
                                                                        # 'max_depth':[9],
                                                                        'num_leaves' : [900,1200,1500],
                                                                    'learning_rate': [0.05,0.1],
                                                                     'n_estimators': [900,1200,1600],
                                                                        'device': ['gpu'],
                                                                        'gpu_platform_id': [0],
                                                                        'gpu_device_id': [0]})

    return ss, gbm_model, x_test, y_test

def train_model(df):
    ## 拦截异常
    warnings.filterwarnings(action='ignore', category=ConvergenceWarning)
    y = np.log(df[args.PRICE])
    x = df.drop(args.PRICE, axis=1, inplace=False)
    # 数据的分割，
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=14)
    # 标准化
    ss = StandardScaler(with_mean=True, with_std=True)
    x = ss.fit_transform(x)
    x_test = ss.transform(x_test)

    class grid():
        def __init__(self, model):
            self.model = model

        def grid_get(self, X, y, param_grid):
            grid_search = GridSearchCV(self.model, param_grid, cv=5, scoring="neg_mean_squared_error")
            grid_search.fit(X, y)
            print(grid_search.best_params_, np.sqrt(-grid_search.best_score_))
            #         grid_search.cv_results_['mean_test_score'] = np.sqrt(-grid_search.cv_results_['mean_test_score'])
            #         print(pd.DataFrame(grid_search.cv_results_)[['params','mean_test_score','std_test_score']])
            return grid_search

    xgb_model = grid(xgb.XGBRegressor(objective="reg:squarederror", n_jobs=-1)).grid_get(x, y,{
                                                                                    'gpu_id': [0],
                                                                                   'tree_method': ['gpu_hist'],
                                                                                    # 'reg_alpha': [0.05, 0.1],
                                                                                    # 'reg_lambda': [0.05, 0.1],
                                                                                    # 'subsample': [0.6, 0.7],
                                                                                    # 'colsample_bytree': [0.6, 0.7],
                                                                                    # 'gamma': [0.1, 0.2],
                                                                                    # 'min_child_weight': [2,4,6],
                                                                                    'max_depth': [15],
                                                                                    'learning_rate': [0.1],
                                                                                    'n_estimators': [8000]})

    return ss, xgb_model, x_test, y_test

def train_model2(df):
    ## 拦截异常
    warnings.filterwarnings(action='ignore', category=ConvergenceWarning)
    y = np.log(df[FLAGS.PRICE])
    x = df.drop(FLAGS.PRICE, axis=1, inplace=False)
    ## Pipeline常用于并行调参
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import PolynomialFeatures
    from sklearn.linear_model import LassoCV, RidgeCV, ElasticNetCV
    models = [
        Pipeline([
            ('poly', PolynomialFeatures()),
            ('linear', RidgeCV(alphas=np.logspace(-3, 1, 20)))
        ])
        # Pipeline([
        #     ('poly', PolynomialFeatures()),
        #     ('linear', LassoCV(alphas=np.logspace(-3, 1, 20)))
        # ])
    ]

    # 参数字典
    parameters = {
        "poly__degree": [3, 2, 1],
        "poly__interaction_only": [True, False],  # 不产生交互项，如X1*X2
        "poly__include_bias": [True, False],  # 多项式幂为零的特征作为线性模型中的截距
        "linear__fit_intercept": [True, False]
    }

    # 数据的分割，
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=14)
    # 标准化
    ss = StandardScaler(with_mean=True, with_std=True)
    x_train = ss.fit_transform(x_train)
    x_test = ss.transform(x_test)

    titles = ['Ridge', 'Lasso']
    # 获取模型并设置参数
    model = GridSearchCV(models[0], param_grid=parameters, cv=5, n_jobs=-1)  # 五折交叉验证
    # 模型训练-网格搜索
    model.fit(x_train, y_train)
    # 模型效果值获取（最优参数）

    print("%s算法:最优参数:" % titles[0], model.best_params_)
    print("%s算法:R值=%.3f" % (titles[0], model.best_score_))
    return ss, model, x_test, y_test

#提取当前时间往前推一年的数据
def get_time_data(datas):
    datas['date'] = pd.to_datetime(datas['核损通过时间'])
    # datas['date'] = datas['date'].map(lambda x:x.strftime('%Y-%m-%d'))
    datas['date'] = datas['date'].map(lambda x: pd.to_datetime(x).date())
    num = len(datas) - 1
    end_time = datas['date'][num]
    print(type(end_time))
    # end_time=pd.to_datetime(end_time)
    # 得到今年的的时间 （年份） 得到的today_year等于2016年
    today_year = end_time.year
    # 今年的时间减去1，得到去年的时间。last_year等于2015
    last_year = int(end_time.year) - 1
    # 得到今年的每个月的时间。today_year_months等于1 2 3 4 5 6 7 8 9，
    today_year_months = range(1, end_time.month + 2)
    # 得到去年的每个月的时间  last_year_months 等于10 11 12
    last_year_months = range(end_time.month + 1, 13)
    # 定义列表去年的数据
    data_list_lasts = []
    # 通过for循环，得到去年的时间夹月份的列表
    # 先遍历去年每个月的列表
    for last_year_month in last_year_months:
        # 定义date_list 去年加上去年的每个月
        date_list = '%s-%s' % (last_year, last_year_month)
        # 通过函数append，得到去年的列表
        data_list_lasts.append(date_list)

    data_list_todays = []
    # 通过for循环，得到今年的时间夹月份的列表
    # 先遍历今年每个月的列表
    for today_year_month in today_year_months:
        # 定义date_list 去年加上今年的每个月
        data_list = '%s-%s' % (today_year, today_year_month)
        # 通过函数append，得到今年的列表
        data_list_todays.append(data_list)
    # 去年的时间数据加上今年的时间数据得到年月时间列表
    data_year_month = data_list_lasts + data_list_todays
    data_year_month.reverse()
    start_time = pd.to_datetime(data_year_month[-1]).date()
    end_time = end_time
    # datas = datas.loc[(datas['date'] <= end_time) & (datas['date'] >= start_time)]
    return start_time,end_time
#取一年的数据
def get_time_from_table_1year(end_time):
    today_year = end_time.year
    # 今年的时间减去1，得到去年的时间。last_year等于2015
    last_year = int(end_time.year) - 1
    today_day = end_time.day - 1
    # 得到今年的每个月的时间。today_year_months等于1 2 3 4 5 6 7 8 9，
    today_year_months = range(1, end_time.month + 1)
    # 得到去年的每个月的时间  last_year_months 等于10 11 12
    last_year_months = range(end_time.month, 13)
    # 定义列表去年的数据
    data_list_lasts = []
    # 通过for循环，得到去年的时间夹月份的列表
    # 先遍历去年每个月的列表
    for last_year_month in last_year_months:
        # 定义date_list 去年加上去年的每个月
        date_list = '%s-%s' % (last_year, last_year_month)
        # 通过函数append，得到去年的列表
        data_list_lasts.append(date_list)

    data_list_todays = []
    # 通过for循环，得到今年的时间夹月份的列表
    # 先遍历今年每个月的列表
    for today_year_month in today_year_months:
        # 定义date_list 去年加上今年的每个月
        data_list = '%s-%s' % (today_year, today_year_month)
        # 通过函数append，得到今年的列表
        data_list_todays.append(data_list)
    # 去年的时间数据加上今年的时间数据得到年月时间列表
    data_year_month = data_list_lasts + data_list_todays
    data_year_month.reverse()
    start_time = pd.to_datetime(data_year_month[-1])
    delta = timedelta(days=today_day)
    start_time = start_time + delta
    end_time = end_time
    # datas = datas.loc[(datas['date'] <= end_time) & (datas['date'] >= start_time)]
    return start_time, end_time


def compname_process(x):
    x=str(x)
    if '喷漆' in x:
        con=x.replace('喷漆','').replace('/','')
    elif '(含调整)' in x:
        con=x.replace('(含调整)','').replace('/','')
    elif '拆装' in x:
        con=x.replace('拆装','').replace('/B柱','')
    elif '整形修复(中)' in x:
        con=x.replace('整形修复(中)','').replace('/B柱','')
    elif '整形修复(小)' in x:
        con=x.replace('整形修复(小)','').replace('/B柱','')
    elif '整形修复(大)' in x:
        con=x.replace('整形修复(大)','').replace('/B柱','')
    elif '修复' in x:
        con=x.replace('修复','').replace('/','')
    elif '/B柱' in x:
        con=x.replace('/B柱','')
    elif '/' in x:
        con = x.replace('/', '')
    else:
        con=x
    return con

def pinhua(x1,x2):
    x1=float(x1)
    x2=float(x2)
    if x1>=x2:
        x2=x1*1.1
    else:
        x2=x2
    return round(x2,-1)

#A档和B档比较，谁小取谁，作为第一个档次的基准
def pinhua1(x1,x2):
    x1=float(x1)
    x2=float(x2)
    if x1<=x2:
        con=x1
    else:
        con=x2
    return con


# 原厂编码处理
def get_original_bianma(code1, code2):
    code1 = str(code1)
    code2 = str(code2)
    # flag = []  # 存放处理后的编码，编码为0表示不符合条件
    re_code1=re.findall('[a-zA-Z]',code1)
    re_code2 = re.findall('[a-zA-Z]', code2)
    if not pd.isnull(code1):
        code1 = ''.join(code1.split())  # 去掉原厂编码中的空格
    if not pd.isnull(code2):
        code2 = ''.join(code2.split())  # 去掉标准编码中的空格
    if (len(str(code1)) >6 and len(set(str(code1))) > 1):  # 原厂编码大于6位且不是重复数字，则取原厂编码
        flag = code1
    elif re_code1 and len(str(code1))==6 and len(set(str(code1)))> 1:
        flag=code1
    elif (len(str(code1)) >=6 and len(set(str(code1))) == 1):  # 原厂编码大于等于6位且为重复数字，若标准编码大于6位且不是重复数字，则取标准编码，否则编码为0
        if (len(str(code2)) >6 and len(set(str(code2))) > 1):
            flag = code2
        else:
            flag = code1
    elif (len(str(code1)) <6):  # 原厂编码小于6位，标准编码大于6位且不为重复数字，则取标准编码，否则编码为0
        if (len(str(code2)) > 6 and len(set(str(code2))) > 1):
            flag = code2
        elif re_code2 and len(str(code2)) == 6:
            flag = code2
        #           print(flags)
        else:
            flag = code1
    elif code1 == 'None' and len(code2):
        flag = code2
    #             print(flags)
    elif code2 == 'None' and len(code1):
        flag = code1
    else:
        flag = 0
    # flag.append(flags)
    return flag

#得到标准编码
def get_standard_bianma(code1, code2):
    code1 = str(code1)
    code2 = str(code2)
    # flag = []  # 存放处理后的编码，编码为0表示不符合条件
    # re_code1=re.findall('[a-zA-Z]',code1)
    # re_code2 = re.findall('[a-zA-Z]', code2)
    if not pd.isnull(code1):
        code1 = ''.join(code1.split())  # 去掉原厂编码中的空格
    if not pd.isnull(code2):
        code2 = ''.join(code2.split())  # 去掉标准编码中的空格
    if (len(code1) ==6 and len(set(code1))!=1 and len(code2)!= 6):  # 原厂编码等于6位且不是重复数字，标准编码不是6位，则取原厂编码
        code = code1
    else:
        code=code2
    return code
#标准编码处理
def standard_bianma_process(code):
    try:
        sorted_code=sorted(code,key=lambda x:len(x),reverse=False)
        if len(sorted_code)==1:
            result=sorted_code[0]
        elif len(sorted_code)==2:
            if len(sorted_code[0])>=6:
                result=sorted_code[0]
            else:
                result=sorted_code[1]
        elif len(sorted_code)>2:
            if len(sorted_code[-1])<6:
                result=sorted_code[-1]
            else:
                if  len(sorted_code[0])>=6:
                    result=sorted_code[0]
                else:
                    for k in sorted_code:
                        if len(k)>=6:
                            result=k
                            break
    except Exception as e:
        print(e)
    return [result]

#编码打标签
def get_flag(code):
    code = str(code)
    re_code = re.findall('[a-zA-Z]', code)
    if (len(code) >6 and len(set(code)) > 1):  # 原厂编码大于6位且不是重复数字，则取原厂编码
        flag = 1
    elif len(code)==6 and re_code:
        flag = 1
    else:
        flag = 0
    return flag


# 价格处理，按车系、配件和编码分组后，取最近时间的价格作为厂方价
def get_price(data):
    lst=[]
    time1 = data['OPERATETIMEFORHIS'].tolist()
    price = data['CHANGFANGJIA'].tolist()
    ind = time1.index(max(time1))
    price1 = price[ind]
    lst.append(price1)
    lst.append(max(time1))
    return lst

#计数大于10取推荐值，否则取均值
def get_count_value(count,mode_v,reference_v):
    if count>100:
        result=reference_v
    else:
        result=mode_v
    return result

chgompset2id={'厂方价':1,'品牌价':2,'原厂价':3}
id2chgompset={v:k for k,v in chgompset2id.items()}

region2code={'大连':2102,'广西':4501,'厦门':3502,'安徽':3401,'海南':4601,'吉林':2201,'青岛':3702,'陕西':6101,
           '上海':3101,'重庆':5001,'福建':3501,'甘肃':6201,'辽宁':2101,'内蒙古':1501,'宁波':3302,'宁夏':6401,
           '深圳':4403,'四川':5101,'天津':1201,'浙江':3301,'河北':1301,'河南':4101,'江苏':3201,'江西':3601,
           '山东':3701,'贵州':5201,'山西':1401,'西藏':5401,'云南':5301,'广东':4401,'黑龙江':2301,'湖北':4201,
            '北京': 1101,'新疆': 6501,'青海': 6301,'湖南': 4301}


def count_transform(x1,x2):
    if x1=='' and x2=='':
        con=''
    elif x1!='' and x2=='':
        con=x1
    elif x1=='' and x2!='':
        con=x2
    elif x1>=x2:
        con=x1
    elif x1 < x2:
        con = x2
    else:
        con=''
    return con



