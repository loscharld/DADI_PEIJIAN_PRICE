#! usr/bin/env python3
# -*- coding:utf-8 -*-
import pandas as pd
from ForCall01 import *

def verification():
    path1='pp_yc/全国常用配件模型预测.csv'
    df1=pd.read_csv(open(path1,encoding='utf-8'))

    path2='pp_yc/配件品牌价原厂价统计数据及模型预测.csv'
    df2=pd.read_csv(open(path2,encoding='utf-8'))
    df2=pd.DataFrame(df2,columns=['JIGOU','BRAND_ID','ORIGINALCODE','CHGCOMPSET','count','mode','mean'])
    df=pd.merge(df1,df2,on=['JIGOU','BRAND_ID','ORIGINALCODE','CHGCOMPSET'],how='left')
    df.drop_duplicates(subset=['JIGOU','BRAND_ID','ORIGINALCODE','CHGCOMPSET'],keep='first',inplace=True)
    #统计JIGOU，BRAND_ID，ORIGINALCODE的均值
    national_mean=df.groupby(['BRAND_ID','ORIGINALCODE','CHGCOMPSET'])['mean'].agg(['mean']).reset_index()
    national_mean.rename(columns={'mean':'national_mean'},inplace = True)
    def transform(x):
        try:
            con=round(x)
        except:
            con=np.nan
        return con
    national_mean['national_mean']=national_mean['national_mean'].apply(transform)
    df=pd.merge(df,national_mean,on=['BRAND_ID','ORIGINALCODE','CHGCOMPSET'],how='left')
    #模型预测值与均值的偏差超过一个阈值，用全国均值乘以一个系数去替代
    df['coef_national']=df.apply(lambda x:(x['REFERENCE']-x['national_mean'])/x['national_mean'],axis=1)
    df['REFERENCE']=df.apply(lambda x:round(1.0*x['national_mean']) if x['coef_national']>=0.5 or x['coef_national']<-0.5 else x['REFERENCE'],axis=1)
    df['coef']=df.apply(lambda x:(x['REFERENCE']-x['mean'])/x['mean'],axis=1)
    df.to_csv('pp_yc/常用配件模型预测值验证查看.csv',index=None,encoding='utf-8')
    df=pd.DataFrame(df,columns=df1.columns)
    df.to_csv('pp_yc/常用配件模型预测值验证.csv',index=None,encoding='utf-8')