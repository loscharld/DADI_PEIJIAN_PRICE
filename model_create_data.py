#! usr/bin/env python3
# -*- coding:utf-8 -*-

from ForCall01 import *
import numpy as np
import pandas as pd
import json
from sklearn.externals import joblib

def get_commonname():
    commit='''select * from(
select BRAND_ID,BRAND_NAME,COMMON_NAME,ORIGINALCODE,COMMON_ID,POS_ID,POS_NAME,count(ORIGINALCODE)ct from LB_PEIJIAN_ORIGINAL_HANDLE 
group by  BRAND_ID,BRAND_NAME,COMMON_NAME,ORIGINALCODE,COMMON_ID,POS_ID,POS_NAME order by ct desc )where ct>=10 and rownum<15000001'''
    account="dd_data2/xdf123@10.9.1.169/lbora"
    oracle = useOracle("dd_data2", "xdf123", "LBORA169")
    df=oracle.getData(commit,account)
    df.drop(['CT'],axis=1,inplace=True)
    return df

def create_data():
    df1=get_commonname()
    with open('changfang/peijian2standardcode.json',encoding='utf-8') as f1:
        peijian2standardcode = json.load(f1)
    df1['STANDARD_PART_CODE']=df1['COMMON_NAME'].map(peijian2standardcode)
    with open('pp_yc/jigou2id.json',encoding='utf-8') as f2:
        jigou2id=json.load(f2)
    fd=pd.DataFrame()
    for ch in range(2,4):
        df=pd.DataFrame()
        for jg in jigou2id.keys():
            df2=df1.copy()
            df2['JIGOU']=jg
            df=pd.concat([df,df2],axis=0)
        df['CHGCOMPSET']=ch
        fd=pd.concat([fd,df],axis=0)

    return fd

def model_predict():
    df=create_data()
    df=pd.DataFrame(df,columns=['JIGOU','BRAND_ID','BRAND_NAME','COMMON_NAME','COMMON_ID','POS_ID','POS_NAME','ORIGINALCODE','STANDARD_PART_CODE','CHGCOMPSET'])
    test_data = df[['JIGOU','BRAND_NAME','COMMON_NAME','ORIGINALCODE','CHGCOMPSET']]
    ss = joblib.load("pp_yc/data_ss.model")  ## 加载模型
    gbm_model = joblib.load("pp_yc/gbm.model")  ## 加载模型

    with open('pp_yc/jigou2id.json', encoding='utf-8') as reader1:
        jigou2id = json.load(reader1)

    with open('pp_yc/compname2id.json', encoding='utf-8') as reader2:
        compname2id = json.load(reader2)

    with open('pp_yc/code2id.json', encoding='utf-8') as reader3:
        code2id = json.load(reader3)

    with open('pp_yc/brand2id.json', encoding='utf-8') as reader4:
        brand2id = json.load(reader4)

    test_data['jigou_id'] = test_data['JIGOU'].map(jigou2id)
    test_data['brand_id'] = test_data['BRAND_NAME'].map(brand2id)
    # test_data['cate_name_id'] = test_data[FLAGS.CATE_NAME].map(cate_name2id)
    test_data['compname_id'] = test_data['COMMON_NAME'].map(compname2id)
    test_data['code_id'] = test_data['ORIGINALCODE'].map(code2id)
    test_data.dropna(subset=['jigou_id', 'brand_id', 'compname_id', 'code_id'], how='any', axis=0, inplace=True)
    test_data = test_data.reset_index(drop=True)
    df2 = pd.DataFrame(test_data, columns=['jigou_id', 'brand_id', 'compname_id', 'code_id','CHGCOMPSET'])
    df2 = df2.astype(float)
    x_data = ss.transform(df2)
    datas_pre = list(np.exp(gbm_model.predict(x_data)))
    df3 = pd.DataFrame()
    df3['LGBM_VALUE'] = datas_pre
    df3['REFERENCE'] = df3['LGBM_VALUE'].map(lambda x: round(0.98*x))
    df3=pd.DataFrame(df3,columns=['REFERENCE'])
    test_data.drop(['jigou_id', 'brand_id', 'compname_id', 'code_id'],axis=1,inplace=True)
    df4 = pd.concat([test_data, df3], axis=1)
    df4=pd.merge(df,df4,on=['JIGOU','BRAND_NAME','COMMON_NAME','ORIGINALCODE','CHGCOMPSET'],how='left')
    df4.dropna(subset=['REFERENCE'],how='any', axis=0, inplace=True)
    df4 = pd.DataFrame(df4, columns=['JIGOU','BRAND_ID','BRAND_NAME','COMMON_NAME','COMMON_ID','POS_ID','POS_NAME','ORIGINALCODE','STANDARD_PART_CODE','CHGCOMPSET','REFERENCE'])
    df4.to_csv('pp_yc/全国常用配件模型预测.csv',index=None,encoding='utf-8')
    print('model predict done!')

def tes_model_predict():
    df=create_data()
    df=pd.DataFrame(df,columns=['JIGOU','BRAND_ID','BRAND_NAME','COMMON_NAME','COMMON_ID','POS_ID','POS_NAME','ORIGINALCODE','STANDARD_PART_CODE','CHGCOMPSET'])
    test_data = df[['JIGOU','BRAND_NAME','COMMON_NAME','ORIGINALCODE','CHGCOMPSET']]
    ss = joblib.load("pp_yc/data_ss.model")  ## 加载模型
    gbm_model = joblib.load("pp_yc/gbm.model")  ## 加载模型

    with open('pp_yc/jigou2id.json', encoding='utf-8') as reader1:
        jigou2id = json.load(reader1)

    with open('pp_yc/compname2id.json', encoding='utf-8') as reader2:
        compname2id = json.load(reader2)

    with open('pp_yc/code2id.json', encoding='utf-8') as reader3:
        code2id = json.load(reader3)

    with open('pp_yc/brand2id.json', encoding='utf-8') as reader4:
        brand2id = json.load(reader4)

    test_data['jigou_id'] = test_data['JIGOU'].map(jigou2id)
    test_data['brand_id'] = test_data['BRAND_NAME'].map(brand2id)
    # test_data['cate_name_id'] = test_data[FLAGS.CATE_NAME].map(cate_name2id)
    test_data['compname_id'] = test_data['COMMON_NAME'].map(compname2id)
    test_data['code_id'] = test_data['ORIGINALCODE'].map(code2id)
    test_data.dropna(subset=['jigou_id', 'brand_id', 'compname_id', 'code_id'], how='any', axis=0, inplace=True)
    test_data = test_data.reset_index(drop=True)
    df2 = pd.DataFrame(test_data, columns=['jigou_id', 'brand_id', 'compname_id', 'code_id','CHGCOMPSET'])
    df2 = df2.astype(float)
    x_data = ss.transform(df2)
    datas_pre = list(np.exp(gbm_model.predict(x_data)))
    df3 = pd.DataFrame()
    df3['LGBM_VALUE'] = datas_pre
    df3['REFERENCE'] = df3['LGBM_VALUE'].map(lambda x: round(0.98*x))
    df3=pd.DataFrame(df3,columns=['REFERENCE'])
    test_data.drop(['jigou_id', 'brand_id', 'compname_id', 'code_id'],axis=1,inplace=True)
    df4 = pd.concat([test_data, df3], axis=1)
    df4=pd.merge(df,df4,on=['JIGOU','BRAND_NAME','COMMON_NAME','ORIGINALCODE','CHGCOMPSET'],how='left')
    df4.dropna(subset=['REFERENCE'],how='any', axis=0, inplace=True)
    df4 = pd.DataFrame(df4, columns=['JIGOU','BRAND_ID','BRAND_NAME','COMMON_NAME','COMMON_ID','POS_ID','POS_NAME','ORIGINALCODE','STANDARD_PART_CODE','CHGCOMPSET','REFERENCE'])
    df4.to_csv('pp_yc/测试集全国常用配件模型预测.csv',index=None,encoding='utf-8')
    print('test model predict done!')


if __name__=='__main__':
    model_predict()
    # tes_model_predict()
