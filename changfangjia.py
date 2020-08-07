#! usr/bin/env python3
# -*- coding:utf-8 -*-

from ForCall01 import *
import pandas as pd
from branch_code.dadi_loader import *
import json

def changfang_price():
    oracle = useOracle("dd_data2", "xdf123", "LBORA169")
    tableName='LB_PEIJIAN_ORIGINAL_HANDLE'
    commit="select * from {} t where rownum<15000001".format(tableName)
    datas=oracle.getData(commit,"dd_data2/xdf123@10.9.1.169/lbora")
    datas['BRAND_NAME'] = datas['BRAND_NAME'].replace('\t', '')
    if len(datas):
        datas.drop(['CATE_NAME'], axis=1, inplace=True)
        datas = datas.drop_duplicates()
        datas['CODE'] = datas.apply(lambda row:get_original_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']), axis=1)
        datas['PARTSTANDARDCODE'] = datas.apply(lambda row: get_standard_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']),axis=1)
        standard_code = datas.groupby(['JIGOU','BRAND_NAME','COMMON_NAME'])['PARTSTANDARDCODE'].apply(
            standard_bianma_process).apply(pd.Series).reset_index()
        standard_code.rename(columns={0: 'STANDARD_PART_CODE'}, inplace=True)
        datas = pd.merge(datas, standard_code, on=['JIGOU','BRAND_NAME','COMMON_NAME'], how='left')
        peijian2standardcode=dict(zip(datas['COMMON_NAME'],datas['STANDARD_PART_CODE']))
        with open('changfang/peijian2standardcode.json','w',encoding='utf-8') as w1:
            json.dump(peijian2standardcode,w1,ensure_ascii=False)
        # datas.drop(['ORIGINALCODE', 'PARTSTANDARDCODE'], axis=1, inplace=True)
        datas['WAY_FLAG'] = datas.apply(lambda row: get_flag(row['CODE']), axis=1)
        df=datas[(datas['CHANGFANGJIA']>0) & (datas['WAY_FLAG']==1)]
        #编码异常数据
        # outliers=datas[datas['WAY_FLAG']==0].reset_index(drop=True)
        # if len(outliers):
        #     outliers=pd.DataFrame(outliers,columns=['LOSSAPPROVALID','REGISTNO','JIGOU','DD_BRAND_ID','MODEL_BRAND','MODEL_CATE_NAME','COMMONNAME','POSID','POSNAME','CODE','CHANGFNAG','VERIFYFINALDATE'])
        #     outliers.rename(columns={'JIGOU': 'REGION'}, inplace=True)
        #     outliers.to_excel('outliers/4S店价异常数据.xlsx',index=None,encoding='utf-8')
        df.dropna(subset=['OPERATETIMEFORHIS'],axis=0,how='any',inplace=True)
        df=df.groupby(['JIGOU','BRAND_ID','BRAND_NAME','COMMON_NAME','COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE'])[
            'CHANGFANGJIA','OPERATETIMEFORHIS'].apply(get_price).apply(pd.Series).reset_index()
        df.rename(columns={0:'REFERENCE', 1:'CURRENT_TIME'},inplace=True)
        df['PRICE_TYPE']='4S店价'
        df=pd.DataFrame(df,columns=['JIGOU','BRAND_ID','BRAND_NAME','COMMON_NAME','COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE','PRICE_TYPE','REFERENCE','CURRENT_TIME'])
        # columns = ['LOSSAPPROVALID','REGISTNO','JIGOU','DD_BRAND_ID','MODEL_BRAND','MODEL_CATE_NAME','COMMONNAME','POSID','POSNAME','CODE','CHANGFNAG', 'NEW_TIME']
        datas = df.drop_duplicates().reset_index(drop=True)
        datas.rename(columns={'CODE':'ORIGINALCODE'},inplace=True)
        datas.to_csv('changfang/4S店价.csv',index=None,encoding='utf-8')
    else:
        datas = pd.DataFrame(datas,columns = ['JIGOU','BRAND_ID','BRAND_NAME','CATE_NAME','COMMON_NAME','COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE','PRICE_TYPE','REFERENCE','CURRENT_TIME'])
    print('changfang price done!')
    return datas


if __name__=='__main__':
    start_time = time.time()
    changfang_price()
    print(get_time_dif(start_time))