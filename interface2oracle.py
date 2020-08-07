#! usr/bin/env python3
# -*- coding:utf-8 -*-
import numpy as np
from ForCall01 import *
from branch_code.dadi_loader import *
import pandas as pd
import time

def inter2oracle():
    account="dd_data2/xdf123@10.9.1.169/lbora"
    tableName = 'LB_APMS_PARTS'
    oracle = useOracle("dd_data2", "xdf123", "LBORA169")
    connection = cx_Oracle.connect(account)
    #每次从表LB_PEIJIAN_SYSTEM读取一个机构数据(生成数据插入表LB_APMS_PARTS_LS)
    with open('pp_yc/jigou2id.json',encoding='utf-8') as f2:
        jigou_ls=json.load(f2)
    df_all=pd.DataFrame()
    for jigou in jigou_ls:
        commit1='''select * from LB_PEIJIAN_SYSTEM t where t.JIGOU='{}' '''.format(jigou)
        try:
            cursor = connection.cursor()
            LB_APMS_PARTS = oracle.getDatapart(commit1, cursor)
            # 以4个字段为基准，把4S店价，品牌价和原厂价的价格左连接拼接
            LB_APMS_PARTS_BASE = LB_APMS_PARTS[['PART_ID', 'JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME','ORIGINALCODE']]
            LB_APMS_PARTS_BASE.dropna(subset=['PART_ID'], how='any', axis=0, inplace=True)
            LB_APMS_PARTS_BASE = LB_APMS_PARTS_BASE.drop_duplicates()

            datas1=LB_APMS_PARTS.loc[LB_APMS_PARTS['PRICE_TYPE']=='4S店价']
            datas1=pd.DataFrame(datas1,columns=['PART_ID','JIGOU','JIGOU_ID','BRAND_ID','BRAND_NAME', 'COMMON_NAME', 'COMMON_ID',
                                 'POS_ID', 'POS_NAME','ORIGINALCODE', 'STANDARD_PART_CODE','STATUS','INSERT_TIME','REFERENCE','METHOD'])
            datas1.rename(columns={'REFERENCE':'FACTORY_PRICE','METHOD':'CREATE_WAY'},inplace=True)
            datas1.dropna(subset=['PART_ID'],how='any', axis=0, inplace=True)

            datas2=LB_APMS_PARTS.loc[LB_APMS_PARTS['PRICE_TYPE']=='品牌价']
            datas2=pd.DataFrame(datas2,columns=['PART_ID','ORIGINALCODE','REFERENCE','COUNT'])
            datas2.rename(columns={'REFERENCE': 'BRAND_PRICE','COUNT':'COUNT1'}, inplace=True)
            datas2.dropna(subset=['PART_ID'], how='any', axis=0, inplace=True)

            datas3=LB_APMS_PARTS.loc[LB_APMS_PARTS['PRICE_TYPE']=='原厂价']
            datas3=pd.DataFrame(datas3,columns=['PART_ID','ORIGINALCODE','REFERENCE','COUNT'])
            datas3.rename(columns={'REFERENCE': 'ORIGIN_PRICE','COUNT':'COUNT2'}, inplace=True)
            datas3.dropna(subset=['PART_ID'], how='any', axis=0, inplace=True)

            datasB1 = pd.merge(LB_APMS_PARTS_BASE, datas1,
                               on=['PART_ID', 'JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME','ORIGINALCODE'], how='left')
            datasB12 = pd.merge(datasB1, datas2, on=['PART_ID','ORIGINALCODE'], how='left')
            datasB123 = pd.merge(datasB12, datas3, on=['PART_ID','ORIGINALCODE'], how='left')
            # datasB123.fillna('',inplace=True)
            datasB123['FREQUENCY'] = datasB123.apply(lambda x: count_transform(x['COUNT1'], x['COUNT2']), axis=1)
            datasB123.rename(columns={'ORIGINALCODE': 'ORIGIN_CODE'}, inplace=True)
            datasB123['COMMON_ALIAS'] = ''
            datasB123['SHIYONG_PRICE'] = ''
            datasB123['LAST_TIME'] = ''
            # datasB123=datasB123.astype(str)
            datasB123=pd.DataFrame(datasB123,columns=['PART_ID','JIGOU_ID','JIGOU','BRAND_ID','BRAND_NAME', 'POS_ID', 'POS_NAME','COMMON_ID',
                'ORIGIN_CODE','COMMON_NAME','COMMON_ALIAS','CREATE_WAY','FACTORY_PRICE','ORIGIN_PRICE','BRAND_PRICE','SHIYONG_PRICE','STANDARD_PART_CODE',
                'FREQUENCY','STATUS','INSERT_TIME','LAST_TIME'])
            df_all=pd.concat([df_all,datasB123],axis=0)
        except Exception as e:
            print(e)

    path='yzt/yzt.csv'
    yzt=pd.read_csv(open(path,encoding='utf-8'))
    yzt.rename(columns={'MODEL_BRAND': 'BRAND_NAME','ORIGINALFITS_CODE':'ORIGIN_CODE'}, inplace=True)
    # yzt['JIGOU_ID'] = yzt['JIGOU_ID'].astype(str)
    df_all=pd.merge(df_all,yzt,how='left',on=['JIGOU_ID','BRAND_NAME','ORIGIN_CODE'])
    df_all.replace('',np.nan,inplace=True)
    df_all["FACTORY_PRICE"].fillna(df_all["CHANGFANGJIA"], inplace=True)
    df_all["ORIGIN_PRICE"].fillna(df_all["YUANCHANGJIA"], inplace=True)
    df_all["BRAND_PRICE"].fillna(df_all["PINPAIJIA"], inplace=True)
    # print(df_all.info())
    df_all.drop(['CHANGFANGJIA','YUANCHANGJIA','PINPAIJIA'],axis=1,inplace=True)
    with open('changfang/peijian2standardcode.json', encoding='utf-8') as f1:
        peijian2standardcode = json.load(f1)
    df_all['STANDARD_PART_CODE'] = df_all['COMMON_NAME'].map(peijian2standardcode)
    df_all['STANDARD_PART_CODE'].fillna('999999', inplace=True)
    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    df_all['INSERT_TIME'] = now_time
    df_all = df_all.astype(str)
    df_all.replace('nan','',inplace=True)
    oracle.Batchpeijian13insertDataToTable(df_all, tableName, account)
    print('all done!')




if __name__=='__main__':
    start_time = time.time()
    inter2oracle()
    print(get_time_dif(start_time))