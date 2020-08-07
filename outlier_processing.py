#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import pandas as pd
from data_from_oracle import *
from branch_code.dadi_loader import detectoutliers_train,detectoutliers_stc,get_original_bianma,get_flag,get_standard_bianma,standard_bianma_process
def outlier_process_train(df):
    # 有效编码
    df.drop([args.CATE_NAME], axis=1, inplace=True)
    df['CODE'] = df.apply(lambda row: get_original_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']), axis=1)
    df['WAY_FLAG'] = df.apply(lambda row: get_flag(row['CODE']), axis=1)
    df1 = df.loc[df['WAY_FLAG'] == 1].reset_index(drop=True)
    # #编码异常的
    # outlier1=df.loc[df['WAY_FLAG'] == 0].reset_index(drop=True)
    # if len(outlier1):
    #     outlier1 = pd.DataFrame(outlier1, columns=['LOSSAPPROVALID', 'REGISTNO', 'JIGOU', 'DD_BRAND_ID', 'MODEL_BRAND',
    #                                                'MODEL_CATE_NAME', 'COMMONNAME', 'POSID', 'POSNAME', 'CODE',
    #                                                'CHANGFNAG', 'VERIFYFINALDATE'])
    #     outlier1.rename(columns={'JIGOU': 'REGION'}, inplace=True)
    #     outlier1.to_excel('outliers/品牌价原厂价编码异常数据.xlsx', index=None, encoding='utf-8')
    #箱线图异常值
    outliers=df1.groupby([args.BRAND,args.PRICE_TYPE,'CODE'])[args.PRICE].apply(detectoutliers_train).reset_index()
    #有异常值的取出来
    outliers=outliers[outliers[args.PRICE].notnull()]
    # 将换件价格列拆开
    if len(outliers):
        outliers = outliers.set_index([args.BRAND,args.PRICE_TYPE,'CODE'])[args.PRICE].apply(pd.Series).stack().reset_index()
        outliers.columns = [args.BRAND,args.PRICE_TYPE,'CODE','INDEX',args.PRICE]
        #去重
        outliers=outliers[[args.BRAND,args.PRICE_TYPE,'CODE',args.PRICE]].drop_duplicates()
        outliers['FLAG'] = 1
        df1 = pd.merge(df1, outliers, how='left', on=[args.BRAND,args.PRICE_TYPE,'CODE',args.PRICE])
        #保存异常值
        outliers_table = df1.copy()
        outliers_table = outliers_table[outliers_table['FLAG'] == 1]
        outliers_table = pd.DataFrame(outliers_table,columns=['LOSSAPPROVALID', 'REGISTNO', 'JIGOU', 'BRAND_ID', 'BRAND_NAME',
                                                   'COMMON_NAME', 'POS_ID', 'POS_NAME', 'CODE','CHGCOMPSET','UNITPRICE',
                                                   'VERIFYFINALDATE'])
        outliers_table.rename(columns={'CODE':'ORIGINALCODE'},inplace=True)
        outliers_table.to_csv('outliers/品牌价原厂价价格异常值.csv', index=None, encoding='utf-8')
        #去除异常值
        df1.drop(df1[df1['FLAG']==1].index,inplace=True)
    else:
        df1=df1
    print(df1.shape[0])
    return df1

def outlier_process_stac(df):
    # 有效编码
    df.drop([args.CATE_NAME], axis=1, inplace=True)
    df['CODE'] = df.apply(lambda row: get_original_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']), axis=1)
    df['PARTSTANDARDCODE'] = df.apply(lambda row: get_standard_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']), axis=1)
    standard_code=df.groupby([args.REGION,args.BRAND])['PARTSTANDARDCODE'].apply(standard_bianma_process).apply(pd.Series).reset_index()
    standard_code.rename(columns={0:'STANDARD_PART_CODE'},inplace=True)
    df=pd.merge(df,standard_code,on=[args.REGION,args.BRAND],how='left')
    df['WAY_FLAG'] = df.apply(lambda row: get_flag(row['CODE']), axis=1)
    df = df.loc[df['WAY_FLAG'] == 1].reset_index(drop=True)
    outliers = df.groupby([args.BRAND, args.PRICE_TYPE, 'CODE'])[args.PRICE].apply(
        detectoutliers_train).reset_index()
    # 有异常值的取出来
    outliers = outliers[outliers[args.PRICE].notnull()]
    # 将核损总金额列拆开
    if len(outliers):
        outliers = outliers.set_index([args.BRAND, args.PRICE_TYPE, 'CODE'])[args.PRICE].apply(
            pd.Series).stack().reset_index()
        outliers.columns = [args.BRAND, args.PRICE_TYPE, 'CODE', 'INDEX', args.PRICE]
        # 去重
        outliers = outliers[[args.BRAND, args.PRICE_TYPE, 'CODE', args.PRICE]].drop_duplicates()
        outliers['FLAG'] = 1
        df = pd.merge(df, outliers, how='left', on=[args.BRAND, args.PRICE_TYPE, 'CODE', args.PRICE])
        # 去除异常值
        df.drop(df[df['FLAG'] == 1].index, inplace=True)
    else:
        df = df
    print(df.shape[0])
    return df