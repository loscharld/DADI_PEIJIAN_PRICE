#!/usr/bin/env python 
# -*- coding:utf-8 -*-

from branch_code.dadi_loader import *
from sklearn.externals import joblib
from ForCall01 import *

from outlier_processing import outlier_process_stac


def statistics_pre(handle):
    # extract=Extract()
    df=handle.get_data()
    df=outlier_process_stac(df)
    df=df[[args.REGION,'BRAND_ID',args.BRAND,args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE,args.PRICE]]
    df.dropna(subset=[args.REGION,args.BRAND,args.COMPNAME,'CODE','STANDARD_PART_CODE',args.PRICE_TYPE,args.PRICE],how='any',axis=0,inplace=True)
    df1=df[[args.REGION,'BRAND_ID',args.BRAND,args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE,args.PRICE]]
    statistic_data=df1.groupby([args.REGION,'BRAND_ID',args.BRAND,args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE])[args.PRICE].agg(['count','mean','median']).reset_index()
    mode=df1.groupby([args.REGION,'BRAND_ID',args.BRAND,args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE])[args.PRICE].agg(lambda x: x.value_counts().index[0]).reset_index()
    mode.rename(columns={args.PRICE:'mode'},inplace = True)
    datas=pd.merge(statistic_data,mode,on=[args.REGION,args.BRAND,'BRAND_ID',args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE], how='inner')
    def get_value(x1, x2, x3):
        if x1 > 10:
            con = x2
        else:
            con = x3
        return con
    datas['REFERENCE'] = datas.apply(lambda x: get_value(x['count'], x['mode'], x['median']), axis=1)
    #lgbm测试
    # test_data=datas.copy()
    # ss=joblib.load("pp_yc/data_ss.model") ## 加载模型
    # gbm_model=joblib.load("pp_yc/gbm.model") ## 加载模型
    #
    # with open('pp_yc/jigou2id.json', encoding='utf-8') as reader1:
    #     jigou2id = json.load(reader1)
    #
    # with open('pp_yc/compname2id.json', encoding='utf-8') as reader2:
    #     compname2id = json.load(reader2)
    #
    # with open('pp_yc/code2id.json', encoding='utf-8') as reader3:
    #     code2id = json.load(reader3)
    #
    # with open('pp_yc/brand2id.json', encoding='utf-8') as reader4:
    #     brand2id = json.load(reader4)
    #
    # # with open('pp_yc/cate_name2id.json', encoding='utf-8') as reader5:
    # #     cate_name2id = json.load(reader5)
    # # test_data1=pd.DataFrame()
    # test_data['jigou_id'] = test_data[args.REGION].map(jigou2id)
    # test_data['brand_id'] = test_data[args.BRAND].map(brand2id)
    # # test_data['cate_name_id'] = test_data[args.CATE_NAME].map(cate_name2id)
    # test_data['compname_id'] = test_data[args.COMPNAME].map(compname2id)
    # test_data['code_id'] = test_data['CODE'].map(code2id)
    # test_data.dropna(subset=['jigou_id', 'brand_id','compname_id', 'code_id'], how='any', axis=0, inplace=True)
    # test_data = test_data.reset_index(drop=True)
    # df2=pd.DataFrame(test_data,columns=['jigou_id','brand_id','compname_id','code_id',args.PRICE_TYPE])
    # df2=df2.astype(float)
    # x_data=ss.transform(df2)
    # datas_pre=list(np.exp(gbm_model.predict(x_data)))
    # df3=pd.DataFrame()
    # df3['LGBM_VALUE']=datas_pre
    # df3['REFERENCE']=df3['LGBM_VALUE'].map(lambda x:round(x))
    # # df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
    # df4=pd.concat([test_data,df3],axis=1)
    # df4=pd.DataFrame(df4,columns=[args.REGION,'BRAND_ID',args.BRAND,args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE,
    #     'count', 'mean','median','mode','REFERENCE'])
    # df4.rename(columns={'CODE':'ORIGINALCODE'},inplace=True)
    #
    # # 统计JIGOU，BRAND_ID，ORIGINALCODE的均值
    # national_mean = df4.groupby(['BRAND_ID', 'ORIGINALCODE', 'CHGCOMPSET'])['mean'].agg(['mean']).reset_index()
    # national_mean.rename(columns={'mean': 'national_mean'}, inplace=True)
    #
    # def transform(x):
    #     try:
    #         con = round(x)
    #     except:
    #         con = np.nan
    #     return con
    #
    # national_mean['national_mean'] = national_mean['national_mean'].apply(transform)
    # df4 = pd.merge(df4, national_mean, on=['BRAND_ID', 'ORIGINALCODE', 'CHGCOMPSET'], how='left')
    # # 模型预测值与均值的偏差超过一个阈值，用全国均值乘以一个系数去替代
    # df4['coef'] = df4.apply(lambda x: (x['REFERENCE'] - x['national_mean']) / x['national_mean'], axis=1)
    # df4['REFERENCE'] = df4.apply(lambda x: round(1.01 * x['national_mean']) if x['coef'] >= 1  else x['REFERENCE'], axis=1)
    datas.rename(columns={'CODE': 'ORIGINALCODE'}, inplace=True)
    datas.to_csv('pp_yc/配件品牌价原厂价统计数据及模型预测.csv',index=None,encoding='utf-8')
    handle.logger.info(('statistics done!'))
    # oracle = useOracle("VDATA", "xdf123", "LBORA170")
    # oracle.creatDataFrame(df4,'youqi_statis')
    # oracle.insertDataToTable(df4,'youqi_statis')

def tes_statistics_pre():
    df=get_data()
    df=outlier_process_stac(df)
    df=df[[args.REGION,'BRAND_ID',args.BRAND,args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE,args.PRICE]]
    df.dropna(subset=[args.REGION,args.BRAND,args.COMPNAME,'CODE','STANDARD_PART_CODE',args.PRICE_TYPE,args.PRICE],how='any',axis=0,inplace=True)
    df1=df[[args.REGION,'BRAND_ID',args.BRAND,args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE,args.PRICE]]
    statistic_data=df1.groupby([args.REGION,'BRAND_ID',args.BRAND,args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE])[args.PRICE].agg(['count','mean','median']).reset_index()
    mode=df1.groupby([args.REGION,'BRAND_ID',args.BRAND,args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE])[args.PRICE].agg(lambda x: x.value_counts().index[0]).reset_index()
    mode.rename(columns={args.PRICE:'mode'},inplace = True)
    datas=pd.merge(statistic_data,mode,on=[args.REGION,args.BRAND,'BRAND_ID',args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE], how='inner')

    #lgbm测试
    test_data=datas.copy()
    ss=joblib.load("pp_yc/data_ss.model") ## 加载模型
    gbm_model=joblib.load("pp_yc/gbm.model") ## 加载模型

    with open('pp_yc/jigou2id.json', encoding='utf-8') as reader1:
        jigou2id = json.load(reader1)

    with open('pp_yc/compname2id.json', encoding='utf-8') as reader2:
        compname2id = json.load(reader2)

    with open('pp_yc/code2id.json', encoding='utf-8') as reader3:
        code2id = json.load(reader3)

    with open('pp_yc/brand2id.json', encoding='utf-8') as reader4:
        brand2id = json.load(reader4)

    # with open('pp_yc/cate_name2id.json', encoding='utf-8') as reader5:
    #     cate_name2id = json.load(reader5)
    # test_data1=pd.DataFrame()
    test_data['jigou_id'] = test_data[args.REGION].map(jigou2id)
    test_data['brand_id'] = test_data[args.BRAND].map(brand2id)
    # test_data['cate_name_id'] = test_data[args.CATE_NAME].map(cate_name2id)
    test_data['compname_id'] = test_data[args.COMPNAME].map(compname2id)
    test_data['code_id'] = test_data['CODE'].map(code2id)
    test_data.dropna(subset=['jigou_id', 'brand_id','compname_id', 'code_id'], how='any', axis=0, inplace=True)
    test_data = test_data.reset_index(drop=True)
    df2=pd.DataFrame(test_data,columns=['jigou_id','brand_id','compname_id','code_id',args.PRICE_TYPE])
    df2=df2.astype(float)
    x_data=ss.transform(df2)
    datas_pre=list(np.exp(gbm_model.predict(x_data)))
    df3=pd.DataFrame()
    df3['LGBM_VALUE']=datas_pre
    df3['REFERENCE']=df3['LGBM_VALUE'].map(lambda x:round(x))
    # df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
    df4=pd.concat([test_data,df3],axis=1)
    df4=pd.DataFrame(df4,columns=[args.REGION,'BRAND_ID',args.BRAND,args.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',args.PRICE_TYPE,
        'count', 'mean','median','mode','REFERENCE'])
    df4.rename(columns={'CODE':'ORIGINALCODE'},inplace=True)

    # 统计JIGOU，BRAND_ID，ORIGINALCODE的均值
    national_mean = df4.groupby(['BRAND_ID', 'ORIGINALCODE', 'CHGCOMPSET'])['mean'].agg(['mean']).reset_index()
    national_mean.rename(columns={'mean': 'national_mean'}, inplace=True)

    def transform(x):
        try:
            con = round(x)
        except:
            con = np.nan
        return con

    national_mean['national_mean'] = national_mean['national_mean'].apply(transform)
    df4 = pd.merge(df4, national_mean, on=['BRAND_ID', 'ORIGINALCODE', 'CHGCOMPSET'], how='left')
    # 模型预测值与均值的偏差超过一个阈值，用全国均值乘以一个系数去替代
    df4['coef'] = df4.apply(lambda x: (x['REFERENCE'] - x['national_mean']) / x['national_mean'], axis=1)
    df4['REFERENCE'] = df4.apply(lambda x: round(1.01 * x['national_mean']) if x['coef'] >= 1  else x['REFERENCE'], axis=1)
    df4.to_csv('pp_yc/测试集配件品牌价原厂价统计数据及模型预测.csv',index=None,encoding='utf-8')
    print('test statistics done!')
    # oracle = useOracle("VDATA", "xdf123", "LBORA170")
    # oracle.creatDataFrame(df4,'youqi_statis')
    # oracle.insertDataToTable(df4,'youqi_statis')

if __name__=='__main__':
    os.environ['CUDA_VISIBLE_DEVICES'] = "0"
    start_time=time.time()
    statistics_pre()
    test_statistics_pre()
    print(get_time_dif(start_time))
    # yuanchang_statistics_pre()
