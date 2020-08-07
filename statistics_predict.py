#!/usr/bin/env python 
# -*- coding:utf-8 -*-

from branch_code.dadi_loader import *
from sklearn.externals import joblib
from ForCall01 import *
from data_from_oracle import *
from outlier_processing import outlier_process_stac


def statistics_pre():
    df=get_data()
    df=outlier_process_stac(df)
    df=df[[FLAGS.REGION,'BRAND_ID',FLAGS.BRAND,FLAGS.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',FLAGS.PRICE_TYPE,FLAGS.PRICE]]
    # df.dropna(subset=[FLAGS.is4s,'PAINTING_TYPE',FLAGS.sumveriloss],how='any',axis=0,inplace=True)
    df.dropna(subset=[FLAGS.REGION,FLAGS.BRAND,FLAGS.COMPNAME,'CODE','STANDARD_PART_CODE',FLAGS.PRICE_TYPE,FLAGS.PRICE],how='any',axis=0,inplace=True)
    df1=df[[FLAGS.REGION,'BRAND_ID',FLAGS.BRAND,FLAGS.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',FLAGS.PRICE_TYPE,FLAGS.PRICE]]
    # df1.rename(columns={'PROVINCE':'区域',FLAGS.work_hour_project:'工时项目','VEHSERINAME_ID':'车系档次',FLAGS.is4s:'是否4S店','PAINTING_TYPE':'喷漆类型'},inplace=True)
    statistic_data=df1.groupby([FLAGS.REGION,'BRAND_ID',FLAGS.BRAND,FLAGS.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',FLAGS.PRICE_TYPE])[FLAGS.PRICE].agg(['count','mean','median']).reset_index()
    mode=df1.groupby([FLAGS.REGION,'BRAND_ID',FLAGS.BRAND,FLAGS.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',FLAGS.PRICE_TYPE])[FLAGS.PRICE].agg(lambda x: x.value_counts().index[0]).reset_index()
    mode.rename(columns={FLAGS.PRICE:'mode'},inplace = True)
    datas=pd.merge(statistic_data,mode,on=[FLAGS.REGION,FLAGS.BRAND,'BRAND_ID',FLAGS.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',FLAGS.PRICE_TYPE], how='inner')
    # datas.rename(columns={'count':'计数（条）','mean':'均值','std':'标准差','min':'最小值','25%':'四分之一位','50%':'中位数','75%':'四分之三位','max':'最大值','mode':'众数'},inplace=True)
    # VEHSERINAME_ID2alpha={1:'A（180档）',2:'B（240档）',3:'C（300档）',4:'D（380档）',5:'E（450档）',6:'F（500档）',7:'G（600档）',8:'H（680档）',9:'J(800档）',10:'K（1000档）'}
    # alpha2VEHSERINAME_ID={v:k for k,v in VEHSERINAME_ID2alpha.items()}
    # datas=datas[datas['喷漆类型']=='全喷']
    # datas['statistic1']=datas.apply(lambda x:round(np.median([x['mean'],x['50%'],x['mode']])),axis=1)
    # datas['statistic2']=datas.apply(lambda x:round(np.mean([x['mean'],x['50%'],x['mode']])),axis=1)

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
    # with open('pp_yc/cate_name2id.json', encoding='utf-8') as reader5:
    #     cate_name2id = json.load(reader5)
    # # test_data1=pd.DataFrame()
    # test_data['jigou_id'] = test_data[FLAGS.REGION].map(jigou2id)
    # test_data['brand_id'] = test_data[FLAGS.BRAND].map(brand2id)
    # # test_data['cate_name_id'] = test_data[FLAGS.CATE_NAME].map(cate_name2id)
    # test_data['compname_id'] = test_data[FLAGS.COMPNAME].map(compname2id)
    # test_data['code_id'] = test_data['CODE'].map(code2id)
    # test_data.dropna(subset=['jigou_id', 'brand_id','compname_id', 'code_id'], how='any', axis=0, inplace=True)
    # test_data = test_data.reset_index(drop=True)
    # df2=pd.DataFrame(test_data,columns=['jigou_id','brand_id','compname_id','code_id',FLAGS.PRICE_TYPE])
    # df2=df2.astype(float)
    # x_data=ss.transform(df2)
    # datas_pre=list(np.exp(gbm_model.predict(x_data)))
    # df3=pd.DataFrame()
    # df3['LGBM_VALUE']=datas_pre
    # df3['模型值']=df3['LGBM_VALUE'].map(lambda x:round(x))
    # # df3['上限值'] = df3['参考值'].map(lambda x: round(1.1 * x, -1))
    # df4=pd.concat([test_data,df3],axis=1)
    df4=pd.DataFrame(datas,columns=[FLAGS.REGION,'BRAND_ID',FLAGS.BRAND,FLAGS.COMPNAME,'COMMON_ID','POS_ID','POS_NAME','CODE','STANDARD_PART_CODE',FLAGS.PRICE_TYPE,
        'count', 'mean','median','mode', '模型值'])
    df4.rename(columns={'CODE':'ORIGINALCODE'},inplace=True)
    # df4.rename(columns={'50%': 'median','mode': 'mode1'},inplace=True)
    df4['参考值']=df4.apply(lambda x:get_count_value(x['count'],x['mode'],x['模型值']),axis=1)
    df4.to_csv('pp_yc/配件品牌价原厂价统计数据及模型预测.csv',index=None,encoding='utf-8')
    # oracle = useOracle("VDATA", "xdf123", "LBORA170")
    # oracle.creatDataFrame(df4,'youqi_statis')
    # oracle.insertDataToTable(df4,'youqi_statis')


if __name__=='__main__':
    os.environ['CUDA_VISIBLE_DEVICES'] = "0"

    statistics_pre()
    # yuanchang_statistics_pre()
