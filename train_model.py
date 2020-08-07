from ForCall01 import *
import numpy as np
import pandas as pd
import os
import re
from branch_code.dadi_loader import detectoutliers_stc,get_time_dif,train_model,valuation
import json
import os
import time
from datetime import timedelta
from sklearn.externals import joblib


os.environ['CUDA_VISIBLE_DEVICES'] = "0"

oracle = useOracle("VDATA", "xdf123", "LBORA170")
tableName='peijian'
commit="select * from peijian"
account="vdata/xdf123@10.9.1.170/lbora"
df=oracle.getData(commit,account)

df['NEW_IS4S']=df['NEW_IS4S'].astype(float)
df['CHGCOMPSET']=df['CHGCOMPSET'].astype(int)
df['UNITPRICE']=df['UNITPRICE'].astype(float)
df=df[df['UNITPRICE']>0].reset_index(drop=True)
outliers = df.groupby(['JIGOU', 'MODEL_BRAND', 'COMMONNAME'])['UNITPRICE'].apply(detectoutliers_stc).reset_index()
# 有异常值的取出来
outliers = outliers[outliers['UNITPRICE'].notnull()]
# 将核损总金额列拆开
if len(outliers):
    outliers = outliers.set_index(['JIGOU', 'MODEL_BRAND', 'COMMONNAME'])['UNITPRICE'].apply(pd.Series).stack().reset_index()
    outliers.columns = ['JIGOU', 'MODEL_BRAND', 'COMMONNAME', 'INDEX', 'UNITPRICE']
    # 去重
    outliers = outliers[['JIGOU', 'MODEL_BRAND', 'COMMONNAME', 'UNITPRICE']].drop_duplicates()
    outliers['FLAG'] = 1
    df = pd.merge(df, outliers, how='left', on=['JIGOU', 'MODEL_BRAND', 'COMMONNAME', 'UNITPRICE'])
    # 去除异常值
    df.drop(df[df['FLAG'] == 1].index, inplace=True)
else:
    df = df
print(df.info())
jigou_ls=list(set(df['JIGOU'].tolist()))
jigou2id={jigou_ls[i-1]:i for i in range(1,len(jigou_ls)+1)}
with open('dict/jigou2id.json','w',encoding='utf-8') as writer1:
    json.dump(jigou2id,writer1,ensure_ascii=False)
df['jigou_id']=df['JIGOU'].map(jigou2id)
brand_ls=list(set(df['MODEL_BRAND'].tolist()))
brand2id={brand_ls[i-1]:i for i in range(1,len(brand_ls)+1)}
with open('dict/brand2id.json','w',encoding='utf-8') as writer2:
    json.dump(brand2id,writer2,ensure_ascii=False)
df['brand_id']=df['MODEL_BRAND'].map(brand2id)
compname_ls=list(set(df['COMMONNAME'].tolist()))
compname2id={compname_ls[i-1]:i for i in range(1,len(compname_ls)+1)}
with open('dict/compname2id.json','w',encoding='utf-8') as writer3:
    json.dump(compname2id,writer3,ensure_ascii=False)
df['compname_id']=df['COMMONNAME'].map(compname2id)

# 编码处理
def get_bianma(code1,code2):
    code1=str(code1)
    code2=str(code2)
    # flag = []  # 存放处理后的编码，编码为0表示不符合条件
    # re_code1=re.findall('[a-zA-Z]',code1)
    re_code2=re.findall('[a-zA-Z]',code2)
    if not pd.isnull(code1):
        code1 = ''.join(code1.split())  # 去掉原厂编码中的空格
    if not pd.isnull(code2):
        code2 = ''.join(code2.split())  # 去掉标准编码中的空格
    if (len(str(code1)) > 6 and len(set(str(code1))) > 1):  # 原厂编码大于6位且不是重复数字，则取原厂编码
        flag = code1
    elif (len(str(code1)) > 6 and len(set(str(code1)))  == 1):  # 原厂编码大于6位且为重复数字，若标准编码大于6位且不是重复数字，则取标准编码，否则编码为0
        if (len(str(code2))  > 6 and len(set(str(code2))) > 1):
            flag = code2
        else:
            flag = code1
    elif (len(str(code1)) <= 6):  #  原厂编码小于等于6位，标准编码大于6位且不为重复数字，则取标准编码，否则编码为0
        if (len(str(code2)) > 6 and len(set(str(code2))) > 1):
            flag = code2
        elif re_code2 and len(str(code2))==6:
            flag=code2
#           print(flags)
        else:
            flag = code1
    elif code1=='None' and len(code2):
        flag=code2
#             print(flags)
    elif code2=='None' and len(code1):
        flag = code1
    else:
        flag = 0
    # flag.append(flags)
    return flag
    
def get_flag(code):
    code=str(code)
    if not pd.isnull(code) and (len(str(code)) >= 6 and len(set(str(code))) > 1):  # 原厂编码大于6位且不是重复数字，则取原厂编码
        flag = 1
    else:
        flag = 0
    return flag

df['CODE'] = df.apply(lambda row:get_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']), axis=1)
df['WAY_FLAG'] = df.apply(lambda row:get_flag(row['CODE']), axis=1)
df=df.loc[df['WAY_FLAG']==1].reset_index(drop=True)
code_ls=list(set(df['CODE'].tolist()))
code2id={code_ls[i-1]:i for i in range(1,len(code_ls)+1)}
with open('dict/code2id.json','w',encoding='utf-8') as writer3:
    json.dump(code2id,writer3,ensure_ascii=False)
df['code_id']=df['CODE'].map(code2id)

df=df[(df['CHGCOMPSET']==2)|(df['CHGCOMPSET']==3)]
print(df.info())
df=pd.DataFrame(df,columns=['jigou_id','brand_id','compname_id','code_id','NEW_IS4S','CHGCOMPSET','UNITPRICE'])
df.dropna(subset=['jigou_id','brand_id','compname_id','code_id','NEW_IS4S','CHGCOMPSET','UNITPRICE'],inplace=True)
df=df.astype(float)
print(df.shape[0])
start_time_model = time.time()
ss, gbm_model, x_test, y_test=train_model(df)
print('模型训练耗时为{}'.format(get_time_dif(start_time_model)))
joblib.dump(ss, "pinpai/data_ss.pinpai")  ## 将标准化模型保存
joblib.dump(gbm_model, "gbm.pinpai")  ## 将模型保存
y_pred = gbm_model.predict(x_test)
valuation(y_pred, y_test)