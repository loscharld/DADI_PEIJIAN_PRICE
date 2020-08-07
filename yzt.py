#! usr/bin/env python3
# -*- coding:utf-8 -*-

from ForCall01 import *
import pandas as pd

def yzt_process():
    tableName = 'YZT_BUCHONG_PRICE'
    account = "dd_data2/xdf123@10.9.1.169/lbora"
    oracle = useOracle("dd_data2", "xdf123", "LBORA")
    commit='''select * from {} t '''.format(tableName)
    df=oracle.getData(commit,account)
    def get_price(data):
        lst=[]
        changfangjias=data['CHANGFANGJIA'].tolist()
        yuanchangjias=data['YUANCHANGJIA'].tolist()
        pinpaijias=data['PINPAIJIA'].tolist()
        dcs=data['DC'].tolist()
        ind=dcs.index(max(dcs))
        changfangjia=changfangjias[ind]
        yuanchangjia=yuanchangjias[ind]
        pinpaijia=pinpaijias[ind]
        lst.append(changfangjia)
        lst.append(yuanchangjia)
        lst.append(pinpaijia)
        return lst

    df=df.groupby(['JIGOU_ID','MODEL_BRAND','ORIGINALFITS_CODE'])['CHANGFANGJIA','YUANCHANGJIA','PINPAIJIA','DC'].apply(get_price).apply(pd.Series).reset_index()
    df.rename(columns={0:'CHANGFANGJIA',1:'YUANCHANGJIA',2:'PINPAIJIA'},inplace=True)
    df.dropna(subset=['CHANGFANGJIA'],how='any',axis=0,inplace=True)
    df.to_csv('yzt/yzt.csv',index=None,encoding='utf-8')
    print('yzt process done!')



if __name__=='__main__':
    yzt_process()