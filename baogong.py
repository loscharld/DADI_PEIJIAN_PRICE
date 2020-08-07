#! usr/bin/env python3
# -*- coding:utf-8 -*-

from ForCall01 import *
import pandas as pd

def bg_process():
    tableName = 'lb_bg_data_part_compare'
    account = "dd_data2/xdf123@10.9.1.169/lbora"
    oracle = useOracle("dd_data2", "xdf123", "LBORA")
    commit='''select jigou_id, brand_name, originalcode, fs_price, yc_price, pp_price, dc
  from (select distinct jigou_id,
                        brand_id,
                        originalcode,
                        fs_price,
                        yc_price,
                        pp_price,
                        dc
          from lb_bg_data_part_compare) a
  left join (select distinct brand_id, brand_name
               from LB_DADI_STANDARD_PRODUCT_CARS) b
    on a.brand_id = b.brand_id
 where brand_name is not null'''
    df=oracle.getData(commit,account)
    df.replace(0,np.nan,inplace=True)
    def get_price(data):
        lst=[]
        changfangjias=data['FS_PRICE'].tolist()
        yuanchangjias=data['YC_PRICE'].tolist()
        pinpaijias=data['PP_PRICE'].tolist()
        dcs=data['DC'].tolist()
        ind=dcs.index(max(dcs))
        changfangjia=changfangjias[ind]
        yuanchangjia=yuanchangjias[ind]
        pinpaijia=pinpaijias[ind]
        lst.append(changfangjia)
        lst.append(yuanchangjia)
        lst.append(pinpaijia)
        return lst

    df=df.groupby(['JIGOU_ID','BRAND_NAME','ORIGINALCODE'])['FS_PRICE','YC_PRICE','PP_PRICE','DC'].apply(get_price).apply(pd.Series).reset_index()
    df.rename(columns={0:'CHANGFANGJIA',1:'YUANCHANGJIA',2:'PINPAIJIA'},inplace=True)
    # df.dropna(subset=['CHANGFANGJIA'],how='any',axis=0,inplace=True)
    df.to_csv('baogong/baogong.csv',index=None,encoding='utf-8')
    print('baogong process done!')



if __name__=='__main__':
    bg_process()