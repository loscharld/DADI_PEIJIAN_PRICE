#! usr/bin/env python3
# -*- coding:utf-8 -*-
import pandas as pd
import time
import re
import datetime
import warnings
from ForCall01 import *
from branch_code.dadi_loader import get_time_dif


def left_right():
    # data_left = pd.read_excel(r'D:\1-工作资料\6-2大地自建库三期\7-配件模块\6-左右配件\bendi_left.xlsx')
    # data_right = pd.read_excel(r'D:\1-工作资料\6-2大地自建库三期\7-配件模块\6-左右配件\bendi_right.xlsx')

    commit1='''select distinct BRAND_NAME,COMMON_NAME,ORIGINALCODE,length(ORIGINALCODE)  from LB_PEIJIAN_ORIGINAL_DATA_LOAD t
    where COMMON_NAME like '%左%' and length(ORIGINALCODE)>=6 '''

    commit2='''select distinct BRAND_NAME,COMMON_NAME,ORIGINALCODE,length(ORIGINALCODE)  from LB_PEIJIAN_ORIGINAL_DATA_LOAD t
    where COMMON_NAME like '%右%' and length(ORIGINALCODE)>=6 '''

    oracle = useOracle("dd_data2", "xdf123", "LBORA169")
    account = "dd_data2/xdf123@10.9.1.169/lbora"
    data_left=oracle.getData(commit1,account)
    data_right=oracle.getData(commit2,account)
    def extract_r(x):
        con=str(x).split('（')[-1].split('）')[0]
        return con
    def extract_l(x):
        con=str(x).split('（')[0]
        return con
    data_left['L_R']=data_left['COMMON_NAME'].apply(extract_r)
    data_left['PEIJIAN']=data_left['COMMON_NAME'].apply(extract_l)
    data_right['L_R']=data_right['COMMON_NAME'].apply(extract_r)
    data_right['PEIJIAN']=data_right['COMMON_NAME'].apply(extract_l)
    data_left_1 = data_left.loc[data_left['L_R'] == '左']
    data_right_1 = data_right.loc[data_right['L_R'] == '右']
    data1 = pd.merge(data_left_1, data_right_1, on=['BRAND_NAME', 'PEIJIAN', 'LENGTH(ORIGINALCODE)'], how='inner')

    len1 = len(data1)
    r = 0
    s = 0
    r_list = []
    for r in range(0, len1):
        data1_1 = data1[r:r + 1]
        left_or = data1_1['ORIGINALCODE_x'].values.tolist()
        left_or = list(left_or[0])

        right_or = data1_1['ORIGINALCODE_y'].values.tolist()
        right_or = list(right_or[0])

        len2 = len(left_or)
        len3 = len(right_or)

        s_list = []
        if len2 == len3:
            for i in range(0, len2):
                x = left_or[i]
                #             print (data1_1)
                y = right_or[i]
                if x != y:
                    s = 1
                else:
                    s = 0
                s_list.append(s)
        else:
            s = 99
            s_list.append(s)
        s_sum = sum(s_list)
        r_list.append(s_sum)

    data1['DISTINCT_SUM'] = r_list

    data1_1 = data1[['BRAND_NAME', 'PEIJIAN', 'COMMON_NAME_x', 'ORIGINALCODE_x']]
    data1_2 = pd.pivot_table(data1, index=['BRAND_NAME', 'ORIGINALCODE_y'], values=['L_R_y'], aggfunc=[len])
    data1_2 = data1_2.reset_index(drop=False)
    data1_2.columns = ['BRAND_NAME', 'ORIGINALCODE_y', 'len_y']
    data1_3 = pd.merge(data1, data1_2, on=['BRAND_NAME', 'ORIGINALCODE_y'], how='left')
      # 不同字符计数+右边编码个数

    # 仅一个右边编码，且不同字符数为1或2
    data1_4 = data1_3.loc[data1_3['len_y'] == 1]
    data1_4 = data1_4.loc[data1_4['DISTINCT_SUM'] == 1].reset_index(drop=True)
    # data_end.drop_duplicates(subset=['MODEL_BRAND','PEIJIAN','L_R_x','COMMONNAME_x','LEN','L_R_y','COMMONNAME_y'],keep='first',inplace=True)
    # 选列 改列名
    data1_4.drop_duplicates(subset=['BRAND_NAME', 'ORIGINALCODE_x'], keep=False, inplace=True)
    data1_4.to_csv(r'data/左右-418-1.csv',index=None,encoding='utf-8')
    print('left to right done!')

if __name__=='__main__':
    start_time = time.time()
    left_right()
    print(get_time_dif(start_time))