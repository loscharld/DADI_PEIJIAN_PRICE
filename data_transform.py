#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import numpy as np
import json
# from data_from_oracle import *
from branch_code.dadi_loader import *

def pp_yc_process(df):
    #机构类型转ID
    jigou_ls = list(set(df[args.REGION].tolist()))
    jigou2id = {jigou_ls[i - 1]: i for i in range(1, len(jigou_ls) + 1)}
    with open('pp_yc/jigou2id.json', 'w', encoding='utf-8') as writer1:
        json.dump(jigou2id, writer1, ensure_ascii=False)
    df['jigou_id'] = df[args.REGION].map(jigou2id)
    #品牌转ID
    brand_ls = list(set(df[args.BRAND].tolist()))
    brand2id = {brand_ls[i - 1]: i for i in range(1, len(brand_ls) + 1)}
    with open('pp_yc/brand2id.json', 'w', encoding='utf-8') as writer2:
        json.dump(brand2id, writer2, ensure_ascii=False)
    df['brand_id'] = df[args.BRAND].map(brand2id)

    VEHSERINAME_ID2alpha = {1: 'A（180档）', 2: 'B（240档）', 3: 'C（300档）', 4: 'D（380档）', 5: 'E（450档）', 6: 'F（500档）',
                            7: 'G（600档）', 8: 'H（680档）', 9: 'J(800档）', 10: 'K（1000档）'}
    alpha2VEHSERINAME_ID = {v: k for k, v in VEHSERINAME_ID2alpha.items()}

    #车系转ID
    # cate_name_ls=list(set(df[args.CATE_NAME].tolist()))
    # cate_name2id = {cate_name_ls[i - 1]: i for i in range(1, len(cate_name_ls) + 1)}
    # with open('pp_yc/cate_name2id.json', 'w', encoding='utf-8') as writer3:
    #     json.dump(cate_name2id, writer3, ensure_ascii=False)
    # df['cate_name_id']=df[args.CATE_NAME].map(cate_name2id)
    #配件名称转ID
    compname_ls = list(set(df[args.COMPNAME].tolist()))
    compname2id = {compname_ls[i - 1]: i for i in range(1, len(compname_ls) + 1)}
    with open('pp_yc/compname2id.json', 'w', encoding='utf-8') as writer4:
        json.dump(compname2id, writer4, ensure_ascii=False)
    df['compname_id'] = df[args.COMPNAME].map(compname2id)
    # #有效编码
    # df['CODE'] = df.apply(lambda row: get_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']), axis=1)
    # df['WAY_FLAG'] = df.apply(lambda row: get_flag(row['CODE']), axis=1)
    # df = df.loc[df['WAY_FLAG'] == 1].reset_index(drop=True)
    #编码转ID
    code_ls = list(set(df['CODE'].tolist()))
    code2id = {code_ls[i - 1]: i for i in range(1, len(code_ls) + 1)}
    with open('pp_yc/code2id.json', 'w', encoding='utf-8') as writer3:
        json.dump(code2id, writer3, ensure_ascii=False)
    df['code_id'] = df['CODE'].map(code2id)
    df = pd.DataFrame(df,columns=['jigou_id', 'brand_id','compname_id','code_id',args.PRICE_TYPE, args.PRICE])
    df.dropna(subset=['jigou_id','brand_id','compname_id','code_id',args.PRICE_TYPE,args.PRICE],how='any',axis=0,inplace=True)
    print(len(df))
    df = df.astype(float)
    df_all=pd.DataFrame()
    for i in range(11):
        df_all=pd.concat([df_all,df],axis=0)
    print(len(df_all))
    return df_all

def yuanchang_process(df):
    #机构类型转ID
    jigou_ls = list(set(df[args.REGION].tolist()))
    jigou2id = {jigou_ls[i - 1]: i for i in range(1, len(jigou_ls) + 1)}
    with open('yuanchang/jigou2id.json', 'w', encoding='utf-8') as writer1:
        json.dump(jigou2id, writer1, ensure_ascii=False)
    df['jigou_id'] = df[args.REGION].map(jigou2id)
    #品牌转ID
    brand_ls = list(set(df[args.BRAND].tolist()))
    brand2id = {brand_ls[i - 1]: i for i in range(1, len(brand_ls) + 1)}
    with open('yuanchang/brand2id.json', 'w', encoding='utf-8') as writer2:
        json.dump(brand2id, writer2, ensure_ascii=False)
    df['brand_id'] = df[args.BRAND].map(brand2id)
    #配件名称转ID
    compname_ls = list(set(df[args.COMPNAME].tolist()))
    compname2id = {compname_ls[i - 1]: i for i in range(1, len(compname_ls) + 1)}
    with open('yuanchang/compname2id.json', 'w', encoding='utf-8') as writer3:
        json.dump(compname2id, writer3, ensure_ascii=False)
    df['compname_id'] = df[args.COMPNAME].map(compname2id)
    #有效编码
    df['CODE'] = df.apply(lambda row: get_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']), axis=1)
    df['WAY_FLAG'] = df.apply(lambda row: get_flag(row['CODE']), axis=1)
    df = df.loc[df['WAY_FLAG'] == 1].reset_index(drop=True)
    #编码转ID
    code_ls = list(set(df['CODE'].tolist()))
    code2id = {code_ls[i - 1]: i for i in range(1, len(code_ls) + 1)}
    with open('yuanchang/code2id.json', 'w', encoding='utf-8') as writer3:
        json.dump(code2id, writer3, ensure_ascii=False)
    df['code_id'] = df['CODE'].map(code2id)
    df = pd.DataFrame(df,columns=['jigou_id', 'brand_id', 'compname_id', 'code_id', args.IS4S, args.PRICE])
    df.dropna(subset=['jigou_id', 'brand_id', 'compname_id', 'code_id', args.IS4S, args.PRICE],how='any',axis=0,inplace=True)
    df = df.astype(float)
    return df


if __name__=='__main__':
    pp_yc_process()