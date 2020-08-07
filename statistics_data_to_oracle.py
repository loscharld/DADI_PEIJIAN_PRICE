import numpy as np
from ForCall01 import *
from branch_code.dadi_loader import *
import pandas as pd
import time

def to_oracle():
    path1='outliers/品牌价原厂价价格异常值.csv'
    path2='changfang/4S店价.csv'
    path3='pp_yc/配件品牌价原厂价统计数据及模型预测.csv'
    path4='pp_yc/全国常用配件模型预测.csv'

    df1=pd.read_csv(path1)
    df2=pd.read_csv(open(path2,encoding='utf-8'))
    df3=pd.read_csv(open(path3,encoding='utf-8'))
    df4=pd.read_csv(open(path4,encoding='utf-8'))

    #配件左右对齐
    path6='data/左右-418-1.csv'
    df6=pd.read_csv(open(path6,encoding='utf-8'))
    df6_map=df6[['ORIGINALCODE_x','ORIGINALCODE_y']]
    df6_map.dropna(subset=['ORIGINALCODE_x','ORIGINALCODE_y'],how='any',axis=0,inplace=True)
    map_dict=dict(zip(df6_map['ORIGINALCODE_x'],df6_map['ORIGINALCODE_y']))
    def map_process(data):
        codes = data['ORIGINALCODE'].tolist()
        counts = data['count'].tolist()
        references = data['参考值'].tolist()
        for code_left in codes:
            if code_left in map_dict.keys():
                code_right=map_dict[code_left]
                if code_right in codes:
                    ind_left=codes.index(code_left)
                    ind_right=codes.index(code_right)
                    count_left=counts[ind_left]
                    count_right=counts[ind_right]
                    if count_left>=count_right:
                        references[ind_right]=references[ind_left]
                    else:
                        references[ind_left]=references[ind_right]
        return codes,counts,references

    fenzu_sta = df3.groupby(['JIGOU','BRAND_ID','CHGCOMPSET'])['ORIGINALCODE','count','REFERENCE'].apply(
        map_process).apply(pd.Series).reset_index()
    fenzu_sta.rename(columns={0:'ORIGINALCODE',1:'count',2:'参考值'},inplace=True)

    fenzu_sta1= fenzu_sta.set_index(['JIGOU','BRAND_ID','CHGCOMPSET'])['ORIGINALCODE'].apply(pd.Series).stack().reset_index()
    fenzu_sta1.drop(['level_3'], axis=1, inplace=True)
    fenzu_sta1.rename(columns={0: 'ORIGINALCODE'}, inplace=True)

    fenzu_sta2 = fenzu_sta.set_index(['JIGOU','BRAND_ID','CHGCOMPSET'])['count'].apply(pd.Series).stack().reset_index()
    fenzu_sta2.drop(['JIGOU','BRAND_ID','level_3','CHGCOMPSET'],axis=1,inplace=True)
    fenzu_sta2.rename(columns={0: 'count'}, inplace=True)

    fenzu_sta3 = fenzu_sta.set_index(['JIGOU','BRAND_ID','CHGCOMPSET'])['参考值'].apply(pd.Series).stack().reset_index()
    fenzu_sta3.drop(['JIGOU','BRAND_ID','level_3','CHGCOMPSET'], axis=1, inplace=True)
    fenzu_sta3.rename(columns={0: '参考值'}, inplace=True)
    fenzu_sta=pd.concat([fenzu_sta1,fenzu_sta2,fenzu_sta3],axis=1)

    df3.drop(['参考值'],axis=1,inplace=True)
    df3=pd.merge(df3,fenzu_sta,on=['JIGOU','BRAND_ID','ORIGINALCODE','count','CHGCOMPSET'],how='left')
    df3 = df3.drop_duplicates()

    #异常数据传到数据库
    df1.fillna('',inplace=True)
    df1=df1.astype(str)
    oracle = useOracle("dd_data2", "xdf123", "LBORA")
    table_name1='LB_PEIJIAN_ORIGINAL_ABNORMAL'
    account="dd_data2/xdf123@10.9.1.169/lbora"
    oracle.BatchsysteminsertDataToTable(df1,table_name1,account)

    #4S店价数据处理
    list1=['count','mean','median','mode']
    for i in list1:
        df2[i]=''
    df2=pd.DataFrame(df2,columns=['JIGOU','BRAND_ID','BRAND_NAME','COMMON_NAME','COMMON_ID','POS_ID','POS_NAME','ORIGINALCODE','STANDARD_PART_CODE','PRICE_TYPE','count','mean','median','mode','REFERENCE'])
    # ddd=df1.pivot_table(index=['区域','VEHSERINAME_TYPE','IS4S','工时组','项目名称'],columns=['车系档次'])

    #4S店价品牌价原厂价数据处理并拼接
    df3.rename(columns={'CHGCOMPSET':'PRICE_TYPE','参考值':'REFERENCE'},inplace=True)
    df3['PRICE_TYPE']=df3['PRICE_TYPE'].map(id2chgompset)
    df3=pd.DataFrame(df3,columns=['JIGOU','BRAND_ID','BRAND_NAME','COMMON_NAME','COMMON_ID','POS_ID','POS_NAME','ORIGINALCODE','STANDARD_PART_CODE','PRICE_TYPE','count','mean','median','mode','REFERENCE'])
    df23=pd.concat([df2,df3],axis=0)
    df23['METHOD']=1
    #模型预测数据处理
    df4.rename(columns={'CHGCOMPSET':'PRICE_TYPE'},inplace=True)
    df4['PRICE_TYPE'] = df4['PRICE_TYPE'].map(id2chgompset)
    list2=['count','mean','median','mode']
    for j in list2:
        df4[j]=''
    df4=pd.DataFrame(df4,columns=['JIGOU','BRAND_ID','BRAND_NAME','COMMON_NAME','COMMON_ID','POS_ID','POS_NAME','ORIGINALCODE','STANDARD_PART_CODE','PRICE_TYPE','count','mean','median','mode','REFERENCE'])
    df4['METHOD']=2
    #数据合并
    df234=pd.concat([df23,df4],axis=0)
    df234.drop_duplicates(subset=['JIGOU','BRAND_NAME','ORIGINALCODE','PRICE_TYPE'],keep='first',inplace=True)



    #配件数据上传到数据库
    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    df234['INSERT_TIME'] = now_time
    list3 = ['STATUS', 'Update_Person', 'LAST_TIME']
    for k in list3:
        df23[k] = ''
    df234.rename(columns={'mode': 'mode1'}, inplace=True)
    df234['ID'] = [k for k in range(1, len(df234) + 1)]
    df234['JIGOU_ID'] = df234['JIGOU'].map(region2code)



    #从DATA_PART_ALL表中读取数据
    # commit1='''select t.JIGOU_ID,t.BRAND_ID,t.ORIGINALCODE,t.PART_ID from DATA_PART_ALL t '''
    commit1='''select t.JIGOU_ID,t.BRAND_ID,t.ORIGIN_CODE,t.PART_ID from LB_DATA_AUX_1 t '''
    DATA_PART_ALL=oracle.getData(commit1,account)
    DATA_PART_ALL.rename(columns={'ORIGIN_CODE':'ORIGINALCODE'},inplace=True)
    df234 = pd.merge(df234, DATA_PART_ALL, on=['JIGOU_ID', 'BRAND_ID','ORIGINALCODE'], how='left')
    df234['STATUS'] = 1

    # num = df234.groupby(['JIGOU_ID', 'BRAND_ID','ORIGINALCODE']).count().reset_index()
    # num['PART_ID'] = [id for id in range(10000000, 10000000 + len(num))]
    # num = pd.DataFrame(num, columns=['JIGOU_ID', 'BRAND_ID', 'ORIGINALCODE', 'PART_ID'])
    #
    # df234 = pd.merge(df234, num, on=['JIGOU_ID', 'BRAND_ID', 'ORIGINALCODE'], how='left')
    df234 = pd.DataFrame(df234,columns=['ID','PART_ID','JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID',
                                 'POS_ID', 'POS_NAME','ORIGINALCODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'count', 'mean', 'median', 'mode1',
                                 'REFERENCE','METHOD', 'STATUS', 'Update_Person', 'INSERT_TIME', 'LAST_TIME'])
    with open('changfang/peijian2standardcode.json',encoding='utf-8') as f1:
        peijian2standardcode=json.load(f1)
    df234['STANDARD_PART_CODE']=df234['COMMON_NAME'].map(peijian2standardcode)
    df234['STANDARD_PART_CODE'].fillna('999999',inplace=True)
    df234.fillna('',inplace=True)
    df234=df234.astype(str)
    table_name2='LB_PEIJIAN_SYSTEM'
    oracle.BatchpeijianinsertDataToTable(df234,table_name2,account)
    print('LB_PEIJIAN_SYSTEM DONE!')








    # commit1='''insert into LB_APMS_PARTS(PART_ID,JIGOU,JIGOU_ID,BRAND_ID,BRAND_NAME,COMMON_NAME,COMMON_ID,POS_ID,POS_NAME,ORIGIN_CODE,STANDARD_PART_CODE,
    #                       FREQUENCY,FACTORY_PRICE,CREATE_WAY,STATUS,INSERT_TIME) select t.PART_ID,t.JIGOU,t.JIGOU_ID,t.BRAND_ID,t.BRAND_NAME,t.COMMON_NAME,
    #      t.COMMON_ID,t.POS_ID,t.POS_NAME,t.ORIGINALCODE as ORIGIN_CODE,t.STANDARD_PART_CODE,t.count as FREQUENCY,t.REFERENCE as FACTORY_PRICE,t.METHOD as CREATE_WAY,
    #      1 STATUS,t.INSERT_TIME from PEIJIAN_SYSTEM t where t.PRICE_TYPE = '4S店价' '''
    #
    # commit2='''insert into LB_APMS_PARTS(PART_ID,JIGOU,JIGOU_ID,BRAND_ID,BRAND_NAME,COMMON_NAME,COMMON_ID,POS_ID,POS_NAME,ORIGIN_CODE,STANDARD_PART_CODE,
    #                       FREQUENCY,ORIGIN_PRICE,CREATE_WAY,STATUS,INSERT_TIME) select t.PART_ID,t.JIGOU,t.JIGOU_ID,t.BRAND_ID,t.BRAND_NAME,t.COMMON_NAME,
    #      t.COMMON_ID,t.POS_ID,t.POS_NAME,t.ORIGINALCODE as ORIGIN_CODE,t.STANDARD_PART_CODE,t.count as FREQUENCY,t.REFERENCE as ORIGIN_PRICE,t.METHOD as CREATE_WAY,
    #      1 STATUS,t.INSERT_TIME from PEIJIAN_SYSTEM t where t.PRICE_TYPE = '原厂价' '''
    #
    # commit3='''insert into LB_APMS_PARTS(PART_ID,JIGOU,JIGOU_ID,BRAND_ID,BRAND_NAME,COMMON_NAME,COMMON_ID,POS_ID,POS_NAME,ORIGIN_CODE,STANDARD_PART_CODE,
    #                       FREQUENCY,BRAND_PRICE,CREATE_WAY,STATUS,INSERT_TIME) select t.PART_ID,t.JIGOU,t.JIGOU_ID,t.BRAND_ID,t.BRAND_NAME,t.COMMON_NAME,
    #      t.COMMON_ID,t.POS_ID,t.POS_NAME,t.ORIGINALCODE as ORIGIN_CODE,t.STANDARD_PART_CODE,t.count as FREQUENCY,t.REFERENCE as BRAND_PRICE,t.METHOD as CREATE_WAY,
    #      1 STATUS,t.INSERT_TIME from PEIJIAN_SYSTEM t where t.PRICE_TYPE = '品牌价' '''
#     commit='''insert into LB_APMS_PARTS
#   (PART_ID,
#    JIGOU,
#    JIGOU_ID,
#    BRAND_ID,
#    BRAND_NAME,
#    COMMON_NAME,
#    COMMON_ID,
#    POS_ID,
#    POS_NAME,
#    ORIGIN_CODE,
#    STANDARD_PART_CODE,
#    STATUS,
#    INSERT_TIME,
#    FACTORY_PRICE,
#    ORIGIN_PRICE,
#    BRAND_PRICE,
#    FREQUENCY)
#   select t.*,
#          a.REFERENCE as FACTORY_PRICE,
#          c.REFERENCE as ORIGIN_PRICE,
#          b.REFERENCE as BRAND_PRICE,
#          case
#            when (c.count is null and b.count is null) then
#             1
#            when (b.count > 0 and c.count is null) then
#             b.count
#            when (c.count > 0 and b.count is null) then
#             c.count
#            when (b.count > 0 and c.count > 0 and b.COUNT >= c.COUNT) then
#             b.COUNT
#            else
#             c.COUNT
#          end FREQUENCY
#     from (select distinct PART_ID,
#                           JIGOU,
#                           JIGOU_ID,
#                           BRAND_ID,
#                           BRAND_NAME,
#                           COMMON_NAME,
#                           COMMON_ID,
#                           POS_ID,
#                           POS_NAME,
#                           ORIGINALCODE       as ORIGIN_CODE,
#                           STANDARD_PART_CODE,
#                           1                  STATUS,
#                           INSERT_TIME
#             from PEIJIAN_SYSTEM
#            group by PART_ID,
#                     JIGOU,
#                     JIGOU_ID,
#                     BRAND_ID,
#                     BRAND_NAME,
#                     COMMON_NAME,
#                     COMMON_ID,
#                     POS_ID,
#                     POS_NAME,
#                     ORIGINALCODE,
#                     STANDARD_PART_CODE,
#                     STATUS,
#                     INSERT_TIME) t
#     left join (select PART_ID, JIGOU_ID, BRAND_ID, ORIGINALCODE, REFERENCE
#                  from PEIJIAN_SYSTEM
#                 where PRICE_TYPE = '4S店价'
#                   and REFERENCE > 0) a
#       on t.PART_ID = a.PART_ID
#     left join (select PART_ID, JIGOU_ID, BRAND_ID, ORIGINALCODE, REFERENCE,COUNT
#                  from PEIJIAN_SYSTEM
#                 where PRICE_TYPE = '品牌价'
#                   and REFERENCE > 0) b
#       on t.PART_ID = b.PART_ID
#     left join (select PART_ID, JIGOU_ID, BRAND_ID, ORIGINALCODE, REFERENCE,COUNT
#                  from PEIJIAN_SYSTEM
#                 where PRICE_TYPE = '原厂价'
#                   and REFERENCE > 0) c
#       on t.PART_ID = c.PART_ID
#
# '''

    # commit = '''insert into LB_APMS_PARTS_LS
    #   (PART_ID,
    #    JIGOU,
    #    JIGOU_ID,
    #    BRAND_ID,
    #    BRAND_NAME,
    #    COMMON_NAME,
    #    COMMON_ID,
    #    POS_ID,
    #    POS_NAME,
    #    ORIGIN_CODE,
    #    STANDARD_PART_CODE,
    #    STATUS,
    #    INSERT_TIME,
    #    FACTORY_PRICE,
    #    ORIGIN_PRICE,
    #    BRAND_PRICE,
    #    FREQUENCY)
    #   select t.*,
    #          a.REFERENCE as FACTORY_PRICE,
    #          c.REFERENCE as ORIGIN_PRICE,
    #          b.REFERENCE as BRAND_PRICE,
    #          case
    #            when (c.count is null and b.count is null) then
    #             1
    #            when (b.count > 0 and c.count is null) then
    #             b.count
    #            when (c.count > 0 and b.count is null) then
    #             c.count
    #            when (b.count > 0 and c.count > 0 and b.COUNT >= c.COUNT) then
    #             b.COUNT
    #            else
    #             c.COUNT
    #          end FREQUENCY
    #     from (select distinct PART_ID,
    #                           JIGOU,
    #                           JIGOU_ID,
    #                           BRAND_ID,
    #                           BRAND_NAME,
    #                           COMMON_NAME,
    #                           COMMON_ID,
    #                           POS_ID,
    #                           POS_NAME,
    #                           ORIGINALCODE       as ORIGIN_CODE,
    #                           STANDARD_PART_CODE,
    #                           1                  STATUS,
    #                           INSERT_TIME
    #             from LB_PEIJIAN_SYSTEM
    #            group by PART_ID,
    #                     JIGOU,
    #                     JIGOU_ID,
    #                     BRAND_ID,
    #                     BRAND_NAME,
    #                     COMMON_NAME,
    #                     COMMON_ID,
    #                     POS_ID,
    #                     POS_NAME,
    #                     ORIGINALCODE,
    #                     STANDARD_PART_CODE,
    #                     STATUS,
    #                     INSERT_TIME) t
    #     left join (select PART_ID, JIGOU_ID, BRAND_ID, ORIGINALCODE, REFERENCE
    #                  from LB_PEIJIAN_SYSTEM
    #                 where PRICE_TYPE = '4S店价'
    #                   and REFERENCE > 0) a
    #       on t.PART_ID = a.PART_ID
    #     left join (select PART_ID, JIGOU_ID, BRAND_ID, ORIGINALCODE, REFERENCE,COUNT
    #                  from LB_PEIJIAN_SYSTEM
    #                 where PRICE_TYPE = '品牌价'
    #                   and REFERENCE > 0) b
    #       on t.PART_ID = b.PART_ID
    #     left join (select PART_ID, JIGOU_ID, BRAND_ID, ORIGINALCODE, REFERENCE,COUNT
    #                  from LB_PEIJIAN_SYSTEM
    #                 where PRICE_TYPE = '原厂价'
    #                   and REFERENCE > 0) c
    #       on t.PART_ID = c.PART_ID  where t.PART_ID is not null

    # '''
    # list1=[commit]
    # for comm in list1:
    #     oracle.executeCommitSubmit(comm, account)
if __name__=='__main__':
    to_oracle()






