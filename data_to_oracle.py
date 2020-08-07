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
    df2=pd.read_csv(path2)
    df3=pd.read_csv(path3)
    df4=pd.read_csv(path4)

    #异常数据传到数据库
    df1.fillna('',inplace=True)
    df1=df1.astype(str)
    oracle = useOracle("dd_data2", "xdf123", "LBORA170")
    table_name1='PEIJIAN_ORIGINAL_ABNORMAL'
    account="dd_data2/xdf123@10.9.1.170/lbora"
    oracle.BatchsysteminsertDataToTable(df1,table_name1,account)

    #4S店价数据处理
    list1=['count','mean','median','mode']
    for i in list1:
        df2[i]=''
    df2=pd.DataFrame(df2,columns=['JIGOU','BRAND_ID','BRAND_NAME','COMMON_NAME','COMMON_ID','POS_ID','POS_NAME','ORIGINALCODE','STANDARD_PART_CODE','PRICE_TYPE','count','mean','median','mode','REFERENCE','CURRENT_TIME'])
    # ddd=df1.pivot_table(index=['区域','VEHSERINAME_TYPE','IS4S','工时组','项目名称'],columns=['车系档次'])

    #品牌价原厂价数据处理
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
    df234.drop_duplicates(subset=['JIGOU','BRAND_NAME','COMMON_NAME','ORIGINALCODE','STANDARD_PART_CODE','PRICE_TYPE'],keep='first',inplace=True)



    #配件数据上传到数据库
    now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    df234['INSERT_TIME'] = now_time
    list3 = ['STATUS', 'Update_Person', 'LAST_TIME']
    for k in list3:
        df234[k] = ''
    df234.rename(columns={'mode': 'mode1'}, inplace=True)
    df234['ID'] = [k for k in range(1, len(df234) + 1)]
    df234['JIGOU_ID'] = df234['JIGOU'].map(region2code)


    #从DATA_PART_ALL表中读取数据
    commit1='''select t.JIGOU_ID,t.BRAND_ID,t.ORIGINALCODE,t.PART_ID from DATA_PART_ALL t '''
    DATA_PART_ALL=oracle.getData(commit1,account)
    df234 = pd.merge(df234, DATA_PART_ALL, on=['JIGOU_ID', 'BRAND_ID','ORIGINALCODE'], how='left')


    # num = df234.groupby(['JIGOU_ID', 'BRAND_ID','ORIGINALCODE']).count().reset_index()
    # num['PART_ID'] = [id for id in range(10000000, 10000000 + len(num))]
    # num = pd.DataFrame(num, columns=['JIGOU_ID', 'BRAND_ID', 'ORIGINALCODE', 'PART_ID'])
    #
    # df234 = pd.merge(df234, num, on=['JIGOU_ID', 'BRAND_ID', 'ORIGINALCODE'], how='left')
    df234 = pd.DataFrame(df234,columns=['ID','PART_ID','JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID',
                                 'POS_ID', 'POS_NAME','ORIGINALCODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'count', 'mean', 'median', 'mode1',
                                 'REFERENCE','METHOD', 'STATUS', 'Update_Person', 'INSERT_TIME', 'LAST_TIME'])
    df234.fillna('',inplace=True)
    df234=df234.astype(str)
    table_name2='PEIJIAN_SYSTEM'
    oracle.BatchpeijianinsertDataToTable(df234,table_name2,account)

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
    commit='''insert into LB_APMS_PARTS
  (PART_ID,
   JIGOU,
   JIGOU_ID,
   BRAND_ID,
   BRAND_NAME,
   COMMON_NAME,
   COMMON_ID,
   POS_ID,
   POS_NAME,
   ORIGIN_CODE,
   STANDARD_PART_CODE,
   STATUS,
   INSERT_TIME,
   FACTORY_PRICE,
   ORIGIN_PRICE,
   BRAND_PRICE,
   FREQUENCY)
  select t.*,
         a.REFERENCE as FACTORY_PRICE,
         c.REFERENCE as ORIGIN_PRICE,
         b.REFERENCE as BRAND_PRICE,
         case
           when (c.count is null and b.count is null) then
            1
           when (b.count > 0 and c.count is null) then
            b.count
           when (c.count > 0 and b.count is null) then
            c.count
           when (b.count > 0 and c.count > 0 and b.COUNT >= c.COUNT) then
            b.COUNT
           else
            c.COUNT
         end FREQUENCY
    from (select distinct PART_ID,
                          JIGOU,
                          JIGOU_ID,
                          BRAND_ID,
                          BRAND_NAME,
                          COMMON_NAME,
                          COMMON_ID,
                          POS_ID,
                          POS_NAME,
                          ORIGINALCODE       as ORIGIN_CODE,
                          STANDARD_PART_CODE,
                          1                  STATUS,
                          INSERT_TIME
            from PEIJIAN_SYSTEM
           group by PART_ID,
                    JIGOU,
                    JIGOU_ID,
                    BRAND_ID,
                    BRAND_NAME,
                    COMMON_NAME,
                    COMMON_ID,
                    POS_ID,
                    POS_NAME,
                    ORIGINALCODE,
                    STANDARD_PART_CODE,
                    STATUS,
                    INSERT_TIME) t
    left join (select PART_ID, JIGOU_ID, BRAND_ID, ORIGINALCODE, REFERENCE
                 from PEIJIAN_SYSTEM
                where PRICE_TYPE = '4S店价'
                  and REFERENCE > 0) a
      on t.PART_ID = a.PART_ID
    left join (select PART_ID, JIGOU_ID, BRAND_ID, ORIGINALCODE, REFERENCE,COUNT
                 from PEIJIAN_SYSTEM
                where PRICE_TYPE = '品牌价'
                  and REFERENCE > 0) b
      on t.PART_ID = b.PART_ID
    left join (select PART_ID, JIGOU_ID, BRAND_ID, ORIGINALCODE, REFERENCE,COUNT
                 from PEIJIAN_SYSTEM
                where PRICE_TYPE = '原厂价'
                  and REFERENCE > 0) c
      on t.PART_ID = c.PART_ID

'''
    list1=[commit]
    for comm in list1:
        oracle.executeCommitSubmit(comm, account)
if __name__=='__main__':
    to_oracle()






