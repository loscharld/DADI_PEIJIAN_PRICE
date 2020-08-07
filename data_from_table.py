#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import pandas as pd
# import time
from ForCall01 import *
from branch_code.dadi_loader import get_time_from_table,compname_process,get_time_dif
account="dd_data2/xdf123@10.9.1.170/lbora"
comm1="select max(VERIPFINALDATE) from PRPLCARLOSSAPPROVAL A"
oracle = useOracle("dd_data2", "xdf123", "LBORA170")
date=oracle.getData(comm1,account)
date=pd.to_datetime(date['MAX(VERIPFINALDATE)'][0])
start_time,end_time=get_time_from_table(date)
# comm2='''create table abcd (
#        ID number(10) primary key,
#        LOSSAPPROVALCOMCODE NVARCHAR2(400),
#         VERIFYFINALDATE    NVARCHAR2(400),
#          VEHSERINAME    NVARCHAR2(400),
#          IS4S  NVARCHAR2(400),
#          REPAIRTYPE  NVARCHAR2(400),
#          REPAIRTYPENAME  NVARCHAR2(400),
#          COMPNAME   NVARCHAR2(400),
#          SUMVERILOSS  NVARCHAR2(400),
#          COMPNAME_PRO  NVARCHAR2(400),
#          ACN_COMMON_NAME  NVARCHAR2(400)
#        )'''
# oracle.executeCommit(comm2,account)
# comm3='''CREATE SEQUENCE emp
# 　　INCREMENT BY 1
# 　　START WITH 1
# 　　NOMAXVALUE
# 　　NOCYCLE
# CACHE 20'''
# oracle.executeCommit(comm3,account)
# comm4='''INSERT INTO  abcd (ID,LOSSAPPROVALCOMCODE,VERIFYFINALDATE,VEHSERINAME,IS4S,REPAIRTYPE,REPAIRTYPENAME,COMPNAME,SUMVERILOSS) select emp.nextval, aaa.* from(SELECT A.LOSSAPPROVALCOMCODE,A.VERIFYFINALDATE,A.VEHSERINAME,F.IS4S,R.REPAIRTYPE,\
# R.REPAIRTYPENAME,R.COMPNAME,R.SUMVERILOSS FROM PRPLCARLOSSAPPROVAL A inner join PRPLCLAIMLOSSITEMS C \
# on C.ITEMID=A.ITEMID inner join PRPLCAR D on D.ITEMID=C.ITEMID inner join PRPLREPAIRCHANNEL F \
# on F.ITEMID=A.ITEMID inner join PRPLCARREPAIRFEE R ON R.LOSSAPPROVALID=A.LOSSAPPROVALID \
# WHERE A.VERIFYFLAG in ('1','3') and A.VERIPFLAG in ('1','3','N') \
# AND CASE WHEN A.VERIPFINALDATE IS NULL OR A.VERIFYFINALDATE >= A.VERIPFINALDATE \
# THEN A.VERIFYFINALDATE ELSE A.VERIPFINALDATE END \
# BETWEEN to_date('{0}','YYYY-MM-DD HH24:MI:SS') and to_date('{1}','YYYY-MM-DD HH24:MI:SS'))aaa'''.format(start_time,end_time)
# oracle.executeCommitSubmit(comm4,"dd_data2/xdf123@10.9.1.170/lbora")

comm5='''select t.id,t.compname from {0} t'''.format('abcd')
df=oracle.getData(comm5,account)
df['COMPNAME_PRO']=df['COMPNAME'].apply(compname_process)


start_time=time.time()
print(len(df))
for i in range(len(df)):
    id=df['ID'][i]
    compname_pro=df['COMPNAME_PRO'][i]
    # comm6 = '''Merge into abcd s Using abcd nn On nn.ID='{0}' When matched then update set s.COMPNAME_PRO = '{1}' '''.format(id, compname_pro)
    comm7 = '''update abcd t set t.COMPNAME_PRO='{0}' where t.ID='{1}' '''.format(compname_pro, id)
    oracle.executeCommitSubmit(comm7, account)
print(get_time_dif(start_time))