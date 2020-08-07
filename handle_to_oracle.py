#! usr/bin/env python3
# -*- coding:utf-8 -*-
from ForCall01 import *
from branch_code.dadi_loader import get_time_from_table,get_time_dif

def handle2oracle():
    oracle = useOracle("dd_data2", "xdf123", "LBORA169")
    account = "dd_data2/xdf123@10.9.1.169/lbora"
    comm1='''select  max(OPERATETIMEFORHIS) from  LB_PEIJIAN_ORIGINAL_DATA_LOAD t'''
    date=oracle.getData(comm1,account)
    date=date['MAX(OPERATETIMEFORHIS)'][0]
    start_time,endtime=get_time_from_table(date)
    comm2='''insert into LB_PEIJIAN_ORIGINAL_HANDLE select * from LB_PEIJIAN_ORIGINAL_DATA_LOAD where OPERATETIMEFORHIS between to_date('{}','yyyy/mm/dd hh24:mi:ss') and to_date('{}','yyyy/mm/dd hh24:mi:ss') '''.format(start_time,endtime)
    oracle.executeCommitSubmit(comm2,account)
    print('handle2oracle done!')
if __name__=='__main__':
    start_time=time.time()
    handle2oracle()
    print(get_time_dif(start_time))