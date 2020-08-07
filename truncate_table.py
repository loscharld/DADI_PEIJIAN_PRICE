#!/usr/bin/env python
# -*- coding:utf-8 -*-

from ForCall01 import *
def trunct_table():
    oracle = useOracle("dd_data2", "xdf123", "LBORA169")
    account="dd_data2/xdf123@10.9.1.169/lbora"
    comm1='''truncate table LB_PEIJIAN_SYSTEM'''
    comm2='''truncate table LB_PEIJIAN_ORIGINAL_HANDLE'''
    comm3='''truncate table LB_PEIJIAN_ORIGINAL_ABNORMAL'''
    comm4='''truncate table LB_APMS_PARTS'''
    list1=[comm1,comm2,comm3,comm4]
    for comm in list1:
        oracle.executeCommit(comm,account)
    print('trunct table done!')
if __name__=='__main__':
    trunct_table()
