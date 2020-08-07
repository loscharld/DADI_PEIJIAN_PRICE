#!/usr/bin/env python 
# -*- coding:utf-8 -*-

from data_from_oracle import *
# from get_model import peijian_model
# from statistics import statistics_pre
# from model_create_data import model_predict
from branch_code.dadi_loader import get_time_dif
from sql_oracle import Process
# from left_right_compare import left_right
# from model_verification import verification
def main():
    os.environ['CUDA_VISIBLE_DEVICES'] = "0"
    start_time=time.time()
    handle=Process("dd_data2", "xdf123", "LBORA", "10.9.1.169")
    handle.trunct_table()
    handle.handle2oracle()
    handle.changfang_price()
    statistics_pre(handle)
    peijian_model(handle)
    handle.model_predict()
    verification()
    handle.yzt_process()
    handle.bg_process()
    handle.left_right()
    handle.to_oracle()
    handle.inter2oracle()
    print('总耗时：',get_time_dif(start_time))


if __name__=='__main__':
    main()