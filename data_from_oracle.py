#!/usr/bin/env python 
# -*- coding:utf-8 -*-

from ForCall01 import *
import argparse
from datetime import timedelta

os.environ['CUDA_VISIBLE_DEVICES'] = '0'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'

parser = argparse.ArgumentParser()
parser.add_argument("-LI", "--LOSS_ID", default="LOSSAPPROVALID", help="The input loss id")
parser.add_argument("-RN", "--REPORT_NUMBER", default="REGISTNO", help="The input report number.")
parser.add_argument("-I4", "--IS4S", default="NEW_IS4S", help="The input IS4S.")
parser.add_argument("-PT", "--PRICE_TYPE", default="CHGCOMPSET", help="The input price type.")
parser.add_argument("-PR", "--PRICE", default="UNITPRICE", help="The input price.")
parser.add_argument("-RE", "--REGION", default="JIGOU", help="The input region.")
parser.add_argument("-BR", "--BRAND", default="BRAND_NAME", help="The input brand.")
parser.add_argument("-CN", "--CATE_NAME", default="CATE_NAME", help="The input chexi.")
parser.add_argument("-CO", "--COMPNAME", default="COMMON_NAME", help="The input Accessory description.")
parser.add_argument("-dt", "--data_table_name", default="LB_PEIJIAN_ORIGINAL_HANDLE", help="The input Database table name.")
args = parser.parse_args()

class Extract():
    def __init__(self):
        self.oracle=useOracle("dd_data2", "xdf123", "LBORA169")
        self.account="dd_data2/xdf123@10.9.1.169/lbora"

    def extract_data(self):
        #从数据库读取数据
        commit="select * from {} t where (t.CHGCOMPSET=2 or t.CHGCOMPSET=3) and rownum<15000001".format(args.data_table_name)
        datas=self.oracle.getData(commit,self.account)
        datas.dropna(subset=['JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'ORIGINALCODE'], how='any', axis=0,inplace=True)
        datas = datas[datas['BRAND_NAME'] != 'None']
        datas[args.BRAND].replace('\t', '', regex=True,inplace=True)
        # datas[args.IS4S]=datas[args.IS4S].astype(float)
        datas[args.PRICE_TYPE]=datas[args.PRICE_TYPE].astype(int)
        datas[args.PRICE]=datas[args.PRICE].astype(float)
        datas.dropna(subset=[args.COMPNAME], how='any',axis=0, inplace=True)
        # datas[args.work_hour_project] = datas.apply(lambda x: x['COMMON_NAME'] if str(x[args.work_hour_project]) == 'nan' else x[args.work_hour_project], axis=1)
        return datas

    # 取三个月的数据
    def get_months_from_table(self,end_time):
        today_year = end_time.year
        # 今年的时间减去1，得到去年的时间。last_year等于2015
        last_year = int(end_time.year) - 1
        today_day = end_time.day-1
        # 得到今年的每个月的时间。today_year_months等于1 2 3 4 5 6 7 8 9，
        today_year_months = range(1, end_time.month + 1)
        # 得到去年的每个月的时间  last_year_months 等于10 11 12
        last_year_months = range(end_time.month + 1, 13)
        # 定义列表去年的数据
        data_list_lasts = []
        # 通过for循环，得到去年的时间夹月份的列表
        # 先遍历去年每个月的列表
        for last_year_month in last_year_months:
            # 定义date_list 去年加上去年的每个月
            date_list = '%s-%s' % (last_year, last_year_month)
            # 通过函数append，得到去年的列表
            data_list_lasts.append(date_list)

        data_list_todays = []
        # 通过for循环，得到今年的时间夹月份的列表
        # 先遍历今年每个月的列表
        for today_year_month in today_year_months:
            # 定义date_list 去年加上今年的每个月
            data_list = '%s-%s' % (today_year, today_year_month)
            # 通过函数append，得到今年的列表
            data_list_todays.append(data_list)
        # 去年的时间数据加上今年的时间数据得到年月时间列表
        data_year_month = data_list_lasts + data_list_todays
        data_year_month.reverse()
        start_time = pd.to_datetime(data_year_month[3])
        delta = timedelta(days=today_day)
        start_time = start_time + delta
        end_time = end_time
        # datas = datas.loc[(datas['date'] <= end_time) & (datas['date'] >= start_time)]
        return start_time, end_time

    def extract_months_data(self):
        #从数据库读取数据
        comm1 = '''select  max(OPERATETIMEFORHIS) from  LB_PEIJIAN_ORIGINAL_HANDLE t'''
        date = self.oracle.getData(comm1, self.account)
        date = date['MAX(OPERATETIMEFORHIS)'][0]
        start_time, endtime = self.get_months_from_table(date)
        commit='''select * from {} t where (t.CHGCOMPSET=2 or t.CHGCOMPSET=3) and (t.OPERATETIMEFORHIS between to_date('{}','yyyy/mm/dd hh24:mi:ss') and to_date('{}','yyyy/mm/dd hh24:mi:ss')) and rownum<10000'''.format(args.data_table_name,start_time, endtime)
        datas=self.oracle.getData(commit,self.account)
        datas.dropna(subset=['JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'ORIGINALCODE'], how='any', axis=0,inplace=True)
        datas = datas[datas['BRAND_NAME'] != 'None']
        datas[args.BRAND].replace('\t', '', regex=True,inplace=True)
        # datas[args.IS4S]=datas[args.IS4S].astype(float)
        datas[args.PRICE_TYPE]=datas[args.PRICE_TYPE].astype(int)
        datas[args.PRICE]=datas[args.PRICE].astype(float)
        datas.dropna(subset=[args.COMPNAME], how='any',axis=0, inplace=True)
        # datas[args.work_hour_project] = datas.apply(lambda x: x['COMMON_NAME'] if str(x[args.work_hour_project]) == 'nan' else x[args.work_hour_project], axis=1)
        return datas

    def get_data(self):
        #筛选品牌价和原厂价数据
        datas=self.extract_data()
        df=datas.loc[(datas[args.PRICE_TYPE]==2)|(datas[args.PRICE_TYPE]==3)].reset_index(drop=True)
        #设定一个范围，过滤掉一些异常值
        df=df[df[args.PRICE]>1].reset_index(drop=True)
        print(df.shape[0])
        return df

    def get_months_data(self):
        #筛选品牌价和原厂价数据
        datas=self.extract_months_data()
        df=datas.loc[(datas[args.PRICE_TYPE]==2)|(datas[args.PRICE_TYPE]==3)].reset_index(drop=True)
        #设定一个范围，过滤掉一些异常值
        df=df[df[args.PRICE]>1].reset_index(drop=True)
        print(df.shape[0])
        return df

    def get_yuanchang_data(self):
        #筛选原厂价数据
        datas = self.extract_data()
        df = datas.loc[datas[args.PRICE_TYPE] == 3].reset_index(drop=True)
        # 设定一个范围，过滤掉一些异常值
        df = df[df[args.PRICE] > 0].reset_index(drop=True)
        print(df.shape[0])
        return df


if __name__=='__main__':
    end_time=pd.to_datetime('2019-02-16')
    extract=Extract()
    start_time, end_time=extract.get_months_from_table(end_time)
    print(start_time, end_time)