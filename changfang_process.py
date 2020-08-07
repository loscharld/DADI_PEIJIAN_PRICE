# coding: utf-8
import pandas as pd
import numpy as np
import cx_Oracle
import warnings
warnings.filterwarnings("ignore")
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from socketserver import ThreadingMixIn
class ThreadXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
    pass
import re
from ForCall02 import *

class Priceall:
    def __init__(self, user, password, database):
        self.user = user
        self.password = password
        self.database = database
    
    # 读取某年厂方价数据
    def get_data(self,year):
        conn = cx_Oracle.connect("dd_data2/xdf123@10.9.1.170/lbora")
        cursor = conn.cursor()
        comm='''select distinct f.DD_CATE_ID DD_BRAND_ID,aa.MODEL_BRAND,aa.posid,aa.posname,
      aa.commonid, aa.commoncode,
    aa.commonname,replace(aa.ORIGINALCODE,' ','')ORIGINALCODE,aa.PARTSTANDARDCODE,aa.changfangjia,aa.OPERATETIMEFORHIS
              from (select b.CHGCOMPSET,A.dc, CASE  WHEN B.REFPRICE1 IS NULL THEN
                              decode(A.peacecaseflag, 'N', B.upperlimitprice, B.REFPRICE1)
                             ELSE B.REFPRICE1 END changfangjia,B.offerprice,b.ORIGINALCODE,b.LOSSAPPROVALID,
                           b.posid,b.posname,b.commonid, b.commoncode,b.commonname,a.MODEL_BRAND,a.OPERATETIMEFORHIS,b.PARTSTANDARDCODE
                      FROM (select * from lb_PRPCARINFO where  MODEL_BRAND is not null and dc in 
                      (select distinct dc from LB_DATA_SYNC where SYNC_YEAR = {})) A
                     INNER JOIN (select * from lb_PRPLCARCOMPONENTINFO where
      commonname is not null  
      and dc in (select distinct dc from LB_DATA_SYNC where SYNC_YEAR = {})) B
                        on A.LOSSAPPROVALID = B.LOSSAPPROVALID)aa
                        inner join ( select  CATE_NAME,DD_CATE_ID   from  A_CATEGORY where  CATE_LEVEL=2) f on aa.MODEL_BRAND=f.CATE_NAME 
                        where changfangjia > 0'''.format(year,year)
        cursor.execute(comm)
        x = cursor.description
        columns = [y[0] for y in x]
        cursor01 = cursor.fetchall()
        cursor.close()
        data = pd.DataFrame(cursor01, columns=columns)
        return data
    
    # 分dc读取含换件方案和定损换件单价的数据
    def get_data2(self,dc):
        conn = cx_Oracle.connect("dd_data2/xdf123@10.9.1.170/lbora")
        cursor = conn.cursor()
        comm2='''select  f.DD_CATE_ID DD_BRAND_ID,aa.MODEL_BRAND,aa.posid,aa.posname,
      aa.commonid, aa.commoncode,aa.commonname,aa.ORIGINALCODE,aa.PARTSTANDARDCODE,aa.CHANGFANGJIA,aa.UNITPRICE ,aa.CHGCOMPSET,aa.dc
      from (select B.CHGCOMPSET, A.dc, 
      CASE  WHEN B.REFPRICE1 IS NULL THEN
                              decode(A.peacecaseflag, 'N', B.upperlimitprice, B.REFPRICE1)
                             ELSE B.REFPRICE1 END changfangjia,
      B.reFunitPrice, B.LOCPRICE3,B.offerprice,b.ORIGINALCODE,b.PARTSTANDARDCODE,
      b.LOSSAPPROVALID, b.posid,b.posname,b.commonid, b.commoncode,b.commonname,a.MODEL_BRAND,b.UNITPRICE
              FROM (select * from lb_PRPCARINFO where  MODEL_BRAND is not null and dc = {}) A
             INNER JOIN (select * from lb_PRPLCARCOMPONENTINFO where
                     THIRDFACTORYFLAG != 1 and dc = {}) B
                on A.LOSSAPPROVALID = B.LOSSAPPROVALID )aa                                  
     inner join ( select  CATE_NAME,DD_CATE_ID   from  A_CATEGORY where  CATE_LEVEL=2) f on aa.MODEL_BRAND=f.CATE_NAME
     where  COMMONNAME is not null'''.format(dc,dc)
        cursor.execute(comm2)
        x = cursor.description
        columns = [y[0] for y in x]
        cursor01 = cursor.fetchall()
        cursor.close()
        data = pd.DataFrame(cursor01, columns=columns)
        return data
    
    # 读dc
    def get_dc(self,year):
        conn = cx_Oracle.connect("dd_data2/xdf123@10.9.1.170/lbora")
        cursor = conn.cursor()
        cursor.execute('''select distinct dc from LB_DATA_SYNC t where t.sync_year = {} order by dc'''.format(year))
        x = cursor.description
        columns = [y[0] for y in x]
        cursor01 = cursor.fetchall()
        cursor.close()
        data = pd.DataFrame(cursor01, columns=columns)
        return data
    
    # 编码处理
    def get_bianma(self,code1,code2):
        code1=str(code1)
        code2=str(code2)
        # flag = []  # 存放处理后的编码，编码为0表示不符合条件
        # re_code1=re.findall('[a-zA-Z]',code1)
        re_code2=re.findall('[a-zA-Z]',code2)
        if not pd.isnull(code1):
            code1 = ''.join(code1.split())  # 去掉原厂编码中的空格
        if not pd.isnull(code2):
            code2 = ''.join(code2.split())  # 去掉标准编码中的空格
        if (len(str(code1)) > 6 and len(set(str(code1))) > 1):  # 原厂编码大于6位且不是重复数字，则取原厂编码
            flag = code1
        elif (len(str(code1)) > 6 and len(set(str(code1)))  == 1):  # 原厂编码大于6位且为重复数字，若标准编码大于6位且不是重复数字，则取标准编码，否则编码为0
            if (len(str(code2))  > 6 and len(set(str(code2))) > 1):
                flag = code2
            else:
                flag = 0
        elif (len(str(code1)) <= 6):  #  原厂编码小于等于6位，标准编码大于6位且不为重复数字，则取标准编码，否则编码为0
            if (len(str(code2)) > 6 and len(set(str(code2))) > 1):
                flag = code2
            #标准编码带有字母且长度为6，则取标准编码
            elif re_code2 and len(str(code2))==6:
                flag=code2
    #           print(flags)
            else:
                flag = 0
        elif code1=='None' and len(code2):
            flag=code2
    #             print(flags)
        elif code2=='None' and len(code1):
            flag = code1
        else:
            flag = 0
        # flag.append(flags)
        return flag
    
    def get_flag(self,code):

        code=str(code)
        if not pd.isnull(code) and (len(str(code)) >= 6 and len(set(str(code))) > 1):  # 原厂编码大于6位且不是重复数字，则取原厂编码
            flag = 1
        else:
            flag = 0
        return flag

    def BatchinsertDataToTable(self,data, dataTable):
        connection = cx_Oracle.connect("dd_data2/xdf123@10.9.1.170/lbora")
        cursor = connection.cursor()
        query = "INSERT INTO " + dataTable + " VALUES ({})"
        columns = list(data.columns)
        aidx = list(range(1, len(columns) + 1))
        aidx = [':' + str(i) for i in aidx]
        aname = ','.join(aidx)
        dtHigh = data.shape[0]
        dtWidth = data.shape[1]
        creatVar = locals()
        wholeData = []
        for i in range(dtHigh):
            value_list = []
            for j in range(dtWidth):
                value_list.append("{}".format(str(data.iloc[i, j])))
                ccc=['nan','None']
                value_list=['' if i in ccc  else i for i in value_list]
            wholeData.append(value_list)

        for ii in range(1, len(wholeData)//100+2):
            begin = (ii - 1) * 100
            end = ii * 100
            cursor.executemany(query.format(aname), wholeData[begin:end])
        connection.commit()
        cursor.close()
        connection.close()
        return

    def creatDataFrame(self,df,tableName):
        connection = cx_Oracle.connect("dd_data2/xdf123@10.9.1.170/lbora")
        cursor = connection.cursor()
        columns = df.columns
        character_types = ['NVARCHAR2(400)'] * len(columns)
        jihe = ','.join([k + ' ' + v for k, v in (list(zip(columns, character_types)))])
        commit="create table {}({})".format(tableName,jihe)
        cursor.execute(commit)
        cursor.close()
        connection.close()
        return

    # 价格处理，按车系、配件和编码分组后，取最近时间的价格作为厂方价
    def get_price(self,data):
        lst=[]
        time = data['OPERATETIMEFORHIS'].tolist()
        price = data['CHANGFANGJIA'].tolist()
        ind = time.index(max(time))
        price = price[ind]
        lst.append(price)
        lst.append(max(time))
        return lst
    
    # 获取厂方价数据，并处理编码
    def get_cf(self,year):
        print('开始处理{}年厂方价'.format(year))
        data = self.get_data(year)
        # 数据去重
        data = data.drop_duplicates()
        # 编码处理
        print('处理{}年厂方价编码'.format(year))
        if data.shape[0] != 0:
            data['CODE']  = data.apply(lambda row: self.get_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']), axis=1)
            print('get_bianma is ok')
            data.drop(['ORIGINALCODE','PARTSTANDARDCODE'],axis=1,inplace=True)
            print('get_price start')
            data = data.groupby(['DD_BRAND_ID','MODEL_BRAND','POSID','POSNAME','COMMONID','COMMONCODE','COMMONNAME','CODE'])['CHANGFANGJIA','OPERATETIMEFORHIS'].apply(self.get_price).apply(pd.Series).reset_index()
            print('get_price is ok')
            data.columns = ['DD_BRAND_ID','MODEL_BRAND','POSID','POSNAME','COMMONID','COMMONCODE','COMMONNAME','CODE','CHANGFNAG','NEW_TIME']
            # data.drop(['组内顺序'],axis = 1,inplace=True)
            print('get_flag start')
            # print (data['CODE'][1])
            data['WAY_FLAG'] = data.apply(lambda row: self.get_flag(row['CODE']), axis=1)
            print('get_flag is ok')
            data = data.drop_duplicates()
        else:
            data = pd.DataFrame(columns = ['DD_BRAND_ID','MODEL_BRAND','POSID','POSNAME','COMMONID','COMMONCODE','COMMONNAME','CODE','CHANGFNAG','NEW_TIME','WAY_FLAG'])
        return data
    # 计算比例
    def get_rate(self,dc,data):
        data2 = self.get_data2(dc)
        if data2.shape[0] == 0:
            pass
        else:
            data2['CODE'] = data2.apply(lambda row: self.get_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']), axis=1)
            data2['WAY_FLAG'] = data2.apply(lambda row: self.get_flag(row['CODE']), axis=1)
            data3 = pd.merge(data2, data, how='left', on=['DD_BRAND_ID','MODEL_BRAND','POSID','POSNAME','COMMONID','COMMONCODE','COMMONNAME','CODE','WAY_FLAG'])
            data3 = data3.drop(['CHANGFANGJIA'],axis=1)

            # 计算原厂价，品牌价和适用价的值
            chg = data3['CHGCOMPSET'].tolist()
            unitprice = data3['UNITPRICE'].tolist()
            n = len(chg)
            YUANCHANG = [unitprice[i] if chg[i] == '3' or chg[i] == '市场价格' else np.nan for i in range(n)]
            PINPAI = [unitprice[i] if chg[i] == '2' else np.nan for i in range(n)]
            SHIYONG = [unitprice[i] if chg[i] == '4' else np.nan for i in range(n)]
            data3['YUANCHANG'] = YUANCHANG
            data3['PINPAI'] = PINPAI
            data3['SHIYONG'] = SHIYONG
            # 计算各价格与厂方价的比例
            # data3['YUANCHANG_RATE'] = round(data3['YUANCHANG']/data3['CHANGFNAG'],2)
            # data3['PINPAI_RATE'] = round(data3['PINPAI']/data3['CHANGFNAG'],2)
            # data3['SHIYONG_RATE'] = round(data3['SHIYONG']/data3['CHANGFNAG'],2)
            # data3['COUNT_PART'] = np.nan

            order=['DD_BRAND_ID', 'MODEL_BRAND', 'POSID', 'POSNAME', 'COMMONID',
                   'COMMONCODE', 'COMMONNAME', 'UNITPRICE', 'CHGCOMPSET', 'DC', 'CODE',  'CHANGFNAG','NEW_TIME',
                   'YUANCHANG', 'PINPAI', 'SHIYONG', 'WAY_FLAG','ORIGINALCODE', 'PARTSTANDARDCODE']
            data3=data3[order]
            data3.rename(columns={'CODE':'ORIGINALCODE','ORIGINALCODE':'OLD_ORIGINALCODE'},inplace = True)
            print(data3.shape)
            self.creatDataFrame(data3,'LB_PRLCE_CF')
            self.BatchinsertDataToTable(data3, 'LB_PRLCE_CF')
    #得到整年的价格
    def get_price_all(self,year):
        try:
            data_dc = self.get_dc(year)
            dc = data_dc['DC'].tolist()
            data = self.get_cf(year)
            print('{}年厂方价处理完成'.format(year))
            for i in dc:
                print(i)
                self.get_rate(i,data)
                print('该dc数据处理完成')
            return 100
        except Exception as e:
            return str(e)




# class RequestHandler(SimpleXMLRPCRequestHandler):
#     rpc_paths = ('/RPC2',)
#
#
#     server=ThreadXMLRPCServer(("0.0.0.0", 8801))
#     server.register_introspection_functions()
#     server.register_instance(Priceall())
#     print("server is start...........")
#     # Run the server's main loop
#     server.serve_forever()
#     print("server is end...........")

if __name__=='__main__':
    price=Priceall('dd_data2', 'xdf123', 'LBORA170')
    # price.get_data('2017')
    price.get_price_all('2018')