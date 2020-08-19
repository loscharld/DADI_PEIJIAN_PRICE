#! usr/bin/env python3
# -*- coding:utf-8 -*-
from ForCall01 import *
from branch_code.dadi_loader import *
from sklearn.externals import joblib
import argparse
from datetime import timedelta
import logging
import shutil

if not os.path.exists('results'):
    os.makedirs('results')
if len(os.listdir('results')) > 0:
    shutil.rmtree('results')
    os.makedirs('results')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.removeHandler(logger.handlers[0])
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)

if len(logger.handlers) > 1:
    logger.removeHandler(logger.handlers[-1])
fh = logging.FileHandler('results/results.log', mode='w', encoding='utf-8')
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

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

class Sql_oracle():
    def __init__(self,user,password,database,ip):
        self.user=user
        self.password=password
        self.database=database
        self.ip=ip
        self.oracle=useOracle(self.user, self.password, self.database)
        self.account = "{}/{}@{}/{}".format(self.user,self.password,self.ip,self.database)
        self.logger=logger

class Extract(Sql_oracle):
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
        commit='''select * from {} t where (t.CHGCOMPSET=2 or t.CHGCOMPSET=3) and (t.OPERATETIMEFORHIS between to_date('{}','yyyy/mm/dd hh24:mi:ss') and to_date('{}','yyyy/mm/dd hh24:mi:ss')) '''.format(args.data_table_name,start_time, endtime)
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

class Process(Extract):
    def trunct_table(self):
        comm1 = '''truncate table LB_PEIJIAN_SYSTEM'''
        comm2 = '''truncate table LB_PEIJIAN_ORIGINAL_HANDLE'''
        comm3 = '''truncate table LB_PEIJIAN_ORIGINAL_ABNORMAL'''
        comm4 = '''truncate table LB_APMS_PARTS_BF'''
        list1 = [comm1, comm2, comm3, comm4]
        for comm in list1:
            self.oracle.executeCommit(comm, self.account)
        self.logger.info(('trunct table done!'))

    def handle2oracle(self):
        comm1 = '''select  max(OPERATETIMEFORHIS) from  LB_PEIJIAN_ORIGINAL_DATA_LOAD t'''
        date = self.oracle.getData(comm1, self.account)
        date = date['MAX(OPERATETIMEFORHIS)'][0]
        start_time, endtime = get_time_from_table_1year(date)
        comm2 = '''insert into LB_PEIJIAN_ORIGINAL_HANDLE select * from LB_PEIJIAN_ORIGINAL_DATA_LOAD where OPERATETIMEFORHIS between to_date('{}','yyyy/mm/dd hh24:mi:ss') and to_date('{}','yyyy/mm/dd hh24:mi:ss') '''.format(
            start_time, endtime)
        self.oracle.executeCommitSubmit(comm2, self.account)
        self.logger.info(('handle2oracle done!'))

    def changfang_price(self):
        tableName = 'LB_PEIJIAN_ORIGINAL_HANDLE'
        commit = "select * from {} t where rownum<15000001".format(tableName)
        datas = self.oracle.getData(commit, self.account)
        datas.dropna(subset=['JIGOU','JIGOU_ID','BRAND_ID','BRAND_NAME','ORIGINALCODE'],how='any',axis=0,inplace=True)
        datas=datas[datas['BRAND_NAME']!='None']
        datas['BRAND_NAME'].replace('\t', '',regex=True,inplace=True)
        if len(datas):
            datas.drop(['CATE_NAME'], axis=1, inplace=True)
            datas = datas.drop_duplicates()
            datas['CODE'] = datas.apply(lambda row: get_original_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']),
                                        axis=1)
            datas['PARTSTANDARDCODE'] = datas.apply(
                lambda row: get_standard_bianma(row['ORIGINALCODE'], row['PARTSTANDARDCODE']), axis=1)
            standard_code = datas.groupby(['JIGOU', 'BRAND_NAME', 'COMMON_NAME'])['PARTSTANDARDCODE'].apply(
                standard_bianma_process).apply(pd.Series).reset_index()
            standard_code.rename(columns={0: 'STANDARD_PART_CODE'}, inplace=True)
            datas = pd.merge(datas, standard_code, on=['JIGOU', 'BRAND_NAME', 'COMMON_NAME'], how='left')
            peijian2standardcode = dict(zip(datas['COMMON_NAME'], datas['STANDARD_PART_CODE']))
            with open('changfang/peijian2standardcode.json', 'w', encoding='utf-8') as w1:
                json.dump(peijian2standardcode, w1, ensure_ascii=False)
            # datas.drop(['ORIGINALCODE', 'PARTSTANDARDCODE'], axis=1, inplace=True)
            datas['WAY_FLAG'] = datas.apply(lambda row: get_flag(row['CODE']), axis=1)
            df = datas[(datas['CHANGFANGJIA'] > 0) & (datas['WAY_FLAG'] == 1)]
            # 编码异常数据
            # outliers=datas[datas['WAY_FLAG']==0].reset_index(drop=True)
            # if len(outliers):
            #     outliers=pd.DataFrame(outliers,columns=['LOSSAPPROVALID','REGISTNO','JIGOU','DD_BRAND_ID','MODEL_BRAND','MODEL_CATE_NAME','COMMONNAME','POSID','POSNAME','CODE','CHANGFNAG','VERIFYFINALDATE'])
            #     outliers.rename(columns={'JIGOU': 'REGION'}, inplace=True)
            #     outliers.to_excel('outliers/4S店价异常数据.xlsx',index=None,encoding='utf-8')
            df.dropna(subset=['OPERATETIMEFORHIS'], axis=0, how='any', inplace=True)
            df1 = df.groupby(
                ['BRAND_ID','CODE'])['CHANGFANGJIA', 'OPERATETIMEFORHIS'].apply(get_price).apply(pd.Series).reset_index()
            df1.rename(columns={0: 'CHANGFANGJIA', 1: 'OPERATETIMEFORHIS'}, inplace=True)
            df=pd.merge(df1,df,how='left', on=['BRAND_ID','CODE','CHANGFANGJIA', 'OPERATETIMEFORHIS'])
            df.drop_duplicates(subset=['BRAND_ID','CODE','CHANGFANGJIA', 'OPERATETIMEFORHIS'],keep='first',inplace=True)
            df.rename(columns={'CHANGFANGJIA': 'REFERENCE', 'OPERATETIMEFORHIS': 'CURRENT_TIME'}, inplace=True)
            df['PRICE_TYPE'] = '4S店价'
            df = pd.DataFrame(df, columns=['JIGOU', 'JIGOU_ID','BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID', 'POS_ID',
                                           'POS_NAME', 'CODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'REFERENCE',
                                           'CURRENT_TIME'])
            # columns = ['LOSSAPPROVALID','REGISTNO','JIGOU','DD_BRAND_ID','MODEL_BRAND','MODEL_CATE_NAME','COMMONNAME','POSID','POSNAME','CODE','CHANGFNAG', 'NEW_TIME']
            # datas = df.drop_duplicates().reset_index(drop=True)
            df.rename(columns={'CODE': 'ORIGINALCODE'}, inplace=True)
            df.drop(['JIGOU','JIGOU_ID'],axis=1,inplace=True)
            region_list=list(region2code.keys())
            df_all=pd.DataFrame()
            for region in region_list:
                df['JIGOU']=region
                df['JIGOU_ID']=region2code[region]
                df_all=pd.concat([df_all,df],axis=0)
            df_all = pd.DataFrame(df_all, columns=['JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID',
                                           'POS_ID','POS_NAME', 'ORIGINALCODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'REFERENCE','CURRENT_TIME'])
            df_all.to_csv('changfang/4S店价.csv', index=None, encoding='utf-8')
        else:
            datas = pd.DataFrame(datas,
                                 columns=['JIGOU','JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'CATE_NAME', 'COMMON_NAME', 'COMMON_ID',
                                          'POS_ID', 'POS_NAME', 'ORIGINALCODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'REFERENCE',
                                          'CURRENT_TIME'])
        self.logger.info(('changfang price done!'))
        return datas

    def get_commonname(self):
        commit = '''select * from(
    select BRAND_ID,BRAND_NAME,COMMON_NAME,ORIGINALCODE,COMMON_ID,POS_ID,POS_NAME,count(ORIGINALCODE)ct from LB_PEIJIAN_ORIGINAL_HANDLE 
    group by  BRAND_ID,BRAND_NAME,COMMON_NAME,ORIGINALCODE,COMMON_ID,POS_ID,POS_NAME order by ct desc )where ct>=30 and rownum<15000001'''
        df = self.oracle.getData(commit, self.account)
        df.drop(['CT'], axis=1, inplace=True)
        return df

    def create_data(self):
        df1 = self.get_commonname()
        with open('changfang/peijian2standardcode.json', encoding='utf-8') as f1:
            peijian2standardcode = json.load(f1)
        df1['STANDARD_PART_CODE'] = df1['COMMON_NAME'].map(peijian2standardcode)
        with open('pp_yc/jigou2id.json', encoding='utf-8') as f2:
            jigou2id = json.load(f2)
        fd = pd.DataFrame()
        for ch in range(2, 4):
            df = pd.DataFrame()
            for jg in jigou2id.keys():
                df2 = df1.copy()
                df2['JIGOU'] = jg
                df = pd.concat([df, df2], axis=0)
            df['CHGCOMPSET'] = ch
            fd = pd.concat([fd, df], axis=0)

        return fd

    def model_predict(self):
        df = self.create_data()
        df = pd.DataFrame(df,
                          columns=['JIGOU', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID', 'POS_ID', 'POS_NAME',
                                   'ORIGINALCODE', 'STANDARD_PART_CODE', 'CHGCOMPSET'])
        test_data = df[['JIGOU', 'BRAND_NAME', 'COMMON_NAME', 'ORIGINALCODE', 'CHGCOMPSET']]
        ss = joblib.load("pp_yc/data_ss.model")  ## 加载模型
        gbm_model = joblib.load("pp_yc/gbm.model")  ## 加载模型

        with open('pp_yc/jigou2id.json', encoding='utf-8') as reader1:
            jigou2id = json.load(reader1)

        with open('pp_yc/compname2id.json', encoding='utf-8') as reader2:
            compname2id = json.load(reader2)

        with open('pp_yc/code2id.json', encoding='utf-8') as reader3:
            code2id = json.load(reader3)

        with open('pp_yc/brand2id.json', encoding='utf-8') as reader4:
            brand2id = json.load(reader4)

        test_data['jigou_id'] = test_data['JIGOU'].map(jigou2id)
        test_data['brand_id'] = test_data['BRAND_NAME'].map(brand2id)
        # test_data['cate_name_id'] = test_data[FLAGS.CATE_NAME].map(cate_name2id)
        test_data['compname_id'] = test_data['COMMON_NAME'].map(compname2id)
        test_data['code_id'] = test_data['ORIGINALCODE'].map(code2id)
        test_data.dropna(subset=['jigou_id', 'brand_id', 'compname_id', 'code_id'], how='any', axis=0, inplace=True)
        test_data = test_data.reset_index(drop=True)
        df2 = pd.DataFrame(test_data, columns=['jigou_id', 'brand_id', 'compname_id', 'code_id', 'CHGCOMPSET'])
        df2 = df2.astype(float)
        x_data = ss.transform(df2)
        datas_pre = list(np.exp(gbm_model.predict(x_data)))
        df3 = pd.DataFrame()
        df3['LGBM_VALUE'] = datas_pre
        df3['REFERENCE'] = df3['LGBM_VALUE'].map(lambda x: round(0.98 * x))
        df3 = pd.DataFrame(df3, columns=['REFERENCE'])
        test_data.drop(['jigou_id', 'brand_id', 'compname_id', 'code_id'], axis=1, inplace=True)
        df4 = pd.concat([test_data, df3], axis=1)
        df4 = pd.merge(df, df4, on=['JIGOU', 'BRAND_NAME', 'COMMON_NAME', 'ORIGINALCODE', 'CHGCOMPSET'], how='left')
        df4.dropna(subset=['REFERENCE'], how='any', axis=0, inplace=True)
        df4 = pd.DataFrame(df4,
                           columns=['JIGOU', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID', 'POS_ID', 'POS_NAME',
                                    'ORIGINALCODE', 'STANDARD_PART_CODE', 'CHGCOMPSET', 'REFERENCE'])
        df4.to_csv('pp_yc/全国常用配件模型预测.csv', index=None, encoding='utf-8')
        self.logger.info(('model predict done!'))

    def tes_model_predict(self):
        df = self.create_data()
        df = pd.DataFrame(df,
                          columns=['JIGOU', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID', 'POS_ID', 'POS_NAME',
                                   'ORIGINALCODE', 'STANDARD_PART_CODE', 'CHGCOMPSET'])
        test_data = df[['JIGOU', 'BRAND_NAME', 'COMMON_NAME', 'ORIGINALCODE', 'CHGCOMPSET']]
        ss = joblib.load("pp_yc/data_ss.model")  ## 加载模型
        gbm_model = joblib.load("pp_yc/gbm.model")  ## 加载模型

        with open('pp_yc/jigou2id.json', encoding='utf-8') as reader1:
            jigou2id = json.load(reader1)

        with open('pp_yc/compname2id.json', encoding='utf-8') as reader2:
            compname2id = json.load(reader2)

        with open('pp_yc/code2id.json', encoding='utf-8') as reader3:
            code2id = json.load(reader3)

        with open('pp_yc/brand2id.json', encoding='utf-8') as reader4:
            brand2id = json.load(reader4)

        test_data['jigou_id'] = test_data['JIGOU'].map(jigou2id)
        test_data['brand_id'] = test_data['BRAND_NAME'].map(brand2id)
        # test_data['cate_name_id'] = test_data[FLAGS.CATE_NAME].map(cate_name2id)
        test_data['compname_id'] = test_data['COMMON_NAME'].map(compname2id)
        test_data['code_id'] = test_data['ORIGINALCODE'].map(code2id)
        test_data.dropna(subset=['jigou_id', 'brand_id', 'compname_id', 'code_id'], how='any', axis=0, inplace=True)
        test_data = test_data.reset_index(drop=True)
        df2 = pd.DataFrame(test_data, columns=['jigou_id', 'brand_id', 'compname_id', 'code_id', 'CHGCOMPSET'])
        df2 = df2.astype(float)
        x_data = ss.transform(df2)
        datas_pre = list(np.exp(gbm_model.predict(x_data)))
        df3 = pd.DataFrame()
        df3['LGBM_VALUE'] = datas_pre
        df3['REFERENCE'] = df3['LGBM_VALUE'].map(lambda x: round(0.98 * x))
        df3 = pd.DataFrame(df3, columns=['REFERENCE'])
        test_data.drop(['jigou_id', 'brand_id', 'compname_id', 'code_id'], axis=1, inplace=True)
        df4 = pd.concat([test_data, df3], axis=1)
        df4 = pd.merge(df, df4, on=['JIGOU', 'BRAND_NAME', 'COMMON_NAME', 'ORIGINALCODE', 'CHGCOMPSET'], how='left')
        df4.dropna(subset=['REFERENCE'], how='any', axis=0, inplace=True)
        df4 = pd.DataFrame(df4,
                           columns=['JIGOU', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID', 'POS_ID', 'POS_NAME',
                                    'ORIGINALCODE', 'STANDARD_PART_CODE', 'CHGCOMPSET', 'REFERENCE'])
        df4.to_csv('pp_yc/测试集全国常用配件模型预测.csv', index=None, encoding='utf-8')
        # print('test model predict done!')

    def yzt_process(self):
        commit = '''select jigou_id, jigou, b.brand_name, b.brand_id,pos_id,pos_name,common_id,common_name,originalcode, fs_price, yc_price, pp_price, dc
      from (select distinct jigou_id,
                            jigou,
                            brand_id,
                            originalcode,
                            pos_id,pos_name,common_id,common_name,
                            fs_price,
                            yc_price,
                            pp_price,
                            dc
              from lb_yzt_data_part_compare) a
      left join (select distinct brand_id, brand_name
                   from LB_DADI_STANDARD_PRODUCT_CARS) b
        on a.brand_id = b.brand_id
     where brand_name is not null'''
        df = self.oracle.getData(commit, self.account)

        def get_price(data):
            lst = []
            changfangjias = data['FS_PRICE'].tolist()
            yuanchangjias = data['YC_PRICE'].tolist()
            pinpaijias = data['PP_PRICE'].tolist()
            dcs = data['DC'].tolist()
            ind = dcs.index(max(dcs))
            changfangjia = changfangjias[ind]
            yuanchangjia = yuanchangjias[ind]
            pinpaijia = pinpaijias[ind]
            lst.append(changfangjia)
            lst.append(yuanchangjia)
            lst.append(pinpaijia)
            return lst

        df1 = df.groupby(['JIGOU','JIGOU_ID','BRAND_NAME','BRAND_ID','ORIGINALCODE'])[
            'FS_PRICE', 'YC_PRICE', 'PP_PRICE', 'DC'].apply(get_price).apply(pd.Series).reset_index()
        df1.rename(columns={0: 'CHANGFANGJIA', 1: 'YUANCHANGJIA', 2: 'PINPAIJIA'}, inplace=True)
        # df.dropna(subset=['CHANGFANGJIA'], how='any', axis=0, inplace=True)
        df=pd.merge(df1,df,how='left',on=['JIGOU','JIGOU_ID','BRAND_NAME','BRAND_ID','ORIGINALCODE'])
        df.drop_duplicates(subset=['JIGOU','JIGOU_ID','BRAND_NAME','BRAND_ID','ORIGINALCODE'], keep='first', inplace=True)
        df=pd.DataFrame(df,columns=['JIGOU_ID','JIGOU','BRAND_ID','BRAND_NAME','POS_ID','POS_NAME','COMMON_ID','COMMON_NAME','ORIGINALCODE','CHANGFANGJIA','YUANCHANGJIA','PINPAIJIA'])
        df.to_csv('yzt/yzt.csv', index=None, encoding='utf-8')
        self.logger.info(('yzt process done!'))

    def bg_process(self):
        commit = '''select jigou_id, jigou, b.brand_name, b.brand_id,pos_id,pos_name,common_id,common_name,originalcode, fs_price, yc_price, pp_price, dc
      from (select distinct jigou_id,
                            jigou,
                            brand_id,
                            originalcode,
                            pos_id,pos_name,common_id,common_name,
                            fs_price,
                            yc_price,
                            pp_price,
                            dc
              from lb_bg_data_part_compare) a
      left join (select distinct brand_id, brand_name
                   from LB_DADI_STANDARD_PRODUCT_CARS) b
        on a.brand_id = b.brand_id
     where brand_name is not null'''
        df = self.oracle.getData(commit, self.account)
        df.replace(0, np.nan, inplace=True)

        def get_price(data):
            lst = []
            changfangjias = data['FS_PRICE'].tolist()
            yuanchangjias = data['YC_PRICE'].tolist()
            pinpaijias = data['PP_PRICE'].tolist()
            dcs = data['DC'].tolist()
            ind = dcs.index(max(dcs))
            changfangjia = changfangjias[ind]
            yuanchangjia = yuanchangjias[ind]
            pinpaijia = pinpaijias[ind]
            lst.append(changfangjia)
            lst.append(yuanchangjia)
            lst.append(pinpaijia)
            return lst

        df1 = df.groupby(['JIGOU', 'JIGOU_ID', 'BRAND_NAME', 'BRAND_ID', 'ORIGINALCODE'])[
            'FS_PRICE', 'YC_PRICE', 'PP_PRICE', 'DC'].apply(get_price).apply(pd.Series).reset_index()
        df1.rename(columns={0: 'CHANGFANGJIA', 1: 'YUANCHANGJIA', 2: 'PINPAIJIA'}, inplace=True)
        # df.dropna(subset=['CHANGFANGJIA'], how='any', axis=0, inplace=True)
        df = pd.merge(df1, df, how='left', on=['JIGOU', 'JIGOU_ID', 'BRAND_NAME', 'BRAND_ID', 'ORIGINALCODE'])
        df.drop_duplicates(subset=['JIGOU', 'JIGOU_ID', 'BRAND_NAME', 'BRAND_ID', 'ORIGINALCODE'], keep='first',
                           inplace=True)
        df = pd.DataFrame(df, columns=['JIGOU_ID', 'JIGOU', 'BRAND_ID', 'BRAND_NAME', 'POS_ID', 'POS_NAME', 'COMMON_ID',
                                       'COMMON_NAME', 'ORIGINALCODE', 'CHANGFANGJIA', 'YUANCHANGJIA', 'PINPAIJIA'])
        df.to_csv('baogong/baogong.csv', index=None, encoding='utf-8')
        self.logger.info(('baogong process done!'))

    def left_right(self):
        # data_left = pd.read_excel(r'D:\1-工作资料\6-2大地自建库三期\7-配件模块\6-左右配件\bendi_left.xlsx')
        # data_right = pd.read_excel(r'D:\1-工作资料\6-2大地自建库三期\7-配件模块\6-左右配件\bendi_right.xlsx')

        commit1 = '''select distinct BRAND_NAME,COMMON_NAME,ORIGINALCODE,length(ORIGINALCODE)  from LB_PEIJIAN_ORIGINAL_DATA_LOAD t
        where COMMON_NAME like '%左%' and length(ORIGINALCODE)>=6 '''

        commit2 = '''select distinct BRAND_NAME,COMMON_NAME,ORIGINALCODE,length(ORIGINALCODE)  from LB_PEIJIAN_ORIGINAL_DATA_LOAD t
        where COMMON_NAME like '%右%' and length(ORIGINALCODE)>=6 '''

        data_left = self.oracle.getData(commit1, self.account)
        data_right = self.oracle.getData(commit2, self.account)

        def extract_r(x):
            con = str(x).split('（')[-1].split('）')[0]
            return con

        def extract_l(x):
            con = str(x).split('（')[0]
            return con

        data_left['L_R'] = data_left['COMMON_NAME'].apply(extract_r)
        data_left['PEIJIAN'] = data_left['COMMON_NAME'].apply(extract_l)
        data_right['L_R'] = data_right['COMMON_NAME'].apply(extract_r)
        data_right['PEIJIAN'] = data_right['COMMON_NAME'].apply(extract_l)
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
        data1_4.to_csv(r'data/左右-418-1.csv', index=None, encoding='utf-8')
        self.logger.info(('left to right done!'))

    def to_oracle(self):
        path1 = 'outliers/品牌价原厂价价格异常值.csv'
        path2 = 'changfang/4S店价.csv'
        path3 = 'pp_yc/配件品牌价原厂价统计数据及模型预测.csv'
        path4 = 'yzt/yzt.csv'
        path5 = 'baogong/baogong.csv'
        path6 = 'pp_yc/常用配件模型预测值验证.csv'

        df1 = pd.read_csv(path1)
        df2 = pd.read_csv(open(path2, encoding='utf-8'))
        df3 = pd.read_csv(open(path3, encoding='utf-8'))
        df4 = pd.read_csv(open(path4, encoding='utf-8'))
        df4['METHOD']=2
        df5 = pd.read_csv(open(path5, encoding='utf-8'))
        df5['METHOD']=3
        df6 = pd.read_csv(open(path6, encoding='utf-8'))

        # 异常数据传到数据库
        df1.fillna('', inplace=True)
        df1 = df1.astype(str)
        table_name1 = 'LB_PEIJIAN_ORIGINAL_ABNORMAL'
        self.oracle.BatchsysteminsertDataToTable(df1, table_name1, self.account)

        # 4S店价数据处理
        list1 = ['count', 'mean', 'median', 'mode']
        for i in list1:
            df2[i] = ''
        df2 = pd.DataFrame(df2,columns=['JIGOU', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID', 'POS_ID', 'POS_NAME',
                                    'ORIGINALCODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'count', 'mean', 'median',
                                    'mode', 'REFERENCE'])
        # ddd=df1.pivot_table(index=['区域','VEHSERINAME_TYPE','IS4S','工时组','项目名称'],columns=['车系档次'])

        # 4S店价品牌价原厂价数据处理并拼接
        df3.rename(columns={'CHGCOMPSET': 'PRICE_TYPE', '参考值': 'REFERENCE'}, inplace=True)
        df3['PRICE_TYPE'] = df3['PRICE_TYPE'].map(id2chgompset)
        df3 = pd.DataFrame(df3,
                           columns=['JIGOU', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID', 'POS_ID', 'POS_NAME',
                                    'ORIGINALCODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'count', 'mean', 'median',
                                    'mode', 'REFERENCE'])
        df23 = pd.concat([df2, df3], axis=0)
        df23['METHOD'] = 1

        # 一账通数据、报供数据进行合并
        df45 = pd.concat([df4, df5], axis=0)
        df45.dropna(subset=['BRAND_NAME'], how='any', axis=0, inplace=True)
        df45.rename(columns={'CHANGFANGJIA': '4S店价','YUANCHANGJIA':'原厂价','PINPAIJIA':'品牌价'}, inplace=True)
        df45=df45.set_index(['JIGOU_ID','JIGOU','BRAND_ID','BRAND_NAME','POS_ID','POS_NAME','COMMON_ID','COMMON_NAME','ORIGINALCODE','METHOD']).stack().reset_index()
        df45.rename(columns={'level_10': 'PRICE_TYPE',0:'REFERENCE'}, inplace=True)
        df45.dropna(subset=['REFERENCE'], how='any', axis=0, inplace=True)
        df45 = pd.DataFrame(df45,columns=['JIGOU', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID', 'POS_ID', 'POS_NAME',
                                    'ORIGINALCODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'count', 'mean', 'median',
                                    'mode', 'REFERENCE','METHOD'])

        df2345=pd.concat([df23,df45],axis=0)
        df2345['JIGOU_ID']=df2345['JIGOU'].map(region2code)
        df2345.drop_duplicates(subset=['JIGOU_ID','BRAND_NAME', 'ORIGINALCODE', 'PRICE_TYPE'], keep='first', inplace=True)
        df2345=pd.DataFrame(df2345,columns=['JIGOU','JIGOU_ID','BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID', 'POS_ID', 'POS_NAME',
                                    'ORIGINALCODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'count', 'mean', 'median',
                                    'mode', 'REFERENCE','METHOD'])

        # 模型预测数据处理
        df6.rename(columns={'CHGCOMPSET': 'PRICE_TYPE'}, inplace=True)
        df6['PRICE_TYPE'] = df6['PRICE_TYPE'].map(id2chgompset)
        df6['JIGOU_ID'] = df6['JIGOU'].map(region2code)
        df6 = pd.DataFrame(df6,columns=['JIGOU','JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME', 'COMMON_ID', 'POS_ID', 'POS_NAME',
                                    'ORIGINALCODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'count', 'mean', 'median',
                                    'mode', 'REFERENCE'])
        df6['METHOD'] = 4
        # 数据合并
        df23456 = pd.concat([df2345, df6], axis=0)
        df23456.drop_duplicates(subset=['JIGOU', 'BRAND_NAME', 'ORIGINALCODE', 'PRICE_TYPE'], keep='first', inplace=True)
        df23456.dropna(subset=['ORIGINALCODE'], how='any', axis=0, inplace=True)

        df23456['count'].replace('',0,inplace=True)
        df23456['count'].replace(np.nan, 0, inplace=True)

        df23456_bf=df23456[df23456['PRICE_TYPE']!='4S店价']
        df23456=df23456[df23456['PRICE_TYPE']=='4S店价']
        # 配件左右对齐
        path7 = 'data/左右-418-1.csv'
        df7 = pd.read_csv(open(path7, encoding='utf-8'))
        df7_map = df7[['ORIGINALCODE_x', 'ORIGINALCODE_y']]
        df7_map.dropna(subset=['ORIGINALCODE_x', 'ORIGINALCODE_y'], how='any', axis=0, inplace=True)
        map_dict = dict(zip(df7_map['ORIGINALCODE_x'], df7_map['ORIGINALCODE_y']))

        def map_process(data):
            codes = data['ORIGINALCODE'].tolist()
            counts = data['count'].tolist()
            references = data['REFERENCE'].tolist()
            for code_left in codes:
                if code_left in map_dict.keys():
                    code_right = map_dict[code_left]
                    if code_right in codes:
                        ind_left = codes.index(code_left)
                        ind_right = codes.index(code_right)
                        count_left = counts[ind_left]
                        count_right = counts[ind_right]
                        reference_left=references[ind_left]
                        reference_right=references[ind_right]
                        if count_left > count_right:
                            references[ind_right] = references[ind_left]
                        elif count_left < count_right:
                            references[ind_left] = references[ind_right]
                        else:
                            if reference_left>reference_right:
                                references[ind_left] = references[ind_right]
                            else:
                                references[ind_right]=references[ind_left]

            return codes, counts, references

        fenzu_sta = df23456_bf.groupby(['JIGOU', 'BRAND_ID', 'PRICE_TYPE'])['ORIGINALCODE', 'count', 'REFERENCE'].apply(
            map_process).apply(pd.Series).reset_index()
        fenzu_sta.rename(columns={0: 'ORIGINALCODE', 1: 'count', 2: 'REFERENCE'}, inplace=True)

        fenzu_sta1 = fenzu_sta.set_index(['JIGOU', 'BRAND_ID', 'PRICE_TYPE'])['ORIGINALCODE'].apply(
            pd.Series).stack().reset_index()
        fenzu_sta1.drop(['level_3'], axis=1, inplace=True)
        fenzu_sta1.rename(columns={0: 'ORIGINALCODE'}, inplace=True)

        fenzu_sta2 = fenzu_sta.set_index(['JIGOU', 'BRAND_ID', 'PRICE_TYPE'])['count'].apply(
            pd.Series).stack().reset_index()
        fenzu_sta2.drop(['JIGOU', 'BRAND_ID', 'level_3', 'PRICE_TYPE'], axis=1, inplace=True)
        fenzu_sta2.rename(columns={0: 'count'}, inplace=True)

        fenzu_sta3 = fenzu_sta.set_index(['JIGOU', 'BRAND_ID', 'PRICE_TYPE'])['REFERENCE'].apply(
            pd.Series).stack().reset_index()
        fenzu_sta3.drop(['JIGOU', 'BRAND_ID', 'level_3', 'PRICE_TYPE'], axis=1, inplace=True)
        fenzu_sta3.rename(columns={0: 'REFERENCE'}, inplace=True)
        fenzu_sta = pd.concat([fenzu_sta1, fenzu_sta2, fenzu_sta3], axis=1)

        df23456_bf.drop(['REFERENCE'], axis=1, inplace=True)
        df23456_bf = pd.merge(df23456_bf, fenzu_sta, on=['JIGOU', 'BRAND_ID', 'ORIGINALCODE', 'count', 'PRICE_TYPE'], how='left')
        df23456_bf.drop_duplicates(subset=['JIGOU', 'BRAND_NAME', 'ORIGINALCODE', 'PRICE_TYPE'], keep='first', inplace=True)

        df23456=pd.concat([df23456,df23456_bf],axis=0,sort=False)
        # 配件数据上传到数据库
        now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        df23456['INSERT_TIME'] = now_time
        df23456.rename(columns={'mode': 'mode1'}, inplace=True)
        df23456['ID'] = [k for k in range(1, len(df23456) + 1)]
        # 从LB_DATA_AUX_1表中读取数据
        # commit1='''select t.JIGOU_ID,t.BRAND_ID,t.ORIGINALCODE,t.PART_ID from DATA_PART_ALL t '''
        commit1 = '''select t.JIGOU_ID,t.BRAND_ID,t.ORIGINALCODE,t.PART_ID from LB_DATA_AUX_1 t '''
        DATA_PART_ALL = self.oracle.getData(commit1, self.account)
        DATA_PART_ALL.drop_duplicates(subset=['PART_ID'],keep='first',inplace=True)
        df23456 = pd.merge(df23456, DATA_PART_ALL, on=['JIGOU_ID', 'BRAND_ID', 'ORIGINALCODE'], how='left')
        df23456['STATUS'] = 1

        # num = df234.groupby(['JIGOU_ID', 'BRAND_ID','ORIGINALCODE']).count().reset_index()
        # num['PART_ID'] = [id for id in range(10000000, 10000000 + len(num))]
        # num = pd.DataFrame(num, columns=['JIGOU_ID', 'BRAND_ID', 'ORIGINALCODE', 'PART_ID'])
        #
        # df234 = pd.merge(df234, num, on=['JIGOU_ID', 'BRAND_ID', 'ORIGINALCODE'], how='left')
        df23456 = pd.DataFrame(df23456,
                             columns=['ID', 'PART_ID', 'JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME',
                                      'COMMON_ID',
                                      'POS_ID', 'POS_NAME', 'ORIGINALCODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'count',
                                      'mean', 'median', 'mode1',
                                      'REFERENCE', 'METHOD', 'STATUS', 'Update_Person', 'INSERT_TIME', 'LAST_TIME'])
        with open('changfang/peijian2standardcode.json', encoding='utf-8') as f1:
            peijian2standardcode = json.load(f1)
        df23456['STANDARD_PART_CODE'] = df23456['COMMON_NAME'].map(peijian2standardcode)
        df23456['STANDARD_PART_CODE'].fillna('999999', inplace=True)
        df23456['count'].replace(0,'', inplace=True)
        df23456.fillna('', inplace=True)
        df23456 = df23456.astype(str)
        table_name2 = 'LB_PEIJIAN_SYSTEM'
        self.oracle.BatchpeijianinsertDataToTable(df23456, table_name2, self.account)
        #从表LB_PEIJIAN_SYSTEM取part_id为空的数据，关联融合表，并更新LB_PEIJIAN_SYSTEM的空的part_id
        com1='''TRUNCATE TABLE LB_DATA_AUX11'''
        com2='''insert into LB_DATA_AUX11(part_id,JIGOU_ID,BRAND_ID,ORIGINALCODE)
select lb_data_aux1_id.nextval,a.* from 
(select  jigou_id,brand_id,originalcode from  LB_PEIJIAN_SYSTEM where part_id is null and jigou is not null
group by jigou_id,brand_id,originalcode)a'''
        com3='''insert into lb_data_aux_1 
select * from lb_data_aux11'''
        com4='''truncate table LB_DATA_AUX3'''
        com5='''insert into LB_DATA_AUX3(part_id,JIGOU_ID,BRAND_ID,MODEL_ID,ORIGINALCODE)
select a.part_id,a.JIGOU_ID,a.BRAND_ID,MODEL_ID,a.ORIGINALCODE  
from lb_data_aux11 a left join (select distinct brand_id, originalcode, model_id from LB_DATA_PART_COMPARE)b
on a.brand_id=b.brand_id and a.originalcode=b.originalcode'''
        sql_all1 = [com1, com2, com3, com4, com5]
        for com in sql_all1:
            self.oracle.executeCommitSubmit(com, self.account)
        df23456.drop('PART_ID',axis=1,inplace=True)
        com6='''select * from lb_data_aux_1'''
        lb_data_aux=self.oracle.getData(com6,self.account)
        lb_data_aux=lb_data_aux.astype(str)
        df23456=pd.merge(df23456,lb_data_aux,on=['JIGOU_ID', 'BRAND_ID', 'ORIGINALCODE'],how='left')
        df23456=pd.DataFrame(df23456,columns=['ID', 'PART_ID', 'JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME',
                                      'COMMON_ID','POS_ID', 'POS_NAME', 'ORIGINALCODE', 'STANDARD_PART_CODE', 'PRICE_TYPE', 'count',
                                      'mean', 'median', 'mode1',
                                      'REFERENCE', 'METHOD', 'STATUS', 'Update_Person', 'INSERT_TIME', 'LAST_TIME'])
        com6_1='''truncate table {}'''.format(table_name2)
        self.oracle.executeCommitSubmit(com6_1, self.account)
        self.oracle.BatchpeijianinsertDataToTable(df23456, table_name2, self.account)
        df23456.drop(df23456.index, inplace=True)
        self.logger.info(('LB_PEIJIAN_SYSTEM DONE!'))

        com7='''truncate table LB_DATA_AUX22'''
        com8='''INSERT INTO LB_DATA_AUX22
  SELECT LB_DATA_AUX2_id.NEXTVAL, c.*
    from (select JIGOU_ID, MODEL_ID, ORIGINALCODE
            from LB_DATA_AUX3 m
           where model_id is not null
             and not exists
           (select 1
                    from LB_DATA_AUX_2 n
                   where m.JIGOU_ID = n.JIGOU_ID
                     and m.MODEL_ID = n.MODEL_ID
                     and m.ORIGINALCODE = n.ORIGINALCODE)
           group by JIGOU_ID, MODEL_ID, ORIGINALCODE) c'''
        com9='''insert into lb_data_aux_2 
select *  from lb_data_aux22'''
        sql_all2=[com7,com8,com9]
        for com in sql_all2:
            self.oracle.executeCommitSubmit(com, self.account)

        com10_1='''select * from LB_DATA_AUX3'''
        com10_2='''select * from LB_DATA_AUX22'''
        LB_DATA_AUX3=self.oracle.getData(com10_1,self.account)
        LB_DATA_AUX3.drop('MODEL_PART_ID',axis=1,inplace=True)
        LB_DATA_AUX22=self.oracle.getData(com10_2,self.account)
        LB_DATA_AUX3=pd.merge(LB_DATA_AUX3,LB_DATA_AUX22,on=['JIGOU_ID','MODEL_ID','ORIGINALCODE'],how='left')
        LB_DATA_AUX3=pd.DataFrame(LB_DATA_AUX3,columns=['JIGOU_ID','BRAND_ID','PART_ID','MODEL_ID','MODEL_PART_ID','ORIGINALCODE'])
        com10_3='''truncate table LB_DATA_AUX3'''
        self.oracle.executeCommitSubmit(com10_3, self.account)
        LB_DATA_AUX3=LB_DATA_AUX3.astype(str)
        LB_DATA_AUX3.replace('nan','', inplace=True)
        self.oracle.BatchinsertDataToTable(LB_DATA_AUX3, "LB_DATA_AUX3",self.account)

        com11='''insert into LB_DATA_MODEL_PART
select LB_DATA_MODEL_PART_id.Nextval,a.*,'0' sjy_number ,sysdate insert_time 
from LB_DATA_AUX3 a'''
        com12='''insert into LB_ALL_MODEL_PART 
select LB_ALL_MODEL_PART_id.Nextval,a.*,sysdate insert_time  from LB_DATA_AUX3 a'''
        com13='''insert into LB_APMS_MODEL_PARTS_bf
select LB_APMS_MODEL_PARTS_bf_id.nextval, C.*, sysdate INSERT_TIME 
from(select jigou_id, part_id, model_id, model_part_id from LB_DATA_AUX3)C'''
        sql_all3=[com11,com12,com13]
        for com in sql_all3:
            self.oracle.executeCommitSubmit(com, self.account)
        self.logger.info(('Other Table Commit Done!'))


    def inter2oracle(self):
        tableName = 'LB_APMS_PARTS_BF'
        connection = cx_Oracle.connect(self.account)
        # 每次从表LB_PEIJIAN_SYSTEM读取一个机构数据(生成数据插入表LB_APMS_PARTS_LS)
        with open('pp_yc/jigou2id.json', encoding='utf-8') as f2:
            jigou_ls = json.load(f2)
        df_all = pd.DataFrame()
        for jigou in jigou_ls:
            commit1 = '''select * from LB_PEIJIAN_SYSTEM t where t.JIGOU='{}' '''.format(jigou)
            try:
                cursor = connection.cursor()
                LB_APMS_PARTS = self.oracle.getDatapart(commit1, cursor)
                LB_APMS_PARTS.rename(columns={'METHOD': 'CREATE_WAY'}, inplace=True)
                # 以4个字段为基准，把4S店价，品牌价和原厂价的价格左连接拼接
                LB_APMS_PARTS_BASE = LB_APMS_PARTS[
                    ['PART_ID', 'JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'ORIGINALCODE']]
                LB_APMS_PARTS_BASE.dropna(subset=['PART_ID'], how='any', axis=0, inplace=True)
                LB_APMS_PARTS_BASE = LB_APMS_PARTS_BASE.drop_duplicates()

                datas1 = LB_APMS_PARTS.loc[LB_APMS_PARTS['PRICE_TYPE'] == '4S店价']
                datas1 = pd.DataFrame(datas1,columns=['PART_ID', 'JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'COMMON_NAME',
                                               'COMMON_ID',
                                               'POS_ID', 'POS_NAME', 'ORIGINALCODE', 'STANDARD_PART_CODE', 'STATUS',
                                               'INSERT_TIME', 'REFERENCE', 'CREATE_WAY'])
                datas1.rename(columns={'REFERENCE': 'FACTORY_PRICE'}, inplace=True)
                datas1.dropna(subset=['PART_ID'], how='any', axis=0, inplace=True)

                datas2 = LB_APMS_PARTS.loc[LB_APMS_PARTS['PRICE_TYPE'] == '品牌价']
                datas2 = pd.DataFrame(datas2, columns=['PART_ID', 'ORIGINALCODE', 'REFERENCE','CREATE_WAY','COUNT'])
                datas2.rename(columns={'REFERENCE': 'BRAND_PRICE', 'COUNT': 'COUNT1','CREATE_WAY':'CREATE_WAY1'}, inplace=True)
                datas2.dropna(subset=['PART_ID'], how='any', axis=0, inplace=True)

                datas3 = LB_APMS_PARTS.loc[LB_APMS_PARTS['PRICE_TYPE'] == '原厂价']
                datas3 = pd.DataFrame(datas3, columns=['PART_ID', 'ORIGINALCODE', 'REFERENCE','CREATE_WAY', 'COUNT'])
                datas3.rename(columns={'REFERENCE': 'ORIGIN_PRICE', 'COUNT': 'COUNT2','CREATE_WAY':'CREATE_WAY2'}, inplace=True)
                datas3.dropna(subset=['PART_ID'], how='any', axis=0, inplace=True)

                datasB1 = pd.merge(LB_APMS_PARTS_BASE, datas1,
                                   on=['PART_ID', 'JIGOU', 'JIGOU_ID', 'BRAND_ID', 'BRAND_NAME', 'ORIGINALCODE'],
                                   how='left')
                datasB12 = pd.merge(datasB1, datas2, on=['PART_ID', 'ORIGINALCODE'], how='left')
                datasB12['CREATE_WAY'].fillna(datasB12['CREATE_WAY1'],inplace=True)
                datasB123 = pd.merge(datasB12, datas3, on=['PART_ID', 'ORIGINALCODE'], how='left')
                datasB123['CREATE_WAY'].fillna(datasB123['CREATE_WAY2'], inplace=True)
                datasB123.drop_duplicates(subset=['PART_ID'], keep='first', inplace=True)


                datasB123['COUNT1'].replace(np.nan,0,inplace=True)
                datasB123['COUNT2'].replace(np.nan,0,inplace=True)
                # datasB123.fillna('',inplace=True)
                datasB123['FREQUENCY'] = datasB123.apply(lambda x: count_transform(x['COUNT1'], x['COUNT2']), axis=1)
                datasB123['FREQUENCY'].replace(0,'',inplace=True)

                datasB123=pd.merge(datasB123,LB_APMS_PARTS,on=['PART_ID','JIGOU','JIGOU_ID','BRAND_ID','BRAND_NAME','ORIGINALCODE'],how='left')
                datasB123.drop_duplicates(subset=['PART_ID'], keep='first', inplace=True)
                #空缺字段填充
                datasB123['POS_ID_x'].fillna(datasB123['POS_ID_y'],inplace=True)
                datasB123['POS_NAME_x'].fillna(datasB123['POS_NAME_y'],inplace=True)
                datasB123['COMMON_ID_x'].fillna(datasB123['COMMON_ID_y'],inplace=True)
                datasB123['COMMON_NAME_x'].fillna(datasB123['COMMON_NAME_y'],inplace=True)

                datasB123.rename(columns={'ORIGINALCODE': 'ORIGIN_CODE','POS_ID_x': 'POS_ID','POS_NAME_x': 'POS_NAME',
                                          'COMMON_ID_x': 'COMMON_ID','COMMON_NAME_x': 'COMMON_NAME','CREATE_WAY_x': 'CREATE_WAY','STANDARD_PART_CODE_x': 'STANDARD_PART_CODE'}, inplace=True)
                datasB123['STATUS']=1
                datasB123['COMMON_ALIAS'] = ''
                datasB123['SHIYONG_PRICE'] = ''
                datasB123['LAST_TIME'] = ''
                # datasB123=datasB123.astype(str)
                datasB123 = pd.DataFrame(datasB123,
                                         columns=['PART_ID', 'JIGOU_ID', 'JIGOU', 'BRAND_ID', 'BRAND_NAME', 'POS_ID',
                                                  'POS_NAME', 'COMMON_ID',
                                                  'ORIGIN_CODE', 'COMMON_NAME', 'COMMON_ALIAS', 'CREATE_WAY',
                                                  'FACTORY_PRICE', 'ORIGIN_PRICE', 'BRAND_PRICE', 'SHIYONG_PRICE',
                                                  'STANDARD_PART_CODE',
                                                  'FREQUENCY', 'STATUS', 'INSERT_TIME', 'LAST_TIME'])
                df_all = pd.concat([df_all, datasB123], axis=0)
            except Exception as e:
                print(e)

        # path = 'yzt/yzt.csv'
        # yzt = pd.read_csv(open(path, encoding='utf-8'))
        # yzt.rename(columns={'MODEL_BRAND': 'BRAND_NAME', 'ORIGINALFITS_CODE': 'ORIGIN_CODE'}, inplace=True)
        # # yzt['JIGOU_ID'] = yzt['JIGOU_ID'].astype(str)
        # df_all = pd.merge(df_all, yzt, how='left', on=['JIGOU_ID', 'BRAND_NAME', 'ORIGIN_CODE'])
        # df_all.replace('', np.nan, inplace=True)
        # df_all["FACTORY_PRICE"].fillna(df_all["CHANGFANGJIA"], inplace=True)
        # df_all["ORIGIN_PRICE"].fillna(df_all["YUANCHANGJIA"], inplace=True)
        # df_all["BRAND_PRICE"].fillna(df_all["PINPAIJIA"], inplace=True)
        # # print(df_all.info())
        # df_all.drop(['CHANGFANGJIA', 'YUANCHANGJIA', 'PINPAIJIA'], axis=1, inplace=True)
        # with open('changfang/peijian2standardcode.json', encoding='utf-8') as f1:
        #     peijian2standardcode = json.load(f1)
        # df_all['STANDARD_PART_CODE'] = df_all['COMMON_NAME'].map(peijian2standardcode)
        # df_all['STANDARD_PART_CODE'].fillna('999999', inplace=True)
        now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        df_all['INSERT_TIME'] = now_time
        df_all = df_all.astype(str)
        df_all.replace('nan', '', inplace=True)
        self.oracle.Batchpeijian13insertDataToTable(df_all, tableName, self.account)
        self.logger.info(('all done!'))


if __name__=='__main__':
    handle=Process("dd_data2", "xdf123", "LBORA", "10.9.1.169")
    # handle.changfang_price()
    handle.left_right()