# 大地配件价格智能模型
从数据库里抽取数据集，预测全国每个区域车型的配件价格

# 项目介绍
为了降低工时费在车险理赔中的不确定因素性，完善理赔数据，为业务发展和KPI考核方案制定带来保障，智能配件标准化项目的实施刻不容缓。


# 项目运行方式
代码模块化
1、配置要求：要求配置cx_Oracle数据库及相应的python依赖包。cx_Oracle数据库用户名密码记住。
2、从数据库里读取数据：data_from_oracle.py
3、异常值处理：outlier_processing.py
4、数据清洗及转化：data_transform.py
5、训练模型：get_model.py
6、推断，提供接口：infer.py
7、数据统计及模型预测结果:statistics_predict.py

#部署到服务器运行方式：
#cd到项目路径下
1、cd /home/appuser/lihui/DADI_PEIJIAN_PRICE
#后台运行
2、nohup python all_start_run.py >> /home/appuser/lihui/DADI_PEIJIAN_PRICE/data/my.log 2>&1 &
关联到的文件有 ForCall01.py，sql_oracle.py，dadi_loader.py


#接口
提供接口：nohup python interface.py >> /home/appuser/lihui/DADI_PEIJIAN_PRICE/data/my.log 2>&1 &
客户端测试：client.py



# 总结
1、本项目的数据来源：大地保险公司全国36个机构定损过的历史数据，以cx_Oracle作为存储，从数据库里提取数据集，以预测全国每个区域车型的配件价格
2、数据量：取6个月的数据，大概100万数据
3、本项目以业务驱动，经过异常数据处理，数据清洗，过滤，得到高质量数据，特征工程化后建模XGBoost回归模型
4、模型训练过程中遇到的难题：80%的样本数都是1条，按照常规训练效果不理想；处理过程：做了数据增强，因为模型在叶子节点由于样本数过低就不会细分
5、模型以均方根误差rmse作为评价指标，rmse值为0.068.
6、以统计模型统计每一个区域的配件价格，包括样本数，均值，中位数，众数等，并对这些统计数据做XGboost预测；
7、生成整套模型预测数据。
8、数据验证：模型预测值与均值之间的偏差比例在一定范围（-20%与20%）之间认为是准确的
9、数据填充：对于大地生产数据未出现的配件价格用一账通的数据及报供的数据填充
10、左右配件对齐：按原厂编码字段做左右配件价格一样处理，原厂编码长度一样且其中有一位不一样，判定为左右配件，最终的价格取值为哪个配件的样本多，就取哪个价格；
11、配件的数据整理，上传到oracle数据库接口表



