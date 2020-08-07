from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor
import pandas as pd
from sklearn.datasets import load_boston
from keras.layers import Dense,Dropout,Activation,Input
from keras.models import Sequential,Model
from sklearn.model_selection import train_test_split
from numpy import *
from keras import metrics
from branch_code.dadi_loader import train_model,get_time_dif,valuation,train_model1
from sklearn.externals import joblib
from data_from_oracle import *
from outlier_processing import outlier_process_train
from data_transform import pp_yc_process,yuanchang_process
from sklearn.preprocessing import StandardScaler

def load_data():
    df_train=pd.read_csv("data\\train.csv")
    df_test=pd.read_csv("data\\test.csv")
    return df_train,df_test

def make_model(InputSize):
    model=Sequential()
    model.add(Dense(units=90,activation='relu',input_shape=(InputSize,)))
    model.add(Dropout(0.1))
    model.add(Dense(units=50,activation='relu'))
    model.add(Dropout(0.1))
    model.add(Dense(units=1,activation=None))
    model.compile(loss='mean_squared_error',optimizer='adam',metrics=[metrics.mae])
    print(model.summary())
    return model

if __name__ == '__main__':
    os.environ['CUDA_VISIBLE_DEVICES'] = "0"
    start_time_model = time.time()
    df = get_data()
    df = outlier_process_train(df)
    df = pp_yc_process(df)
    y = np.log(df[args.PRICE])
    x = df.drop(args.PRICE, axis=1, inplace=False)
    # 数据的分割，
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=14)
    # 标准化
    ss = StandardScaler(with_mean=True, with_std=True)
    x_train = ss.fit_transform(x_train)
    x_test = ss.transform(x_test)
    # df_train,df_test=load_data()
    # X_train,X_test,y_train,y_test=train_test_split(df_train.drop('sell_price',axis=1).values,df_train['sell_price'].values,test_size=0.1,random_state=0)
    model=make_model(5)
    model.fit(x_train,y_train,batch_size=32,epochs=100,verbose=1,validation_data=(x_test,y_test),shuffle=True)
    pred=model.predict(x_test.values)
    submissionFile=pd.DataFrame({'price':pred.reshape(1,-1)[0]})
    submissionFile.to_csv("submission.csv",index=False)