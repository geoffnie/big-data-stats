# -*- coding: utf-8 -*-
"""
Created on Thu Apr 22 15:00:59 2021

@author: jiuxin
"""

#-*- coding: utf-8 -*-
import os
import pymysql
import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine
import gc
import time 

#定义数据库连接
engine = create_engine("mysql+pymysql://{}:{}@{}/{}".format('root', '123456', '192.168.52.100:3306', 'tmp'))    
con = engine.connect()#创建连接

#获取维度表
df_gw_park = pd.read_csv(r"./网关对应园区.csv").replace(np.nan, '', regex=True)

#获取所有文件路径及文件夹
for root, dirs, files in os.walk(r"./"):
#    print('root_dir:', root)
#    print('sub_dirs:', dirs)
    print('files:', files)
    file_list = files
    print("==============")
    print(len(file_list))

#获取本地当前文件夹下所有包含’。csv‘及’druid_data_‘的文件名称
all_csv_origin_file = [ x for x in file_list if '.csv' in x and 'druid_data_' in x ].sort()

#对列表文件名称排序
all_csv_origin_file.sort()

time_start = time.time()

#对每个文件与维度表进行关联，并且统计数据插入mysql表中，同时释放变量内存
for file in all_csv_origin_file:
    #获取事实表
    df_data = pd.read_csv(r"./{0}".format(file)).replace(np.nan, '', regex=True)
    keyslist = ['gw_id']
    #合并关联数据
    df_imerge = pd.merge(df_data, df_gw_park,  how='left', on = keyslist, left_index=False, right_index=False, suffixes=('', '_y'))
    #获取分组统计数据
    df_groupby = df_imerge.groupby(by=['park']).count()[['pd_motr_pnt_id']].rename(columns={'pd_motr_pnt_id':'count_num'},inplace=False)
    #定义新增列数据
    df_groupby['date_time'] = file.split('.')[0].split('_data_')[1]
    #插入数据到数据库
    df_groupby.to_sql(name='druid_1week_data', con=con, if_exists='append', index=True)  
    print("=============: "+ file) 
    #释放变量内存
    del df_data, df_imerge, df_groupby
    gc.collect()
time_end = time.time() - time_start
print(time_end)   





