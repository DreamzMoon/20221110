# -*- coding: UTF-8 -*-
"""
### 数据库增删改查 ###
Create time:20181202
Update time:20181212
Author     :etom_zlj
Support version ：python 2.x and 3.x
"""

import numpy as np
import pandas as pd
import pymysql
from sqlalchemy import create_engine

def create_table(config):
    """在数据库中创建表"""
    
    host = config['host']
    port = config['port']
    user = config['user']
    password = config['password']
    database = config['database']
    charset = config['charset']
    tablename = config['tablename']
    db = pymysql.connect(host=host, port=port, user=user, password=password,
                         database=database, charset=charset)
    cursor = db.cursor()
    try:
        sql = 'create table if not exists '+ tablename +'(Range_Left VARCHAR(255),Range_Right VARCHAR (255),'\
              'KZG_MGP VARCHAR(255),LMRK_P VARCHAR(255),LMRK_T VARCHAR(255),LMXF_P1 VARCHAR(255),LMXF_T1 VARCHAR(255),' \
              'ZIK121_GF VARCHAR(255),ZIK122_GF VARCHAR(255),ZIK123_GF VARCHAR(255),' \
              'E_grinder_m VARCHAR(255),E_CirculatingFan_m VARCHAR(255),E_S_m VARCHAR(255)'
        cursor.execute(sql)
        db.commit()
        db.close()
    except:
        pass

def data_insert(data,config):
    """
    # 插入数据
    :param data:        待添加的数据
    :return:
    """
    db = config['database']
    host = config['host']
    port = str(config['port'])
    username = config['user']
    password = config['password']
    tablename = config['tablename'] 

    write_mode = 'append'
    connect_url = 'mysql+pymysql://' + username + ':' + password + '@' + host + ':' + port + '/' + db + '?charset=utf8'
    mysql_connect = create_engine(connect_url)
    pd.io.sql.to_sql(data, tablename, mysql_connect, schema=db, if_exists=write_mode)

def del_data(config,condition='1=1'):
    """
    根据条件删除对应的数据
    :param condition:       删除条件
    :return:
    """
    host = config['host']
    port = config['port']
    user = config['user']
    password = config['password']
    database = config['database']
    charset = config['charset']
    tablename = config['tablename'] 

    db = pymysql.connect(host=host, port=port, user=user, password=password,
                         database=database, charset=charset)
    cursor = db.cursor()
    sql = 'delete from {table} where {condition}'.format(table=tablename, condition=condition)
    try:
        if cursor.execute(sql):
            db.commit()
    except Exception as e:
        db.rollback()
    db.close()

def update_data(condition, data,config):
    """
    根据条件更新相关数据(先删除相关条件信息，在添加相关数据)
    :param condition:   更新条件
    :param data:        待添加的数据
    :return:
    """
    del_data(condition)
    data_insert(data,config)

def query_data(config,condition='1=1'):
    """
    查询相关条件的数据
    :param condition:   查询条件
    :return:            表中相关条件的数据行数
    """
    host = config['host']
    port = config['port']
    user = config['user']
    password = config['password']
    database = config['database']
    charset = config['charset']
    tablename = config['tablename'] 

    db = pymysql.connect(host=host, port=port, user=user, password=password,
                         database=database, charset=charset)
    cursor = db.cursor()

    sql = 'select * from {table} where {condition}'.format(table=tablename, condition=condition)
    count = 0
    try:
        cursor.execute(sql)
        count = cursor.rowcount
        # row = cursor.fetchone()
        # while row:
        #     print("One:", row)
        #     row = cursor.fetchone()
        # results = cursor.fetchall()
        # print('result:', results)
        # print('result type:', type(results))
        # for row in results:
        #     print(row)
    except Exception as e:
        print('query failed!', e)
    db.close()
    return count
