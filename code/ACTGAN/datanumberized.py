# -*- coding: utf-8 -*-
"""
Created on Fri Jul 27 15:08:20 2018
numberied
@author: Founder
"""

import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
#import random

# Hyper Parameters
start = 0
BATCH_SIZE = 27        # 
NumOfF = 14  

#outp = [[0] * 14] * 90

#文件读取
file_read = 'F:/678/data/Tomcat-outdou.csv'

#spark读取数据
def sparkdata(file):
    data = pd.read_csv(file, engine = 'python')
    data = data.sort_values(by='time')  
    data = data.drop('time', 1)
      
    d1 = data['spark.reducer.maxSizeInFlight']
    d2 = data['spark.shuffle.file.buffer']
    data = data.drop('spark.reducer.maxSizeInFlight', 1)
    data = data.drop('spark.shuffle.file.buffer', 1)
    
    char = ['spark.shuffle.compress', 'spark.shuffle.spill.compress', 'spark.broadcast.compress',
            'spark.rdd.compress', 'spark.io.compression.codec']
    for c in char:
        data = pd.concat([data, pd.get_dummies(data[c], prefix=c)], 1)
        data = data.drop(c, 1)
    
    data['spark.reducer.maxSizeInFlight'] = d1
    data['spark.shuffle.file.buffer'] = d2
    data = pd.concat([data, pd.get_dummies(data['spark.serializer'], prefix=c)], 1)
    data = data.drop('spark.serializer', 1)
    
    return data
    

def kafkadata(file):
    data = pd.read_csv(file, engine = 'python')

    data = data.drop('L', 1)
    
    data = data.sort_values(by='T', ascending=False)  
    data = data.drop('T', 1)
    
    data = pd.concat([data, pd.get_dummies(data['compression.type'], prefix='compression.type')], 1)
    data = data.drop('compression.type', 1)
    
    return data


def kafkadataL(file):
    data = pd.read_csv(file, engine = 'python')

    data = data.drop('T', 1)
    
    data = data.sort_values(by='L')  
    data = data.drop('L', 1)
    
    data = pd.concat([data, pd.get_dummies(data['compression.type'], prefix='compression.type')], 1)
    data = data.drop('compression.type', 1)
    
    return data


def hivedata(file):
    data = pd.read_csv(file, engine = 'python')
    data = data.sort_values(by='time')  
    data = data.drop('time', 1)    
    
    char = ['hive.stats.fetch.partition.stats', 'hive.optimize.index.autoupdate', 
            'hive.merge.mapfiles', 'hive.exec.parallel']
    for c in char:
        data = pd.concat([data, pd.get_dummies(data[c], prefix=c)], 1)
        data = data.drop(c, 1)

    return data


def redisdata(file):
    data = pd.read_csv(file, engine = 'python')
    data = data.sort_values(by='SET:requests per second', ascending=False)  
    data = data.drop('SET:requests per second', 1)    

    data = pd.concat([data, pd.get_dummies(data['repl-disable-tcp-nodelay'], prefix='repl-disable-tcp-nodelay')], 1)
    data = data.drop('repl-disable-tcp-nodelay', 1)

    return data


def cassandradata(file):
    data = pd.read_csv(file, engine = 'python')
    data = data.sort_values(by='Throughput', ascending=False)  
    data = data.drop('Throughput', 1)    
    
    char = ['compaction_preheat_key_cache', 'multithreaded_compaction', 'commitlog_sync']
    for c in char:
        data = pd.concat([data, pd.get_dummies(data[c], prefix=c)], 1)
        data = data.drop(c, 1)

    return data


def hbasedata(file):
    data = pd.read_csv(file, engine = 'python')
    data = data.sort_values(by='run time')  
    data = data.drop('run time', 1)    
    
    char = ['hfile.block.index.cacheonwrite', 'hbase.hregion.memstore.mslab.enabled']
    for c in char:
        data = pd.concat([data, pd.get_dummies(data[c], prefix=c)], 1)
        data = data.drop(c, 1)

    return data


def tomcatdata(file):
    data = pd.read_csv(file, engine='python')
    data = data.sort_values(by='Throughput', ascending=False)
    data = data.drop('Throughput', 1)

    char = ['protocol', 'enableLookups', 'disableUploadTimeout']
    for c in char:
        data = pd.concat([data, pd.get_dummies(data[c], prefix=c)], 1)
        data = data.drop(c, 1)

    # 将向量移动到原来枚举值的位置
    data.insert(3, 'protocol_org.apache.coyote.http11.Http11AprProtocol',
                data.pop('protocol_org.apache.coyote.http11.Http11AprProtocol'))
    data.insert(4, 'protocol_org.apache.coyote.http11.Http11NioProtocol',
                data.pop('protocol_org.apache.coyote.http11.Http11NioProtocol'))
    data.insert(5, 'protocol_org.apache.coyote.http11.Http11Protocol',
                data.pop('protocol_org.apache.coyote.http11.Http11Protocol'))

    data.insert(12, 'enableLookups_False', data.pop('enableLookups_False'))
    data.insert(13, 'enableLookups_True', data.pop('enableLookups_True'))

    data.insert(15, 'disableUploadTimeout_False', data.pop('disableUploadTimeout_False'))
    data.insert(16, 'disableUploadTimeout_True', data.pop('disableUploadTimeout_True'))

    return data

def mysqldata(file):
    data = pd.read_csv(file, engine = 'python')
    data = data.sort_values(by='tpmc', ascending=False)  
    data = data.drop('tpmc', 1)
    
    return data

