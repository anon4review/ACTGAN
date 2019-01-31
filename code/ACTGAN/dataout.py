# -*- coding: utf-8 -*-
"""
Created on Sun Aug 19 11:16:30 2018

@author: Founder
"""

import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
#import random


def cassandra_out(file):
    data = pd.read_csv(file, engine='python')
    hang = data.iloc[:, 0].size

    # 对数据进行按列取小数并进行合并
    data1 = data.iloc[:, 0:5].round(0)
    data2 = data.iloc[:, 5].round(1)
    data3 = data.iloc[:, 6].round(2)
    data4 = data.iloc[:, 7:].round(0)

    data = pd.concat([data1, data2, data3, data4], axis=1)

    data = pd.concat([data, pd.DataFrame(columns=[
        'compaction_preheat_key_cache', 'multithreaded_compaction', 'commitlog_sync'])], 1)

    for index in range(hang):

        # 将向量转换成枚举值
        if data.loc[[index]].values[0][8] == 1 and data.loc[[index]].values[0][9] == 0:
            data.ix[index, ['compaction_preheat_key_cache']] = 'FALSE'
        elif data.loc[[index]].values[0][8] == 0 and data.loc[[index]].values[0][9] == 1:
            data.ix[index, ['compaction_preheat_key_cache']] = 'TRUE'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][10] == 1 and data.loc[[index]].values[0][11] == 0:
            data.ix[index, ['multithreaded_compaction']] = 'FALSE'
        elif data.loc[[index]].values[0][10] == 0 and data.loc[[index]].values[0][11] == 1:
            data.ix[index, ['multithreaded_compaction']] = 'TRUE'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][12] == 1 and data.loc[[index]].values[0][13] == 0:
            data.ix[index, ['commitlog_sync']] = 'batch'
        elif data.loc[[index]].values[0][12] == 0 and data.loc[[index]].values[0][13] == 1:
            data.ix[index, ['commitlog_sync']] = 'periodic'
        else:
            data.drop([index], inplace=True)
            continue

    # 删去枚举值所在的列
    data = data.drop(['8', '9', '10', '11', '12', '13'], 1)

    # 对列进行重命名
    data.rename(columns={'0': 'column_index_size_in_kb', '1': 'commitlog_sync_period_in_ms',
                         '2': 'compaction_throughput_mb_per_sec',
                         '3': 'in_memory_compaction_limit_in_mb', '4': 'memtable_flush_queue_size',
                         '5': 'reduce_cache_capacity_to', '6': 'reduce_cache_sizes_at',
                         '7': 'commitlog_segment_size_in_mb'}, inplace=True)
        
    return data
    
def hive_out(file):
    data = pd.read_csv(file, engine='python')
    #data = pd.DataFrame(file)
    hang = data.iloc[:, 0].size
    # lie = data.columns.size

    # 对数据进行按列取小数并进行合并
    data1 = data[['0', '1', '2', '3', '4']].round(0)
    data2 = data[['5']].round(1)
    data3 = data[['6', '7', '8', '9', '10', '11', '12', '13']].round(0)

    data = pd.concat([data1, data2, data3], axis=1)

    data = pd.concat([data, pd.DataFrame(columns=['hive.stats.fetch.partition.stats', 'hive.optimize.index.autoupdate',
                                                  'hive.merge.mapfiles', 'hive.exec.parallel'])], 1)

    for index in range(hang):
        # 将向量转换成枚举值
        if data.loc[[index]].values[0][6] == 1 and data.loc[[index]].values[0][7] == 0:
            data.ix[index, ['hive.stats.fetch.partition.stats']] = 'False'
        elif data.loc[[index]].values[0][6] == 0 and data.loc[[index]].values[0][7] == 1:
            data.ix[index, ['hive.stats.fetch.partition.stats']] = 'True'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][8] == 1 and data.loc[[index]].values[0][9] == 0:
            data.ix[index, ['hive.optimize.index.autoupdate']] = 'False'
        elif data.loc[[index]].values[0][8] == 0 and data.loc[[index]].values[0][9] == 1:
            data.ix[index, ['hive.optimize.index.autoupdate']] = 'True'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][10] == 1 and data.loc[[index]].values[0][11] == 0:
            data.ix[index, ['hive.merge.mapfiles']] = 'False'
        elif data.loc[[index]].values[0][10] == 0 and data.loc[[index]].values[0][11] == 1:
            data.ix[index, ['hive.merge.mapfiles']] = 'True'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][12] == 1 and data.loc[[index]].values[0][13] == 0:
            data.ix[index, ['hive.exec.parallel']] = 'False'
        elif data.loc[[index]].values[0][12] == 0 and data.loc[[index]].values[0][13] == 1:
            data.ix[index, ['hive.exec.parallel']] = 'True'
        else:
            data.drop([index], inplace=True)
            continue

    # 删去枚举值所在的列
    data = data.drop(['6', '7', '8', '9', '10', '11', '12', '13'], 1)

    # 对列进行重命名
    data.rename(columns={'0': 'hive.exec.reducers.bytes.per.reducer', '1': 'hive.exec.parallel.thread.number',
                         '2': 'hive.join.cache.size', '3': 'hive.merge.smallfiles.avgsize',
                         '4': 'hive.mapjoin.bucket.cache.size', '5': 'hive.map.aggr.hash.percentmemory'}, inplace=True)

    return data


def hbase_out(file):
    data = pd.read_csv(file, engine='python')
    hang = data.iloc[:, 0].size
    # lie = data.columns.size

    # 对数据进行按列取小数并进行合并
    data1 = data[['0']].round(2)
    data2 = data[['1']].round(1)
    data3 = data[['2']].round(2)
    data4 = data[['3', '4', '5', '6', '7', '8', '9', '10', '11']].round(0)

    data = pd.concat([data1, data2, data3, data4], axis=1)

    data = pd.concat([data, pd.DataFrame(columns=['hfile.block.index.cacheonwrite',
                                                  'hbase.hregion.memstore.mslab.enabled'])], 1)

    for index in range(hang):
        # 将向量转换成枚举值
        if data.loc[[index]].values[0][8] == 1 and data.loc[[index]].values[0][9] == 0:
            data.ix[index, ['hfile.block.index.cacheonwrite']] = 'False'
        elif data.loc[[index]].values[0][8] == 0 and data.loc[[index]].values[0][9] == 1:
            data.ix[index, ['hfile.block.index.cacheonwrite']] = 'True'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][10] == 1 and data.loc[[index]].values[0][11] == 0:
            data.ix[index, ['hbase.hregion.memstore.mslab.enabled']] = 'False'
        elif data.loc[[index]].values[0][10] == 0 and data.loc[[index]].values[0][11] == 1:
            data.ix[index, ['hbase.hregion.memstore.mslab.enabled']] = 'True'
        else:
            data.drop([index], inplace=True)
            continue

    # 删去枚举值所在的列
    data = data.drop(['8', '9', '10', '11'], 1)

    # 对列进行重命名
    data.rename(columns={'0': 'hfile.block.cache.size', '1': 'hbase.regionserver.global.memstore.upperLimit',
                         '2': 'hbase.regionserver.global.memstore.lowerLimit',
                         '3': 'hbase.regionserver.handler.count', '4': 'hbase.hregion.memstore.block.multiplier',
                         '5': 'hbase.hstore.compactionThreshold', '6': 'hbase.hstore.blockingStoreFiles',
                         '7': 'hbase.hregion.memstore.flush.size'}, inplace=True)

    return data


def redis_out(file):
    data = pd.read_csv(file, engine='python')
    hang = data.iloc[:, 0].size

    # 对数据进行按列取小数并进行合并
    data = data.round(0)

    data = pd.concat([data, pd.DataFrame(columns=['repl-disable-tcp-nodelay'])], 1)

    for index in range(hang):

        # 将向量转换成枚举值
        if data.loc[[index]].values[0][8] == 1 and data.loc[[index]].values[0][9] == 0:
            data.ix[index, ['repl-disable-tcp-nodelay']] = 'no'
        elif data.loc[[index]].values[0][8] == 0 and data.loc[[index]].values[0][9] == 1:
            data.ix[index, ['repl-disable-tcp-nodelay']] = 'yes'
        else:
            data.drop([index], inplace=True)
            continue


    # 删去枚举值所在的列
    data = data.drop(['8', '9'], 1)

    # 对列进行重命名
    data.rename(columns={'0': 'repl-backlog-size', '1': 'hash-max-ziplist-value',
                         '2': 'hash-max-ziplist-entries',
                         '3': 'list-max-ziplist-size', '4': 'active-defrag-ignore-bytes',
                         '5': 'active-defrag-threshold-lower', '6': 'hll-sparse-max-bytes',
                         '7': 'hz'}, inplace=True)

    # 添加单位 'mb'
    data['repl-backlog-size'] = ['% imb' % i for i in data['repl-backlog-size']]
    data['active-defrag-ignore-bytes'] = ['% imb' % i for i in data['active-defrag-ignore-bytes']]

    return data


def mysql_out(file):
    data = pd.read_csv(file, engine='python')
    hang = data.iloc[:, 0].size

    # 对数据进行按列取小数并进行合并
    data = data.round(0)

    # 对列进行重命名
    data.rename(columns={'0': 'sort_buffer_size', '1': ' join_buffer_size',
                         '2': ' innodb_autoextend_increment', '3': ' innodb_buffer_pool_size',
                         '4': ' innodb_additional_mem_pool_size',
                         '5': ' innodb_log_buffer_size', '6': ' table_open_cache',
                         '7': ' thread_cache_size', '8': ' query_cache_limit',
                         '9': ' max_allowed_packet', '10': ' max_connect_errors',
                         '11': ' tmp_table_size', '12': ' max_heap_table_size'}, inplace=True)

    '''    
    # 添加单位 'KB' 'MB'
    data['sort_buffer_size'] = ['% iKB' % i for i in data['sort_buffer_size']]
    data[' join_buffer_size'] = ['% iKB' % i for i in data[' join_buffer_size']]

    data[' innodb_autoextend_increment'] = ['% iMB' % i for i in data[' innodb_autoextend_increment']]
    data[' innodb_buffer_pool_size'] = ['% iMB' % i for i in data[' innodb_buffer_pool_size']]
    data[' innodb_additional_mem_pool_size'] = ['% iMB' % i for i in data[' innodb_additional_mem_pool_size']]
    data[' innodb_log_buffer_size'] = ['% iMB' % i for i in data[' innodb_log_buffer_size']]
    data[' query_cache_limit'] = ['% iMB' % i for i in data[' query_cache_limit']]
    data[' max_allowed_packet'] = ['% iMB' % i for i in data[' max_allowed_packet']]
    data[' tmp_table_size'] = ['% iMB' % i for i in data[' tmp_table_size']]
    data[' max_heap_table_size'] = ['% iMB' % i for i in data[' max_heap_table_size']]
    '''
    
    return data


def tomcat_out(file):
    data = pd.read_csv(file, engine='python')
    hang = data.iloc[:, 0].size

    # 对数据进行按列取小数并进行合并
    data = data.round(0)

    data = pd.concat([data, pd.DataFrame(columns=['protocol', 'enableLookups', 'disableUploadTimeout'])], 1)

    for index in range(hang):

        # 将向量转换成枚举值
        if data.loc[[index]].values[0][3] == 1 and data.loc[[index]].values[0][4] == 0 and \
                data.loc[[index]].values[0][5] == 0:
            data.ix[index, ['protocol']] = 'org.apache.coyote.http11.Http11AprProtocol'
        elif data.loc[[index]].values[0][3] == 0 and data.loc[[index]].values[0][4] == 1 and \
                data.loc[[index]].values[0][5] == 0:
            data.ix[index, ['protocol']] = 'org.apache.coyote.http11.Http11NioProtocol'
        elif data.loc[[index]].values[0][3] == 0 and data.loc[[index]].values[0][4] == 0 and \
             data.loc[[index]].values[0][5] == 1:
            data.ix[index, ['protocol']] = 'org.apache.coyote.http11.Http11Protocol'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][12] == 1 and data.loc[[index]].values[0][13] == 0:
            data.ix[index, ['enableLookups']] = 'FALSE'
        elif data.loc[[index]].values[0][12] == 0 and data.loc[[index]].values[0][13] == 1:
            data.ix[index, ['enableLookups']] = 'TRUE'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][15] == 1 and data.loc[[index]].values[0][16] == 0:
            data.ix[index, ['disableUploadTimeout']] = 'FALSE'
        elif data.loc[[index]].values[0][15] == 0 and data.loc[[index]].values[0][16] == 1:
            data.ix[index, ['disableUploadTimeout']] = 'TRUE'
        else:
            data.drop([index], inplace=True)
            continue

    # 删去枚举值所在的列
    data = data.drop(['3', '4', '5', '12', '13', '15', '16'], 1)

    # 对列进行重命名
    data.rename(columns={'0': 'XX:MaxNewSize', '1': 'Xms', '2': 'Xmx',
                         '6': 'maxHttpHeaderSize', '7': 'maxThreads', '8': 'minSpareThreads',
                         '9': 'maxSpareThreads', '10': 'minProcessors', '11': 'acceptCount',
                         '14': 'connectionTimeout', '17': 'compressionMinSize'}, inplace=True)

    # 移动最后含枚举值的几列
    data.insert(3, 'protocol', data.pop('protocol'))
    data.insert(10, 'enableLookups', data.pop('enableLookups'))
    data.insert(12, 'disableUploadTimeout', data.pop('disableUploadTimeout'))

    return data


def kafka_out(file):
    data = pd.read_csv(file, engine='python')
    hang = data.iloc[:, 0].size

    # 对数据进行按列取小数并进行合并
    data = data.round(0)

    data = pd.concat([data, pd.DataFrame(columns=['compression.type'])], 1)

    for index in range(hang):

        # 将向量转换成枚举值
        if data.loc[[index]].values[0][10] == 1 and data.loc[[index]].values[0][11] == 0 \
                and data.loc[[index]].values[0][12] == 0 and data.loc[[index]].values[0][13] == 0:
            data.ix[index, ['compression.type']] = 'gzip'

        elif data.loc[[index]].values[0][10] == 0 and data.loc[[index]].values[0][11] == 1 \
                and data.loc[[index]].values[0][12] == 0 and data.loc[[index]].values[0][13] == 0:
            data.ix[index, ['compression.type']] = 'lz4'
        elif data.loc[[index]].values[0][10] == 0 and data.loc[[index]].values[0][11] == 0 \
                and data.loc[[index]].values[0][12] == 1 and data.loc[[index]].values[0][13] == 0:
            data.ix[index, ['compression.type']] = 'none'
        elif data.loc[[index]].values[0][10] == 0 and data.loc[[index]].values[0][11] == 0 \
                and data.loc[[index]].values[0][12] == 0 and data.loc[[index]].values[0][13] == 1:
            data.ix[index, ['compression.type']] = 'snappy'
        else:
            data.drop([index], inplace=True)
            continue

    # 删去枚举值所在的列
    data = data.drop(['10', '11', '12', '13'], 1)

    # 对列进行重命名
    data.rename(columns={'0': 'num.network.threads', '1': 'num.io.threads',
                         '2': 'queued.max.requests',
                         '3': 'num.replica.fetchers', '4': 'socket.receive.buffer.bytes',
                         '5': 'socket.send.buffer.bytes', '6': 'socket.request.max.bytes',
                         '7': 'buffer.memory', '8': 'batch.size', '9': 'linger.ms'}, inplace=True)

    return data


def spark_out(file):
    data = pd.read_csv(file, engine='python')
    hang = data.iloc[:, 0].size

    # 对数据进行按列取小数并进行合并
    data.iloc[:, 0:2] = data.iloc[:, 0:2].round(0)
    data.iloc[:, 4:] = data.iloc[:, 4:].round(0)

    data = pd.concat([data, pd.DataFrame(columns=['spark.shuffle.compress', 'spark.shuffle.spill.compress',
        'spark.broadcast.compress', 'spark.rdd.compress', 'spark.io.compression.codec', 'spark.serializer'])], 1)

    for index in range(hang):

        # 将向量转换成枚举值
        if data.loc[[index]].values[0][5] == 1 and data.loc[[index]].values[0][6] == 0:
            data.ix[index, ['spark.shuffle.compress']] = 'FALSE'
        elif data.loc[[index]].values[0][5] == 0 and data.loc[[index]].values[0][6] == 1:
            data.ix[index, ['spark.shuffle.compress']] = 'TRUE'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][7] == 1 and data.loc[[index]].values[0][8] == 0:
            data.ix[index, ['spark.shuffle.spill.compress']] = 'FALSE'
        elif data.loc[[index]].values[0][7] == 0 and data.loc[[index]].values[0][8] == 1:
            data.ix[index, ['spark.shuffle.spill.compress']] = 'TRUE'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][9] == 1 and data.loc[[index]].values[0][10] == 0:
            data.ix[index, ['spark.broadcast.compress']] = 'FALSE'
        elif data.loc[[index]].values[0][9] == 0 and data.loc[[index]].values[0][10] == 1:
            data.ix[index, ['spark.broadcast.compress']] = 'TRUE'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][11] == 1 and data.loc[[index]].values[0][12] == 0:
            data.ix[index, ['spark.rdd.compress']] = 'FALSE'
        elif data.loc[[index]].values[0][11] == 0 and data.loc[[index]].values[0][12] == 1:
            data.ix[index, ['spark.rdd.compress']] = 'TRUE'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][13] == 1 and data.loc[[index]].values[0][14] == 0 and \
                data.loc[[index]].values[0][15] == 0:
            data.ix[index, ['spark.io.compression.codec']] = 'lz4'
        elif data.loc[[index]].values[0][13] == 0 and data.loc[[index]].values[0][14] == 1 and \
                data.loc[[index]].values[0][15] == 0:
            data.ix[index, ['spark.io.compression.codec']] = 'lzf'
        elif data.loc[[index]].values[0][13] == 0 and data.loc[[index]].values[0][14] == 0 and \
             data.loc[[index]].values[0][15] == 1:
            data.ix[index, ['spark.io.compression.codec']] = 'snappy'
        else:
            data.drop([index], inplace=True)
            continue

        if data.loc[[index]].values[0][18] == 1 and data.loc[[index]].values[0][19] == 0:
            data.ix[index, ['spark.serializer']] = 'org.apache.spark.serializer.JavaSerializer'
        elif data.loc[[index]].values[0][18] == 0 and data.loc[[index]].values[0][19] == 1:
            data.ix[index, ['spark.serializer']] = 'org.apache.spark.serializer.KryoSerializer'
        else:
            data.drop([index], inplace=True)
            continue

    # 删去枚举值所在的列
    data = data.drop(['5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '18', '19'], 1)

    # 对列进行重命名
    data.rename(columns={'0': 'spark.executor.cores', '1': 'spark.executor.memory',
                         '2': 'spark.memory.fraction', '3': 'spark.memory.storageFraction',
                         '4': 'spark.default.parallelism', '16': 'spark.reducer.maxSizeInFlight',
                         '17': 'spark.shuffle.file.buffer'}, inplace=True)

    # 移动最后含枚举值的几列
    data.insert(5, 'spark.shuffle.compress', data.pop('spark.shuffle.compress'))
    data.insert(6, 'spark.shuffle.spill.compress', data.pop('spark.shuffle.spill.compress'))
    data.insert(7, 'spark.broadcast.compress', data.pop('spark.broadcast.compress'))
    data.insert(8, 'spark.rdd.compress', data.pop('spark.rdd.compress'))
    data.insert(9, 'spark.io.compression.codec', data.pop('spark.io.compression.codec'))

    return data


file_read = 'E:/GANs/kdd/113/DandT-out.csv'
data = kafka_out(file_read)
#print(data)

#data = data.iloc[start:(start+BATCH_SIZE),:11]
#pa = np.vstack((pa0, pa1))

#test_data = np.column_stack((test_data, pa0)) 
#test_data = pd.DataFrame(test_data)
#test_data.rename(columns=ColumnsName, inplace=True)

data.to_csv(file_read, index=False)