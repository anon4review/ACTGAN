# -*- coding: utf-8 -*-
"""
Created on August 2ed 13:59:14 2018
GAN
@author: Founder
"""

import tensorflow as tf
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
#import matplotlib.pyplot as plt
import random
import datanumberized as dn

tf.set_random_seed(1)   # 设置图级随机seed
np.random.seed(1)

# Hyper Parameters
Epoch = 150000
NumOfLine = 16
BATCH_SIZE = 32         # 分隔
LR_G = 0.0001           # learning rate for generator
LR_D = 0.0001           # learning rate for discriminator
N_IDEAS = 5             # think of this as number of ideas for generating an art work (Generator)
#NumOfF = 14            # it could be total point G can draw in the canvas

#wordcount/random
file_read = 'E:/GANs/kdd/113/DandT.csv'
#data = dn.sparkdata(file_read)
data = dn.kafkadata(file_read)
#data = dn.hivedata(file_read)
#data = dn.redisdata(file_read)
#data = dn.cassandradata(file_read)
#data = dn.hbasedata(file_read)
#data = dn.tomcatdata(file_read)
#data = dn.mysqldata(file_read)
print(data)
NumOfF = data.columns.size
data = data.iloc[:BATCH_SIZE,:NumOfF]
data = data.as_matrix()

#标准化    
ss = StandardScaler()
data = ss.fit_transform(data)

new_points = np.linspace(1, 10, NumOfF)
i = 0
outp = [[0] * NumOfF] * 50

tf.reset_default_graph() 

'''
#扩大样本操作 指数级别
def Cwork():
    clist = [random.randint(0, BATCH_SIZE-1) for _ in range(NumOfLine)]#进行C操作
    datause = np.zeros(shape=(NumOfLine, ART_COMPONENTS))#建立恐惧镇
    j = 0
    for c in clist:
        #返回某一行   data[i:i+1]
        datause[j] = data[c]
        j = j + 1
        
    return datause
'''

#扩大样本操作 Ank
def Cwork():
    clist = random.sample(range(BATCH_SIZE), NumOfLine)
    datause = np.zeros(shape=(NumOfLine, NumOfF))#建立恐惧镇
    j = 0
    for c in clist:
        # 返回某一行   data[i:i+1]
        datause[j] = data[c]
        j = j + 1
        
    return datause


with tf.variable_scope('Generator'):                            # 返回一个用于定义创建variable（层）的op的上下文管理器
    G_in = tf.placeholder(tf.float32, [None, N_IDEAS])          # random ideas (could from normal distribution)
    G_l1 = tf.layers.dense(G_in, 128, tf.nn.relu)
    G_out = tf.layers.dense(G_l1, NumOfF)               # making a painting from these random ideas

with tf.variable_scope('Discriminator'):
    real_f = tf.placeholder(tf.float32, [None, NumOfF], name='real_in')   # receive art work from the famous artist
    D_l0 = tf.layers.dense(real_f, 128, tf.nn.relu, name='l')
    p_real = tf.layers.dense(D_l0, 1, tf.nn.sigmoid, name='out')              # probability that the art work is made by artist
    # reuse layers for generator
    D_l1 = tf.layers.dense(G_out, 128, tf.nn.relu, name='l', reuse=True)            # receive art work from a newbie like G
    p_fake = tf.layers.dense(D_l1, 1, tf.nn.sigmoid, name='out', reuse=True)  # probability that the art work is made by artist


D_loss = -tf.reduce_mean(tf.log(p_real) + tf.log(1-p_fake))             # 核心公式
G_loss = tf.reduce_mean(tf.log(1-p_fake))
'''
D_loss = -tf.reduce_mean(tf.log(tf.clip_by_value(p_real, 1e-10,1.0)) + tf.log(tf.clip_by_value(1-p_real, 1e-10,1.0)))             # 核心公式
G_loss = tf.reduce_mean(tf.log(tf.clip_by_value(1-p_fake, 1e-10,1.0)))
'''

train_D = tf.train.AdamOptimizer(LR_D).minimize(
    D_loss, var_list=tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES, scope='Discriminator'))
train_G = tf.train.AdamOptimizer(LR_G).minimize(
    G_loss, var_list=tf.get_collection(tf.GraphKeys.TRAINABLE_VARIABLES, scope='Generator'))

sess = tf.Session()
sess.run(tf.global_variables_initializer())                     # run内参数：初始化参数的本来面目

#outp = 

#plt.ion()   # something about continuous plotting
for step in range(Epoch):
    
    dataused = Cwork()
    #print('第',step,'次迭代', sep='')
    
    if step % 10000 == 0:
        print(dataused)
        print('第',step,'次迭代', sep='')
        
    G_ideas = np.random.randn(NumOfLine, N_IDEAS)
    G_paintings, pa0, Dl = sess.run([G_out, p_real, D_loss, train_D, train_G],    # train and get results
                                    {G_in: G_ideas, real_f: dataused})[:3]
    
    '''
    print(dataused)
    print(G_paintings[0])
    print('第',step,'次迭代', sep='')
    
    '''
    if step > (Epoch - 50):
        outp[i] = G_paintings[0]
        print(outp[i])
        i = i + 1
        #outp = dataframe(G_paintings[0])
        #outp.to_csv('E:/G生成.csv')

print(G_paintings[0])
outp = ss.inverse_transform(outp)
output = file_read.strip('.csv')   
output = output + '-out.csv'
outp = pd.DataFrame(outp)
outp.to_csv(output, index=False)

#outp = dn.cassandradata_out(outp)
#outp = dn.hbase_out(outp)
#outp = dn.hive_out(output)
#outp = dn.mysql_out(outp)
#outp = dn.redis_out(outp)

#outp.to_csv(output, index=False)