# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 16:57:24 2018

@author: Founder
"""

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def getTD(data):
    d1 = data['T']
    d2 = data['L']
    d = pd.concat([d1, d2], 1)
    
    l = []
    for i in range(len(d)):
        l.append(1/data['T'][i])
        #d['1/T'][i] =  d['T'][i]
        
    s = {'1/T' : l}
    zeng = pd.DataFrame(s)
    d = pd.concat([d, zeng], 1)
    return d
    
data_T_in = getTD(pd.read_csv('E:/GANs/kdd/1030/data/L100 P1 R1 forTP.csv', engine='python'))
data_T_out = getTD(pd.read_csv('E:/GANs/kdd/1030/data/result_of_T.csv', engine='python'))
data_D_in = getTD(pd.read_csv('E:/GANs/kdd/1030/data/L100 P1 R1 forDelay.csv', engine='python'))
data_D_out = getTD(pd.read_csv('E:/GANs/kdd/1030/data/result_of_D.csv', engine='python'))
data_sample = getTD(pd.read_csv('E:/GANs/kdd/1030/data/sample-outdou.csv', engine='python'))

a = plt.scatter(data_T_in['1/T'], data_T_in['L'], s=1, c='r', )
b = plt.scatter(data_T_out['1/T'], data_T_out['L'], s=1, c='y')
c = plt.scatter(data_D_in['1/T'], data_D_in['L'], s=1, c='b')
d = plt.scatter(data_D_out['1/T'], data_D_out['L'], s=1, c='g')
e = plt.scatter(data_sample['1/T'], data_sample['L'], s=1, c='k')
plt.legend([a, b, c, d, e], ["T_in", "T_out", "D_in", "D_out", "sample"], loc=1)
plt.xlabel('1/T')
plt.ylabel('L')
plt.grid(True)
plt.savefig('five_data.png', dpi=800)
plt.show()