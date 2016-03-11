# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 19:33:17 2016

@author: fgnovo
"""
import os
import numpy as np
import pandas as pd

path = "/Users/fgnovo/Dropbox/apec"
os.chdir(path)
df13=pd.read_csv('df13',sep=',')
df14=pd.read_csv('df14',sep=',')
#********************************
#a = pd.unique(df13.key.ravel())
#b = pd.unique(df14.key.ravel())
#c = np.in1d(a,b)
#print(len(a))
#print(len(b))
#print(np.where(c==False))
#print(a[np.where(c==False)])
df = pd.merge(df13,df14,on=['cuc','ref_catastral'],how="right")
df['height'] = 0
#select only values purpose == V and select de max floor by ref_catastral
df_v = df[df.purpose == 'V  ']
mask =df_v.groupby(['cuc','ref_catastral']).agg('idxmax')
df_vh = df.loc[mask['floor']].reset_index()
df_vh.height =df_vh.floor+1 
df_vh = df_vh[['cuc','ref_catastral','height']]
df = pd.merge(df,df_vh,on=['cuc','ref_catastral'],how='left')
df = df.fillna(0)
df['height'] = df['height_x']+df['height_y']
df.height = df.height.astype(int)
df = df[['cuc','ref_catastral','year_const','floor','preservation','purpose','t_reform','tipology','year_reform','height']]
print(df.dtypes)
print(df.head())
print('x')
