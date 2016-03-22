# -*- coding: utf-8 -*-
"""
Created on Sat Mar  5 19:33:17 2016

@author: fgnovo
"""
import os
import numpy as np
import pandas as pd
from scipy.stats.mstats import mode


def select_tipology(x):
    if x['tipology'].min()==1:
        x['tipology'] = 1
    else:
        x['tipology'] = mode(x['tipology'])[0][0]
    return(x.iloc[0])

path = "/Users/fgnovo/workspace/python-apec/data/log_data"
os.chdir(path)
df13=pd.read_csv('df13Monterroso',sep=',')
df14=pd.read_csv('df14Monterroso',sep=',')
#********************************
df = pd.merge(df13,df14,on=['cuc','ref_catastral'],how="right")
dfcat = df.groupby('ref_catastral').apply(select_tipology)
print('FIN')
