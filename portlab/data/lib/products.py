# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import math
import glob
import os.path
import sys
from os.path import basename
from datetime import datetime

sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from common.lib.db import query_mysql, query_kdb


def get_products(date, symbology, host="10.59.3.166", port=48883):
    qry = "select from pr where date = %s" % (date.strftime('%Y.%m.%d'))
    df = query_kdb(host, port, qry)
    df['ic'] = df['ic'].apply(bytes.decode)
    df['ty'] = df['ty'].apply(bytes.decode)
    df['sym'] = df['ic'].apply(symbology)
    df = df.set_index('sym')
    return (df)    
    
def get_lotsize(date, symbology, host="10.59.3.166", port=48883):
    qry = "select from pr where date = %s" % (date.strftime('%Y.%m.%d'))
    df = query_kdb(host, port, qry)
    df = df[df['ty'] == b'5'] # filter for equities
    df['ic'] = df['ic'].apply(bytes.decode)
    df['sym'] = df['ic'].apply(symbology)
    df = df.set_index('sym')
    df = df[['tunit']]
    return (df)    
    
def get_hedge(date, symbology, prefix, days_to_expire=5, host="10.59.3.166", port=48883):
    expiry_days = lambda x, y: (x['edate'] - y).days
    df = get_products(date, symbology, host=host, port=port)
    df = df[(df['ic'].str.startswith(prefix)) & (df['ty'] == '1')]
    df['edate'] = pd.to_datetime(df['edate'].astype(str), format='%Y%m%d')
    df['diff'] = df.apply(expiry_days, axis=1, args=(date,))
    df = df[df['diff'] >=days_to_expire].sort_values('edate')
    return (df)    