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

    
def get_equity_spread(date, symbology, days_back=5, host="10.59.2.162", port=58001):
    qry = "0!select.avgsprd[%s;%d]" % (date.strftime('%Y.%m.%d'), days_back)
    df = query_kdb(host, port, qry)
    df['ic'] = df['ic'].apply(bytes.decode)
    df['sym'] = df['ic'].apply(symbology)
    df = df.set_index('sym')
    return (df)       
    
def get_equity_turnover(date, symbology, days_back=20, host="10.59.2.162", port=58001):
    qry = "0!select.avgto[%s;%d]" % (date.strftime('%Y.%m.%d'), days_back)
    df = query_kdb(host, port, qry)
    df['ic'] = df['ic'].apply(bytes.decode)
    df['sym'] = df['ic'].apply(symbology)
    df = df.set_index('sym')
    return (df)         
    
def get_equity_volume(date, symbology, days_back=30, host="10.59.2.162", port=58001):
    qry = "0!select.avgvol[%s;%d]" % (date.strftime('%Y.%m.%d'), days_back)
    df = query_kdb(host, port, qry)
    df['ic'] = df['ic'].apply(bytes.decode)
    df['sym'] = df['ic'].apply(symbology)
    df = df.set_index('sym')
    return (df)        