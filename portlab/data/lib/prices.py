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

def get_equity_prices(date, symbology, host="10.59.2.162", port=58000):
    qry = "0!select last tpx, last date by ic from tr where date = %s" % (date.strftime('%Y.%m.%d'))
    df = query_kdb(host, port, qry)
    df['ic'] = df['ic'].apply(bytes.decode)
    df['sym'] = df['ic'].apply(symbology)
    df = df.set_index('sym')
    return (df)
    
def get_equity_prices_rt(date, symbology, host="10.59.3.166", port=48883):
    qry = "select ic, prevpx:bpx, lastpx:bpx^tpx from (`ic xkey select ic, bpx from pr where date=%s) lj (`ic xkey select last[tpx] by ic from tr where date=%s)" % (date.strftime('%Y.%m.%d'), date.strftime('%Y.%m.%d'))
    df = query_kdb(host, port, qry)
    df['ic'] = df['ic'].apply(bytes.decode)
    df['sym'] = df['ic'].apply(symbology)
    df = df.set_index('sym')
    return (df)    
    
def get_equity_ohlc(date, symbology, host="10.59.2.162", port=58000):
    qry = "0!select op:first tpx, hi:max tpx, lo:min tpx, cl:last tpx by date,ic from tr where date = %s" % (date.strftime('%Y.%m.%d'))
    df = query_kdb(host, port, qry)
    df['ic'] = df['ic'].apply(bytes.decode)
    df['sym'] = df['ic'].apply(symbology)
    df = df.set_index('sym')
    return (df)   
    
def get_equity_snapshot(date, time, symbology, host="10.59.3.166", port=48883):
    qry = "select ic, bpx^tpx from (`ic xkey select ic, bpx from pr where date=%s) lj (select last tpx by ic from (select last tpx, sum tv by ic, 30 xbar recordtm.minute from (select recordtm + 08:00:00.000, ic, tpx, tv from tr where date=%s)) where minute <= %s)" % (date.strftime('%Y.%m.%d'), date.strftime('%Y.%m.%d'), time)
    #qry = "0!select last tpx by ic from (select last tpx, sum tv by ic, 30 xbar recordtm.minute from (select recordtm + 08:00:00.000, ic, tpx, tv from tr where date=%s)) where minute <= %s" % (date.strftime('%Y.%m.%d'), time)
    df = query_kdb(host, port, qry)
    df['ic'] = df['ic'].apply(bytes.decode)
    df['sym'] = df['ic'].apply(symbology)
    df = df.set_index('sym')[['tpx']]
    return (df)    