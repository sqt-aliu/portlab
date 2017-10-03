# -*- coding: utf-8 -*-
import glob
import os
import re
import sys
import pandas as pd
from datetime import datetime
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

def get_eligible_shorts_hk(date):
    hkex_symbology = lambda x: str(x).zfill(4) + '.HK'
    elig_shorts = sorted(glob.glob("/dfs/raw/live/hkex/*/*.shorts.csv"), reverse=True)
    elig_shorts_df = None
    for elig_short in elig_shorts:
        match = re.search("(20[0-2][0-9]{5})", elig_short)
        if match and (datetime.strptime(match.group(), '%Y%m%d') <= date):
            info("Reading Eligible Shorts %s" % (elig_short))
            elig_shorts_df = pd.read_csv(elig_short, skiprows=4)
            break
    elig_shorts_df['Ticker'] = elig_shorts_df['No.'].apply(hkex_symbology)
    elig_shorts_df['Eligible to Short'] = True
    elig_shorts_df = elig_shorts_df[['Ticker', 'Eligible to Short']]
    elig_shorts_df.columns = ['Ticker', 'EligibleToShort']
    elig_shorts_df = elig_shorts_df.set_index('Ticker')
    return (elig_shorts_df)
    
def get_borrows_nomura_hk(date):
    # check borrows
    borrow_file = "/home/sqtdata/dfs/raw/live/nomu.day/%s/%s.hkd.csv" % (date.strftime('%Y%m%d'), date.strftime('%Y%m%d'))
    info ("Reading borrow file " + borrow_file)
    borrow_df = pd.read_csv(borrow_file)
    borrow_df.columns = [x.strip() for x in borrow_df.columns]
    borrow_df = borrow_df[['Ric','Quantity','Fee']]
    borrow_df.columns = ['Ticker','BorrowQty','BorrowFee']
    borrow_df = borrow_df.set_index('Ticker')
    return (borrow_df)
    
def get_restricted_list(date):
    restricted_symbology = lambda x: str(x).zfill(4)
    restricted_pattern = "/home/sqtdata/dfs/raw/live/rest.day/%s/%s.*.restricted.csv" % (date.strftime('%Y%m%d'), date.strftime('%Y%m%d'))
    restricted_files = sorted(glob.glob(restricted_pattern), reverse=True)
    info("Reading Restricted List %s" % (restricted_files[0]))
    restricted_df = pd.read_csv(restricted_files[0])
    restricted_df['Ticker'] = restricted_df['code'].apply(restricted_symbology) + "." + restricted_df['exchange']
    restricted_df['Restricted'] = True
    restricted_df = restricted_df[['Ticker','Restricted']]
    restricted_df = restricted_df.set_index('Ticker')
    return (restricted_df)
    
def get_closing_auction_session_hk():
    hkex_symbology = lambda x: str(x).zfill(4) + '.HK'
    cas_df = pd.read_csv("https://www.hkex.com.hk/eng/market/sec_tradinfra/vcm_cas/Documents/List%20of%20CAS%20securities.csv", skiprows=5)
    cas_df['Ticker'] = cas_df['Stock Code'].apply(hkex_symbology)
    cas_df['CAS'] = True
    cas_df = cas_df[['Ticker','CAS']]
    cas_df = cas_df.set_index('Ticker')
    return (cas_df)