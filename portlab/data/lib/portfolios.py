# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import math
import glob
import os.path
import sys
from os.path import basename
from datetime import datetime
from sqlalchemy import create_engine, exc
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from common.lib.db import query_mysql, query_kdb
    
def get_portfolio(portfolio, date, dbconn="mysql+mysqlconnector://sqtdata:sqtdata123@10.59.4.20:3306/portfolios"):
    qry = "select * from report where portfolio = '%s' and date = '%s'" % (portfolio, date.strftime('%Y-%m-%d'))
    df = query_mysql(dbconn, qry, verbose=False)
    df = df.set_index('ticker')
    return (df)
    
def get_portfolios(date, dbconn="mysql+mysqlconnector://sqtdata:sqtdata123@10.59.4.20:3306/portfolios"):
    qry = "select * from report where date = '%s'" % (date.strftime('%Y-%m-%d'))
    df = query_mysql(dbconn, qry, verbose=False)
    df = df.set_index('ticker')
    return (df)    
        
def get_dividends(startdate, enddate, dbconn="mysql+mysqlconnector://sqtdata:sqtdata123@10.59.4.20:3306/equities"):
    qry = "select * from dvd where date >= '%s' and date <= '%s'" % (startdate.strftime('%Y-%m-%d'), enddate.strftime('%Y-%m-%d'))
    df = query_mysql(dbconn, qry, verbose=False)
    df = df.set_index('ticker')
    return (df)    
    
def get_trades(portfolio, date, dbconn="mysql+mysqlconnector://sqtdata:sqtdata123@10.59.4.20:3306/portfolios"):
    qry = "select * from trades where portfolio = '%s' and date = '%s'" % (portfolio, date.strftime('%Y-%m-%d'))
    df = query_mysql(dbconn, qry, verbose=False)
    df = df.set_index('ticker')
    return (df)    
    
def set_orders(orders, dbconn, dryrun=True):
    if not dryrun:
        db = create_engine(dbconn, echo=False)
        db.connect()
        
    for index, row in orders.iterrows():
        sql = "REPLACE INTO orders VALUES ('%s','%s','%s','%s',%d, CURRENT_TIMESTAMP())" % (row['title'], row['date'].strftime('%Y-%m-%d'), str(index), row['side'], row['ordqty'])
        try:
            info("Executing query [%s]" % (sql))
            if not dryrun:  
                db.execute(sql)
        except exc.IntegrityError as e:
            error("DB Integrity Error: %s, sql=%s" % (e, sql))
        except exc.SQLAlchemyError as e:
            error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))    
            
def set_trades(execs, dbconn, dryrun=True):
    if not dryrun:
        db = create_engine(dbconn, echo=False)
        db.connect()
        
    for index, row in execs.iterrows():
        sql = "REPLACE INTO trades VALUES ('%s','%s','%s','%s',%d,%f,%f,%d)" % (row['portfolio'], row['date'], str(index), row['side'], row['execqty'], row['avgpx'], row['comms'], int(row['mult']))
        try:
            info("Executing query [%s]" % (sql))
            if not dryrun:  
                db.execute(sql)
        except exc.IntegrityError as e:
            error("DB Integrity Error: %s, sql=%s" % (e, sql))
        except exc.SQLAlchemyError as e:
            error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))        
            
def set_reports(portfolios, dbconn, dryrun=True):
    if not dryrun:
        db = create_engine(dbconn, echo=False)
        db.connect()
        
    for index, row in portfolios.iterrows():
        sql = "REPLACE INTO report VALUES ('%s','%s','%s',%d,%f,%d,%f,%d,%f,%d,%f,%f,%f,%f,%f)" % (row['portfolio'], row['date'], str(index), row['sodqty'], row['sodnot'], row['buyqty'], row['buynot'], row['sellqty'], row['sellnot'], row['eodqty'], row['eodnot'], row['comms'], row['divs'], row['grosspnl'], row['netpnl'])
        try:
            info("Executing query [%s]" % (sql))
            if not dryrun:  
                db.execute(sql)
        except exc.IntegrityError as e:
            error("DB Integrity Error: %s, sql=%s" % (e, sql))
        except exc.SQLAlchemyError as e:
            error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))                   
            
def set_prices(portfolios, dbconn, dryrun=True):
    if not dryrun:
        db = create_engine(dbconn, echo=False)
        db.connect()
        
    for index, row in portfolios.iterrows():
        sql = "REPLACE INTO prices VALUES ('%s','%s',%f,%f)" % (row['date'], str(index), row['prevpx'], row['lastpx'])
        try:
            info("Executing query [%s]" % (sql))
            if not dryrun:  
                db.execute(sql)
        except exc.IntegrityError as e:
            error("DB Integrity Error: %s, sql=%s" % (e, sql))
        except exc.SQLAlchemyError as e:
            error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))                               
            
def set_totals(dbconn, dryrun=True):
    if not dryrun:
        db = create_engine(dbconn, echo=False)
        db.connect()
        
    sql = "REPLACE INTO totals SELECT portfolio, CURRENT_DATE() as date, ticker, sum(comms) as totcomms, sum(divs) as totdivs, sum(netpnl) as totpnl FROM report WHERE date <= CURRENT_DATE() GROUP BY portfolio, ticker"
    try:
        info("Executing query [%s]" % (sql))
        if not dryrun:  
            db.execute(sql)
    except exc.IntegrityError as e:
        error("DB Integrity Error: %s, sql=%s" % (e, sql))
    except exc.SQLAlchemyError as e:
        error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))                   
                            
def set_month_totals(dbconn, dryrun=True):
    if not dryrun:
        db = create_engine(dbconn, echo=False)
        db.connect()
        
    sql = "REPLACE INTO totals SELECT portfolio, CURRENT_DATE() as date, ticker, sum(comms) as totcomms, sum(divs) as totdivs, sum(netpnl) as totpnl FROM report WHERE date >= DATE_FORMAT(CURRENT_DATE() ,'%Y-%m-01') and date <= CURRENT_DATE() GROUP BY portfolio, ticker"
    try:
        info("Executing query [%s]" % (sql))
        if not dryrun:  
            db.execute(sql)
    except exc.IntegrityError as e:
        error("DB Integrity Error: %s, sql=%s" % (e, sql))
    except exc.SQLAlchemyError as e:
        error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))                   
                                                        