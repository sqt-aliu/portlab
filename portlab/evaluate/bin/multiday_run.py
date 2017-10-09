#!/opt/anaconda3/bin/python -u
import getopt
import os.path
import sys
import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from common.lib.cal import business_days
from common.lib.db import query_mysql
from common.lib.sym import local_hk_symbology
from data.lib.portfolios import set_reports, set_corporate_actions, get_portfolio, get_dividends
from data.lib.prices import get_equity_prices_rt

def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-c, --exchcode\texchange code")
    print  ("  \t-d, --database\tdatabase connection string")
    print  ("  \p-p, --portfolio\tportfolio name")
    print  ("  \t-s, --start\tstart date")
    print  ("  \t-e, --end\tend date")    
    print  ("  \t-r, --dryrun\tdry run")      
    print  ("  \t-h,\t\t\thelp")

def format_time(time):
    today = datetime.strftime(datetime.now(), "%Y%m%d")
    return datetime.strptime(today + "T" + time, "%Y%m%dT%H:%M:%S")
    
def init_sod(sDate, sPortfolio, dbConn, exchCode, dryRun):
    positions = get_portfolio(sPortfolio, sDate)
    # positions = positions[positions['eodqty'] != 0] # remove empty positions
    # set today's trade date
    positions['date'] = sDate.strftime('%Y-%m-%d')
    # carry over eod to sod
    positions['sodqty'] = positions['eodqty']
    positions['sodnot'] = positions['eodnot']
    # zero out entries
    positions[['buyqty','sellqty','eodqty']] = 0
    positions[['buynot','sellnot','eodnot','grosspnl','netpnl','comms','divs']] = 0.
    # prepare to submit entries
    if positions.shape[0] > 0:
        dvd = get_dividends(sDate, sDate)
        dvd = dvd[['dividend','split']]
        posdvd = positions.join(dvd, how='inner').copy(deep=True)
        if posdvd.shape[0] > 0:
            # adjustment required
            posdvd['dividend'] = posdvd['dividend'].fillna(0.)
            posdvd['split'] = posdvd['split'].fillna(1.)
            posdvd['oldqty'] = posdvd['sodqty']
            for index, row in posdvd.iterrows():
                info("Adjusting div/split %s, div=%f, split=%f" % (index, row['dividend'], row['split']))
                positions.ix[index, 'divs'] =  positions.ix[index, 'sodqty'] * row['dividend']
                positions.ix[index, 'sodqty'] =  positions.ix[index, 'sodqty'] * (1./row['split'])
                # record adjustment for records
                posdvd.ix[index, 'cashadj'] = positions.ix[index, 'sodqty'] * row['dividend']
                posdvd.ix[index, 'newqty'] = positions.ix[index, 'sodqty'] * (1./row['split'])
                
            # prepare insert to portfolios/corpactions database
            set_corporate_actions(posdvd, dbConn, dryrun=dryRun)
        else:
            info("No adjustments found")
            
        # prepare insert to portfolios/reports database
        set_reports(positions, dbConn, dryrun=dryRun)
        
def init_eod(eDate, ePortfolio, dbConn, exchCode, dryRun):
    func_buy_qty = lambda x: x['execqty'] if x['side'] in ['B','C'] else 0.
    func_sell_qty = lambda x: x['execqty'] if x['side'] in ['S','H'] else 0.
    
    reports_query = "select * from report where portfolio = '%s' and date = '%s'" % (ePortfolio, eDate.strftime('%Y-%m-%d'))
    trades_query = "select * from trades where portfolio = '%s' and date = '%s'" % (ePortfolio, eDate.strftime('%Y-%m-%d'))
    reports = query_mysql(dbConn, reports_query, verbose=True)
    info("%d report record(s) found" % (reports.shape[0]))
    reports = reports.set_index(['portfolio','ticker'])
    trades = query_mysql(dbConn, trades_query, verbose=True)
    info("%d trades record(s) found" % (trades.shape[0]))
    if trades.shape[0] > 0:
        trades['tbuyqty'] = trades.apply(func_buy_qty, axis=1)
        trades['tbuynot'] = trades['tbuyqty'] * trades['avgpx'] * trades['mult']
        trades['tsellqty'] = trades.apply(func_sell_qty, axis=1)
        trades['tsellnot'] = trades['tsellqty'] * trades['avgpx'] * trades['mult']
        trades['tcomms'] = trades['comms']
        trades_totals = trades.groupby(['portfolio','ticker'])['tbuyqty','tbuynot','tsellqty','tsellnot','tcomms'].sum()
    else:
        trades_totals = pd.DataFrame(columns=['portfolio','ticker','tbuyqty','tbuynot','tsellqty','tsellnot','tcomms'])
        trades_totals = trades_totals.set_index(['portfolio','ticker'])
    
    portfolio = reports.join(trades_totals, how='outer')
    # copy over trade notional
    portfolio[['buyqty','buynot','sellqty','sellnot','comms']] = portfolio[['tbuyqty','tbuynot','tsellqty','tsellnot','tcomms']]
    # fill zeros for missing data
    portfolio = portfolio.fillna(0.)
    portfolio = portfolio.reset_index().set_index('ticker')
    # price portfolio
    prices = get_equity_prices_rt(eDate, local_hk_symbology)
    
    portfolio = portfolio.join(prices, how='left')
    
    # compute eodqty
    portfolio['eodqty'] = portfolio['sodqty'] + (portfolio['buyqty']-portfolio['sellqty']) 
    portfolio['eodnot'] = portfolio['eodqty'] * portfolio['lastpx']
    portfolio['grosspnl'] = (portfolio['eodnot'] - (portfolio['buynot'] - portfolio['sellnot']) - portfolio['sodnot']) + portfolio['divs']
    portfolio['netpnl'] = portfolio['grosspnl'] - portfolio['comms']
    portfolio['date'] = eDate.strftime('%Y-%m-%d')
    
    # prepare insert to database
    set_reports(portfolio, dbConn, dryrun=dryRun)
   
def main(argv):   
    argDBConn = ""
    argExchCode = ""
    argPortfolio = ""
    argStart = ""
    argEnd = ""
    argDryRun = True    
    try:
        opts, args = getopt.getopt(argv,"hrc:d:s:e:p:",["dryrun","database=","exchcode=","start=","end=","portfolio="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()
        elif opt in ("-d", "--database"):
            argDBConn = arg               
        elif opt in ("-c", "--exchcode"):
            argExchCode = arg
        elif opt in ("-p", "--portfolio"):
            argPortfolio = arg            
        elif opt in ("-s", "--start"):
            argStart = datetime.strptime(arg, '%Y%m%d')
        elif opt in ("-e", "--end"):
            argEnd = datetime.strptime(arg, '%Y%m%d')     
        elif opt in ("-r", "--dryrun"):
            argDryRun = False                  
            
    if len(argDBConn) == 0 or len(argExchCode) == 0 or len(argPortfolio) == 0:
        print_usage()
        exit(0)
    if argStart > argEnd:
        error("Start date must be less than End date")
        print_usage()
        exit(0)        
    
    dates = business_days(argStart, argEnd, argExchCode)
    for date in dates:
        info("Running reports for %s" % (date.strftime('%Y-%m-%d')))
        init_sod(date, argPortfolio, argDBConn, argExchCode, argDryRun)
        init_eod(date, argPortfolio, argDBConn, argExchCode, argDryRun)
    
if __name__ == '__main__':
    main(sys.argv[1:])
    
