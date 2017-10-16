#!/opt/anaconda3/bin/python -u
import getopt
import os.path
import sys
import pandas as pd
from time import sleep
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from common.lib.cal import business_days, today
from common.lib.db import query_mysql
from common.lib.sym import local_hk_symbology
from data.lib.portfolios import set_reports, set_prices, set_totals, get_portfolio, get_dividends
from data.lib.prices import get_equity_prices_rt
from data.lib.products import get_products

def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-d, --database\tdatabase connection string")
    print  ("  \t-c, --exchcode\texchange code")    
    print  ("  \t-f, --finish\tfinish time(default: 17:00")    
    print  ("  \t-s, --sleep\tsleep in seconds(default: 5")    
    print  ("  \t-r, --dryrun\tdry run")      
    print  ("  \t-h,\t\thelp")

def format_time(time):
    return datetime.strptime(datetime.strftime(datetime.now(), "%Y%m%d") + "T" + time, "%Y%m%dT%H:%M:%S")
    
def init_intraday(dbConn, exchCode, dryRun):
    func_buy_qty = lambda x: x['execqty'] if x['side'] in ['B','C'] else 0.
    func_sell_qty = lambda x: x['execqty'] if x['side'] in ['S','H'] else 0.
    
    reports_query = "select * from report where date = '%s'" % (today().strftime('%Y-%m-%d'))
    trades_query = "Select * from trades where date = '%s'" % (today().strftime('%Y-%m-%d'))
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
    # products
    products = get_products(today(), local_hk_symbology)
    products = products[['tmult']] # take only multiplier column    
    # price portfolio
    prices = get_equity_prices_rt(today(), local_hk_symbology)
    
    portfolio = portfolio.join(products, how='left').join(prices, how='left')
    
    # compute eodqty
    portfolio['eodqty'] = portfolio['sodqty'] + (portfolio['buyqty']-portfolio['sellqty']) 
    portfolio['eodnot'] = portfolio['eodqty'] * portfolio['lastpx'] * portfolio['tmult']
    portfolio['grosspnl'] = (portfolio['eodnot'] - (portfolio['buynot'] - portfolio['sellnot']) - portfolio['sodnot']) + portfolio['divs']
    portfolio['netpnl'] = portfolio['grosspnl'] - portfolio['comms']
    portfolio['date'] = today().strftime('%Y-%m-%d')
    
    # prepare insert to database
    set_reports(portfolio, dbConn, dryrun=dryRun)
    
    # submit prices
    set_prices(portfolio, dbConn, dryrun=dryRun)
    
    # run totals 
    set_totals(dbConn, dryrun=dryRun)
    
def main(argv):   
    argDBConn = ""
    argExchCode = ""
    argFinish = "17:00:00"
    argSleep = 1
    argDryRun = True
    
    try:
        opts, args = getopt.getopt(argv,"hrd:c:f:s:",["dryrun","database=","exchcode=","finish=","sleep="])
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
        elif opt in ("-f", "--finish"):
            argFinish = arg            
        elif opt in ("-s", "--sleep"):
            argSleep = arg           
        elif opt in ("-r", "--dryrun"):
            argDryRun = False               
            
    if len(argDBConn) == 0 or len(argExchCode) == 0:
        print_usage()
        exit(0)
    
    finishTime = format_time(argFinish)    
    info("Dry Run is turned %s" % ("ON" if argDryRun else "OFF"))
    while datetime.now() <= finishTime:     
        try:
            init_intraday(argDBConn, argExchCode, argDryRun)
        except:
            error("Unexpected error: %s" % (str(sys.exc_info()[0])))
        finally:
            sleep(argSleep)
    
if __name__ == '__main__':
    main(sys.argv[1:])
    
