#!/opt/anaconda3/bin/python -u
import getopt
import os.path
import sys
import pandas as pd
import numpy as np
from time import sleep
from datetime import datetime
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from common.lib.cal import business_days, today
from common.lib.db import query_kdb
from common.lib.sym import local_hk_symbology
from data.lib.portfolios import get_portfolios
from data.lib.prices import get_equity_ohlc

def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-c, --exchcode\texchange code")    
    print  ("  \t-s, --stats\tstats folder")  
    print  ("  \t-o, --output\toutput folder")  
    print  ("  \t-t, --timer\ttimer")
    print  ("  \t-h,\t\thelp")

def format_time(time="17:00:00"):
    return datetime.strptime(datetime.strftime(datetime.now(), "%Y%m%d") + "T" + time, "%Y%m%dT%H:%M:%S")

def tick_size(x):
    y = 0.01
    if x >= 0.01 and x < 0.25:
        y = 0.001
    elif x >= 0.25 and x < 0.50:
        y = 0.005
    elif x >= 0.50 and x < 10.:
        y = 0.01
    elif x >= 10. and x < 20.:
        y = 0.02
    elif x >= 20. and x < 100.:
        y = 0.05
    elif x >= 100. and x < 200.:
        y = 0.1
    elif x >= 200. and x < 500.:
        y = 0.2
    elif x >= 500. and x < 1000.:
        y = 0.5
    elif x >= 1000. and x < 2000.:
        y = 1.
    elif x >= 2000. and x < 5000.:
        y = 2.
    elif x >= 5000. and x < 9995.:
        y = 5.
    return (y)
    
    
def breakout(x):
    if x['thi']>=x['hhi']:
        return 'STRONG SELL'
    elif x['tlo'] <=x['hlo']:
        return 'STRONG BUY'
    elif ((abs(x['hhi']-x['thi']))/tick_size(x['thi'])) <= 4:
        return 'SELL'
    elif ((abs(x['tlo']-x['hlo']))/tick_size(x['tlo'])) <= 4:
        return 'BUY'
    return ''

def init_loop(outputFile, histOHLC, quoteSize):
    # get portfolios    
    portfolios = get_portfolios(today(), 'mysql+mysqlconnector://sqtprod:sqtprod123@10.59.3.170:3306/portfolios')
    
    # get ohlc
    today_ohlc = get_equity_ohlc(today(), local_hk_symbology, host="10.59.3.166", port=48883)
    today_ohlc = today_ohlc.reset_index()
    today_ohlc = today_ohlc[['sym','op','hi','lo','cl']]
    today_ohlc.columns = ['Ticker','top','thi','tlo','tcl']
    today_ohlc = today_ohlc.set_index('Ticker')
    
    today_range = lambda x: 'SELL' if x['tcl']>=x['thi'] else ('BUY' if x['tcl']<=x['tlo'] else '')
    signals = portfolios.join(today_ohlc, how='left').join(histOHLC, how='left').join(quoteSize, how='left')
    signals['breakout'] = signals.apply(breakout, axis=1)
    signals['range'] = signals.apply(today_range, axis=1)
    signals = signals[['sodqty','eodqty','top','thi','tlo','tcl','hop','hhi','hlo','hcl','quotesize','breakout','range']]
    
    info("Writing output to %s" % (outputFile))
    signals.to_csv(outputFile)    
    
def init_signals(outputDir, statsDir, timerSecs):
    quote_sz_df = None
    hist_ohlc_df = None
    quote_sz_file = "%s/%s.quotesz.csv" % (statsDir, today().strftime("%Y%m%d"))
    if os.path.exists(quote_sz_file):
        quote_sz_df = pd.read_csv(quote_sz_file, index_col=0)
    else:
        fatal("Missing Quote Size file %s" % (quote_sz_file))

    hist_ohlc_file = "%s/%s.histohlc.csv" % (statsDir, today().strftime("%Y%m%d"))
    if os.path.exists(hist_ohlc_file):
        hist_ohlc_df = pd.read_csv(hist_ohlc_file, index_col=0)
    else:
        fatal("Missing Historical OHLC file %s" % (hist_ohlc_file))

    output_file = "%s/%s.signals.csv" % (outputDir, today().strftime("%Y%m%d"))

    finish_time = format_time()
    info("Finish Time = %s" % (finish_time))
    while datetime.now() <= finish_time:     
        try:
             init_loop(output_file, hist_ohlc_df, quote_sz_df)
        except:
            error("Unexpected error: %s" % (str(sys.exc_info()[0])))
        finally:
            sleep(timerSecs)
    
def main(argv):   
    argExchCode = ""
    argStats = "/home/sqtprod/bca/stats"
    argOutput = "/home/sqtprod/bca/signals"
    argTimer = 5
    try:
        opts, args = getopt.getopt(argv,"hc:s:o:",["exchcode=","stats=","output=","timer="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()              
        elif opt in ("-c", "--exchcode"):
            argExchCode = arg    
        elif opt in ("-s", "--stats"):
            argStats = arg
        elif opt in ("-o", "--output"):
            argOutput = arg  
        elif opt in ("-t", "--timer"):
            argTimer = int(arg)
                                    
    if len(argExchCode) == 0 or len(argStats) == 0 or len(argOutput) == 0:
        print_usage()
        exit(0)

    if len(business_days(today(), today(), argExchCode)) == 1:
        info("Stats Directory = %s" % (argStats))
        info("Output Directory = %s" % (argOutput))
        info("Timer = %d" % (argTimer))
        init_signals(argOutput, argStats, argTimer)
    else:
        fatal("Skipping...holiday")
    
if __name__ == '__main__':
    main(sys.argv[1:])
    

