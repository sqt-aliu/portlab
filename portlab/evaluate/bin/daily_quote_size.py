#!/opt/anaconda3/bin/python -u
import getopt
import os.path
import math
import sys
import numpy as np
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from common.lib.cal import business_days, today
from common.lib.db import query_kdb
from common.lib.sym import local_hk_symbology
from data.lib.products import get_products

def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-c, --exchcode\texchange code")    
    print  ("  \t-n, --minpartrate\tminimum participation rate")  
    print  ("  \t-x, --maxpartrate\tmaximum participation rate")  
    print  ("  \t-l, --maxlotsize\tmaximum lot size")  
    print  ("  \t-t, --timeinterval\ttime interval (twap)")  
    print  ("  \t-m, --capital\t\tcapital")  
    print  ("  \t-o, --output\t\toutput")  
    print  ("  \t-h,\t\thelp")

def init_quote_size(outputDir, minPartRate, maxPartRate, maxLotSz, timeInterval, capital):
    round_lot = lambda x: math.floor(x['capital'] / (x['bpx'] / x['lotsz'])) * x['lotsz']
    twap_sz = lambda x: max(x['lotsz'], round((x['qty']/timeInterval)/x['lotsz']) * x['lotsz'])
    avg_quote_sz = lambda x: math.ceil(((x['bsz']+x['asz'])*.5)/x['lotsz']) * x['lotsz'] if (not np.isnan(x['bsz']) and not np.isnan(x['asz'])) else (x['bsz'] if np.isnan(x['asz']) else np.isnan(x['bsz']))
    min_part_sz = lambda x: max(x['lotsz'], round((minPartRate*x['quote_sz'])/x['lotsz'])*x['lotsz'])
    max_part_sz = lambda x: max(x['lotsz'], round((maxPartRate*x['quote_sz'])/x['lotsz'])*x['lotsz'])
    max_lot_sz = lambda x: x['lotsz'] * maxLotSz
    show_sz = lambda x: min(x['tv'], x['twap_sz'], x['max_part_sz'], x['max_lot_sz'])
    show_missing_sz = lambda x: x['lotsz'] if np.isnan(x['quotesize']) or x['quotesize'] < x['lotsz'] else x['quotesize']
    
    products = get_products(today(), local_hk_symbology, host="10.59.3.166", port=48883)
    products = products.reset_index()
    products = products[products['ty'] == '5']
    products['Ticker'] = products['sym']
    products = products[['Ticker','tunit','bpx']]
    products.columns = ['ticker','lotsz','bpx']
    products = products[products['bpx'] > 0.]
    products = products.set_index('ticker')
  
    # get average quote size (past 10 days, every 5 minutes)
    quote_size = query_kdb("10.59.2.162", 58001, "select.avgsize[%s;10;5]" % (today().strftime("%Y.%m.%d")))
    quote_size['ic'] = quote_size['ic'].apply(bytes.decode).apply(local_hk_symbology)
    quote_size = quote_size.set_index('ic')

    trade_size = query_kdb("10.59.2.162", 58001, "select.avgtrd[%s;10]" % (today().strftime("%Y.%m.%d")))
    trade_size['ic'] = trade_size['ic'].apply(bytes.decode).apply(local_hk_symbology)
    trade_size = trade_size.set_index('ic')

    sizing_list = products.join(quote_size, how='left').join(trade_size, how='left')
    sizing_list['capital'] = capital
    sizing_list['qty'] = sizing_list.apply(round_lot, axis=1)
    sizing_list['quote_sz'] = sizing_list.apply(avg_quote_sz, axis=1)
    sizing_list['twap_sz'] = sizing_list.apply(twap_sz, axis=1).astype(int).fillna(0)
    sizing_list['min_part_sz'] = sizing_list.apply(min_part_sz, axis=1).astype(int).fillna(0)
    sizing_list['max_part_sz'] = sizing_list.apply(max_part_sz, axis=1).astype(int).fillna(0)
    sizing_list['max_lot_sz'] = sizing_list.apply(max_lot_sz, axis=1).astype(int).fillna(0)
    sizing_list['quotesize'] = sizing_list.apply(show_sz, axis=1)
    sizing_list['quotesize'] = sizing_list.apply(show_missing_sz, axis=1).astype(int)
    
    sizing_list = sizing_list[['quotesize']]
    output_file = "%s/%s.quotesz.csv" % (outputDir, today().strftime("%Y%m%d"))
    info("Writing output to %s" % (output_file))
    sizing_list.to_csv(output_file)
    
    
def main(argv):   
    argExchCode = ""
    argMinPartRate = 0.01
    argMaxPartRate = 0.05
    argMaxLotSz = 5
    argTimeInterval = 10
    argCapital = 2e6
    argOutput = "/home/sqtprod/bca/stats"
    try:
        opts, args = getopt.getopt(argv,"hc:n:x:l:t:m:o:",["exchcode=","minpartrate=","maxpartrate=","maxlotsize=","timeinterval=","capital=","output="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()              
        elif opt in ("-c", "--exchcode"):
            argExchCode = arg    
        elif opt in ("-n", "--minpartrate"):
            argMinPartRate = float(arg)
        elif opt in ("-x", "--maxpartrate"):
            argMaxPartRate = float(arg)        
        elif opt in ("-l", "--maxlotsize"):
            argMaxLotSz = int(arg)
        elif opt in ("-t", "--timeinterval"):
            argTimeInterval = int(arg)            
        elif opt in ("-m", "--capital"):
            argCapital = float(arg)   
        elif opt in ("-o", "--output"):
            argOutput = arg  
                                    
                                    
    if len(argExchCode) == 0:
        print_usage()
        exit(0)

    if len(business_days(today(), today(), argExchCode)) == 1:
        info("Minimum Participation Rate = %f" % (argMinPartRate))
        info("Maximum Participation Rate = %f" % (argMaxPartRate))
        info("Maximum Lot Size = %d" % (argMaxLotSz))
        info("Time Interval Slices = %d" % (argTimeInterval))
        info("Capital = %d" % (argCapital))
        info("Output Directory = %s" % (argOutput))
        init_quote_size(argOutput, argMinPartRate, argMaxPartRate, argMaxLotSz, argTimeInterval, argCapital)
    else:
        fatal("Skipping...holiday")
    
if __name__ == '__main__':
    main(sys.argv[1:])
    

