#!/opt/anaconda3/bin/python -u
import getopt
import os.path
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
    print  ("  \t-d, --days\thistorical days back")  
    print  ("  \t-o, --output\toutput")  
    print  ("  \t-h,\t\thelp")

def init_historical_ohlc(outputDir, days):
    # get x days OHLC
    query_ohlc = "select.histohlc[%s;%d]" % (today().strftime("%Y.%m.%d"), days)
    hist_ohlc = query_kdb("10.59.2.162", 58001, query_ohlc)
    hist_ohlc['ic'] = hist_ohlc['ic'].apply(bytes.decode)
    hist_ohlc['Ticker'] = hist_ohlc['ic'].apply(local_hk_symbology)
    hist_ohlc = hist_ohlc.set_index('Ticker')
    hist_ohlc = hist_ohlc[['op','hi','lo','cl']]
    hist_ohlc.columns = ['hop','hhi','hlo','hcl']
    hist_ohlc = hist_ohlc.sort_index()
    output_file = "%s/%s.histohlc.csv" % (outputDir, today().strftime("%Y%m%d"))
    info("Writing output to %s" % (output_file))
    hist_ohlc.to_csv(output_file)
    
def main(argv):   
    argExchCode = ""
    argDays = 21
    argOutput = "/home/sqtprod/bca/stats"
    try:
        opts, args = getopt.getopt(argv,"hc:d:o:",["exchcode=","days=","output="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()              
        elif opt in ("-c", "--exchcode"):
            argExchCode = arg    
        elif opt in ("-d", "--days"):
            argDays = int(arg)
        elif opt in ("-o", "--output"):
            argOutput = arg  
                                    
                                    
    if len(argExchCode) == 0 or argDays <= 0:
        print_usage()
        exit(0)

    if len(business_days(today(), today(), argExchCode)) == 1:
        info("Historical Days = %d" % (argDays))
        info("Output Directory = %s" % (argOutput))
        init_historical_ohlc(argOutput, argDays)
    else:
        fatal("Skipping...holiday")
    
if __name__ == '__main__':
    main(sys.argv[1:])
    

