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
from data.lib.reports import set_charges

__ACCOUNTID__ = "CPB10860"

def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-c, --exchcode\t\texchange code")
    print  ("  \t-d, --database\t\tdatabase connection string")
    print  ("  \t-p, --portfolio\t\tportfolio name")
    print  ("  \t-s, --start\t\tstart date")
    print  ("  \t-e, --end\t\tend date")    
    print  ("  \t-i, --input\t\tinput directory")    
    print  ("  \t-r, --dryrun\t\tdry run")      
    print  ("  \t-h,\t\t\thelp")

def format_time(time):
    today = datetime.strftime(datetime.now(), "%Y%m%d")
    return datetime.strptime(today + "T" + time, "%Y%m%dT%H:%M:%S")
    
def interest_payments(iDate, iPortfolio, dbConn, exchCode, inputDir, dryRun):
    inputFile = "%s/SUB_%s_20953280.CSV" % (inputDir, iDate.strftime("%Y-%m-%d"))
    if os.path.exists(inputFile):
        info("Reading file %s" % (inputFile))
        interest_df = pd.read_csv(inputFile, skiprows=1, parse_dates=['From Date','To Date'])
        interest_df = interest_df[interest_df['Account ID'] == __ACCOUNTID__] # filter by account
        interest_df = interest_df[['Long/Short Indicator','Balance Type','To Date','Interest Amount']]
        interest_df.columns = ['indicator','type','date','amount']
        interest_df['type'] = interest_df['type'] + ' Interest'
        interest_df['portfolio'] = iPortfolio

        # prepare insert to portfolios/charges database
        set_charges(interest_df, dbConn, dryrun=dryRun)        
    else:
        warn("File %s not found." % (inputFile))
   
def main(argv):   
    argDBConn = ""
    argExchCode = ""
    argPortfolio = ""
    argStart = ""
    argEnd = ""
    argDryRun = True    
    argInput = "/home/sqtdata/dfs/raw/live/bcar.day/reports"
    try:
        opts, args = getopt.getopt(argv,"hrc:d:s:e:p:i:",["dryrun","database=","exchcode=","start=","end=","portfolio=","input="])
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
        elif opt in ("-i", "--input"):
            argInput = arg            
        elif opt in ("-r", "--dryrun"):
            argDryRun = False                  
            
    if len(argDBConn) == 0 or len(argExchCode) == 0 or len(argPortfolio) == 0 or len(argInput) == 0:
        print_usage()
        exit(0)
    if argStart > argEnd:
        error("Start date must be less than End date")
        print_usage()
        exit(0)        
    
    dates = business_days(argStart, argEnd, argExchCode)
    for date in dates:
        info("Running payments for %s" % (date.strftime('%Y-%m-%d')))
        interest_payments(date, argPortfolio, argDBConn, argExchCode, argInput, argDryRun)
    
if __name__ == '__main__':
    main(sys.argv[1:])
    
