#!/opt/anaconda3/bin/python -u
import getopt
import os.path
import sys
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from common.lib.cal import business_days, today
from common.lib.db import query_mysql
from data.lib.portfolios import set_reports, set_corporate_actions, get_portfolio, get_dividends

def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-d, --database\tdatabase connection string")
    print  ("  \t-c, --exchcode\texchange code")    
    print  ("  \t-r, --dryrun\tdry run")  
    print  ("  \t-h,\t\thelp")

def init_sod(dbConn, exchCode, dryRun):
    days = business_days(today() - timedelta(days=10), today() - timedelta(days=1), exchCode)
    query = "select distinct portfolio from report where date = '%s'" % (days[-1].strftime('%Y-%m-%d'))
    portnames = query_mysql(dbConn, query, verbose=True)
    if portnames is None or portnames.shape[0] == 0:
        info("No portfolio(s) found containing positions")
    else:
        info("%d portfolio(s) found" % (portnames.shape[0]))
        for index, row in portnames.iterrows():
            positions = get_portfolio(row['portfolio'], days[-1])
            # positions = positions[positions['eodqty'] != 0] # remove empty positions
            # set today's trade date
            positions['date'] = today().strftime('%Y-%m-%d')
            # carry over eod to sod
            positions['sodqty'] = positions['eodqty']
            positions['sodnot'] = positions['eodnot']
            # zero out entries
            positions[['buyqty','sellqty','eodqty']] = 0
            positions[['buynot','sellnot','eodnot','grosspnl','netpnl','comms','divs']] = 0.
            # prepare to submit entries
            if positions.shape[0] > 0:
                dvd = get_dividends(today(), today())
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

    
def main(argv):   
    argDBConn = ""
    argExchCode = ""
    argDryRun = True
    try:
        opts, args = getopt.getopt(argv,"hrd:c:",["dryrun","database=","exchcode="])
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
        elif opt in ("-r", "--dryrun"):
            argDryRun = False
            
    if len(argDBConn) == 0 or len(argExchCode) == 0:
        print_usage()
        exit(0)

    if len(business_days(today(), today(), argExchCode)) == 1:
        info("Dry Run is turned %s" % ("ON" if argDryRun else "OFF"))
        init_sod(argDBConn, argExchCode, argDryRun)
    else:
        fatal("Skipping...holiday")
    
if __name__ == '__main__':
    main(sys.argv[1:])
    
