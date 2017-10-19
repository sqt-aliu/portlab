#!/opt/anaconda3/bin/python -u
import getopt
import glob
import os
import os.path
import re
import sys
import pandas as pd
from datetime import datetime, timedelta
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from common.lib.cal import business_days, today

def print_usage():
    print  ("  Usage: %s [options]" % (os.path.basename(__file__))) 
    print  ("  Options:")
    print  ("  \t-b, --broker\tbroker")
    print  ("  \t-i, --input\tinput directory")
    print  ("  \t-o, --output\toutput directory")    
    print  ("  \t-h,\t\thelp")
     
def extract_reports(broker, indir, outdir):
    search = "%s/%s/*/SUB_*_209*.CSV.pgp" % (indir, broker)
    files = glob.glob(search)
    for file in files:
        match = re.search("(20[0-2][0-9]{5})", file)
        if match:
            filedate = match.group()
            filedatetime = datetime.strptime(filedate, "%Y%m%d")
            if filedatetime >= datetime.strptime("20170901", "%Y%m%d"):
                extract_file(file, outdir)
        else:
            warn("No date match found in %s" % (file))
                
def extract_file(filename, outdir):
    outfile = "%s/%s" % (outdir, os.path.basename(filename).replace(".pgp", ""))
    if not os.path.exists(outfile):
        if not os.path.getsize(filename) == 0:
            outcmd = "echo SQT001 | /usr/bin/gpg --passphrase-fd 0 --batch --yes --output %s %s" % (outfile, filename)
            os.system(outcmd)
            if os.path.exists(outfile):
                info("Extract successful for %s" % (outfile))
            else:
                warn("Extract unsuccessful for %s [%s]" % (outfile, outcmd))
        else:
            warn("File %s is size 0. Skipping..." % (filename))
    else:
        info("File %s already exists" % (outfile))
     
def main(argv):   
    argBroker = "nomura"
    argInput = "/home/sqtdata/archive"
    argOutput = ""
    try:
        opts, args = getopt.getopt(argv,"hb:i:o:",["broker=","input=","output="])
    except getopt.GetoptError:
        print_usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print_usage()
            sys.exit()
        elif opt in ("-b", "--broker"):
            argBroker = arg               
        elif opt in ("-i", "--input"):
            argInput = arg               
        elif opt in ("-o", "--output"):
            argOutput = arg        
      
    if len(argBroker) == 0 or len(argInput) == 0 or len(argOutput) == 0:
        print_usage()
        exit(0)
    
    extract_reports(argBroker, argInput, argOutput)
    
if __name__ == '__main__':
    main(sys.argv[1:])
    
