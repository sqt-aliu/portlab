# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os.path
import sys
from os.path import basename
from datetime import datetime
from sqlalchemy import create_engine, exc
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn
from common.lib.db import query_mysql, query_kdb
    

def set_charges(payments, dbconn, dryrun=True):
    if not dryrun:
        db = create_engine(dbconn, echo=False)
        db.connect()
        
    for index, row in payments.iterrows():
        sql = "REPLACE INTO charges VALUES ('%s','%s','%s','%s',%f)" % (row['portfolio'], row['date'].strftime('%Y-%m-%d'), row['type'], row['indicator'], row['amount'])
        try:
            info("Executing query [%s]" % (sql))
            if not dryrun:  
                db.execute(sql)
        except exc.IntegrityError as e:
            error("DB Integrity Error: %s, sql=%s" % (e, sql))
        except exc.SQLAlchemyError as e:
            error("DB SQLAlchemy Error: %s, sql=%s" % (e, sql))    
            
