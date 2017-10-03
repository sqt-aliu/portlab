import pandas as pd
import numpy as np
import os.path
import sys
from datetime import datetime
from sqlalchemy import create_engine, exc
from qpython import qconnection
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../..'))
from common.lib.log import debug, error, fatal, info, warn

def query_mysql(dbConn, dbQuery, verbose=False):
    dfSQL = None
    try:
        if verbose: info("Creating MariaDB Engine...")
        db = create_engine(dbConn, echo=False)
        conn = db.connect()

        if verbose: info("Executing query [%s]" % (dbQuery))
        dfSQL = pd.read_sql_query(dbQuery, conn)
        conn.close()
    except exc.SQLAlchemyError as e:
        error("Database Error: %s, conn=%s" % (e, dbConn))
    except:
        error("Unknown Error: %s, conn=%s" % (sys.exc_info()[0], dbConn))
        error(str(sys.exc_info()))

    return (dfSQL)
    
def query_kdb(dbHost, dbPort, dbQuery, pandas=True):
    qc = qconnection.QConnection(host = dbHost, port = dbPort, pandas=pandas, numpy_temporals = True)
    qc.open()
    df = qc.sync(dbQuery, pandas = pandas)
    return (df)    