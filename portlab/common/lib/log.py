from sys import exit
from datetime import datetime

# Logging
def debug(msg):
    print ("[%s][DEBUG] %s" % (str(datetime.now()), msg))

def error(msg):
    print ("[%s][ERROR] %s" % (str(datetime.now()), msg))

def fatal(msg):
    print ("[%s][FATAL] %s" % (str(datetime.now()), msg))
    exit(-1)

def info(msg):
    print ("[%s][INFO] %s" % (str(datetime.now()), msg))

def warn(msg):
    print ("[%s][WARN] %s" % (str(datetime.now()), msg))