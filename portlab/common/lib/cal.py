import csv
from datetime import datetime, timedelta

def exchange_holidays(code, holidays="/dfs/misc/exchange_holidays.csv"):
    dates = []
    if code is not None:
        with open(holidays, 'r') as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row[0] == 'date' and row[1] == code and row[2] == 'N':
                   dates.append(datetime.strptime(row[0], "%Y%m%d"))
    return dates
    
def calendar_days(start, end):   
    dates = []
    delta = timedelta(days=1)
    current = start
    while current <= end:
        if current <= end:
            dates.append(current)
            current += delta
    return (dates)      
    
def week_days(start, end):   
    dates = []
    delta = timedelta(days=1)
    current = start
    while current <= end:
        if current <= end:
            if current.weekday() not in set([5,6]):
                dates.append(current)
            current += delta
    return (dates)      
    
def business_days(start, end, exchcode=""):
    holidays = exchange_holidays(exchcode)
    dates = []
    delta = timedelta(days=1)
    current = start
    while current <= end:
        if current <= end:
            if current.weekday() not in set([5,6]) and current not in holidays:
                dates.append(current)
            current += delta
    return (dates)