import numpy as np

# core functions
totalreturn = lambda x: (x[-1]/x[0])-1
finalreturn = lambda x: x[-1]
sharpe = lambda x: np.sqrt(12) * (np.mean(x) / np.std(x))
ann_vol = lambda x: np.sqrt(12) * np.std(x)
cagr = lambda x: ((((x[-1]) / x[0])) ** (12.0/(x.count()-1))) - 1
cagr2 = lambda x: ((np.mean(x)+1) ** 12) -1

def max_drawdown(X):
    mdd = 0
    peak = X[0]
    for x in X:
        if x > peak: 
            peak = x
        dd = (peak - x)
        if dd > mdd:
            mdd = dd
    return mdd 
    
def avg_max_drawdown(X):
    ldd = []
    mdd = 0
    peak = X[0]
    for x in X:
        if x > peak: 
            peak = x
        dd = (peak - x)
        if dd > mdd:
            mdd = dd
            ldd.append(mdd)
    return 0 if len(ldd) == 0 else np.mean(ldd)     

def bca_max_drawdown(X):
    mdd = 0
    peak = X[0]
    for x in X:
        if x > peak: 
            peak = x
        if peak != 0:
            dd = 1-x/peak
        if dd > mdd:
            mdd = dd
    return mdd  
    