# -*- coding: utf-8 -*-
import math

def round_to(n, base=0.01):
    return round(base*round(n/base), 2)

def round_up(n, base=0.01):
    return round(base*math.ceil(n/base), 2)
    
def round_down(n, base=0.01):
    return round(base*math.floor(n/base), 2)    

hk_fees = lambda x: round_to(x*0.000027, 0.01) + round_to(x*0.00005, 0.01) + round_to(x*0.0003, 0.01) + round_up(x*0.001, 1)
    
