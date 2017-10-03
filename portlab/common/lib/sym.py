# -*- coding: utf-8 -*-

# symbology across different markets and vendors

# bloomberg
bloomberg_symbology = lambda x: str(int(x.split('.')[0])) + ' ' + x.split('.')[1] + ' EQUITY'

# bca research
bca_symbology = lambda x: x.split(':')[0].zfill(4) + '.' + x.split(':')[1]

# hk local 
local_hk_symbology = lambda x: str(x) if (str(x)[0]).isalpha() else str(x).zfill(4) + '.HK'