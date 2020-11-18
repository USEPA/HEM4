# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 15:42:20 2020

@author: Steve Fudge
"""

import math

def round_half_up(n, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(n*multiplier + 0.5) / multiplier
