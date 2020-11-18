# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 11:31:24 2019

@author: Steve Fudge

This function rounds a given floating point number using the ROUND_HALF_UP
rule and returns an integer. The Python 3 ROUND() function with no decimal places
follows the "round half to even" rule.

"""

import math

def normal_round(n):
    if n - math.floor(n) < 0.5:
        return math.floor(n)
    return math.ceil(n)