# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 09:51:23 2020

@author: MMORRIS
"""

# Find the nearest meteorology station

import numpy as np
import pandas as pd
from geopy.distance import lonlat, distance
from com.sca.hem4.log.Logger import Logger


def find_met(ylat, xlon, metlib_df):
    """
    Returns the meteorological station closest to the facility
    """
    # create numpy arrays
    lat = metlib_df['surflat'].values
    lon = metlib_df['surflon'].values
    
    dist = np.ones(len(lat))
    
    facility = (xlon, ylat)
    for i in np.arange(len(lon)):
        station = (lon[i], lat[i])
        dist[i] = round(distance(lonlat(*facility), lonlat(*station)).kilometers, 4)
    
    index = np.where(dist==dist.min())[0][0]

    distance2fac = dist[index]
    surf_file = metlib_df['surffile'][index]
    upper_file = metlib_df['upperfile'][index]
    surfyear = int(metlib_df['surfyear'][index])
    # Note: remove white space from surfcity and uacity, Aermod will not allow spaces in the city name
    surfdata_str = str(metlib_df['surfwban'][index]) + " " + str(int(metlib_df['surfyear'][index])) + " " + str(metlib_df['surfcity'][index]).replace(" ","")
    uairdata_str = str(metlib_df['uawban'][index]) + " " + str(int(metlib_df['surfyear'][index])) + " " + str(metlib_df['uacity'][index]).replace(" ","")
    prof_base = str(metlib_df['elev'][index])
    
    return surf_file, upper_file, surfdata_str, uairdata_str, prof_base, distance2fac, surfyear


def return_met(facid, faclat, faclon, surfname, metlib_df):
    """
    Returns the meteorological information for a specific surface station name
    """    
    metrow = metlib_df.loc[metlib_df['surffile'].str.upper() == surfname.upper()]
    if metrow.empty == True:
        emessage = ("Meteorology station " + surfname + " was chosen for facility " + facid + "\n"
                    "That station is not in the meteorlogical library. The facility will be skipped")
        Logger.logMessage(emessage)
        raise Exception(emessage)
     
    facility = (faclon, faclat)    
    station = (metrow['surflon'].iloc[0], metrow['surflat'].iloc[0])
    distance2fac = round(distance(lonlat(*facility), lonlat(*station)).kilometers, 4)
    surf_file = metrow['surffile'].iloc[0]
    upper_file = metrow['upperfile'].iloc[0]
    surfyear = metrow['surfyear'].iloc[0]
    # Note: remove white space from surfcity and uacity, Aermod will not allow spaces in the city name
    surfdata_str = str(metrow['surfwban'].iloc[0]) + " " + str(int(metrow['surfyear'].iloc[0])) + " " + str(metrow['surfcity'].iloc[0]).replace(" ","")
    uairdata_str = str(metrow['uawban'].iloc[0]) + " " + str(int(metrow['surfyear'].iloc[0])) + " " + str(metrow['uacity'].iloc[0]).replace(" ","")
    prof_base = str(metrow['elev'].iloc[0])
    
    return surf_file, upper_file, surfdata_str, uairdata_str, prof_base, distance2fac, surfyear
    
        