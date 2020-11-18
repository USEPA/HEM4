import time

import numpy as np
import sys
import math as m
import pandas as pd

from pyproj import Proj, transform, Transformer, _datadir, datadir
from com.sca.hem4.log.Logger import Logger

utmzone = 'utmzone';
utme = 'utme';
utmn = 'utmn';
utmz = 'utmz';

# Caches for projections and transformers...avoiding the continual creation of these objects
# helps performance tremendously.
projections = {}
transformers = {}

class UTM:
    """
    A utility class with functions related to UTM zones.
    """

    @staticmethod
    def zonetxt(zone):
        if int(zone) < 10:
            zonetxt = '0' + str(zone)
        else:
            zonetxt = str(zone)
        return zonetxt

    @staticmethod
    def getZone(zonestr):
        # returns the zone number portion of a zone string (e.g. '16N')
        hemilist = ['N', 'S']
        if any(elem in zonestr for elem in hemilist):
            return zonestr[:-1]
        else:
            return zonestr
     
    @staticmethod
    def getBand(row):
        # returns the hemisphere (N or S) portion of a zone string; if none return N
        N_or_S = "N"
        if "S" in row:
            N_or_S = "S"
        return N_or_S


    @staticmethod
    def zone2use(el_df):

        """
        Create a common UTM Zone for this facility from the emission locations.

        All emission sources input to Aermod must have UTM coordinates
        from a single UTM zone. This function will determine the single
        UTM zone (and hemisphere) to use. Parameter is the emissions
        location data frame.

        """
      
        # First, check for any utm zones provided by the user in the emission location file
        utmzones_df = el_df["utmzone"].loc[el_df["location_type"] == "U"]
               
        if utmzones_df.shape[0] > 0:
            # there are some; find the smallest one
            utmzones_df['utmzone'] = utmzones_df.apply(lambda row: UTM.getZone(row))
            utmzones_df['utmband'] = utmzones_df.apply(lambda row: UTM.getBand(row))
            min_utmzu = int(np.nan_to_num(utmzones_df['utmzone']).min(axis=0))
            min_utmbu = utmzones_df['utmband'].min()
        else:
            min_utmzu = 0
            min_utmbu = "Z"

        # Next, compute utm zones from any user provided longitudes and find smallest
        lon_df = el_df[["lon"]].loc[el_df["location_type"] == "L"]
        if lon_df.shape[0] > 0:
            lon_df["z"] = ((lon_df["lon"]+180)/6 + 1).astype(int)
            min_utmzl = int(np.nan_to_num(lon_df["z"]).min(axis=0))
        else:
            min_utmzl = 0
            
        lat_df = el_df[["lat"]].loc[el_df["location_type"] == "L"]
        if lat_df.shape[0] > 0 and lat_df["lat"].min() < 0:
            min_utmbl = "S"
        else:
            min_utmbl = "N"

        if min_utmzu == 0:
            utmzone = min_utmzl
        else:
            if min_utmzl == 0:
                utmzone = min_utmzu
            else:
                utmzone = min(min_utmzu, min_utmzl)

        hemi = min(min_utmbu, min_utmbl)
        
        if utmzone == 0:
            emessage = "Error! UTM zone is 0"
            Logger.logMessage(emessage)
            raise Exception(emessage)
        if hemi == "Z":
            emessage = "Error! Hemisphere of UTM zone could not be determined."
            Logger.logMessage(emessage)
            raise Exception(emessage)

        return utmzone, hemi

    @staticmethod
    def utm2ll(utmn,utme,zone):
        zonenum = UTM.getZone(zone)
        zonehemi = UTM.getBand(zone)
        zonetxt = UTM.zonetxt(zonenum)
                
        if zonehemi == "N":
            epsg = 'epsg:326'+str(zonetxt)
        else:
            epsg = 'epsg:327'+str(zonetxt)

        transformer = UTM.getTransformer(epsg, 'epsg:4326')
        lon,lat = transformer.transform(utme, utmn)
        return lat, lon

    @staticmethod
    def ll2utm(lat,lon):
        
        zone = int((lon + 180)/6 + 1)
        zonetxt = UTM.zonetxt(zone)
        
        if lat < 0:
            hemi = "S"
        else:
            hemi = "N"  
        
        if hemi == "N":
            epsg = 'epsg:326'+str(zonetxt)
        else:
            epsg = 'epsg:327'+str(zonetxt)

        transformer = UTM.getTransformer('epsg:4326', epsg)

        # Use the cached transformer to perform the transformation more quickly!
        # see https://pyproj4.github.io/pyproj/stable/advanced_examples.html#optimize-transformations
        utme, utmn = transformer.transform(lon, lat)

        utme = round(utme)
        utmn = round(utmn)
        
        return utmn, utme, zone, hemi, epsg
        

    @staticmethod
    def ll2utm_alt(lat,lon,zoneUsed, hemiUsed):
        realN, realE, realZone, realHemi, realepsg = UTM.ll2utm(lat,lon)
        if zoneUsed == realZone:
            return realN, realE
        else:
            if realZone > zoneUsed:
                epsgUsed = 'epsg:' + str(int(realepsg.split(sep=':')[1]) - 1)
            else:
                epsgUsed = 'epsg:' + str(int(realepsg.split(sep=':')[1]) + 1)
                
            if zoneUsed == 60 and realHemi == "N":
                epsgUsed = "epsg:32660"
            if zoneUsed == 60 and realHemi == "S":
                epsgUsed = "epsg:32760"

            transformer = UTM.getTransformer(realepsg, epsgUsed)
            utme, utmn = transformer.transform(realE, realN)
            return round(utmn), round(utme)
  
    @staticmethod
    def center(sourcelocs, facutmznum, fachemi):

        """
        This method computes the center of a facility from the emission
        location UTM coordinates. The overall facility UTM zone and hemisphere are needed.
        """
                
        # Fill up lists of x and y coordinates of all source vertices    
        vertx_l = []
        verty_l = []
        for index, row in sourcelocs.iterrows():
    
            vertx_l.append(row["utme"])
            verty_l.append(row["utmn"])
    
            # If this is an area source, add the other 3 corners to vertex list
            if row["source_type"].upper() == "A":
                angle_rad = m.radians(row["angle"])
                utme1 = row["utme"] + row["lengthx"] * m.cos(angle_rad)
                utmn1 = row["utmn"] - row["lengthx"] * m.sin(angle_rad)
                utme2 = (row["utme"] + (row["lengthx"] * m.cos(angle_rad)) +
                         (row["lengthy"] * m.sin(angle_rad)))
                utmn2 = (row["utmn"] + (row["lengthy"] * m.cos(angle_rad)) -
                         (row["lengthx"] * m.sin(angle_rad)))
                utme3 = row["utme"] + row["lengthy"] * m.sin(angle_rad)
                utmn3 = row["utmn"] + row["lengthy"] * m.cos(angle_rad)
                vertx_l.append(utme1)
                vertx_l.append(utme2)
                vertx_l.append(utme3)
                verty_l.append(utmn1)
                verty_l.append(utmn2)
                verty_l.append(utmn3)
    
            # If this is a volume source, then add the vertices of it
            if row["source_type"].upper() == "V":
                utme1 = row["utme"] + row["lengthx"] * m.sqrt(2)/2
                utmn1 = row["utmn"] - row["lengthy"] * m.sqrt(2)/2
                utme2 = row["utme"] + row["lengthx"] * m.sqrt(2)/2
                utmn2 = row["utmn"] + row["lengthy"] * m.sqrt(2)/2
                utme3 = row["utme"] - row["lengthx"] * m.sqrt(2)/2
                utmn3 = row["utmn"] + row["lengthy"] * m.sqrt(2)/2
                vertx_l.append(utme1)
                vertx_l.append(utme2)
                vertx_l.append(utme3)
                verty_l.append(utmn1)
                verty_l.append(utmn2)
                verty_l.append(utmn3)
    
            # If line or buoyant line source, add second vertex
            if row["source_type"].upper() == "N" or row["source_type"].upper() == "B":
                vertx_l.append(row["utme_x2"])
                verty_l.append(row["utmn_y2"])            
    
        vertx_a = np.array(vertx_l)
        verty_a = np.array(verty_l)

        
        # Combine the x and y vertices lists into list of tuples and then get a
        # unique list of vertices of the form (x, y) where x=utme and y=utmn
        sourceverts = list(zip(vertx_l, verty_l))
        unique_verts = list(set(sourceverts))
    
        
        # Find the two vertices that are the farthest apart
        # Also find the corners of the modeling domain
    
        max_dist = 0
        max_x = min_x = vertx_a[0]
        max_y = min_y = verty_a[0]
    
        if len(unique_verts) > 1: #more than one source coordinate
            
            # initialize
            xmax1 = unique_verts[0][0]
            ymax1 = unique_verts[0][1]
            xmax2 = unique_verts[1][0]
            ymax2 = unique_verts[1][1]
            
            for i in range(0, len(unique_verts)-1):
                
                # corners
                max_x = max(max_x, unique_verts[i][0])
                max_y = max(max_y, unique_verts[i][1])
                min_x = min(min_x, unique_verts[i][0])
                min_y = min(min_y, unique_verts[i][1])
                
                # find farthest apart
                j = i + 1
                for k in range(j, len(unique_verts)):
                    dist = m.sqrt((unique_verts[i][0] - unique_verts[k][0])**2 + 
                                  (unique_verts[i][1] - unique_verts[k][1])**2)
                    if dist > max_dist:
                        max_dist = dist
                        xmax1 = unique_verts[i][0]
                        ymax1 = unique_verts[i][1]
                        xmax2 = unique_verts[k][0]
                        ymax2 = unique_verts[k][1]
                                    
            # Calculate the center of the facility in utm coordinates
            cenx = round((xmax1 + xmax2) / 2)
            ceny = round((ymax1 + ymax2) / 2)
            
        else: #single source coordinate
    
            # Calculate the center of the facility in utm coordinates
            cenx = round(max_x)
            ceny = round(max_y)


        # Compute the lat/lon of the center
        utmz = str(facutmznum) + fachemi
        cenlat, cenlon = UTM.utm2ll(ceny, cenx, utmz)
     
        return cenx, ceny, cenlon, cenlat, max_dist, vertx_a, verty_a

    # This method returns the correct transformer to use. It comes either from
    # the cache (if it's been requested previously) or from the Transformer instantiation
    # method. Note that the transformers cache is keyed by the concatentation of the two
    # projection epsg values.
    @staticmethod
    def getTransformer(epsg1, epsg2):
        key = epsg1 + epsg2
        if key in transformers:
            transformer = transformers[key]
        else:
            if epsg1 in projections:
                p1 = projections[epsg1]
            else:
                p1 = Proj(init = epsg1)
                projections[epsg1] = p1

            if epsg2 in projections:
                p2 = projections[epsg2]
            else:
                p2 = Proj(init = epsg2)
                projections[epsg2] = p2

            transformer = Transformer.from_proj(p1, p2)
            transformers[key] = transformer

        return transformer

