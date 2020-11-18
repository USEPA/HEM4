# -*- coding: utf-8 -*-
"""
Created on Wed Nov  1 13:56:59 2017
@author: sfudge, cstolte
"""
import pandas as pd
import numpy as np
from fastkml import kml, SchemaData, Data
from fastkml.geometry import Geometry, Point, Polygon
from fastkml import ExtendedData
from fastkml.styles import LineStyle, PolyStyle, IconStyle, LabelStyle, BalloonStyle
from lxml.etree import CDATA
from pygeoif import LinearRing, LineString
from xml.sax.saxutils import unescape
from operator import itemgetter
from collections import OrderedDict
import zipfile
from os.path import basename
import math
import os
from itertools import combinations

from com.sca.hem4.support.UTM import UTM


class KMLWriter():
    """
    Creates KMZ files suitable for viewing in Google Earth.
    """

    def __init__(self):
        self.ns = "{http://www.opengis.net/kml/2.2}"

    def write_kml_emis_loc(self, model):
        """
        Create KMZ of all sources from all facilities. 
        """            
        
        # Define the name of the output kml file
        allkml_fname = model.rootoutput + "AllFacility_source_locations.kml"
       
        # Create a dataframe of emission source locations for all facilities being modeled
        srcmap = self.create_sourcemap(model, None)
        
        # Define kml object
        kml_source_loc = kml.KML()

        document = kml.Document(ns=self.ns, id='emisloc', name='srcmap', description='Exported from HEM4')
        document.isopen = 1

        # Schema
        schema = kml.Schema(ns=self.ns, id="srcmap_schema", name="srcmap")
        schema.append("string", "Source_id", "Sourceid")
        document.append_schema(schema)

        # Areasrc style...
        document.append_style(self.getAreaSrcStyle())

        # Ptsrc style...
        document.append_style(self.getPtSrcStyle())

        # center style...
        document.append_style(self.getCenterStyle())
        
        # Iterate over srcmap DF to get facility id, source ids, source type and location parameters
        for facid, group in srcmap.groupby(["fac_id"]):
            
            # Subset srcmap to this facility
            sub_map = srcmap.loc[srcmap.fac_id==facid]

            # Determine the center of the facility. If provided by the user, use it. Otherwise compute an
            # average from the emission source locations (lat/lons)
            fcenter = model.faclist.dataframe[model.faclist.dataframe['fac_id']==facid]['fac_center'].iloc[0]
            if fcenter != "":
                
                # User supplied
                components = fcenter.split(',')
                if components[0] == "L":
                    avglat = float(components[1])
                    avglon = float(components[2])
                else:
                    ceny = int(float(components[1]))
                    cenx = int(float(components[2]))   
                    zone = components[3].strip()
                    avglat, avglon = UTM.utm2ll(ceny, cenx, zone)

            else:

                # Not supplied, compute average                
                faclatlons = sub_map[['lat', 'lon']].values.tolist()
                latlon_array = np.array(faclatlons)
                lats = latlon_array[:,0:1]
                lons = latlon_array[:,1:2]
                if (len(np.unique(lats)) > 1) or (len(np.unique(lons)) > 1): #more than one source location
                    maxdist = 0.0
                    for pair in combinations(faclatlons, 2):
                        firstpair = tuple(pair[0])
                        secondpair = tuple(pair[1])
                        d = self.distance(firstpair, secondpair)
                        if d > maxdist:
                            maxdist = d
                            maxpair = pair
                    avglat, avglon = self.midpoint(maxpair[0], maxpair[1])
                                    
                else:
                    avglat = latlon_array[0,0]
                    avglon = latlon_array[0,1]
            
            # Setup an Emission Sources folder for this facility
            name_str = "Facility " + facid + " Emission sources"
            es_folder = kml.Folder(ns=self.ns, name=name_str)
  
            # Facility center placemark
            placemark = kml.Placemark(ns=self.ns, name="Facility center",
                                      description=CDATA("<div align='center'>Center of facility " +
                                                        facid + " </div>"),
                                      styleUrl="#center")
            point = Point(avglon, avglat, 0.0)
            placemark.geometry = Geometry(ns=self.ns, altitude_mode="relativeToGround", geometry=point)
            es_folder.append(placemark)
                      
            for name, group in sub_map.groupby(["source_id","source_type"]):
                sname = name[0]
                stype = name[1]

                # Emission sources  Point, Capped, Horizontal
                if stype == 'P' or stype == 'C' or stype == 'H':

                    placemark = kml.Placemark(ns=self.ns, name=sname,
                                              description=CDATA("<div align='center'>" + sname + "</div>"),
                                              styleUrl="#Ptsrc")

                    point = Point(group.iloc[0]['lon'], group.iloc[0]['lat'], 0.0)
                    placemark.geometry = Geometry(ns=self.ns, altitude_mode="relativeToGround", geometry=point)

                    es_folder.append(placemark)

                # Area, Volume or Polygon
                elif stype == 'A' or stype == 'V' or stype == 'I':
                                        
                    placemark = kml.Placemark(ns=self.ns, name=sname,
                                              description=CDATA("<div align='center'>" + sname + "</div>"),
                                              styleUrl="#Areasrc")
                    
                    simpleData = Data(name="SourceId", value=sname)
                    data = [simpleData]
                    schemaData = SchemaData(ns=self.ns, schema_url="#Source_map_schema", data=data)
                    elements = [schemaData]
                    placemark.extended_data = ExtendedData(ns=self.ns, elements=elements)

                    latlons = []
                    for index, row in group.iterrows():
                        coord = (row["lon"], row["lat"], 0)
                        latlons.append(coord)

                    linearRing = LinearRing(coordinates=latlons)
                    polygon = Polygon(shell=linearRing.coords)
                    placemark.geometry = Geometry(ns=self.ns, extrude=0, altitude_mode="clampToGround", tessellate=1,
                                                  geometry=polygon)

                    es_folder.append(placemark)

                # Line or Bouyant Line
                elif stype == 'N' or stype == 'B':
                    
                    placemark = kml.Placemark(ns=self.ns, name=sname,
                                              description=CDATA("<div align='center'>" + sname + "</div>"),
                                              styleUrl="#Linesrc")

                    ps_style = kml.Style(ns=self.ns)
                    style = LineStyle(ns=self.ns, width=group.iloc[0]['line_width'], color="7c8080ff")
                    ps_style.append_style(style)
                    placemark.append_style(ps_style)

                    lineString = LineString([(group.iloc[0]['lon'], group.iloc[0]['lat']), (group.iloc[0]['lon_x2'],
                                            group.iloc[0]['lat_y2'])])
                    placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=lineString)

                    es_folder.append(placemark)

            # Append emission source folder for this facility
            document.append(es_folder)
        
        # Finished
        kml_source_loc.append(document)
        # Write the KML file
        self.writeToFile(allkml_fname, kml_source_loc)
        
        # Create KMZ file
        kmztype = 'allsources'
        allkmz_fname = allkml_fname.replace('.kml', '.kmz')
        self.createKMZ(kmztype, allkml_fname, allkmz_fname)

    def write_facility_kml(self, facid, faccen_lat, faccen_lon, outdir, model):
        """
        Create the source/risk KML file for a given facility
        """

        # Define the name of the output kml file
        fackml_fname = outdir + str(facid) + "_source_risk.kml"
        
        # Setup a dictionary to hold the real names of the TOSHIs
        hinames = {'hi_resp':'respiratory', 'hi_live':'liver', 'hi_neur':'neurological',
                  'hi_deve':'developmental', 'hi_repr':'reproductive', 'hi_kidn':'kidney', 
                  'hi_ocul':'ocular', 'hi_endo':'endocrine', 'hi_hema':'hematological',
                  'hi_immu':'immunological', 'hi_skel':'skeletal', 'hi_sple':'spleen',
                  'hi_thyr':'thyroid', 'hi_whol':'wholebody'}


        # Create a dataframe of polar receptor risk by receptor and pollutant        
        polarsum = model.all_polar_receptors_df.groupby(['distance', 'angle', 'lat', 'lon', 'pollutant'],
                                                        as_index=False)[['conc']].sum()
        polarmerge1  = polarsum.merge(model.haplib.dataframe, on='pollutant')[['distance', 'angle',
                                     'lat','lon','pollutant','conc','ure','rfc']]
        polarmerge2  = polarmerge1.merge(model.organs.dataframe, on='pollutant', how='left')[['distance', 'angle',
                                     'lat','lon','pollutant','conc','ure','rfc','resp','liver',
                                     'neuro','dev','reprod','kidney','ocular','endoc','hemato','immune',
                                     'skeletal','spleen','thyroid','wholebod']]
        polarmerge2['risk'] = polarmerge2['conc'] * polarmerge2['ure']
        hilist = (('hi_resp','resp'), ('hi_live','liver'), ('hi_neur','neuro'), ('hi_deve','dev'),
                  ('hi_repr','reprod'), ('hi_kidn','kidney'), ('hi_ocul','ocular'), ('hi_endo','endoc'),
                  ('hi_hema','hemato'), ('hi_immu','immune'), ('hi_skel','skeletal'), ('hi_sple','spleen'),
                  ('hi_thyr','thyroid'), ('hi_whol','wholebod'))
        for his in hilist:
            polarmerge2[his[0]] = polarmerge2.apply(lambda row: self.calcHI(row['conc'], 
                                 row['rfc'], row[his[1]]), axis=1)

        # Create a dataframe of inner receptor risk by receptor and pollutant
        innersum = model.all_inner_receptors_df.groupby(['fips', 'block', 'lat', 'lon', 'pollutant'],
                                                        as_index=False)[['conc']].sum()
        if not innersum.empty:
            innermerge1  = innersum.merge(model.haplib.dataframe, on='pollutant')[['fips','block'
                                         ,'lat','lon','pollutant','conc','ure','rfc']]
            innermerge2  = innermerge1.merge(model.organs.dataframe, on='pollutant', how='left')[['fips','block',
                                         'lat','lon','pollutant','conc','ure','rfc','resp',
                                         'liver','neuro','dev','reprod','kidney','ocular',
                                         'endoc','hemato','immune','skeletal','spleen',
                                         'thyroid','wholebod']]
            innermerge2['risk'] = innermerge2['conc'] * innermerge2['ure']
            for his in hilist:
                innermerge2[his[0]] = innermerge2.apply(lambda row: self.calcHI(row['conc'], 
                                     row['rfc'], row[his[1]]), axis=1)

                
        # Create KML object and define the document
        fac_kml = kml.KML()        
        docWithHeader = self.createDocumentWithHeader()

        # Create the sourcemap dataframe for this facility (emission source locations)
        srcmap = self.create_sourcemap(model, facid)

        # Define an emission source folder and populate it
        es_folder = kml.Folder(ns=self.ns, name="Emission sources")
        
        for name, group in srcmap.groupby(["source_id","source_type"]):
            sname = name[0]
            stype = name[1]

            # Emission sources  Point, Capped, Horizontal
            if stype == 'P' or stype == 'C' or stype == 'H':

                placemark = kml.Placemark(ns=self.ns, name=sname,
                                          description=CDATA("<div align='center'>" + sname + "</div>"),
                                          styleUrl="#Ptsrc")

                point = Point(group.iloc[0]['lon'], group.iloc[0]['lat'], 0.0)
                placemark.geometry = Geometry(ns=self.ns, altitude_mode="relativeToGround", geometry=point)

                es_folder.append(placemark)

            # Area, Volume or Polygon
            elif stype == 'A' or stype == 'V' or stype == 'I':

                placemark = kml.Placemark(ns=self.ns, name=sname,
                                          description=CDATA("<div align='center'>" + sname + "</div>"),
                                          styleUrl="#Areasrc")

                simpleData = Data(name="SourceId", value=sname)
                data = [simpleData]
                schemaData = SchemaData(ns=self.ns, schema_url="#Source_map_schema", data=data)
                elements = [schemaData]
                placemark.extended_data = ExtendedData(ns=self.ns, elements=elements)

                latlons = []
                for index, row in group.iterrows():
                    coord = (row["lon"], row["lat"], 0)
                    latlons.append(coord)
                    
                linearRing = LinearRing(coordinates=latlons)                               
                polygon = Polygon(shell=linearRing.coords)
                placemark.geometry = Geometry(ns=self.ns, extrude=0, altitude_mode="clampToGround", tessellate=1,
                                              geometry=polygon)

                es_folder.append(placemark)

            # Line or Bouyant Line
            elif stype == 'N' or stype == 'B':

                placemark = kml.Placemark(ns=self.ns, name=sname,
                                          description=CDATA("<div align='center'>" + sname + "</div>"),
                                          styleUrl="#Linesrc")

                ps_style = kml.Style(ns=self.ns)
                style = LineStyle(ns=self.ns, width=group.iloc[0]['line_width'], color="7c8080ff")
                ps_style.append_style(style)
                placemark.append_style(ps_style)

                lineString = LineString([(group.iloc[0]['lon'], group.iloc[0]['lat']), (group.iloc[0]['lon_x2'],
                                        group.iloc[0]['lat_y2'])])
                placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=lineString)

                es_folder.append(placemark)

        docWithHeader.append(es_folder)

        # Facility center folder
        cen_folder = kml.Folder(ns=self.ns, name="Domain center")
        cen_folder.isopen = 0

        placemark = kml.Placemark(ns=self.ns, name="Domain center",
                                  description=CDATA("<div align='center'>Plant center</div>"),
                                  styleUrl="#center")
        point = Point(faccen_lon, faccen_lat, 0.0)
        placemark.geometry = Geometry(ns=self.ns, altitude_mode="relativeToGround", geometry=point)
        cen_folder.append(placemark)
        docWithHeader.append(cen_folder)

        # Facility MIR folder (only display if MIR > 0)
        mir_info = model.max_indiv_risk_df[model.max_indiv_risk_df['parameter']=='Cancer risk'][[
                                            'parameter','value','distance','rec_type',
                                            'lat','lon']].reset_index(drop=True)
        mirrnd = round(mir_info.at[0,'value']*1000000, 2)
        mirtype = mir_info.at[0,'rec_type']
        mirdist = round(mir_info.at[0,'distance'], 2)
        mirlat = mir_info.at[0,'lat']
        mirlon = mir_info.at[0,'lon']

        if mirrnd > 0:
            mir_folder = kml.Folder(ns=self.ns, name="MIR")
            mir_folder.isopen = 0
            placemark = kml.Placemark(ns=self.ns, name="MIR",
                                      description=CDATA("<div align='center'><B>MIR Receptor</B><br />" + \
                                      "<B>Receptor type: "+mirtype+"</B><br />" + \
                                      "<B>Distance from facility (m): "+str(mirdist)+"</B><br /><br />" + \
                                      "MIR (in a million) = " +str(mirrnd)+"<br /></div>"),
                                      styleUrl="#mir")
            point = Point(mirlon, mirlat, 0.0)
            placemark.geometry = Geometry(ns=self.ns, altitude_mode="relativeToGround", geometry=point)
            mir_folder.append(placemark)
            docWithHeader.append(mir_folder)

 
        # Make sure there are inner receptors       
        if not innersum.empty:
        
            #-------------- User receptor cancer risk -------------------------------------
            urec_df = innermerge2.loc[innermerge2['block'].str.upper().str.contains('U')]
            if not urec_df.empty:
                urcr_folder = kml.Folder(ns=self.ns, name="User receptor cancer risk")
                urcr_folder.isopen = 0
                for block, group in urec_df.groupby(["block"]):
                    ublock = group.iloc[0]['block']
                    ulat = group.iloc[0]['lat']
                    ulon = group.iloc[0]['lon']
    
                    urtot = group['risk'].sum() * 1000000
                    urrnd = round(urtot,2)
                    
                    description = "<div align='center'><B>User Receptor</B> <br />" + \
                                  "<B> ID: " + ublock + "</B> <br /> \n" + \
                                  "<B> HEM4 Estimated Cancer Risk (in a million) </B> <br /> \n" + \
                                  "    " + "Total = " + str(urrnd) + "<br /><br /> \n"
                    if urrnd > 0:
                        description += "    " + "<U> Top Pollutants Contributing to Total Cancer Risk </U> <br /> \n"
                        # create dictionary to hold summed risk of each pollutant
                        urhap_sum = {}
                        for index, row in group.iterrows():
                            if row["pollutant"] not in urhap_sum:
                                urhap_sum[row["pollutant"]] = row["risk"]
                            else:
                                pol = urhap_sum[row["pollutant"]]
                                risksum = row["risk"] + pol
                                urhap_sum[row["pollutant"]] = risksum
    
                        #sort the dictionary by descending value
                        sorted_urhap_sum = OrderedDict(sorted(urhap_sum.items(), key=itemgetter(1),
                                                              reverse=True))
    
                        # check to make sure large enough value to keep
                        for k, x in sorted_urhap_sum.items():
                            z = round(x*1000000, 2)     # risk in a million
                            if z > 0.005:
                                description = description + "    " + format(k) + " = " + format(z) + "<br /> \n"
    
                    description += "</div>"
                    
                    # Choose style based on risk level
                    if urrnd <= 20:
                        styletag = "#u20"
                    elif (urrnd > 20) & (urrnd < 100):
                        styletag = "#u20to100"
                    else:
                        styletag = "#u100"
                    
                    ur_placemark = kml.Placemark(ns=self.ns, name="User Receptor: " + ublock,
                                                 description=CDATA(description),
                                                 styleUrl=styletag)
                    point = Point(ulon, ulat, 0.000)
                    ur_placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
                    urcr_folder.append(ur_placemark)
    
                docWithHeader.append(urcr_folder)


            #------------------------ User receptor TOSHI -----------------------------
            if not urec_df.empty:
                                
                urt_folder = kml.Folder(ns=self.ns, name="User receptor TOSHI")
                urt_folder.isopen = 0
                for block, group in urec_df.groupby(["block"]):
                    ublock = group.iloc[0]['block']
                    ulat = group.iloc[0]['lat']
                    ulon = group.iloc[0]['lon']
    
                    # Sum each toshi in this group
                    ug = group[['hi_resp','hi_live','hi_neur','hi_deve','hi_repr','hi_kidn',
                                'hi_ocul','hi_endo','hi_hema','hi_immu','hi_skel','hi_sple',
                                'hi_thyr','hi_whol']].sum(axis=0)
                    
                    # Identify the max toshi
                    maxi = ug.idxmax()
                    maxtoshi = hinames[maxi]
                    maxtoshival = round(ug[maxi], 2)
    
                    
                    description = "<div align='center'><B> User Receptor</B> <br /> \n" + \
                                  "    " + "<B> ID: " + ublock + "</B> <br /> \n" + \
                                  "    " + "<B> HEM4 Estimated Maximum TOSHI (" + maxtoshi + ") </B> <br /> \n" + \
                                  "    " + "Total = " + str(maxtoshival) + "<br /><br /> \n"
                                  
                    if maxtoshival > 0:
                        description += "    " + "<U> Top Pollutants Contributing to TOSHI </U> <br /> \n"
                        # create dictionary to hold summed non-cancer of each pollutant
                        urhap_sum = {}
                        for index, row in group.iterrows():
                            if row["pollutant"] not in urhap_sum:
                                urhap_sum[row["pollutant"]] = row[maxi]
                            else:
                                pol = urhap_sum[row["pollutant"]]
                                risksum = row[maxi] + pol
                                urhap_sum[row["pollutant"]] = risksum
    
                        #sort the dictionary by descending value
                        sorted_urhap_sum = OrderedDict(sorted(urhap_sum.items(), key=itemgetter(1),
                                                              reverse=True))
    
                        # check to make sure large enough value to keep
                        for k, x in sorted_urhap_sum.items():
                            z = round(x, 3)
                            if z > 0.001:
                                description = description + "    " + format(k) + " = " + format(z) + "<br /> \n"
    
                    description += "</div>"
                    
                    # Choose style based on risk level
                    if maxtoshival <= 1:
                        styletag = "#u20"
                    elif (maxtoshival > 1) & (maxtoshival < 10):
                        styletag = "#u20to100"
                    else:
                        styletag = "#u100"
                    
                    ur_placemark = kml.Placemark(ns=self.ns, name="User Receptor: " + ublock,
                                                 description=CDATA(description),
                                                 styleUrl=styletag)
                    point = Point(ulon, ulat, 0.000)
                    ur_placemark.visibility = 0
                    ur_placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
                    urt_folder.append(ur_placemark)
    
                docWithHeader.append(urt_folder)


            #------------------ Inner census block receptor cancer risk ------------------------------
            ir_folder = kml.Folder(ns=self.ns, name="Census block cancer risk")
            ir_folder.isopen = 0
            
            # Exclude user receptors
            cblks = innermerge2.loc[~innermerge2['block'].str.upper().str.contains('U')]
            for loc, group in cblks.groupby(["lat","lon"]):
                slat = loc[0]
                slon = loc[1]
                sBlock = group.iloc[0]['block']
    
                cbtot = group['risk'].sum() * 1000000
                cbrnd = round(cbtot,2)
    
                description = "<div align='center'><B> Census Block Receptor</B> <br /> \n" + \
                              "    " + "<B> Block: " + str(group.iloc[0]['block']) + "</B> <br /> \n" + \
                              "    " + "<B> HEM4 Estimated Cancer Risk (in a million) </B> <br /> \n" + \
                              "    " + "Total = " + str(cbrnd) + "<br /><br /> \n"
    
                if cbrnd > 0:
                    description += "    " + "<U> Top Pollutants Contributing to Total Cancer Risk </U> <br /> \n"
    
                    # create dictionary to hold summed risk of each pollutant
                    cbhap_sum = {}
                    for index, row in group.iterrows():
    
                        #keys = cbhap_sum.keys()
                        if row["pollutant"] not in cbhap_sum:
                            cbhap_sum[row["pollutant"]] = row["risk"]
                            #inp_file.write(row["pollutant"])
                        else:
                            pol = cbhap_sum[row["pollutant"]]
                            risksum = row["risk"] + pol
                            cbhap_sum[row["pollutant"]] = risksum
    
                    #sort the dictionary by descending value
                    sorted_cbhap_sum = OrderedDict(sorted(cbhap_sum.items(), key=itemgetter(1),
                                                          reverse=True))
    
                    # check to make sure large enough value to keep
                    for k, v in sorted_cbhap_sum.items():
                        w = round(v*1000000, 2)     # risk in a million
                        if w > 0.005:
                            description += "    " + format(k) + " = " + format(w) + "<br /> \n"
    
                description += "</div>"
    
                # Choose style based on risk level
                if cbrnd <= 20:
                    styletag = "#b20"
                elif (cbrnd > 20) & (cbrnd < 100):
                    styletag = "#b20to100"
                else:
                    styletag = "#b100"
                    
                point = Point(slon, slat, 0.0000)
                placemark = kml.Placemark(ns=self.ns, name="Block Receptor " + str(sBlock),
                                          description=CDATA(description),
                                          styleUrl=styletag)
                placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
                ir_folder.append(placemark)
    
            docWithHeader.append(ir_folder)
    
    
            #------------------------ Inner census block receptor TOSHI -----------------------------
            irt_folder = kml.Folder(ns=self.ns, name="Census block TOSHI")
            irt_folder.isopen = 0
    
            for loc, group in cblks.groupby(["lat","lon"]):
                slat = loc[0]
                slon = loc[1]
                sfips = group.iloc[0]['fips']
                sblock = group.iloc[0]['block']
    
                # Sum each toshi in this group
                sg = group[['hi_resp','hi_live','hi_neur','hi_deve','hi_repr','hi_kidn',
                            'hi_ocul','hi_endo','hi_hema','hi_immu','hi_skel','hi_sple',
                            'hi_thyr','hi_whol']].sum(axis=0)
                
                # Identify the max toshi
                maxi = sg.idxmax()
                maxtoshi = hinames[maxi]
                maxtoshival = round(sg[maxi], 2)
                            
                description = "<div align='center'><B> Census Block Receptor</B> <br /> \n" + \
                              "    " + "<B> FIPs: " + sfips + " Block: " + sblock + "</B> <br /> \n" + \
                              "    " + "<B> HEM4 Estimated Maximum TOSHI (" + maxtoshi + ") </B> <br /> \n" + \
                              "    " + "Total = " + str(maxtoshival) + "<br /><br /> \n"
    
                if maxtoshival > 0:
                    description += "    " + "<U> Top Pollutants Contributing to TOSHI </U> <br /> \n"
    
                    # create dictionary to hold summed non-cancer of each pollutant
                    cbhap_sum = {}
                    for index, row in group.iterrows():
    
                        #keys = cbhap_sum.keys()
                        if row["pollutant"] not in cbhap_sum:
                            cbhap_sum[row["pollutant"]] = row[maxi]
                            #inp_file.write(row["pollutant"])
                        else:
                            pol = cbhap_sum[row["pollutant"]]
                            risksum = row[maxi] + pol
                            cbhap_sum[row["pollutant"]] = risksum
    
                    #sort the dictionary by descending value
                    sorted_cbhap_sum = OrderedDict(sorted(cbhap_sum.items(), key=itemgetter(1),
                                                          reverse=True))
    
                    # check to make sure large enough value to keep
                    for k, v in sorted_cbhap_sum.items():
                        if v > 0.001:
                            vrnd = round(v,3)
                            description += "    " + format(k) + " = " + format(vrnd) + "<br /> \n"
    
                description += "</div>"
    
                # Choose style based on HI level
                if maxtoshival <= 1:
                    styletag = "#b20"
                elif (maxtoshival > 1) & (maxtoshival < 10):
                    styletag = "#b20to100"
                else:
                    styletag = "#b100"
    
                point = Point(slon, slat, 0.0000)
                placemark = kml.Placemark(ns=self.ns, name="Block Receptor " + str(sBlock),
                                          description=CDATA(description),
                                          styleUrl=styletag)
                placemark.visibility = 0
                placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
                irt_folder.append(placemark)
    
            docWithHeader.append(irt_folder)


        #---------------- Polar receptor cancer risk ------------------------------------
        pr_folder = kml.Folder(ns=self.ns, name="Polar receptor cancer risk")
        pr_folder.isopen = 0

        for loc, group in polarmerge2.groupby(["lat","lon"]):
            slat = loc[0]
            slon = loc[1]
            pg_dist = str(round(group.iloc[0]['distance'],0))
            pg_angle = str(round(group.iloc[0]['angle'],0))

            pgtot = group['risk'].sum() * 1000000
            pgrnd = round(pgtot,2)

            description = "<div align='center'><B> Polar Receptor</B> <br />" + \
                          "    " + "<B> Distance: " + pg_dist + " Angle: " + pg_angle + "</B> <br /> \n" + \
                          "    " + "<B> HEM4 Estimated Cancer Risk (in a million) </B> <br /> \n" + \
                          "    " + "Total = " + str(pgrnd) + "<br /><br /> \n"

            if pgrnd > 0:
                description += "    " + "<U> Top Pollutants Contributing to Total Cancer Risk </U> <br /> \n"

                # create dictionary to hold summed risk of each pollutant
                pghap_sum = {}
                for index, row in group.iterrows():
                    if row["pollutant"] not in pghap_sum:
                        pghap_sum[row["pollutant"]] = row["risk"]
                    else:
                        pol = pghap_sum[row["pollutant"]]
                        risksum = row["risk"] + pol
                        pghap_sum[row["pollutant"]] = risksum
                        
                #sort the dictionary by descending value
                sorted_pghap_sum = OrderedDict(sorted(pghap_sum.items(), key=itemgetter(1),
                                                      reverse=True))

                # check to make sure large enough value to keep
                for k, v in sorted_pghap_sum.items():
                    z = round(v*1000000, 2)     # risk in a million
                    if z > 0.005:
                        description += "    " + format(k) + " = " + format(z) + "<br /> \n"

            description += "</div>"

            # Choose style based on risk level
            if pgrnd <= 20:
                styletag = "#s20"
            elif (pgrnd > 20) & (pgrnd < 100):
                styletag = "#s20to100"
            else:
                styletag = "#s100"

            point = Point(slon, slat, 0.0000)
            placemark = kml.Placemark(ns=self.ns, name="Polar Receptor Distance: " + pg_dist + " Angle: " + str(group.iloc[0]['angle']),
                                      description=CDATA(description),
                                      styleUrl=styletag)
            placemark.visibility = 0
            placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
            pr_folder.append(placemark)

        docWithHeader.append(pr_folder)


        #---------------- Polar receptor TOSHI -----------------------------------------
        prt_folder = kml.Folder(ns=self.ns, name="Polar TOSHI")
        prt_folder.isopen = 0

        for loc, group in polarmerge2.groupby(["lat","lon"]):
            slat = loc[0]
            slon = loc[1]
            pg_dist = str(round(group.iloc[0]['distance'],0))
            pg_angle = str(round(group.iloc[0]['angle'],0))

            # Sum each toshi in this group
            sg = group[['hi_resp','hi_live','hi_neur','hi_deve','hi_repr','hi_kidn',
                        'hi_ocul','hi_endo','hi_hema','hi_immu','hi_skel','hi_sple',
                        'hi_thyr','hi_whol']].sum(axis=0)
            
            # Identify the max toshi
            maxi = sg.idxmax()
            maxtoshi = hinames[maxi]
            maxtoshival = round(sg[maxi], 2)

            description = "<div align='center'><B> Polar Receptor</B> <br /> \n" + \
                          "    " + "<B> Distance: " + pg_dist + " Angle: " + pg_angle + "</B> <br /> \n" + \
                          "    " + "<B> HEM4 Estimated Max TOSHI (" + maxtoshi + ") </B> <br /> \n" + \
                          "    " + "Total = " + str(maxtoshival) + "<br /><br /> \n"

            if maxtoshival > 0:
                description += "    " + "<U> Top Pollutants Contributing to TOSHI </U> <br /> \n"

                # create dictionary to hold summed toshi of each pollutant
                pghi_sum = {}
                for index, row in group.iterrows():

                    if row["pollutant"] not in pghi_sum:
                        pghi_sum[row["pollutant"]] = row[maxi]
                    else:
                        pol = pghi_sum[row["pollutant"]]
                        toshisum = row[maxi] + pol
                        pghi_sum[row["pollutant"]] = toshisum

                #sort the dictionary by descending value
                sorted_pghi_sum = OrderedDict(sorted(pghi_sum.items(), key=itemgetter(1),
                                                     reverse=True))

                # check to make sure large enough value to keep
                for k, v in sorted_pghi_sum.items():
                    if v > 0.001:
                        vrnd = round(v,3)
                        description += "    " + format(k) + " = " + format(vrnd) + "<br /> \n"

            description += "</div>"

            # Choose style based on HI level
            if maxtoshival <= 1:
                styletag = "#s20"
            elif (maxtoshival > 1) & (maxtoshival < 10):
                styletag = "#s20to100"
            else:
                styletag = "#s100"

            point = Point(slon, slat, 0.0000)
            placemark = kml.Placemark(ns=self.ns, name="Polar Receptor Distance: " + pg_dist + " Angle: " + pg_angle,
                                      description=CDATA(description),
                                      styleUrl=styletag)
            placemark.visibility = 0
            placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
            prt_folder.append(placemark)

        docWithHeader.append(prt_folder)
        fac_kml.append(docWithHeader)
        
        # Finished, write the kml file
        self.writeToFile(fackml_fname, fac_kml)

        # Create KMZ file
        kmztype = 'facilityrisk'
        fackmz_fname = fackml_fname.replace('.kml', '.kmz')
        self.createKMZ(kmztype, fackml_fname, fackmz_fname)


    def write_facility_kml_NonCensus(self, facid, faccen_lat, faccen_lon, outdir, model):
        """
        Create the source/risk KML file for a given facility that used Alternate Receptors
        """
        
        # Define the name of the output kml file
        fackml_fname = outdir + str(facid) + "_source_risk.kml"
        
        # Setup a dictionary to hold the real names of the TOSHIs
        hinames = {'hi_resp':'respiratory', 'hi_live':'liver', 'hi_neur':'neurological',
                  'hi_deve':'developmental', 'hi_repr':'reproductive', 'hi_kidn':'kidney', 
                  'hi_ocul':'ocular', 'hi_endo':'endocrine', 'hi_hema':'hematological',
                  'hi_immu':'immunological', 'hi_skel':'skeletal', 'hi_sple':'spleen',
                  'hi_thyr':'thyroid', 'hi_whol':'wholebody'}


        # Create a dataframe of polar receptor risk by receptor and pollutant        
        polarsum = model.all_polar_receptors_df.groupby(['distance', 'angle', 'lat', 'lon', 'pollutant'],
                                                        as_index=False)[['conc']].sum()
        polarmerge1  = polarsum.merge(model.haplib.dataframe, on='pollutant')[['distance', 'angle',
                                     'lat','lon','pollutant','conc','ure','rfc']]
        polarmerge2  = polarmerge1.merge(model.organs.dataframe, on='pollutant', how='left')[['distance', 'angle',
                                     'lat','lon','pollutant','conc','ure','rfc','resp','liver',
                                     'neuro','dev','reprod','kidney','ocular','endoc','hemato','immune',
                                     'skeletal','spleen','thyroid','wholebod']]
        polarmerge2['risk'] = polarmerge2['conc'] * polarmerge2['ure']
        hilist = (('hi_resp','resp'), ('hi_live','liver'), ('hi_neur','neuro'), ('hi_deve','dev'),
                  ('hi_repr','reprod'), ('hi_kidn','kidney'), ('hi_ocul','ocular'), ('hi_endo','endoc'),
                  ('hi_hema','hemato'), ('hi_immu','immune'), ('hi_skel','skeletal'), ('hi_sple','spleen'),
                  ('hi_thyr','thyroid'), ('hi_whol','wholebod'))
        for his in hilist:
            polarmerge2[his[0]] = polarmerge2.apply(lambda row: self.calcHI(row['conc'], 
                                 row['rfc'], row[his[1]]), axis=1)

        # Create a dataframe of inner receptor risk by receptor and pollutant
        innersum = model.all_inner_receptors_df.groupby(['rec_id', 'lat', 'lon', 'pollutant'],
                                                        as_index=False)[['conc']].sum()

        if not innersum.empty:
            innermerge1  = innersum.merge(model.haplib.dataframe, on='pollutant')[['rec_id',
                                          'lat','lon','pollutant','conc','ure','rfc']]
            innermerge2  = innermerge1.merge(model.organs.dataframe, on='pollutant', how='left')[['rec_id',
                                         'lat','lon','pollutant','conc','ure','rfc','resp',
                                         'liver','neuro','dev','reprod','kidney','ocular',
                                         'endoc','hemato','immune','skeletal','spleen',
                                         'thyroid','wholebod']]
            innermerge2['risk'] = innermerge2['conc'] * innermerge2['ure']
            for his in hilist:
                innermerge2[his[0]] = innermerge2.apply(lambda row: self.calcHI(row['conc'], 
                                     row['rfc'], row[his[1]]), axis=1)

                
        # Create KML object and define the document
        fac_kml = kml.KML()        
        docWithHeader = self.createDocumentWithHeader()

        # Create the sourcemap dataframe for this facility (emission source locations)
        srcmap = self.create_sourcemap(model, facid)

        # Define an emission source folder and populate it
        es_folder = kml.Folder(ns=self.ns, name="Emission sources")
        
        for name, group in srcmap.groupby(["source_id","source_type"]):
            sname = name[0]
            stype = name[1]

            # Emission sources  Point, Capped, Horizontal
            if stype == 'P' or stype == 'C' or stype == 'H':

                placemark = kml.Placemark(ns=self.ns, name=sname,
                                          description=CDATA("<div align='center'>" + sname + "</div>"),
                                          styleUrl="#Ptsrc")

                point = Point(group.iloc[0]['lon'], group.iloc[0]['lat'], 0.0)
                placemark.geometry = Geometry(ns=self.ns, altitude_mode="relativeToGround", geometry=point)

                es_folder.append(placemark)

            # Area, Volume or Polygon
            elif stype == 'A' or stype == 'V' or stype == 'I':

                placemark = kml.Placemark(ns=self.ns, name=sname,
                                          description=CDATA("<div align='center'>" + sname + "</div>"),
                                          styleUrl="#Areasrc")

                simpleData = Data(name="SourceId", value=sname)
                data = [simpleData]
                schemaData = SchemaData(ns=self.ns, schema_url="#Source_map_schema", data=data)
                elements = [schemaData]
                placemark.extended_data = ExtendedData(ns=self.ns, elements=elements)

                latlons = []
                for index, row in group.iterrows():
                    coord = (row["lon"], row["lat"], 0)
                    latlons.append(coord)
                    
                linearRing = LinearRing(coordinates=latlons)                               
                polygon = Polygon(shell=linearRing.coords)
                placemark.geometry = Geometry(ns=self.ns, extrude=0, altitude_mode="clampToGround", tessellate=1,
                                              geometry=polygon)

                es_folder.append(placemark)

            # Line or Bouyant Line
            elif stype == 'N' or stype == 'B':

                placemark = kml.Placemark(ns=self.ns, name=sname,
                                          description=CDATA("<div align='center'>" + sname + "</div>"),
                                          styleUrl="#Linesrc")

                ps_style = kml.Style(ns=self.ns)
                style = LineStyle(ns=self.ns, width=group.iloc[0]['line_width'], color="7c8080ff")
                ps_style.append_style(style)
                placemark.append_style(ps_style)

                lineString = LineString([(group.iloc[0]['lon'], group.iloc[0]['lat']), (group.iloc[0]['lon_x2'],
                                        group.iloc[0]['lat_y2'])])
                placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=lineString)

                es_folder.append(placemark)

        docWithHeader.append(es_folder)

        # Facility center folder
        cen_folder = kml.Folder(ns=self.ns, name="Domain center")
        cen_folder.isopen = 0

        placemark = kml.Placemark(ns=self.ns, name="Domain center",
                                  description=CDATA("<div align='center'>Plant center</div>"),
                                  styleUrl="#center")
        point = Point(faccen_lon, faccen_lat, 0.0)
        placemark.geometry = Geometry(ns=self.ns, altitude_mode="relativeToGround", geometry=point)
        cen_folder.append(placemark)
        docWithHeader.append(cen_folder)

        # Facility MIR folder (only display if MIR > 0)
        mir_info = model.max_indiv_risk_df[model.max_indiv_risk_df['parameter']=='Cancer risk'][[
                                            'parameter','value','distance','rec_type',
                                            'lat','lon']].reset_index(drop=True)
        mirrnd = round(mir_info.at[0,'value']*1000000, 2)
        mirtype = mir_info.at[0,'rec_type']
        mirdist = round(mir_info.at[0,'distance'], 2)
        mirlat = mir_info.at[0,'lat']
        mirlon = mir_info.at[0,'lon']

        if mirrnd > 0:
            mir_folder = kml.Folder(ns=self.ns, name="MIR")
            mir_folder.isopen = 0
            placemark = kml.Placemark(ns=self.ns, name="MIR",
                                      description=CDATA("<div align='center'><B>MIR Receptor</B><br />" + \
                                      "<B>Receptor type: "+mirtype+"</B><br />" + \
                                      "<B>Distance from facility (m): "+str(mirdist)+"</B><br /><br />" + \
                                      "MIR (in a million) = " +str(mirrnd)+"<br /></div>"),
                                      styleUrl="#mir")
            point = Point(mirlon, mirlat, 0.0)
            placemark.geometry = Geometry(ns=self.ns, altitude_mode="relativeToGround", geometry=point)
            mir_folder.append(placemark)
            docWithHeader.append(mir_folder)
        
        
        # Make sure there are inner receptors
        if not innersum.empty:
        
            #-------------- User receptor cancer risk -------------------------------------        
            urec_df = innermerge2.loc[innermerge2['rec_id'].str.upper().str.contains('U')]
            if not urec_df.empty:
                urcr_folder = kml.Folder(ns=self.ns, name="User receptor cancer risk")
                urcr_folder.isopen = 0
                for block, group in urec_df.groupby(['rec_id']):
                    uid = group.iloc[0]['rec_id']
                    ulat = group.iloc[0]['lat']
                    ulon = group.iloc[0]['lon']
    
                    urtot = group['risk'].sum() * 1000000
                    urrnd = round(urtot,2)
                    
                    description = "<div align='center'><B>User Receptor</B> <br />" + \
                                  "<B> ID: " + uid + "</B> <br /> \n" + \
                                  "<B> HEM4 Estimated Cancer Risk (in a million) </B> <br /> \n" + \
                                  "    " + "Total = " + str(urrnd) + "<br /><br /> \n"
                    if urrnd > 0:
                        description += "    " + "<U> Top Pollutants Contributing to Total Cancer Risk </U> <br /> \n"
                        # create dictionary to hold summed risk of each pollutant
                        urhap_sum = {}
                        for index, row in group.iterrows():
                            if row["pollutant"] not in urhap_sum:
                                urhap_sum[row["pollutant"]] = row["risk"]
                            else:
                                pol = urhap_sum[row["pollutant"]]
                                risksum = row["risk"] + pol
                                urhap_sum[row["pollutant"]] = risksum
    
                        #sort the dictionary by descending value
                        sorted_urhap_sum = OrderedDict(sorted(urhap_sum.items(), key=itemgetter(1),
                                                              reverse=True))
    
                        # check to make sure large enough value to keep
                        for k, x in sorted_urhap_sum.items():
                            z = round(x*1000000, 2)     # risk in a million
                            if z > 0.005:
                                description = description + "    " + format(k) + " = " + format(z) + "<br /> \n"
    
                    description += "</div>"
                    
                    # Choose style based on risk level
                    if urrnd <= 20:
                        styletag = "#u20"
                    elif (urrnd > 20) & (urrnd < 100):
                        styletag = "#u20to100"
                    else:
                        styletag = "#u100"
                    
                    ur_placemark = kml.Placemark(ns=self.ns, name="User Receptor: " + uid,
                                                 description=CDATA(description),
                                                 styleUrl=styletag)
                    point = Point(ulon, ulat, 0.000)
                    ur_placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
                    urcr_folder.append(ur_placemark)
    
                docWithHeader.append(urcr_folder)
    
    
            #------------------------ User receptor TOSHI -----------------------------
            if not urec_df.empty:
                urt_folder = kml.Folder(ns=self.ns, name="User receptor TOSHI")
                urt_folder.isopen = 0
                for block, group in urec_df.groupby(['rec_id']):
                    uid = group.iloc[0]['rec_id']
                    ulat = group.iloc[0]['lat']
                    ulon = group.iloc[0]['lon']
    
                    # Sum each toshi in this group
                    ug = group[['hi_resp','hi_live','hi_neur','hi_deve','hi_repr','hi_kidn',
                                'hi_ocul','hi_endo','hi_hema','hi_immu','hi_skel','hi_sple',
                                'hi_thyr','hi_whol']].sum(axis=0)
                    
                    # Identify the max toshi
                    maxi = ug.idxmax()
                    maxtoshi = hinames[maxi]
                    maxtoshival = round(ug[maxi], 2)
    
                    
                    description = "<div align='center'><B> User Receptor</B> <br /> \n" + \
                                  "    " + "<B> ID: " + uid + "</B> <br /> \n" + \
                                  "    " + "<B> HEM4 Estimated Maximum TOSHI (" + maxtoshi + ") </B> <br /> \n" + \
                                  "    " + "Total = " + str(maxtoshival) + "<br /><br /> \n"
                                  
                    if maxtoshival > 0:
                        description += "    " + "<U> Top Pollutants Contributing to TOSHI </U> <br /> \n"
                        # create dictionary to hold summed non-cancer of each pollutant
                        urhap_sum = {}
                        for index, row in group.iterrows():
                            if row["pollutant"] not in urhap_sum:
                                urhap_sum[row["pollutant"]] = row[maxi]
                            else:
                                pol = urhap_sum[row["pollutant"]]
                                risksum = row[maxi] + pol
                                urhap_sum[row["pollutant"]] = risksum
    
                        #sort the dictionary by descending value
                        sorted_urhap_sum = OrderedDict(sorted(urhap_sum.items(), key=itemgetter(1),
                                                              reverse=True))
    
                        # check to make sure large enough value to keep
                        for k, x in sorted_urhap_sum.items():
                            z = round(x, 3)
                            if z > 0.001:
                                description = description + "    " + format(k) + " = " + format(z) + "<br /> \n"
    
                    description += "</div>"
                    
                    # Choose style based on risk level
                    if maxtoshival <= 1:
                        styletag = "#u20"
                    elif (maxtoshival > 1) & (maxtoshival < 10):
                        styletag = "#u20to100"
                    else:
                        styletag = "#u100"
                    
                    ur_placemark = kml.Placemark(ns=self.ns, name="User Receptor: " + uid,
                                                 description=CDATA(description),
                                                 styleUrl=styletag)
                    point = Point(ulon, ulat, 0.000)
                    ur_placemark.visibility = 0
                    ur_placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
                    urt_folder.append(ur_placemark)
    
                docWithHeader.append(urt_folder)
    
    
            #------------------ Inner receptor cancer risk ------------------------------
            ir_folder = kml.Folder(ns=self.ns, name="Inner receptor cancer risk")
            ir_folder.isopen = 0
            
            # Exclude user receptors
            cblks = innermerge2.loc[~innermerge2['rec_id'].str.upper().str.contains('U')]
            for loc, group in cblks.groupby(["lat","lon"]):
                slat = loc[0]
                slon = loc[1]
                srecid = group.iloc[0]['rec_id']
    
                cbtot = group['risk'].sum() * 1000000
                cbrnd = round(cbtot,2)
    
                description = "<div align='center'><B> Inner Receptor</B> <br /> \n" + \
                              "    " + "<B> Receptor ID: " + str(group.iloc[0]['rec_id']) + "</B> <br /> \n" + \
                              "    " + "<B> HEM4 Estimated Cancer Risk (in a million) </B> <br /> \n" + \
                              "    " + "Total = " + str(cbrnd) + "<br /><br /> \n"
    
                if cbrnd > 0:
                    description += "    " + "<U> Top Pollutants Contributing to Total Cancer Risk </U> <br /> \n"
    
                    # create dictionary to hold summed risk of each pollutant
                    cbhap_sum = {}
                    for index, row in group.iterrows():
    
                        #keys = cbhap_sum.keys()
                        if row["pollutant"] not in cbhap_sum:
                            cbhap_sum[row["pollutant"]] = row["risk"]
                            #inp_file.write(row["pollutant"])
                        else:
                            pol = cbhap_sum[row["pollutant"]]
                            risksum = row["risk"] + pol
                            cbhap_sum[row["pollutant"]] = risksum
    
                    #sort the dictionary by descending value
                    sorted_cbhap_sum = OrderedDict(sorted(cbhap_sum.items(), key=itemgetter(1),
                                                          reverse=True))
    
                    # check to make sure large enough value to keep
                    for k, v in sorted_cbhap_sum.items():
                        w = round(v*1000000, 2)     # risk in a million
                        if w > 0.005:
                            description += "    " + format(k) + " = " + format(w) + "<br /> \n"
    
                description += "</div>"
    
                # Choose style based on risk level
                if cbrnd <= 20:
                    styletag = "#b20"
                elif (cbrnd > 20) & (cbrnd < 100):
                    styletag = "#b20to100"
                else:
                    styletag = "#b100"
                    
                point = Point(slon, slat, 0.0000)
                placemark = kml.Placemark(ns=self.ns, name="Inner Receptor " + str(srecid),
                                          description=CDATA(description),
                                          styleUrl=styletag)
                placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
                ir_folder.append(placemark)
    
            docWithHeader.append(ir_folder)
    
    
            #------------------------ Inner receptor TOSHI -----------------------------
            irt_folder = kml.Folder(ns=self.ns, name="Inner receptor TOSHI")
            irt_folder.isopen = 0
    
            for loc, group in cblks.groupby(["lat","lon"]):
                slat = loc[0]
                slon = loc[1]
                srecid = group.iloc[0]['rec_id']
    
                # Sum each toshi in this group
                sg = group[['hi_resp','hi_live','hi_neur','hi_deve','hi_repr','hi_kidn',
                            'hi_ocul','hi_endo','hi_hema','hi_immu','hi_skel','hi_sple',
                            'hi_thyr','hi_whol']].sum(axis=0)
                
                # Identify the max toshi
                maxi = sg.idxmax()
                maxtoshi = hinames[maxi]
                maxtoshival = round(sg[maxi], 2)
                            
                description = "<div align='center'><B> Inner Receptor</B> <br /> \n" + \
                              "    " + "<B> Receptor ID: " + srecid + "</B> <br /> \n" + \
                              "    " + "<B> HEM4 Estimated Maximum TOSHI (" + maxtoshi + ") </B> <br /> \n" + \
                              "    " + "Total = " + str(maxtoshival) + "<br /><br /> \n"
    
                if maxtoshival > 0:
                    description += "    " + "<U> Top Pollutants Contributing to TOSHI </U> <br /> \n"
    
                    # create dictionary to hold summed non-cancer of each pollutant
                    cbhap_sum = {}
                    for index, row in group.iterrows():
    
                        #keys = cbhap_sum.keys()
                        if row["pollutant"] not in cbhap_sum:
                            cbhap_sum[row["pollutant"]] = row[maxi]
                            #inp_file.write(row["pollutant"])
                        else:
                            pol = cbhap_sum[row["pollutant"]]
                            risksum = row[maxi] + pol
                            cbhap_sum[row["pollutant"]] = risksum
    
                    #sort the dictionary by descending value
                    sorted_cbhap_sum = OrderedDict(sorted(cbhap_sum.items(), key=itemgetter(1),
                                                          reverse=True))
    
                    # check to make sure large enough value to keep
                    for k, v in sorted_cbhap_sum.items():
                        if v > 0.001:
                            vrnd = round(v,3)
                            description += "    " + format(k) + " = " + format(vrnd) + "<br /> \n"
    
                description += "</div>"
    
                # Choose style based on HI level
                if maxtoshival <= 1:
                    styletag = "#b20"
                elif (maxtoshival > 1) & (maxtoshival < 10):
                    styletag = "#b20to100"
                else:
                    styletag = "#b100"
    
                point = Point(slon, slat, 0.0000)
                placemark = kml.Placemark(ns=self.ns, name="Inner Receptor " + str(srecid),
                                          description=CDATA(description),
                                          styleUrl=styletag)
                placemark.visibility = 0
                placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
                irt_folder.append(placemark)
    
            docWithHeader.append(irt_folder)


        #---------------- Polar receptor cancer risk ------------------------------------
        pr_folder = kml.Folder(ns=self.ns, name="Polar receptor cancer risk")
        pr_folder.isopen = 0

        for loc, group in polarmerge2.groupby(["lat","lon"]):
            slat = loc[0]
            slon = loc[1]
            pg_dist = str(round(group.iloc[0]['distance'],0))
            pg_angle = str(round(group.iloc[0]['angle'],0))

            pgtot = group['risk'].sum() * 1000000
            pgrnd = round(pgtot,2)

            description = "<div align='center'><B> Polar Receptor</B> <br />" + \
                          "    " + "<B> Distance: " + pg_dist + " Angle: " + pg_angle + "</B> <br /> \n" + \
                          "    " + "<B> HEM4 Estimated Cancer Risk (in a million) </B> <br /> \n" + \
                          "    " + "Total = " + str(pgrnd) + "<br /><br /> \n"

            if pgrnd > 0:
                description += "    " + "<U> Top Pollutants Contributing to Total Cancer Risk </U> <br /> \n"

                # create dictionary to hold summed risk of each pollutant
                pghap_sum = {}
                for index, row in group.iterrows():
                    if row["pollutant"] not in pghap_sum:
                        pghap_sum[row["pollutant"]] = row["risk"]
                    else:
                        pol = pghap_sum[row["pollutant"]]
                        risksum = row["risk"] + pol
                        pghap_sum[row["pollutant"]] = risksum
                        
                #sort the dictionary by descending value
                sorted_pghap_sum = OrderedDict(sorted(pghap_sum.items(), key=itemgetter(1),
                                                      reverse=True))

                # check to make sure large enough value to keep
                for k, v in sorted_pghap_sum.items():
                    z = round(v*1000000, 2)     # risk in a million
                    if z > 0.005:
                        description += "    " + format(k) + " = " + format(z) + "<br /> \n"

            description += "</div>"

            # Choose style based on risk level
            if pgrnd <= 20:
                styletag = "#s20"
            elif (pgrnd > 20) & (pgrnd < 100):
                styletag = "#s20to100"
            else:
                styletag = "#s100"

            point = Point(slon, slat, 0.0000)
            placemark = kml.Placemark(ns=self.ns, name="Polar Receptor Distance: " + pg_dist + " Angle: " + str(group.iloc[0]['angle']),
                                      description=CDATA(description),
                                      styleUrl=styletag)
            placemark.visibility = 0
            placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
            pr_folder.append(placemark)

        docWithHeader.append(pr_folder)


        #---------------- Polar receptor TOSHI -----------------------------------------
        prt_folder = kml.Folder(ns=self.ns, name="Polar TOSHI")
        prt_folder.isopen = 0

        for loc, group in polarmerge2.groupby(["lat","lon"]):
            slat = loc[0]
            slon = loc[1]
            pg_dist = str(round(group.iloc[0]['distance'],0))
            pg_angle = str(round(group.iloc[0]['angle'],0))

            # Sum each toshi in this group
            sg = group[['hi_resp','hi_live','hi_neur','hi_deve','hi_repr','hi_kidn',
                        'hi_ocul','hi_endo','hi_hema','hi_immu','hi_skel','hi_sple',
                        'hi_thyr','hi_whol']].sum(axis=0)
            
            # Identify the max toshi
            maxi = sg.idxmax()
            maxtoshi = hinames[maxi]
            maxtoshival = round(sg[maxi], 2)

            description = "<div align='center'><B> Polar Receptor</B> <br /> \n" + \
                          "    " + "<B> Distance: " + pg_dist + " Angle: " + pg_angle + "</B> <br /> \n" + \
                          "    " + "<B> HEM4 Estimated Max TOSHI (" + maxtoshi + ") </B> <br /> \n" + \
                          "    " + "Total = " + str(maxtoshival) + "<br /><br /> \n"

            if maxtoshival > 0:
                description += "    " + "<U> Top Pollutants Contributing to TOSHI </U> <br /> \n"

                # create dictionary to hold summed toshi of each pollutant
                pghi_sum = {}
                for index, row in group.iterrows():

                    if row["pollutant"] not in pghi_sum:
                        pghi_sum[row["pollutant"]] = row[maxi]
                    else:
                        pol = pghi_sum[row["pollutant"]]
                        toshisum = row[maxi] + pol
                        pghi_sum[row["pollutant"]] = toshisum

                #sort the dictionary by descending value
                sorted_pghi_sum = OrderedDict(sorted(pghi_sum.items(), key=itemgetter(1),
                                                     reverse=True))

                # check to make sure large enough value to keep
                for k, v in sorted_pghi_sum.items():
                    if v > 0.001:
                        vrnd = round(v,3)
                        description += "    " + format(k) + " = " + format(vrnd) + "<br /> \n"

            description += "</div>"

            # Choose style based on HI level
            if maxtoshival <= 1:
                styletag = "#s20"
            elif (maxtoshival > 1) & (maxtoshival < 10):
                styletag = "#s20to100"
            else:
                styletag = "#s100"

            point = Point(slon, slat, 0.0000)
            placemark = kml.Placemark(ns=self.ns, name="Polar Receptor Distance: " + pg_dist + " Angle: " + pg_angle,
                                      description=CDATA(description),
                                      styleUrl=styletag)
            placemark.visibility = 0
            placemark.geometry = Geometry(ns=self.ns, altitude_mode="clampToGround", geometry=point)
            prt_folder.append(placemark)

        docWithHeader.append(prt_folder)
        fac_kml.append(docWithHeader)
        
        # Finished, write the kml file
        self.writeToFile(fackml_fname, fac_kml)

        # Create KMZ file
        kmztype = 'facilityrisk'
        fackmz_fname = fackml_fname.replace('.kml', '.kmz')
        self.createKMZ(kmztype, fackml_fname, fackmz_fname)

    def calcHI(self, conc, rfc, endpoint):
        """
        Compute a specific HI value
        """
        hazard_index = (0 if rfc == 0 else (conc/rfc/1000)*endpoint)
        return hazard_index
        
    def writeToFile(self, filename, kml):
        """
        Write a KML instance to a file.
        :param filename:
        :param kml: a fastKml KML instance
        """
        file = open(filename, "w")

        pretty = kml.to_string(prettyprint=True)
        usingPhysicalWidth = self.usePhysicalWidth(pretty)
        file.write(unescape(usingPhysicalWidth))
        file.close()

    # Currently fastkml does not implement the gx extension types, but we want to specify a physical width
    # instead of a pixel width. Therefore we are falling back on some hackery to replace the generated
    # <width> tags with <gx:physicalWidth> tags.
    def usePhysicalWidth(self, input):

        # First add the gx namespace to the KML element
        defaultNS = 'xmlns="http://www.opengis.net/kml/2.2"'
        gxNS = 'xmlns:gx="http://www.google.com/kml/ext/2.2"'

        # ...then, replace widths with physicalWidths
        input = input.replace(defaultNS, defaultNS + " " + gxNS)
        input = input.replace('<width>', '<gx:physicalWidth>')
        input = input.replace('</width>', '</gx:physicalWidth>')

        return input

    def createKMZ(self, ftype, kmlfname, kmzfname):
        """
        Zip a KML file into a KMZ file.
        :param ftype: type of KML to zip, either all sources or facility risk
        :param kmlname: KML filename
        :param kmzname: KMZ filename
        """
        if ftype == 'allsources':
            zf = zipfile.ZipFile(kmzfname, mode='w')
            zf.write(kmlfname, basename(kmlfname))
            zf.write('resources/drawCircle.png', 'drawCircle.png')
            zf.write('resources/drawCenter.png', 'drawCenter.png')
            zf.close()
        else:
            zf = zipfile.ZipFile(kmzfname, mode='w')
            zf.write(kmlfname, basename(kmlfname))
            zf.write('resources/drawCircle.png', 'drawCircle.png')
            zf.write('resources/drawRectangle.png', 'drawRectangle.png')
            zf.write('resources/drawRectangle_ur.png', 'drawRectangle_ur.png')
            zf.write('resources/drawCenter.png', 'drawCenter.png')
            zf.write('resources/drawCross.png', 'drawCross.png')
            zf.close()
        
        # Delete the KML file
        os.remove(kmlfname)
            
            
    def set_width(self, row, buoy_linwid):
        """
        Set the width of a line or buoyant line source.
        :param row:
        :param buoy_linwid:
        :return: line width
        """
        if row["source_type"] == "N":
            linwid = row["lengthx"]
        elif row["source_type"] == "B":
            linwid = buoy_linwid["avglin_wid"].iloc[0]
        else:
            linwid = 0

        return linwid

    def distance(self, origin, destination):
        """
        Compute the distance in km between two pairs of lat/lons
        :param origin: first pair of lat/lon (tuple)
        :param destination: second pair of lat/lon (tuple)
        :return: distance in km
        """
        lat1, lon1 = origin
        lat2, lon2 = destination
        radius = 6371 # earth radius in km

        dlat = math.radians(lat2-lat1)
        dlon = math.radians(lon2-lon1)
        a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
            * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        d = radius * c
        
        return d

    def midpoint(self, p1, p2):
        """
        Compute the midpoint between two pairs of lat/lons
        :param p1: first lat/lon
        :param p2: second lat/lon
        :return: lat/lon of midpoint
        """
        lat1, lon1 = p1
        lat2, lon2 = p2
        lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
        dlon = lon2 - lon1
        dx = math.cos(lat2) * math.cos(dlon)
        dy = math.cos(lat2) * math.sin(dlon)
        lat3 = math.atan2(math.sin(lat1) + math.sin(lat2), math.sqrt((math.cos(lat1) + dx) * (math.cos(lat1) + dx) + dy * dy))
        lon3 = lon1 + math.atan2(dy, math.cos(lat1) + dx)
        return(math.degrees(lat3), math.degrees(lon3))

    def create_sourcemap(self, model, facil_id):
        """
        Create the source map dataframe needed for the source location KML.
        :return: dataframe of emission locations
        """
        if facil_id == None:
            # Create an array of all facility ids being modeled
            faclist = model.faclist.dataframe.fac_id.values
        else:
            faclist = [facil_id]

        # Loop over all facility ids and populate the sourcemap dataframe
        source_map = pd.DataFrame()

        for row in faclist:

            # Emission location info for one facility. Keep certain columns.
            emislocs = model.emisloc.dataframe.loc[model.emisloc.dataframe.fac_id == row]
            [["fac_id","source_id","source_type","lon","lat","utmzone","x2","y2",
              "location_type","lengthx","lengthy","angle"]]
            
            # If facility has a polygon source, get the vertices for this facility and append to emislocs
            if any(emislocs.source_type == "I") == True:
                polyver = model.multipoly.dataframe.loc[model.multipoly.dataframe.fac_id == row]
                [["fac_id","source_id","lon","lat","utmzone","location_type"]]
                # Assign source_type
                polyver["source_type"] = "I"
                # remove the I source_type rows from emislocs before appending polyver to avoid duplicate rows
                emislocs = emislocs[emislocs.source_type != "I"]
                # Append polyver to emislocs
                emislocs = emislocs.append(polyver)

            # If facility has a buoyant line source, get the line width
            if any(emislocs.source_type == "B") == True:
                buoy_linwid = model.multibuoy.dataframe.loc[model.multibuoy.dataframe.fac_id == row]
                [["fac_id","avglin_wid"]]
            else:
                buoy_linwid = pd.DataFrame()

            # Create a line width column for line and buoyant line sources
            emislocs["line_width"] = emislocs.apply(lambda row: self.set_width(row,buoy_linwid), axis=1)

            # Replace NaN with blank or 0 in emislocs. Default utmzone to 0N.
            emislocs = emislocs.fillna({"utmzone":'0N', "source_type":"", "x2":0, "y2":0})

            # Determine the common utm zone to use for this facility and the hemisphere
            facutmzonenum, hemi = UTM.zone2use(emislocs)
            facutmzonestr = str(facutmzonenum) + hemi


            # Compute lat/lon of any user supplied UTM coordinates
            emislocs[["lat", "lon"]] = emislocs.apply(lambda row: UTM.utm2ll(row["lat"],row["lon"],row["utmzone"]) 
                               if row['location_type']=='U' else [row["lat"],row["lon"]], result_type="expand", axis=1)

            # Next compute UTM coordinates using the common zone
            emislocs[["utmn", "utme"]] = emislocs.apply(lambda row: UTM.ll2utm_alt(row["lat"],row["lon"],facutmzonenum,hemi)
                               , result_type="expand", axis=1)

            # Compute lat/lon of any x2 and y2 coordinates that were supplied as UTM
            emislocs[['lat_y2', 'lon_x2']] = emislocs.apply(lambda row: UTM.utm2ll(row["y2"],row["x2"],row["utmzone"]) 
                              if row['location_type']=='U' else [row["y2"],row["x2"]], result_type="expand", axis=1)
            
            # Compute UTM coordinates of lat_x2 and lon_y2 using the common zone
            emislocs[['utmn_y2', 'utme_x2']] = emislocs.apply(lambda row: UTM.ll2utm_alt(row["lat_y2"],row["lon_x2"],facutmzonenum,hemi)
                              , result_type="expand", axis=1)

            # Pull out any area/volume sources and create vertices of each corner
            areavol = emislocs[(emislocs.source_type=="A") | (emislocs.source_type=="V")]

            if areavol.empty == False:
                new_rows = []
                for index, row in areavol.iterrows():                    
                    newrow = row.copy()
                    if row["source_type"] == "A":
                        # Area sources
                        radangle = np.radians(row["angle"])
                        new_rows.append(newrow.tolist())  # vertex 1
                        newrow["utme"] = row["utme"] + row["lengthx"] * np.cos(radangle)
                        newrow["utmn"] = row["utmn"] - row["lengthx"] * np.sin(radangle)
                        latitude, longitude = UTM.utm2ll(newrow["utmn"],newrow["utme"],facutmzonestr)
                        newrow["lat"] = latitude
                        newrow["lon"] = longitude
                        new_rows.append(newrow.tolist())  # vertex 2
                        newrow["utme"] = row["utme"] + row["lengthx"] * np.cos(radangle) \
                                                        + row["lengthy"] * np.sin(radangle)
                        newrow["utmn"] = row["utmn"] - row["lengthx"] * np.sin(radangle) \
                                                        + row["lengthy"] * np.cos(radangle)
                        latitude, longitude = UTM.utm2ll(newrow["utmn"],newrow["utme"],facutmzonestr)
                        newrow["lat"] = latitude
                        newrow["lon"] = longitude
                        new_rows.append(newrow.tolist())  # vertex 3
                        newrow["utme"] = row["utme"] + row["lengthy"] * np.sin(radangle)
                        newrow["utmn"] = row["utmn"] + row["lengthy"] * np.cos(radangle)
                        latitude, longitude = UTM.utm2ll(newrow["utmn"],newrow["utme"],facutmzonestr)
                        newrow["lat"] = latitude
                        newrow["lon"] = longitude
                        new_rows.append(newrow.tolist())  # vertex 4
                        newrow["utme"] = row["utme"]
                        newrow["utmn"] = row["utmn"]
                        newrow["lat"] = row["lat"]
                        newrow["lon"] = row["lon"]
                        new_rows.append(newrow.tolist())  # repeat vertex 1
                    else:
                        # Volume sources
                        newrow["utme"] = row["utme"] - row["horzdim"]/2
                        newrow["utmn"] = row["utmn"] - row["horzdim"]/2
                        latitude, longitude = UTM.utm2ll(newrow["utmn"],newrow["utme"],facutmzonestr)
                        newrow["lat"] = latitude
                        newrow["lon"] = longitude
                        new_rows.append(newrow.tolist())  # vertex 1
                        newrow["utme"] = row["utme"] + row["horzdim"]/2
                        newrow["utmn"] = row["utmn"] - row["horzdim"]/2
                        latitude, longitude = UTM.utm2ll(newrow["utmn"],newrow["utme"],facutmzonestr)
                        newrow["lat"] = latitude
                        newrow["lon"] = longitude
                        new_rows.append(newrow.tolist())  # vertex 2
                        newrow["utme"] = row["utme"] + row["horzdim"]/2
                        newrow["utmn"] = row["utmn"] + row["horzdim"]/2
                        latitude, longitude = UTM.utm2ll(newrow["utmn"],newrow["utme"],facutmzonestr)
                        newrow["lat"] = latitude
                        newrow["lon"] = longitude
                        new_rows.append(newrow.tolist())  # vertex 3
                        newrow["utme"] = row["utme"] - row["horzdim"]/2
                        newrow["utmn"] = row["utmn"] + row["horzdim"]/2
                        latitude, longitude = UTM.utm2ll(newrow["utmn"],newrow["utme"],facutmzonestr)
                        newrow["lat"] = latitude
                        newrow["lon"] = longitude
                        new_rows.append(newrow.tolist())  # vertex 4
                        newrow["utme"] = row["utme"] - row["horzdim"]/2
                        newrow["utmn"] = row["utmn"] - row["horzdim"]/2
                        latitude, longitude = UTM.utm2ll(newrow["utmn"],newrow["utme"],facutmzonestr)
                        newrow["lat"] = latitude
                        newrow["lon"] = longitude
                        new_rows.append(newrow.tolist())  # repeat vertex 1
                
                # Remove the area/volume rows from emislocs and append the area/volume vertices list
                emislocs = emislocs[(emislocs.source_type != "A") & (emislocs.source_type != "V")]
                emislocs = emislocs.append(pd.DataFrame(new_rows, columns=emislocs.columns)).reset_index()
                            
            # Append to source_map
            source_map = source_map.append(emislocs)

        return source_map

    def create_facility_sourcemap(self, facid, model):
        """
        Create the source map dataframe needed for the source locations of a particular facility.
        :return: dataframe of emission locations
        """

        fac_source_map = pd.DataFrame()

        # Emission location info for one facility. Keep certain columns.
        emislocs = model.emisloc.dataframe.loc[model.emisloc.dataframe.fac_id == facid]
        [["fac_id","source_id","source_type","lon","lat","utmzone","x2","y2","location_type","lengthx"]]

        # If facility has a polygon source, get the vertices for this facility and append to emislocs
        if any(emislocs.source_type == "I") == True:
            polyver = model.multipoly.dataframe.loc[model.multipoly.dataframe.fac_id == facid]
            [["fac_id","source_id","lon","lat","utmzone","location_type"]]
            # Assign source_type
            polyver["source_type"] = "I"
            # remove the I source_type rows from emislocs before appending polyver to avoid duplicate rows
            emislocs = emislocs[emislocs.source_type != "I"]
            # Append polyver to emislocs
            emislocs = emislocs.append(polyver)

        # If facility has a buoyant line source, get the line width
        if any(emislocs.source_type == "B") == True:
            buoy_linwid = model.multibuoy.dataframe.loc[model.multibuoy.dataframe.fac_id == facid]
            [["fac_id","avglin_wid"]]
        else:
            buoy_linwid = pd.DataFrame()

        # Create a line width column for line and buoyant line sources
        emislocs["line_width"] = emislocs.apply(lambda row: self.set_width(row,buoy_linwid), axis=1)

        # Replace NaN with blank or 0 in emislocs. Default utmzone to 0N.
        emislocs = emislocs.fillna({"utmzone":'0N', "source_type":"", "x2":0, "y2":0})

        # Determine the common utm zone to use for this facility and the hemisphere
        facutmzonenum, hemi = UTM.zone2use(emislocs)


        # Compute lat/lon of any user supplied UTM coordinates
        emislocs[["lat", "lon"]] = emislocs.apply(lambda row: UTM.utm2ll(row["lat"],row["lon"],row["utmzone"]) 
                           if row['location_type']=='U' else [row["lat"],row["lon"]], result_type="expand", axis=1)

        # Next compute UTM coordinates using the common zone
        emislocs[["utmn", "utme"]] = emislocs.apply(lambda row: UTM.ll2utm_alt(row["lat"],row["lon"],facutmzonenum,hemi)
                           , result_type="expand", axis=1)

        # Compute lat/lon of any x2 and y2 coordinates that were supplied as UTM
        emislocs[['lat_y2', 'lon_x2']] = emislocs.apply(lambda row: UTM.utm2ll(row["y2"],row["x2"],row["utmzone"]) 
                          if row['location_type']=='U' else [row["y2"],row["x2"]], result_type="expand", axis=1)

        # Compute UTM coordinates of x2 and y2 using the common zone
        emislocs[['utmn_y2', 'utme_x2']] = emislocs.apply(lambda row: UTM.ll2utm_alt(row["y2"],row["x2"],facutmzonenum,hemi)
                          , result_type="expand", axis=1)

        # Append to source_map
        fac_source_map = fac_source_map.append(emislocs)

        return fac_source_map
    
    def createDocumentWithHeader(self):
        """
        Create a KML Document object with preset styles and schema.
        :return: the Document instance
        """
        document = kml.Document(ns=self.ns, name='srcmap', description='Exported from HEM4')
        document.isopen = 1

        # Schema
        schema = kml.Schema(ns=self.ns, id="srcmap_schema", name="srcmap")
        schema.append("string", "Source_id", "Sourceid")
        document.append_schema(schema)

        # Areasrc style...
        document.append_style(self.getAreaSrcStyle())

        # Ptsrc style...
        document.append_style(self.getPtSrcStyle())

        # center style...
        center_style = self.getBaseStyle(id="center")
        center_style.append_style(IconStyle(ns=self.ns, color="ff0000ff", icon_href="drawCenter.png"))
        document.append_style(center_style)

        # s20 style...
        document.append_style(self.getS20Style())

        # s20to100 style...
        s20to100_style = self.getBaseStyle(id="s20to100")
        s20to100_style.append_style(IconStyle(ns=self.ns, color="ff00ffff", icon_href="drawCircle.png"))
        document.append_style(s20to100_style)

        # s100 style...
        s100_style = self.getBaseStyle(id="s100")
        s100_style.append_style(IconStyle(ns=self.ns, color="ff0000ff", icon_href="drawCircle.png"))
        document.append_style(s100_style)

        # b20 style...
        document.append_style(self.getB20Style())

        # b20to100 style...
        b20to100_style = self.getBaseStyle(id="b20to100")
        b20to100_style.append_style(IconStyle(ns=self.ns, color="ff00ffff", icon_href="drawRectangle.png"))
        document.append_style(b20to100_style)

        # b100 style...
        b100_style = self.getBaseStyle(id="b100")
        b100_style.append_style(IconStyle(ns=self.ns, color="ff0000ff", icon_href="drawRectangle.png"))
        document.append_style(b100_style)

        # u20 style...
        u20_style = self.getBaseStyle(id="u20")
        u20_style.append_style(IconStyle(ns=self.ns, color="ff00ff00", icon_href="drawRectangle_ur.png"))
        document.append_style(u20_style)

        # u20to100 style...
        u20to100_style = self.getBaseStyle(id="u20to100")
        u20to100_style.append_style(IconStyle(ns=self.ns, color="ff00ffff", icon_href="drawRectangle_ur.png"))
        document.append_style(u20to100_style)

        # b100 style...
        u100_style = self.getBaseStyle(id="u100")
        u100_style.append_style(IconStyle(ns=self.ns, color="ff0000ff", icon_href="drawRectangle_ur.png"))
        document.append_style(u100_style)

        # mir style...
        mir_style = self.getBaseStyle(id="mir")
        mir_style.append_style(IconStyle(ns=self.ns, icon_href="drawCross.png"))
        document.append_style(mir_style)

        return document

    def copyUTMColumns(self, utmn, utme):
        return [utmn, utme]

    def getAreaSrcStyle(self):
        as_style = kml.Style(ns=self.ns, id="Areasrc")
        as_style.append_style(LineStyle(ns=self.ns, color="ff000000"))
        as_style.append_style(PolyStyle(ns=self.ns, color="7c8080ff"))
        as_style.append_style(BalloonStyle(ns=self.ns, bgColor="ffffffff", text="$[description]"))
        return as_style

    def getPtSrcStyle(self):
        ps_style = self.getBaseStyle(id="Ptsrc")
        ps_style.append_style(IconStyle(ns=self.ns, color="ff8080ff", icon_href="drawCircle.png"))
        return ps_style

    def getCenterStyle(self):
        center_style = self.getBaseStyle(id="center")
        center_style.append_style(IconStyle(ns=self.ns, color="ff0000ff", icon_href="drawCenter.png"))
        return center_style

    def getS20Style(self):
        s20_style = self.getBaseStyle(id="s20")
        s20_style.append_style(IconStyle(ns=self.ns, color="ff00ff00", icon_href="drawCircle.png"))
        return s20_style

    def getB20Style(self):
        b20_style = self.getBaseStyle(id="b20")
        b20_style.append_style(IconStyle(ns=self.ns, color="ff00ff00", icon_href="drawRectangle.png"))
        return b20_style

    def getBaseStyle(self, id):
        base_style = kml.Style(ns=self.ns, id=id)
        base_style.append_style(LabelStyle(ns=self.ns, color="00000000"))
        base_style.append_style(BalloonStyle(ns=self.ns, bgColor="ffffffff", text="$[description]"))
        return base_style