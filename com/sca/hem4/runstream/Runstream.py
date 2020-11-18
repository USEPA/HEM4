# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 10:23:14 2018

@author: dlindsey

"""
import math
import os

import com.sca.hem4.FindMet as fm
from com.sca.hem4.model.Model import *
from com.sca.hem4.support.UTM import *
from com.sca.hem4.support.NormalRounding import *
from com.sca.hem4.upload.EmissionsLocations import *

class Runstream():
    """
    
    
    """
    
    def __init__(self, facops_df, emislocs_df, hapemis_df,  
                 buoyant_df = None, polyver_df = None, bldgdw_df = None, 
                 partdia_df = None, landuse_df = None, seasons_df = None,
                 emisvar_df = None, model = None):
        
     
        
        self.facoptn_df = facops_df
        self.emisloc_df = emislocs_df
        self.hapemis = hapemis_df
        self.buoyant_df = buoyant_df
        self.polyver_df = polyver_df
        self.bldgdw_df = bldgdw_df
        self.partdia_df = partdia_df
        self.landuse_df = landuse_df
        self.seasons_df = seasons_df
        self.emisvar_df = emisvar_df
        self.model = model
        self.urban = False
        
        
        
        
        # Facility ID
        self.facid = self.facoptn_df['fac_id'].iloc[0]                 
        
        #open file to write
        self.inp_f = open(os.path.join("aermod", "aermod.inp"), "w")
        
        
                
    def build_co(self, phase, innerblks, outerblks):
        """
        Creates CO section of Aermod.inp file
        """
                      
    # Hours -------------------------------------------------------------------
                
        self.hours_value = int(self.facoptn_df['hours'].iloc[0])
    
        av_t = [1,2,3,4,6,8,12,24]          # Possible Averaging Time Periods

        self.annual = self.facoptn_df['annual'].iloc[0] == "Y"
        met_annual = " ANNUAL " if self.annual else " PERIOD "
        if self.facoptn_df['acute'].iloc[0] == 'N':
            self.hours = met_annual
        elif self.hours_value in av_t:
            self.hours = str(self.hours_value) + met_annual
        else:
            self.hours = str(1) + met_annual
            
    # Elevations --------------------------------------------------------------
           
        self.eleva = self.facoptn_df['elev'].iloc[0]                        

        if self.model.altRec_optns.get('altrec_flat', None):
            optel = " FLAT "
        elif self.eleva == "Y":
            optel = " ELEV "
        else:
            optel = " FLAT "
    
    # deposition & depletion --------------------------------------------------
        
        #check whether or not to overwrite phase based on exluding all sources
        
        if phase['phase'] is None:
            self.phaseType = 'C'
        else:
            self.phaseType = phase['phase']
        
        #if vapor or particle run
        if phase['phase'] in ['V', 'P']:
            
            #check sources in exlcusion list
            phaseSet = set(self.hapemis['source_id'].tolist())
            exclusionSet = set(self.model.sourceExclusion)
            
            #if all sources are in the exclusiom list then overwrite phase
            difference = phaseSet.difference(exclusionSet)
            
            if len(difference) == 0:
                phase['phase'] = ''
        
        #logic for phase setting in model options
                
        if phase['phase'] == 'P':
            optdp = phase['settings'][0]
            titletwo = "CO TITLETWO  Particle-phase emissions \n"
            
        
        elif phase['phase'] == 'V':
            optdp = phase['settings'][0]
            titletwo = "CO TITLETWO  Vapor-phase emissions \n"
            
        else:
            optdp = ''
            titletwo = "CO TITLETWO  Combined particle and vapor-phase emissions \n"
    
        self.model.model_optns['titletwo'] = titletwo

    # Building downwash option ------------------------------------------------
        self.blddw = self.facoptn_df['bldg_dw'].iloc[0]
        
    # FASTALL Model Option for AERMOD -----------------------------------------
        fasta = self.facoptn_df['fastall'].iloc[0]                     
    
        if fasta == "Y":
            optfa = " FASTALL "
        else:
            optfa = ""

    # CO Section ----------------------------------------------------------
        
        co1 = "CO STARTING  \n"
        co2 = "CO TITLEONE  " + str(self.facid) + "\n"
        co3 = titletwo   
        co4 = "CO MODELOPT  CONC  ALPHA  BETA " + optdp + optel + optfa + "\n"  
    

        self.inp_f.write(co1)
        self.inp_f.write(co2)
        self.inp_f.write(co3)
        self.inp_f.write(co4)
        
        #determine alternate receptor status
        altrec = self.model.altRec_optns.get("altrec", None)
        
        #check for user specified urban option
        if self.facoptn_df['rural_urban'].values[0] == 'U':
            self.urban = True
            urbanopt = "CO URBANOPT " + str(self.facoptn_df['urban_pop'].values[0]) + "\n"
            self.inp_f.write(urbanopt)
             
        #if rural is forced, leave urban as false
        elif self.facoptn_df['rural_urban'].values[0] == 'R':
            
            self.urban = False
        
        #if there is nothing, default is to determine an urban option from the census data
        # unless alternate receptors are being used then leave urban as false
        else:
            if not altrec:
                # Get shortest distance in innerblks and check for urban population
                # Exclude user-supplied receptors and user receptors already in the census data
                if not innerblks.empty:
                    inn_wo_ur = innerblks[~innerblks['idmarplot'].str.contains('U')]
                    closest = inn_wo_ur.nsmallest(1, 'distance')
                    if closest['urban_pop'].values[0] > 0:
                        self.urban = True
                        urbanopt = "CO URBANOPT  " + str(closest['urban_pop'].values[0]) + "\n"
                        self.inp_f.write(urbanopt)
                        
                else: #get shortest distance from outerblocks 
                    out_wo_ur = outerblks[~outerblks['idmarplot'].str.contains('U')]
                    closest = out_wo_ur.nsmallest(1, 'distance')
                    if closest['urban_pop'].values[0] > 0:
                        self.urban = True
                        urbanopt = "CO URBANOPT " + str(closest['urban_pop'].values[0]) + "\n"
                        self.inp_f.write(urbanopt)
        
        #set urban in model options
        self.model.model_optns['urban'] = self.urban

                    
        # Landuse Options for Deposition
        if phase['phase'] == 'V':
            landuseYN = 'N'
            for word in optdp.split():
                if word == 'DDEP' or word == 'DRYDPLT':
                    landuseYN = 'Y'
            if landuseYN == 'Y':                        
                landval = self.landuse_df[self.landuse_df.columns[1:]].values[0]
                coland = ("CO GDLANUSE " + " ".join(map(str, landval)) + '\n')
                self.inp_f.write(coland)
        
                # Season Options for Deposition
                seasval = self.seasons_df[self.seasons_df.columns[1:]].values[0]
                coseas = ("CO GDSEASON " + " ".join(map(str,seasval)) + '\n')
                self.inp_f.write(coseas)

        co5 = "CO AVERTIME  " + self.hours + "\n"
        co6 = "CO POLLUTID  UNITHAP \n"
        co7 = "CO RUNORNOT  RUN \n"
        co8 = "CO FINISHED  \n" + "\n"
    
        self.inp_f.write(co5)
        self.inp_f.write(co6)
        self.inp_f.write(co7)
        self.inp_f.write(co8)    
    
    def build_so(self, phase):
        """
        Function writes SO section of Aermod.inp, names source types and 
        their parameters
        
        """
         
        srid = self.emisloc_df['source_id'][:]                           # Source ID
        cord = self.emisloc_df['location_type'][:]                       # Coordinate System
        xco1 = self.emisloc_df['utme'].apply(lambda x: normal_round(x))  # X-Coordinate
        yco1 = self.emisloc_df['utmn'].apply(lambda x: normal_round(x))  # Y-Coordinate
        srct = self.emisloc_df['source_type'][:]                         # Source Type
        lenx = self.emisloc_df['lengthx'][:]                             # Length in X-Direction
        leny = self.emisloc_df['lengthy'][:]                             # Length in Y-Direction
        angl = self.emisloc_df['angle'][:]                               # Angle of Emission Location
        latr = self.emisloc_df['horzdim'][:]                             # Initial Lateral/Horizontal Emission
        vert = round(self.emisloc_df['vertdim'][:],2)                    # Initial Vertical Emission
        relh = round(self.emisloc_df['areavolrelhgt'][:],2)              # Release Height
        stkh = round(self.emisloc_df['stkht'][:],3)                      # Stack Height
        diam = round(self.emisloc_df['stkdia'][:],3)                     # Stack Diameter
        emiv = round(self.emisloc_df['stkvel'][:],7)                     # Stack Exit Velocity
        temp = round(self.emisloc_df['stktemp'][:],2)                    # Stack Exit Temperature
        elev = self.emisloc_df['elev'][:]                                # Elevation of Source Location
        xco2 = self.emisloc_df['utme_x2'].apply(lambda x: normal_round(x))  # Second X-Coordinate
        yco2 = self.emisloc_df['utmn_y2'].apply(lambda x: normal_round(x))  # Second Y-Coordinate
    
    # initialize variable used to determine the first buoyant line source (if there is one)
        first_buoyant = 0

    # Need to confirm lenx is not 0
        area = self.emisloc_df['lengthx'][:] * self.emisloc_df['lengthy'][:]
    
    
    # Checks that may be done outside of this program
    
    #checks for emission variation and extracts source ids from linked txt file
        if self.emisvar_df is not None and type(self.emisvar_df) == str:
            so_col = []
            with open(self.emisvar_df) as fobj:
                for line in fobj:
                    row = line.split()
                    so_col.append(row[6])
            
            var_sources = set(so_col).tolist()

    
    # Lat/Lon check also needs to be inserted
        lenx[np.isnan(lenx)] = 0
        leny[np.isnan(leny)] = 0
        angl[np.isnan(angl)] = 0
        latr[np.isnan(latr)] = 0
        vert[np.isnan(vert)] = 0
        relh[np.isnan(relh)] = 0
        stkh[np.isnan(stkh)] = 0
        diam[np.isnan(diam)] = 0
        emiv[np.isnan(emiv)] = 0
        temp[np.isnan(temp)] = 0
        elev[np.isnan(elev)] = 0
        xco2[np.isnan(xco2)] = 0
        yco2[np.isnan(yco2)] = 0
         
        so1 = "SO STARTING \n"
        so2 = "SO ELEVUNIT METERS \n"
    
        self.inp_f.write(so1)
        self.inp_f.write(so2)
    
        #loop through sources
        for index, row in self.emisloc_df.iterrows():

            # Do not write excluded sources. These are sources that are 100% particle or 100% gaseous
            # and the opposite phase is being modeled.
            if srid[index] not in self.model.sourceExclusion.get(self.phaseType+self.facid, ''):
                    
                # Point Source ----------------------------------------------------
        
                if srct[index] == 'P':
                    soloc = ("SO LOCATION " + str(srid[index]) + " POINT " + 
                             str(xco1[index]) + " " + str(yco1[index]) + " " +
                             str(elev[index]) + "\n")
                    
                    soparam = ("SO SRCPARAM " + str(srid[index]) + " 1000 " + 
                               str(stkh[index]) + " " + str(temp[index]) + " " +
                               str(emiv[index]) + " " + str(diam[index]) + "\n")
                    
                    self.inp_f.write(soloc)
                    self.inp_f.write(soparam)
                    
                    if self.urban == True:
                        urbanopt = "SO URBANSRC " + str(srid[index]) + "\n"
                        self.inp_f.write(urbanopt)
    
                    if self.blddw == "Y":
                        self.get_blddw(srid[index])
                        
                    if phase['phase'] == 'P':
                        self.get_particle(srid[index])
                        
                    elif phase['phase'] == 'V':
                        self.get_vapor(srid[index])
                          
                    if (self.emisvar_df is not None and type(self.emisvar_df) != str
                        and srid[index] in self.emisvar_df['source_id'].values ):
                        self.get_variation(srid[index])
                    
                    #if linked file
                    elif (self.emisvar_df is not None and 
                          type(self.emisvar_df) == str and srid[index] in var_sources ):
                        
                        solink = ("SO HOUREMIS " + self.emisvar_df + " " + 
                                  srid[index] + " \n")
                        self.inp_f.write(solink)
        
                    
                # Horizontal Point Source ----------------------------------------
        
                elif srct[index] == 'H':
                    soloc = ("SO LOCATION " + str(srid[index]) + " POINTHOR " + 
                             str(xco1[index]) + " " + str(yco1[index]) + " " + 
                             str(elev[index]) + "\n")
                    
                    soparam = ("SO SRCPARAM " + str(srid[index]) + " 1000 " + 
                               str(stkh[index]) + " " + str(temp[index]) + " " + 
                               str(emiv[index]) + " " + str(diam[index]) + "\n")
                    
                    self.inp_f.write(soloc)
                    self.inp_f.write(soparam)
                    
                    if self.urban == True:
                        urbanopt = "SO URBANSRC " + str(srid[index]) + "\n"
                        self.inp_f.write(urbanopt)
                    
                    if self.blddw == "Y":
                        self.get_blddw(srid[index])
                        
                    if phase['phase'] == 'P':
                        self.get_particle(srid[index])
                        
                    elif phase['phase'] == 'V':
                        self.get_vapor(srid[index])
                        
                    if (self.emisvar_df is not None and type(self.emisvar_df) != str
                        and srid[index] in self.emisvar_df['source_id'].values ):
                        self.get_variation(srid[index])
                    
                     #if linked file
                    elif (self.emisvar_df is not None and 
                          type(self.emisvar_df) == str and srid[index] in var_sources ):
                        
                        solink = ("SO HOUREMIS " + self.emisvar_df + " " + 
                                  srid[index] + " \n")
                        self.inp_f.write(solink)    
                    
                # Capped Point Source ---------------------------------------------------
                
                elif srct[index] == 'C':
                    soloc = ("SO LOCATION " + str(srid[index]) + " POINTCAP " + 
                             str(xco1[index]) + " " + str(yco1[index]) + " " + 
                             str(elev[index]) + "\n")
                    
                    soparam = ("SO SRCPARAM " + str(srid[index]) + " 1000 " + 
                               str(stkh[index]) + " " + str(temp[index]) + " " + 
                               str(emiv[index]) + " " + str(diam[index]) + "\n")
                    
                    self.inp_f.write(soloc)
                    self.inp_f.write(soparam)
                    
                    if self.urban == True:
                        urbanopt = "SO URBANSRC " + str(srid[index]) + "\n"
                        self.inp_f.write(urbanopt)
                    
                    if self.blddw == "Y":
                        self.get_blddw(srid[index])
                        
                    if phase['phase'] == 'P':
                        self.get_particle(srid[index])
                        
                    elif phase['phase'] == 'V':
                        self.get_vapor(srid[index])
                        
                    if (self.emisvar_df is not None and type(self.emisvar_df) != str
                        and srid[index] in self.emisvar_df['source_id'].values ):
                        self.get_variation(srid[index])
                    
                     #if linked file
                    elif (self.emisvar_df is not None and 
                          type(self.emisvar_df) == str and srid[index] in var_sources ):
                        
                        solink = ("SO HOUREMIS " + self.emisvar_df + " " + 
                                  srid[index] + " \n")
                        self.inp_f.write(solink)
                    
                 # Area Source ---------------------------------------------------
        
                elif srct[index] == 'A':
                    soloc = ("SO LOCATION " + str(srid[index]) + " AREA " + 
                             str(xco1[index]) + " " + str(yco1[index]) + " " + 
                             str(elev[index]) + "\n")
                    
                    soparam = ("SO SRCPARAM " + str(srid[index]) + " " + 
                               str(1000/area[index]) + " " + str(relh[index]) + " " 
                               + str(lenx[index]) + " " + str(leny[index]) + " " + 
                               str(angl[index]) + " " + str(vert[index]) + "\n") 
                    
                    self.inp_f.write(soloc)
                    self.inp_f.write(soparam)
                    
                    if self.urban == True:
                        urbanopt = "SO URBANSRC " + str(srid[index]) + "\n"
                        self.inp_f.write(urbanopt)
                    
                    if self.blddw == "Y":
                        self.get_blddw(srid[index])
                        
                    if phase['phase'] == 'P':
                        self.get_particle(srid[index])
                        
                    elif phase['phase'] == 'V':
                        self.get_vapor(srid[index])
                        
                    if (self.emisvar_df is not None and type(self.emisvar_df) != str
                        and srid[index] in self.emisvar_df['source_id'].values ):
                        self.get_variation(srid[index])
                    
                     #if linked file
                    elif (self.emisvar_df is not None and 
                          type(self.emisvar_df) == str and srid[index] in var_sources ):
                        
                        solink = ("SO HOUREMIS " + self.emisvar_df + " " + 
                                  srid[index] + " \n")
                        self.inp_f.write(solink)
                        
                # Volume Source --------------------------------------------------
        
                elif srct[index] == 'V':
                    soloc = ("SO LOCATION " + str(srid[index]) + " VOLUME " + 
                             str(xco1[index]) + " " + str(yco1[index]) + " " + 
                             str(elev[index]) + "\n")
                    
                    soparam = ("SO SRCPARAM " + str(srid[index]) + " 1000 " + 
                               str(relh[index]) + " " + str(latr[index]) + " " + 
                               str(vert[index]) + "\n")
                    
                    self.inp_f.write(soloc)
                    self.inp_f.write(soparam)
                    
                    if self.urban == True:
                        urbanopt = "SO URBANSRC " + str(srid[index]) + "\n"
                        self.inp_f.write(urbanopt)
                    
                    if self.blddw == "Y":
                        self.get_blddw(srid[index])
                        
                    if phase['phase'] == 'P':
                        self.get_particle(srid[index])
                        
                    elif phase['phase'] == 'V':
                        self.get_vapor(srid[index])
                    
                    if ((self.emisvar_df is not None) and (type(self.emisvar_df) != str)
                        and (srid[index] in self.emisvar_df['source_id'].values)):
                        self.get_variation(srid[index])
                    
                    #if linked file
                    elif ((self.emisvar_df is not None) and 
                          (type(self.emisvar_df) == str) and (srid[index] in var_sources)):
                        
                        solink = ("SO HOUREMIS " + self.emisvar_df + " " + 
                                  srid[index] + " \n")
                        self.inp_f.write(solink)
                              
                # Area Polygon (Irregular) Source --------------------------------
        
                elif srct[index] == 'I':
                    # subset polyver_df to one source_id
                    
                    poly_srid = list(self.polyver_df[self.polyver_df['source_id']==
                                                     srid[index]]['source_id'][:])
                    
                    poly_utme = list(self.polyver_df[self.polyver_df['source_id']==
                                                     srid[index]]['utme'][:])
        
                    poly_utmn = list(self.polyver_df[self.polyver_df['source_id']==
                                                     srid[index]]['utmn'][:])
                                    
                    poly_numv = list(self.polyver_df[self.polyver_df['source_id']==
                                                     srid[index]]['numvert'][:])
                    
                    poly_area = list(self.polyver_df[self.polyver_df['source_id']==
                                                     srid[index]]['area'][:])
                            
                    soloc = ("SO LOCATION " + str(srid[index]) + " AREAPOLY " + 
                             str(xco1[index]) + " " + str(yco1[index]) + " " + \
                             " " + str(elev[index]) + "\n")
                    
                    soparam = ("SO SRCPARAM " + str(srid[index]) + " " + 
                               str(1000/(poly_area[0])) + " " + str(relh[index]) + 
                               " " + str(poly_numv[0]) + "\n")
                    
                    self.inp_f.write(soloc)
                    self.inp_f.write(soparam)
                                        
                    vert_start = "SO AREAVERT " + str(poly_srid[0]) + " "
                    vert_coor = ""
                    for i in np.arange(len(poly_srid)):
                        if (i+1) % 6 == 0 or i == len(poly_srid)-1:
                            vert_coor = (vert_coor + str(poly_utme[i]) + " " + 
                                         str(poly_utmn[i]) + " ")
                            
                            vert_line = vert_start + vert_coor + "\n"
                            self.inp_f.write(vert_line)
                            
                            vert_coor = ""
                        else:
                            vert_coor = (vert_coor + str(poly_utme[i]) + " " + 
                                         str(poly_utmn[i]) + " ")
                            ##write something?
                        
                    if self.urban == True:
                        urbanopt = "SO URBANSRC " + str(srid[index]) + "\n"
                        self.inp_f.write(urbanopt)
                    
                    if self.blddw == "Y":
                        self.get_blddw(srid[index])
                        
                    if phase['phase'] == 'P':
                        self.get_particle(srid[index])
                        
                    elif phase['phase'] == 'V':
                        self.get_vapor(srid[index])
                    
                    if ((self.emisvar_df is not None) and (type(self.emisvar_df) != str)
                        and (srid[index] in self.emisvar_df['source_id'].values)):
                        self.get_variation(srid[index])
    
                     #if linked file
                    elif ((self.emisvar_df is not None) and 
                          (type(self.emisvar_df) == str) and (srid[index] in var_sources)):
                        
                        solink = ("SO HOUREMIS " + self.emisvar_df + " " + 
                                  srid[index] + " \n")
                        self.inp_f.write(solink)
                        
                 # Line Source ----------------------------------------------------
        
                elif srct[index] == 'N':
                    soloc = ("SO LOCATION " + str(srid[index]) + " LINE " + 
                             str(xco1[index]) + " " + str(yco1[index]) + " " + 
                             str(xco2[index]) + " " + str(yco2[index]) + 
                             " " + str(elev[index]) + "\n")
                    
                    line_len = (math.sqrt((xco1[index] - xco2[index])**2 + 
                                          (yco1[index] - yco2[index])**2))
                    
                    soparam = ("SO SRCPARAM " + str(srid[index]) + " " + 
                               str( round(1000 / ( lenx[index] * line_len ), 10 ) ) + " " + 
                               str(relh[index]) + " " + str(lenx[index]) + " " + 
                               str(vert[index]) + "\n")
                    
                    self.inp_f.write(soloc)
                    self.inp_f.write(soparam)
                    
                    if self.urban == True:
                        urbanopt = "SO URBANSRC " + str(srid[index]) + "\n"
                        self.inp_f.write(urbanopt)
                    
                    if self.blddw == "Y":
                        self.get_blddw(srid[index])
                        
                    if phase['phase'] == 'P':
                        self.get_particle(srid[index])
                        
                    elif phase['phase'] == 'V':
                        self.get_vapor(srid[index])
                    
                    if ((self.emisvar_df is not None) and (type(self.emisvar_df) != str)
                        and (srid[index] in self.emisvar_df['source_id'].values)):
                        self.get_variation(srid[index])
                        
                    #if linked file
                    elif ((self.emisvar_df is not None) and 
                          (type(self.emisvar_df) == str) and (srid[index] in var_sources)):
                        
                        solink = ("SO HOUREMIS " + self.emisvar_df + " " + 
                                  srid[index] + " \n")
                        self.inp_f.write(solink)
                        
                    
                # Buoyant Line Source ---------------------------------------------
                
                elif srct[index] == 'B':
                    soloc = ("SO LOCATION " + str(srid[index]) + " BUOYLINE " + 
                             str(xco1[index]) + " " + str(yco1[index]) + " " + 
                             str(xco2[index]) + " " + str(yco2[index]) + " " + 
                             str(elev[index]) + "\n")
                    
                    blemis = 1000 
                    soparam = ("SO SRCPARAM " + str(srid[index]) + " " + 
                               str(blemis) + " " + str(relh[index]) + "\n")
                    
                    if first_buoyant == 0:
                        sobuopa = ("SO BLPINPUT " + str(self.buoyant_df['avgbld_len'].iloc[0]) + 
                                   " " + str(self.buoyant_df['avgbld_hgt'].iloc[0]) + 
                                   " " + str(self.buoyant_df['avgbld_wid'].iloc[0]) + 
                                   " " + str(self.buoyant_df['avglin_wid'].iloc[0]) + 
                                   " " + str(self.buoyant_df['avgbld_sep'].iloc[0]) + 
                                   " " + str(self.buoyant_df['avgbuoy'].iloc[0]) + "\n")
                        
                        first_buoyant = 1
                        self.inp_f.write(sobuopa)
                    
                    self.inp_f.write(soloc)
                    self.inp_f.write(soparam)
                    
                    if self.urban == True:
                        urbanopt = "SO URBANSRC " + str(srid[index]) + "\n"
                        self.inp_f.write(urbanopt)
                    
                    if self.blddw == "Y":
                        self.get_blddw(srid[index])
                        
                    if phase['phase'] == 'P':
                        self.get_particle(srid[index])
                        
                    elif phase['phase'] == 'V':
                        self.get_vapor(srid[index])
                        
                    if ((self.emisvar_df is not None) and (type(self.emisvar_df) != str)
                        and (srid[index] in self.emisvar_df['source_id'].values)):
                        self.get_variation(srid[index])
                   
                    #if linked file
                    elif ((self.emisvar_df is not None) and 
                          (type(self.emisvar_df) == str) and (srid[index] in var_sources)):
                        
                        solink = ("SO HOUREMIS " + self.emisvar_df + " " + 
                                  srid[index] + " \n")
                        self.inp_f.write(solink)

                
             
             # SO Source groups ---------------------------------------------
            
        self.uniqsrcs = srid.unique()
        for i in np.arange(len(self.uniqsrcs)): 
            if self.uniqsrcs[i] not in self.model.sourceExclusion.get(self.phaseType+self.facid, ''):
                sogroup = ("SO SRCGROUP " + self.uniqsrcs[i] + " " + 
                           self.uniqsrcs[i] + "-" + self.uniqsrcs[i] + "\n")
                self.inp_f.write(sogroup)
        so3 = "SO FINISHED \n" + "\n"
        self.inp_f.write(so3)
                
        
    def build_re(self, discrecs_df, cenx, ceny, polar_df):
        """
        Writes RE section to aer.inp
        
        """
        self.polar_df = polar_df
        newline = "\n"
        
        res = "RE STARTING  " + "\n"
        self.inp_f.write(res)
 

    ## Discrete Receptors

        recx = list(discrecs_df[utme][:])
        recy = list(discrecs_df[utmn][:])
        rece = list(discrecs_df[elev][:]) # Elevations
        rech = list(discrecs_df[hill][:]) # Hill height

        for i in np.arange(len(recx)):
            if self.eleva == "Y":
                redec = ("RE DISCCART  " + str("{:.0f}".format(recx[i])) + " " + str("{:.0f}".format(recy[i])) + 
                         " " + str(normal_round(rece[i])) + " " + 
                         str(normal_round(rech[i])) + "\n")
            else:
                redec = "RE DISCCART  " + str("{:.0f}".format(recx[i])) + " " + str("{:.0f}".format(recy[i])) + "\n"
            self.inp_f.write(redec)

            
    ## Polar Recptors
    
        rep = "RE GRIDPOLR polgrid1 STA" + "\n"
        repo = ("RE GRIDPOLR polgrid1 ORIG " + str(cenx) + " " + 
                str(ceny) + "\n")
        repd = "RE GRIDPOLR polgrid1 DIST "

        self.inp_f.write(rep)
        self.inp_f.write(repo)
        self.inp_f.write(repd)

        recep_dis = self.polar_df["distance"].unique()
        num_rings = len(recep_dis)
        
        for i in np.arange(num_rings):
            repdis = str(recep_dis[i]) + " "
            self.inp_f.write(repdis)

        self.inp_f.write(newline)

        repi = "RE GRIDPOLR polgrid1 DDIR "
        self.inp_f.write(repi)

        recep_dir = self.polar_df["angle"].unique()
        num_sectors = len(recep_dir)
        for i in np.arange(num_sectors):
            repdir = str(recep_dir[i]) + " "
            self.inp_f.write(repdir)

        self.inp_f.write(newline)
    
        #add elevations and hill height if user selected it
        if self.eleva == "Y":
            for i in range(1, num_sectors+1):
                indexStr = "S" + str(i) + "R1"
                repelev0 = ("RE GRIDPOLR polgrid1 ELEV " + 
                            str(self.polar_df["angle"].loc[indexStr]) + " ")
                
                self.inp_f.write(repelev0)
                
                for j in range(1, num_rings+1):
                    indexStr = "S" + str(i) + "R" + str(j)
                    repelev1 = str(self.polar_df["elev"].loc[indexStr]) + " "
                    self.inp_f.write(repelev1)

                self.inp_f.write(newline)

                rephill0 = ("RE GRIDPOLR polgrid1 HILL " + 
                            str(self.polar_df["angle"].loc[indexStr]) + " ")
                
                self.inp_f.write(rephill0)
                
                for j in range(1, num_rings+1):
                    indexStr = "S" + str(i) + "R" + str(j)
                    rephill1 = str(self.polar_df["hill"].loc[indexStr]) + " "
                    self.inp_f.write(rephill1)
                self.inp_f.write(newline)
        
        repe = "RE GRIDPOLR polgrid1 END" + "\n"
        self.inp_f.write(repe)
        
        ref = "RE FINISHED  " + "\n"
        self.inp_f.write(ref)
        
        
    def build_me(self, cenlat, cenlon):
        """
        Writes the ME section to aer.inp
        """
        user_station = self.facoptn_df['met_station'].iloc[0]
        if user_station == '':
            surf_file, upper_file, surfdata_str, uairdata_str, prof_base, distance, year = \
                                    fm.find_met(cenlat, cenlon, self.model.metlib.dataframe)
        else:
            surf_file, upper_file, surfdata_str, uairdata_str, prof_base, distance, year = \
                                    fm.return_met(self.facid, cenlat, cenlon, user_station, self.model.metlib.dataframe)
            
        
        jsta = 1
        
        if ( year % 4 ) == 0:
            jend = 366
        else:
            jend = 365
    
        mes    = "ME STARTING \n"
        me_sfc = "ME SURFFILE  metdata\\" + surf_file + "\n"
        me_pfl = "ME PROFFILE  metdata\\" + upper_file + "\n"
        me_sud = "ME SURFDATA  " + surfdata_str +  "\n"
        me_uad = "ME UAIRDATA  " + uairdata_str + "\n"
        me_prb = "ME PROFBASE  " + str(prof_base) + "\n"
        
        me_strtend = ""
        if self.facoptn_df['annual'].iloc[0] != 'Y' and self.facoptn_df['period_start'].iloc[0] != '' and self.facoptn_df['period_end'].iloc[0] != '':
            me_strtend = "ME STARTEND  " + self.facoptn_df['period_start'].iloc[0] + self.facoptn_df['period_end'].iloc[0] + "\n"
        mef = "ME FINISHED \n"
    
        self.inp_f.write(mes)
        self.inp_f.write(me_sfc)
        self.inp_f.write(me_pfl)
        self.inp_f.write(me_sud)
        self.inp_f.write(me_uad)
        self.inp_f.write(me_prb)

        if len(me_strtend) > 0:
            self.inp_f.write(me_strtend)
        self.inp_f.write(mef)

        return surf_file, distance
        
    def build_ou(self):
        """
        Writes OU section of aer.inp
        """
        
        acute = self.facoptn_df['acute'].iloc[0] #move to ou
        acute_hrs = self.facoptn_df['hours'].iloc[0]
        if acute == "":
            acute = "N"
        
        ou = "OU STARTING \n"
        self.inp_f.write(ou)
    
        ou = "OU FILEFORM EXP \n"
        self.inp_f.write(ou)

        met_annual_keyword = " ANNUAL " if self.annual else " PERIOD "
        for j in np.arange(len(self.uniqsrcs)):  
            if self.uniqsrcs[j] not in self.model.sourceExclusion.get(self.phaseType+self.facid, ''):
                ou = ("OU PLOTFILE" + met_annual_keyword + self.uniqsrcs[j] +
                      " plotfile.plt 35 \n")
                self.inp_f.write(ou)

        # Output seasonhr plot file if temporal output is requested
        if self.model.temporal == True:
            for k in np.arange(len(self.uniqsrcs)):
                if self.uniqsrcs[k] not in self.model.sourceExclusion.get(self.phaseType+self.facid, ''):
                    ou = ("OU SEASONHR " + self.uniqsrcs[k] +
                          " seasonhr.plt 36 \n")
                    self.inp_f.write(ou)

        if acute == "Y":
            hivalstr = str(self.facoptn_df['hivalu'].iloc[0])
            recacu = "OU RECTABLE "  + str(acute_hrs) + " " + hivalstr + "\n"
            self.inp_f.write(recacu)
            for k in np.arange(len(self.uniqsrcs)):  
                if self.uniqsrcs[k] not in self.model.sourceExclusion.get(self.phaseType+self.facid, ''):
                    acuou = ("OU PLOTFILE " + str(acute_hrs) + " " + self.uniqsrcs[k] + 
                          " " + hivalstr + " maxhour.plt 40 \n")
                    self.inp_f.write(acuou)

            #set in model options
            self.model.model_optns['acute'] = True
            
        
        ou = "OU FINISHED \n"
        self.inp_f.write(ou)
    
        self.inp_f.close()
        
    def get_blddw(self, srid):
        """
        Compiles and writes building downwash parameters for a given source
        to aer.inp
        
        """
        
        newline = "\n"
        
        bldgdim_df = self.bldgdw_df
        bldg_srid = bldgdim_df['source_id'][:]
        bldg_keyw = bldgdim_df['keyword'][:]
        
        for i, r in bldg_srid.iteritems():
            if srid == bldg_srid[i]:
                row = bldgdim_df.loc[i,]
                values = row.tolist()
                if values[2] == 'XBADJ':     
                    #keywords too short so adding 4 spaces to preserve columns                                
                    bldgdim = ('SO ' + values[2] + "    " +  
                               " ".join(str(x) for x in values[3:]))
                    self.inp_f.write(bldgdim)
                    self.inp_f.write(newline)
                    
                elif values[2] == 'YBADJ':
                    #keywords too short so adding 4 spaces to preserve columns                               
                    bldgdim = ('SO ' + values[2] + "    " +  
                               " ".join(str(x) for x in values[3:]))
                    self.inp_f.write(bldgdim)
                    self.inp_f.write(newline)
                
                else:
                    bldgdim = (" ".join(str(x) for x in values[1:]))
                    self.inp_f.write(bldgdim)
                    self.inp_f.write(newline)
                    
    def get_particle(self, srid):
        """
        Compiles and writes paremeters for particle deposition/depletion 
        by source
        """
            
       #get values for this source id
        if srid not in self.model.sourceExclusion.get(self.phaseType+self.facid, ''):
            emisloc = self.emisloc_df.loc[self.emisloc_df.source_id == srid]
            method2 = emisloc[method].iloc[0] == 2

            if method2:
                massfrac_val = emisloc[massfrac].iloc[0]
                partdiam_val = emisloc[partdiam].iloc[0]
                someth2 = "SO METHOD_2 " + srid + " " + str(massfrac_val) + " " + str(partdiam_val) + "\n"
                self.inp_f.write(someth2)
            else:
                partdia_source = self.partdia_df[self.partdia_df['source_id'] == srid]
                part_diam = partdia_source['part_diam'].tolist()
                part_dens = partdia_source['part_dens'].tolist()
                mass_frac = partdia_source['mass_frac'].tolist()


                sopdiam = ("SO PARTDIAM " + str(srid) + " " +
                           " ".join(map(str, part_diam)) +"\n")
                sopdens = ("SO PARTDENS " + str(srid) + " " +
                           " ".join(map(str, part_dens))+"\n")
                somassf = ("SO MASSFRAX " + str(srid) + " " +
                           " ".join(map(str, mass_frac))+"\n")

                self.inp_f.write(sopdiam)
                self.inp_f.write(somassf)
                self.inp_f.write(sopdens)

    def get_vapor(self, srid):
        """
        Compiles and writes parameters for vapor deposition/depletion by source
        """
        if srid not in self.model.sourceExclusion.get(self.phaseType+self.facid, ''):
            
            pollutants = (self.hapemis[(self.hapemis['source_id'] == srid)
                                        & (self.hapemis['part_frac'] < 1)]['pollutant'].str.lower())
            pollutants.reset_index(drop=True, inplace=True)
                           
            params = self.model.gasparams.dataframe.loc[self.model.gasparams.dataframe['pollutant'].isin(pollutants)]
            params.reset_index(drop=True, inplace=True)
                    
            #write values if they exist in the 
            #so there should only be one pollutant per source id for vapor/gas deposition to work
            #currently default values if the size of pollutant list is greater than 1
            
            if len(params) != 1: ## check if len params works for empties
                #log message about defaulting 
                
                da    =    0.07
                dw    =    0.70
                rcl   = 2000.00
                henry =    5.00
                
                sodepos = ("SO GASDEPOS " + str(srid) + " " + str(da) + 
                           " " + str(dw) + " " + str(rcl) + " " + str(henry) + "\n")
                self.inp_f.write(sodepos)
                
            else:
            
            #check gas params dataframe for real values and pull them out for that one source
                    #print('found vapor params', params)
                
                    sodepos = ("SO GASDEPOS " + str(srid) + " " + str(params['da'].iloc[0]) + 
                           " " + str(params['dw'].iloc[0]) + " " + str(params['rcl'].iloc[0]) + 
                           " " + str(params['henry'].iloc[0]) + "\n")
                    self.inp_f.write(sodepos)
   
    def get_variation(self, srid):
        """
        Compiles and writes parameters for emissions variation
        """
        
        #get row
        sourcevar =  self.emisvar_df[self.emisvar_df["source_id"] == srid]
        
        #get qflag
        qflag = sourcevar[sourcevar.columns[2]].str.upper().values[0]
        
        #get variation -- variable number of columns so slice all starting at 4
        variation = sourcevar[sourcevar.columns[3:]].values
        
        #seasons, windspeed or month will have only one list
        if len(variation) == 1:
                    
            var = variation[0].tolist()
            var = ['' if math.isnan(x)==True else x for x in var]
            sotempvar = str("SO EMISFACT " + str(srid) + " " + qflag +  " " +
                         " ".join(map(str, var)) +"\n")
            
            self.inp_f.write(sotempvar)
            
        #everything elese will have lists in multiples of 12   
        else:
                    
            for row in variation:
                var = row.tolist()
                sotempvar = str("SO EMISFACT " + str(srid) + " " + qflag +  " " +
                         " ".join(map(str, var)) +"\n")
            
                self.inp_f.write(sotempvar)
        
                 
                
    
            

        
            
            
        
        
        
             