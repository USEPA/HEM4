# -*- coding: utf-8 -*-
"""
Created on Thu Jun  7 13:08:48 2018

@author: jbaker
"""

import os
import sys

import pandas as pd
from tkinter.filedialog import askopenfilename
from tkinter import messagebox

from com.sca.hem4.upload.EmissionsLocations import source_type
from com.sca.hem4.upload.FacilityList import *


class InputChecker():
    
    def __init__(self, model):
        """
        The Input Checker takes the model class (with all inputs) and checks that
        required inputs (Facilities Options List, Hap Emissions, and Emissions Locations)
        are present and correspond to each other as well as additional optional inputs.
        
        There are two functions, check_required and check_dependent
        
        check_required returns the result dictionary with:
        - any error messages triggered
        - a list of dependent inputs flagged from the FOL
        - a reset key for an incorrectly uploaded required input (if applicable) 
        
        check_dependent
        """
        self.model = model    

    def check_required(self):
        
        # store result in dictionary
        result = {'result': None, 'dependencies': [], 'reset': None  } 
        
        # check if data frame exists
        try:
             self.model.faclist.dataframe
        
        # raise attribute error if it doesn't
        except AttributeError:
            logMsg = ("Facilities list options file uploaded incorrectly," + 
                      " please try again")
            
            result['result'] = logMsg
            result['reset'] = 'fac'
            return result
        else:
            # make sure dataframe isn't empty
            if self.model.faclist.dataframe.empty:
                logMsg = ("There was an error uploading Facilities Options List" + 
                          " file, please try again")
                
                result['result'] = logMsg
                return result

            # extract ids and determine if there are dependent uploads .
            else:
                # find dependents and return which ones need to be checked
                # user receptors
                if 'Y' in self.model.faclist.dataframe[user_rcpt].tolist():
                    result['dependencies'].append(user_rcpt)
                    
                # building downwash
                if 'Y' in self.model.faclist.dataframe[bldg_dw].tolist():
                    result['dependencies'].append('downwash')

        try:
            self.model.hapemis.dataframe
        except AttributeError:
            logMsg2 = "HAP Emissions file uploaded incorrectly, please try again"
            result['result'] = logMsg2
            result['reset'] = 'hap'
            return result
        else:
            if self.model.hapemis.dataframe.empty:
                logMsg2 = ("There was an error uploading HAP Emissions file," +
                           " please try again")
                
                result['result'] = logMsg2
                return result
            else:
                # get locations and source ids
                hfids = set(self.model.hapemis.dataframe[fac_id])

        try:
             self.model.emisloc.dataframe
        except AttributeError:
            logMsg3 = "Emissions Locations file uploaded incorrectly, please try again"
            
            result['result'] = logMsg3
            return result
        else:
            if self.model.emisloc.dataframe.empty:
                logMsg3 = ("There was an error uploading Emissions Locations" + 
                           " file, please try again")
                
                result['result'] = logMsg3
                result['reset'] = 'emis'
                return result
            else:
                # check source types for optional inputs
                if 'I' in self.model.emisloc.dataframe[source_type].tolist():
                    result['dependencies'].append('polyvertex')
                    print('POLYVERTEX FOUND')
                    
                if 'B' in self.model.emisloc.dataframe[source_type].tolist():
                    result['dependencies'].append('bouyant')
                    
                # check for deposition or depletion
                if ['B'] in self.model.faclist.dataframe[phase].tolist():
                    result['dependencies'].append('both')
                    
                if ['V'] in self.model.faclist.dataframe[phase].tolist():
                    result['dependencies'].append('vapor')
                    
                if ['P'] in self.model.faclist.dataframe[phase].tolist():
                    result['dependencies'].append('particle')

        result['result'] = 'ready'
        return result

    def check_dependent(self, optional_list):
        
        # store result in dictionary
        result = {'result': None, 'reset': None  }
        
        for option in optional_list:
            
            if option is 'user_rcpt':
                
                
                
                try:
                    
                    print("checked user receptors")
                    
                    self.model.ureceptr.dataframe
            
                except AttributeError as e:
                    logMsg7 = ("User receptors are specified in the Facilities" +
                              " List Options file, please upload user recptors" + 
                              " for " )
                    
                    result['result'] = logMsg7 + e
                    result['reset'] = 'user_rcpt'
                    return result
                
                else:
                
                    #check facility ids in ureceptors 
                    uids = set(self.model.ureceptr.dataframe[fac_id])
                    fids = set(self.model.faclist.dataframe[self.model.faclist.dataframe[user_rcpt] == 'Y'][fac_id].values)
                    
                    if fids.intersection(uids) != fids:
                        
                        missing = fids - uids
                        
                        logMsg7b = ("Facilities: " + ",".join(list(missing)) + 
                                    " are missing user receptors that were" +
                                    " indicated in the Facilities list options file")
            
                        result['result'] =  logMsg7b
                        result['reset'] = 'user_rcpt'
                        return result

            elif option is 'bouyant':
                
                try:
                    
                    print("checked buoyant")
                    self.model.multibuoy.dataframe
                
                except:
                    logMsg8 = ("Buoyant Line parameters are specified in the " + 
                               " Facilities List Options file, please upload " + 
                               " buoyant line sources for " )

                    result['result'] = logMsg8
                    result['reset'] = 'bouyant_line'
                    return result
                
                else:
                    
                    #check facility ids against bouyant line ids
                    bids = set(self.model.multibuoy.dataframe[fac_id])
                    fids = set(self.model.emisloc.dataframe[self.model.emisloc.dataframe[source_type] == 'B'][fac_id].values)
                    
                    if fids.intersection(bids) != fids:
                        
                        missing = fids - bids
                        
                        logMsg8b = ("Facilities: " + ",".join(list(missing)) + 
                                    " are missing bouyant line sources that" +
                                    " were indicated in the Facilities list" +
                                    " options file")
            
                        result['result'] = logMsg8b
                        result['reset'] = 'bouyant_line'
                        return result
                    
                    else:
                        # set ureceptr model option to TRUE
                        self.model.model_optns['ureceptr'] = True

            elif option is 'polyvertex':
                
                try: 
                    print("checked polyvertex")
                    self.model.multipoly.dataframe
                    
                except:
                    
                    logMsg9 = ("Polyvertex parameters are specified in the " + 
                               " Facilities List Options file, please upload " + 
                               " polyvertex source file " )
                    
                    
                    result['result'] = logMsg9
                    result['reset'] = 'polyvertex'
                    return result
                       
                
                else:
                    
                    # check facility ids against polyvertex ids
                    pids = set(self.model.multipoly.dataframe[fac_id])
                    fids = set(self.model.emisloc.dataframe[self.model.emisloc.dataframe[source_type] == 'I'][fac_id].values)
                    
                    if fids.intersection(pids) != fids:
                        
                        missing = fids - pids
                        
                        logMsg9b = ("Facilities: " + ",".join(list(missing)) + 
                                    " are missing polyvertex sources that" +
                                    " were indicated in the Facilities list" +
                                    " options file")
            
                        result['result'] =  logMsg9b
                        result['reset'] = 'poly_vertex'
                        return result
                    
                    # check source ids
                    # make sure source ids match in hap emissions and emissions location
                    # for facilities in faclist file

                    efac = set(self.model.emisloc.dataframe[fac_id])
                    
                    in_pol = list(fids.intersection(pids)) 
                    in_emis = list(fids.intersection(efac))
                    
                    psource = set(self.model.multipoly.dataframe[self.model.multipoly.dataframe[fac_id].isin(in_pol)][source_id])
                    esource = set(self.model.emisloc.dataframe[self.model.emisloc.dataframe[fac_id].isin(in_emis)][source_id])
            
                    for p in psource:
                        if p not in list(esource):
                            
                            logMsg9c = ("Source ids for Emissions Locations and Polygon Vertex file"+ 
                                       " do not match, please upload corresponding files.")
                            result['result'] =  logMsg9c
                            return result
            
            elif option is 'downwash':
            
                try:
                    print("checked downwash")
                    self.model.bldgdw.dataframe
                    
                except:
                    
                    logMsg14 = ("Building downwash parameters are specified in "+
                                  "the Facilities List Options file, please " +
                                  "upload a Building Downwash file " )
                    
                    result['result'] = logMsg14
                    result['reset'] = 'bldgdw'
                    return result

                else:
            
                    dids = set(self.model.bldgdw.dataframe[fac_id])
                    fids = set(self.model.faclist.dataframe[self.model.faclist.dataframe[bldg_dw] == 'Y'][fac_id].values)
                    
                    if fids.intersection(dids) != fids:
                        
                        missing = fids - dids
                        
                        logMsg14b = ("Facilities: " + ",".join(list(missing)) + 
                                    " are missing building downash specifications" +
                                    " that were indicated in the Facilities list" +
                                    " Options file")
                    
                     
                        result['result'] = logMsg14b
                        result['reset'] = 'bldgdw'
                
                        return result

                    # check source ids
                    # make sure source ids match in hap emissions and emissions location
                    # for facilities in faclist file

                    efac = set(self.model.emisloc.dataframe[fac_id])
                    
                    in_dw = list(fids.intersection(dids)) 
                    in_emis = list(fids.intersection(efac))
                    
                    bdsource = set(self.model.bldgdw.dataframe[self.model.bldgdw.dataframe[fac_id].isin(in_dw)][source_id])
                    esource = set(self.model.emisloc.dataframe[self.model.emisloc.dataframe[fac_id].isin(in_emis)][source_id])
            
                    for d in bdsource:
                        if d not in list(esource):
                            
                            logMsg14c = ("Source ids for Emissions Locations and Building Downwash file"+ 
                                       " do not match, please upload corresponding files.")
                            result['result'] =  logMsg14c
                            return result
            
            elif option is 'particle':

                try:
                    
                    self.model.partdep.dataframe
                    
                except AttributeError:
                    
                    logMsg10 = ("Particle deposition or depletion parameters" +
                                " are specified in the Facilities List Options" +
                                " file. Please upload a Particle Size File." )
                    
                    result['result'] = logMsg10
                    result['reset'] = 'particle'
                    return result
                    
                else:
                    
                    partids = set(self.model.partdep.dataframe[fac_id])
                    fids = set(self.model.faclist.dataframe[self.model.faclist.dataframe[phase] == 'P'][fac_id].values)
                    
                    if fids.intersection(partids) != fids:
                        missing = fids - partids
                        
                        logMsg10b = ("Facilities: " + ",".join(list(missing)) + 
                                    " are missing particle size specifications that" +
                                    " were indicated in the Facilities list" +
                                    " Options file")
            
                        result['result'] =  logMsg10b
                        result['reset'] = 'particle'
                        return result

                    # check source ids
                    # make sure source ids match in hap emissions and emissions location
                    # for facilities in faclist file

                    efac = set(self.model.emisloc.dataframe[fac_id])
                    
                    in_part = list(fids.intersection(partids)) 
                    in_emis = list(fids.intersection(efac))
                    
                    partsource = set(self.model.partdep.dataframe[self.model.partdep.dataframe[fac_id].isin(in_part)][source_id])
                    esource = set(self.model.emisloc.dataframe[self.model.emisloc.dataframe[fac_id].isin(in_emis)][source_id])
            
                    for part in partsource:
                        if part not in list(esource):
                            
                            logMsg10c = ("Source ids for Emissions Locations and Particle Size file"+ 
                                       " do not match, please upload corresponding files.")
                            result['result'] =  logMsg10c
                            return result

            elif option is 'vapor':
                
                try:
                    
                    self.model.landuse.dataframe
                    
                except:
                    
                    logMsg11 = ("Vapor deposition or depletion parameters" +
                                " are specified in the Facilities List Options" +
                                " file. Please upload a Land Use File." )
                    
                    result['result'] = logMsg11
                    result['reset'] = 'vapor'
                    
                else:
                    
                    try:
                        
                        self.model.seasons.dataframe
                        
                    except:
                        
                        logMsg12 = ("Vapor deposition or depletion parameters" +
                                " are specified in the Facilities List Options" +
                                " file. Please upload a Land Use File." )
                    
                        result['result'] = logMsg12
                        result['reset'] = 'vapor'
                        return result
                    
                    
                    
                    else:
                        
                        landids = set(self.model.landuse.dataframe[fac_id])
                        sids = set(self.model.seasons.dataframe[fac_id])
                        fids = set(self.model.faclist.dataframe[self.model.faclist.dataframe[phase] == 'V'][fac_id].values)
                        
                        if fids.intersection(landids) != fids:
                            missing = fids - landids
                            
                            logMsg11b = ("Facilities: " + ",".join(list(missing)) + 
                                        " are missing land use specifications that" +
                                        " were indicated in the Facilities list" +
                                        " Options file")
                
                            result['result'] =  logMsg11b
                            result['reset'] = 'particle'
                            return result
                        
                        
                        if fids.intersection(sids) != fids:
                            missing = fids - sids
                            
                            logMsg12b = ("Facilities: " + ",".join(list(missing)) + 
                                        " are missing seasonal vegetation " +
                                        "specifications that were indicated in the "+
                                        "Facilities List Options file")
                
                            result['result'] =  logMsg12b
                            result['reset'] = 'particle'
    
                            
                            return result
            
            elif option is 'both':

                vdepo = self.model.faclist.dataframe['vdep'].fillna("").tolist()[0]                       # Vapor Deposition
                pdepo = self.model.faclist.dataframe['pdep'].fillna("").tolist()[0]                       # Particle Deposition
                
                vdepl = self.model.faclist.dataframe['vdepl'].fillna("").tolist()[0]                       # Vapor Depletion
                pdepl =self.model.faclist.dataframe['pdepl'].fillna("").tolist()[0]
                
                no = ['NO', 'no', 'No']    #condition with no 'No's'
                
                if (no not in vdepo and no not in pdepo and no not in vdepl and
                    no not in pdepl):
                    
                    
                    try:
                    
                        self.model.partdep.dataframe
                    
                    except:
                    
                        logMsg15 = ("Particle deposition or depletion parameters" +
                                    " are specified in the Facilities List Options" +
                                    " file. Please upload a Particle Size File." )
                        
                        result['result'] = logMsg15
                        result['reset'] = 'particle'
                        return result
                    
                    
                    else:
                    
                        try:
                        
                            self.model.landuse.dataframe
                        
                        except:
                        
                            logMsg16 = ("Vapor deposition or depletion parameters" +
                                    " are specified in the Facilities List Options" +
                                    " file. Please upload a Land Use File." )
                        
                            result['result'] = logMsg16
                            result['reset'] = 'vapor'
                            return result
                        
                            
                        else:
                            
                            try:
                                
                                self.model.seasons.dataframe
                                
                            except:
                                
                                logMsg17 = ("Vapor deposition or depletion parameters" +
                                        " are specified in the Facilities List Options" +
                                        " file. Please upload a Land Use File." )
                            
                                result['result'] = logMsg17
                                result['reset'] = 'vapor'
                                return result
                    
                            else:
                                #need to compare
                                pass

                # WHAT about both conditions that have a NO on dep or depl?
                pass

        result['result'] = 'ready'
        return result

