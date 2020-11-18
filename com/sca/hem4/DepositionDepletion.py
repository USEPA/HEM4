# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 15:21:42 2018

@author: dlindsey
"""
import numpy as np
#for testing
#facops = pd.read_excel("Template_Multi_Facility_List_Options_dep_deplt_test.xlsx")
#facops.rename(columns={'FacilityID':'fac_id'}, inplace=True)
from com.sca.hem4.upload.EmissionsLocations import method


def check_phase(r):
        
    print('facility', r['fac_id'])
                
    print('vdep:', r['vdep'])
    
    print('vdepl:', r['vdepl'])
    
    print('pdep:', r['pdep'])
    
    print ('pdepl:', r['pdepl'])

    if r['dep'] != 'nan':
        dep = r['dep'].upper()
    else:
        dep = ''


    if r['depl'] != 'nan':
        depl = r['depl'].upper()
    else:
        depl = ''

    
    if r['vdep'] != 'nan':
        vdep = r['vdep'].upper()
    else:
        vdep = ''
    
    
    if r['vdepl'] != 'nan':
        vdepl = r['vdepl'].upper()        
    else:
        vdepl = ''
    
    
    if r['pdep'] != 'nan':
        pdep = r['pdep'].upper()        
    else:
        pdep = ''
    
    if r['pdepl'] != 'nan':
            pdepl = r['pdepl'].upper()       
    else:
        pdepl = ''
        
    poss = ['DO', 'WO', 'WD']

    phaseResult = []


    if dep == 'Y' and depl == 'N' and vdep == 'NO' and vdepl == 'NO' and pdep == 'NO' and pdepl == 'NO':
        # Special case where only breakout of particle and vapor is needed in the outputs, but no dep/depl
        phase = 'Z'
        phaseResult.append(phase)
        
    if dep == 'Y' or depl == 'Y':
            
        if vdep in poss or vdepl in poss:
            if pdep in poss:
                phase = 'B'
                phaseResult.append(phase)
                
            elif pdepl in poss:
                phase = 'B'
                phaseResult.append(phase)      
        
        if vdep in poss or vdepl in poss: 
        
            if pdep not in poss:
                phase = 'V'
                phaseResult.append(phase)
                
            elif pdepl not in poss:
                phase = 'V'
                phaseResult.append(phase)
            
        if pdep in poss or pdepl in poss:
        
            if vdep not in poss:
                phase = 'P'
                phaseResult.append(phase)
                
            elif vdepl not in poss:
                phase = 'P'
                phaseResult.append(phase)
    
       
    if len(phaseResult) > 1:
        phaseResult = 'B'
        
    elif len(phaseResult) == 1:
        phaseResult = phaseResult[0]
        
    else:
        phaseResult = ''
        
     
    print('phaseResult:', phaseResult)
    return(phaseResult)

def check_dep(faclist_df, emisloc_df):
    """
    Looks through deposition and depletion flags and returns optional inputs and 
    dataframe with keywords.
    
    """
    
    inputs = []
                
            
    
    phase = faclist_df[['fac_id', 'phase']].values

    
#    phase = phaseList
#    print('NEW PHASE LIST:', phase)

    
    deposition = faclist_df['dep'].tolist()
    vapor_depo = faclist_df['vdep'].tolist()
    part_depo = faclist_df['pdep'].tolist()
    
    depletion = faclist_df['depl'].tolist()
    vapor_depl = faclist_df['vdepl'].tolist()
    part_depl = faclist_df['pdepl'].tolist()
    
#    print("phase", phase)
#    
#    print("deposition:", deposition, type(deposition))
#    print("vapor deposition:", vapor_depo)
#    print("particle deposition:", part_depo)
#    
#    
#    print("depletion:", depletion)
#    print("vapor depletion", vapor_depl)
#    print("particle depletion", part_depl)
    
    #loop through each positionally
    i = 0
    for fac_id, p in phase:
        
        
        if p == 'P':

            if usingMethodOne(emisloc_df):
                #add facid
                options = [fac_id]
                options.append('particle size')

                inputs.append(options)
                            
        elif p == 'V':
            
            #add facid
            options = [fac_id]

            if (deposition[i] == 'Y' or depletion[i] == 'Y'):
                
                if (('DO' in vapor_depo[i]) or ('WD' in vapor_depo[i])):
                    options.append('land use')
                    options.append('seasons')

                if (('DO' in vapor_depl[i]) or ('WD' in vapor_depl[i])):
                    options.append('land use')
                    options.append('seasons')
            
            inputs.append(options)
      
        elif p == 'B':
                        
            #add facid
            options = [fac_id]
                        
            if (deposition[i] == 'Y' and depletion[i] != 'Y'): 
            
                strarr = ['DO', 'WO', 'WD']
                if any(c in part_depo[i] for c in strarr):
                    if usingMethodOne(emisloc_df):
                        options.append('particle size')
                
                    if (('WD' in vapor_depo[i]) or ('DO' in vapor_depo[i])):
                        options.append('land use')
                        options.append('seasons')
            
                elif (('NO' in part_depo[i]) and (('WD' in vapor_depo[i]) or ('DO' in vapor_depo[i]))):
                     options.append('land use')
                     options.append('seasons')
                
            elif (depletion[i] == 'Y' and deposition[i] != 'Y'):
                
                strarr = ['DO', 'WO', 'WD']
                if any(c in part_depl[i] for c in strarr):

                    if usingMethodOne(emisloc_df):
                        options.append('particle size')

                    if (('WD' in vapor_depl[i]) or ('DO' in vapor_depl[i])):
                        options.append('land use')
                        options.append('seasons')                        
                
                elif (('NO' in part_depl[i]) and (('WD' in vapor_depl[i]) or ('DO' in vapor_depl[i]))):
                    options.append('land use')
                    options.append('seasons')
                    
                elif (('NO' in part_depl[i]) and ('NO' in vapor_depl[i])):
                    if usingMethodOne(emisloc_df):
                        options.append('particle size')
    
    
            elif (deposition[i] == 'Y' and depletion[i] == 'Y'):
                
                strarr = ['DO', 'WO', 'WD']
                if (any(c in part_depo[i] for c in strarr) or 
                        any(c in part_depl[i] for c in strarr)):

                    if usingMethodOne(emisloc_df):
                        options.append('particle size')
                    
                    strarr = ['WD', 'DO']
                    if (any(c in vapor_depo[i] for c in strarr) or 
                        any(c in vapor_depl[i] for c in strarr)):
                        options.append('land use')
                        options.append('seasons')
                    
                elif (('NO' in part_depo[i]) and ('NO' in part_depl[i])):
                    strarr = ['WD', 'DO']
                    if (any(c in vapor_depo[i] for c in strarr) or 
                        any(c in vapor_depl[i] for c in strarr)):
                        options.append('land use')
                        options.append('seasons')
                    
         
            inputs.append(options)
        i += 1
     
    print("inputs", inputs)  
     
    return(inputs)

def usingMethodOne(emisloc_df):
    return 1 in emisloc_df[method].values

def sort(facops):

    """

    """
    #print('facility slice:', facops)
    #Sprint('phase', facops['phase'])
    phase = facops['phase'].tolist()[0].upper()                    # Phase

    depos = facops['dep'].fillna("").tolist()[0]                       # Deposition
    vdepo = facops['vdep'].fillna("").tolist()[0]                       # Vapor Deposition
    pdepo = facops['pdep'].fillna("").tolist()[0]                       # Particle Deposition

    deple = facops['depl'].fillna("").tolist()[0]                       # Depletion
    vdepl = facops['vdepl'].fillna("").tolist()[0]                       # Vapor Depletion
    pdepl = facops['pdepl'].fillna("").tolist()[0]                       # Particle Depletion

    if depos == "" or depos == "nan":
        depos = "N"

    if deple == "" or deple == "nan":
        deple = "N"

    ## don't forget to call upper!

    if phase == 'P' or phase =='V':

        return single_phase(phase, depos, deple, vdepo, pdepo, vdepl, pdepl)

    elif phase == 'B':

        #construct particle and vapor runs separately
        particle = single_phase('P', depos, deple, None, pdepo, None, pdepl)

        vapor = single_phase('V', depos, deple, vdepo, None, vdepl, None)

        return [particle, vapor]




##order matters for phase 'B'-- particle first, then vapor
def single_phase(phase, depos, deple, vdepo, pdepo, vdepl, pdepl):
    
    opts = []
    
    if phase == "P":
        
        if "Y" in depos:
            
            # deposition is Y
            
            if deple == "N":
                
                # depletion is N - this overrides whatever is put in pdepl
                
                if pdepo == "NO":
                    opts.append("")
                if pdepo == "DO":
                    opts.append(" DDEP NODRYDPLT NOWETDPLT ")
                if pdepo == "WO":
                    opts.append(" WDEP NODRYDPLT NOWETDPLT ")
                if pdepo == "WD":
                    opts.append(" DDEP WDEP NODRYDPLT NOWETDPLT ")
                    
            else:
                
                # depletion is Y - consider pdepl
        
                if pdepo == "NO" and pdepl == "NO":
                    opts.append(" NODRYDPLT NOWETDPLT ")
                if pdepo == "NO" and pdepl == "DO":
                    opts.append(" DRYDPLT NOWETDPLT ")
                if pdepo == "NO" and pdepl == "WO":
                    opts.append(" WETDPLT NODRYDPLT ")
                if pdepo == "NO" and pdepl == "WD":
                    opts.append(" DRYDPLT WETDPLT ")
                
                if pdepo == "DO" and pdepl == "NO":
                    opts.append(" DDEP NODRYDPLT NOWETDPLT ")
                if pdepo == "DO" and pdepl == "DO":
                    opts.append(" DDEP DRYDPLT NOWETDPLT ")
                if pdepo == "DO" and pdepl == "WO":
                    opts.append(" DDEP WETDPLT NODRYDPLT ")
                if pdepo == "DO" and pdepl == "WD":
                    opts.append(" DDEP DRYDPLT WETDPLT ")

                if pdepo == "WO" and pdepl == "NO":
                    opts.append(" WDEP NODRYDPLT NOWETDPLT ")
                if pdepo == "WO" and pdepl == "DO":
                    opts.append(" WDEP DRYDPLT NOWETDPLT ")
                if pdepo == "WO" and pdepl == "WO":
                    opts.append(" WDEP WETDPLT NODRYDPLT ")
                if pdepo == "WO" and pdepl == "WD":
                    opts.append(" WDEP DRYDPLT WETDPLT ")

                if pdepo == "WD" and pdepl == "NO":
                    opts.append(" DDEP WDEP NODRYDPLT NOWETDPLT ")
                if pdepo == "WD" and pdepl == "DO":
                    opts.append(" DDEP WDEP DRYDPLT NOWETDPLT ")
                if pdepo == "WD" and pdepl == "WO":
                    opts.append(" DDEP WDEP WETDPLT NODRYDPLT ")
                if pdepo == "WD" and pdepl == "WD":
                    opts.append(" DDEP WDEP DRYDPLT WETDPLT ")


        else:
            
            # deposition is N - this overrides whatever is put in pdepo
            
            if deple == "N":
                
                # depletion is N
                
                opts.append("")
                    
            else:
                
                # depletion is Y
        
                if pdepl == "NO":
                    opts.append(" NODRYDPLT NOWETDPLT ")
                if pdepl == "DO":
                    opts.append(" DRYDPLT NOWETDPLT ")
                if pdepl == "WO":
                    opts.append(" WETDPLT NODRYDPLT ")
                if pdepl == "WD":
                    opts.append(" DRYDPLT WETDPLT ")
                

    else:
        
        # phase is V
        
        if "Y" in depos:
            
            # deposition is Y
            
            if deple == "N":
                
                # depletion is N - this overrides whatever is put in vdepl
                
                if vdepo == "NO":
                    opts.append("None")
                if vdepo == "DO":
                    opts.append(" DDEP NODRYDPLT NOWETDPLT ")
                if vdepo == "WO":
                    opts.append(" WDEP NODRYDPLT NOWETDPLT ")
                if vdepo == "WD":
                    opts.append(" DDEP WDEP NODRYDPLT NOWETDPLT ")
                    
            else:
                
                # depletion is Y - consider vdepl
        
                if vdepo == "NO" and vdepl == "NO":
                    opts.append(" NODRYDPLT NOWETDPLT ")
                if vdepo == "NO" and vdepl == "DO":
                    opts.append(" DRYDPLT NOWETDPLT ")
                if vdepo == "NO" and vdepl == "WO":
                    opts.append(" WETDPLT NODRYDPLT ")
                if vdepo == "NO" and vdepl == "WD":
                    opts.append(" DRYDPLT WETDPLT ")
                
                if vdepo == "DO" and vdepl == "NO":
                    opts.append(" DDEP NODRYDPLT NOWETDPLT ")
                if vdepo == "DO" and vdepl == "DO":
                    opts.append(" DDEP DRYDPLT NOWETDPLT ")
                if vdepo == "DO" and vdepl == "WO":
                    opts.append(" DDEP WETDPLT NODRYDPLT ")
                if vdepo == "DO" and vdepl == "WD":
                    opts.append(" DDEP DRYDPLT WETDPLT ")

                if vdepo == "WO" and vdepl == "NO":
                    opts.append(" WDEP NODRYDPLT NOWETDPLT ")
                if vdepo == "WO" and vdepl == "DO":
                    opts.append(" WDEP DRYDPLT NOWETDPLT ")
                if vdepo == "WO" and vdepl == "WO":
                    opts.append(" WDEP WETDPLT NODRYDPLT ")
                if vdepo == "WO" and vdepl == "WD":
                    opts.append(" WDEP DRYDPLT WETDPLT ")

                if vdepo == "WD" and vdepl == "NO":
                    opts.append(" DDEP WDEP NODRYDPLT NOWETDPLT ")
                if vdepo == "WD" and vdepl == "DO":
                    opts.append(" DDEP WDEP DRYDPLT NOWETDPLT ")
                if vdepo == "WD" and vdepl == "WO":
                    opts.append(" DDEP WDEP WETDPLT NODRYDPLT ")
                if vdepo == "WD" and vdepl == "WD":
                    opts.append(" DDEP WDEP DRYDPLT WETDPLT ")


        else:
            
            # deposition is N - this overrides whatever is put in pdepo
            
            if deple == "N":
                
                # depletion is N
                
                opts.append("")
                    
            else:
                
                # depletion is Y
        
                if vdepl == "NO":
                    opts.append(" NODRYDPLT NOWETDPLT ")
                if vdepl == "DO":
                    opts.append(" DRYDPLT NOWETDPLT ")
                if vdepl == "WO":
                    opts.append(" WETDPLT NODRYDPLT ")
                if vdepl == "WD":
                    opts.append(" DRYDPLT WETDPLT ")


    print('Keyword', opts)
    return {'phase': phase, 'settings': opts}

