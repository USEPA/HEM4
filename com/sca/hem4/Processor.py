import os
import shutil
import threading
from datetime import datetime
import pdb

import pandas as pd

from com.sca.hem4.SaveState import SaveState
from com.sca.hem4.log.Logger import Logger
from com.sca.hem4.runner.FacilityRunner import FacilityRunner
from com.sca.hem4.writer.excel.FacilityMaxRiskandHI import FacilityMaxRiskandHI
from com.sca.hem4.writer.excel.FacilityCancerRiskExp import FacilityCancerRiskExp
from com.sca.hem4.writer.excel.FacilityTOSHIExp import FacilityTOSHIExp
from com.sca.hem4.writer.kml.KMLWriter import KMLWriter
from com.sca.hem4.inputsfolder.InputsPackager import InputsPackager

import traceback
from collections import defaultdict
import uuid

from tkinter import messagebox


threadLocal = threading.local()

class Processor():

    abort = None
    def __init__ (self, nav, model, abort):
        self.nav = nav
        self.model = model
        self.abort = abort
        self.exception = None
        print("processor starting")

    def abortProcessing(self):
        self.abort.set()

    def process(self):


        try:
            # create Inputs folder
            inputspkgr = InputsPackager(self.model.rootoutput, self.model)
            inputspkgr.createInputs()
          
        except BaseException as ex:
            print(ex)

       
        Logger.logMessage("RUN GROUP: " + self.model.group_name)
        

        threadLocal.abort = False

        
        #create a Google Earth KML of all sources to be modeled
        try:
            kmlWriter = KMLWriter()
            if kmlWriter is not None:
                kmlWriter.write_kml_emis_loc(self.model)
                Logger.logMessage("KMZ for all sources completed")

        except BaseException as ex:
                self.exception = ex
                fullStackInfo=''.join(traceback.format_exception(
                    etype=type(ex), value=ex, tb=ex.__traceback__))
                message = "An error occurred while trying to create the KML file of all facilities:\n" + fullStackInfo
                print(message)
                Logger.logMessage(message)
           
        else:
          
            print(str(self.model.facids.count()))
            
            Logger.logMessage("Preparing Inputs for " + str(self.model.facids.count()) + " facilities\n")
            
           
            fac_list = []
            for index, row in self.model.faclist.dataframe.iterrows():
                
                facid = row[0]
                #print(facid)
                fac_list.append(facid)
                num = 1
    
    #        Logger.logMessage("The facility ids being modeled: , False)
            Logger.logMessage("The facility ids being modeled: " + ", ".join(fac_list))
    
            success = False
    
            # Create output files with headers for any source-category outputs that will be appended
            # to facility by facility. These won't have any data for now.
            self.createSourceCategoryOutputs()
            
            self.completed = []
            self.skipped = []
            for facid in fac_list:
                print(facid)
                if self.abort.is_set():
                    Logger.logMessage("Aborting processing...")
                    print("abort")
                    return success
                
                
                
                #save version of this gui as is? constantly overwrite it once each facility is done?
                Logger.logMessage("Running facility " + str(num) + " of " + str(len(fac_list)))
                
                success = False
                
                
                try:
                    runner = FacilityRunner(facid, self.model, self.abort)
                    runner.setup()

                except BaseException as ex:

                    self.exception = ex
                    fullStackInfo=''.join(traceback.format_exception(
                        etype=type(ex), value=ex, tb=ex.__traceback__))
    
                    message = "An error occurred while running a facility:\n" + fullStackInfo
                    print(message)
                    Logger.logMessage(message)
                    
                    self.skipped.append(facid)
                    continue

                    ## if the try is successful this is where we would update the 
                    # dataframes or cache the last processed facility so that when 
                    # restart we know which faciltiy we want to start on
                    # increment facility count
                
                  
                try:
                    self.model.aermod
                    
                except:
                    
                    pass
                
                else:
                    if self.model.aermod == False:
                        
                        fac_folder = self.model.rootoutput + str(facid)
                           
                        # move plotfile.plt file
                        plt_version = 'plotfile.plt'
                        
                        # Move aermod.inp, aermod.out, and plotfile.plt to the fac output folder
                        # If phasetype is not empty, rename aermod.out, aermod.inp and plotfile.plt using phasetype
                        # Replace if one is already in there othewrwise will throw error
                        if os.path.isfile(fac_folder + 'aermod.out'):
                            os.remove(fac_folder + 'aermod.out')
            
                        if os.path.isfile(fac_folder + 'aermod.inp'):
                            os.remove(fac_folder + 'aermod.inp')
            
                        if os.path.isfile(fac_folder + plt_version):
                            os.remove(fac_folder + plt_version)
            
                        # move aermod.out file
                        try:
                            output = os.path.join("aermod", "aermod.out")
                            shutil.move(output, fac_folder)
                        except:
                            pass
                        
                        # move aermod.inp file
                        try:
                            inpfile = os.path.join("aermod", "aermod.inp")
                            shutil.move(inpfile, fac_folder)
                        except:
                            pass
                        
                        try:
                            pltfile = os.path.join("aermod", plt_version)
                            shutil.move(pltfile, fac_folder)
                        except:
                            pass
                        
                        # if an acute maxhour.plt plotfile was output by Aermod, move it too
                        maxfile = os.path.join("aermod", "maxhour.plt")
                        if os.path.isfile(maxfile):
                            if os.path.isfile(fac_folder + "maxhour.plt"):
                                os.remove(fac_folder + "maxhour.plt")
                            shutil.move(maxfile, fac_folder)
            
                        # if a temporal seasonhr.plt plotfile was output by Aermod, move it too
                        seasonhrfile = os.path.join("aermod", "seasonhr.plt")
                        if os.path.isfile(seasonhrfile):
                            if os.path.isfile(fac_folder + "seasonhr.plt"):
                                os.remove(fac_folder + "seasonhr.plt")
                            shutil.move(seasonhrfile, fac_folder)
                                    
                        self.skipped.append(facid)
                        self.model.aermod = None
                        
                    else:
                        self.completed.append(facid)
                    
                num += 1
                success = True
                

                #reset model options aftr facility
                self.model.model_optns = defaultdict()
                
#                try:  
#                    self.model.save.remove_folder()
#                except:
#                    pass
                
                
         # move the log file to the run dir and re-initialize
        Logger.archiveLog(self.model.rootoutput)
        Logger.initializeLog()
        
        if self.abort.is_set():
            
            
            Logger.logMessage('HEM4 RUN GROUP: ' + str(self.model.group_name) + ' canceled')
            messagebox.showinfo('Run Canceled', 'HEM4 RUN GROUP: ' + str(self.model.group_name) + ' canceled')
            self.nav.abortLabel.destroy()
        
        elif len(self.skipped) == 0:
            
#            self.model.save.remove_folder()
            
            Logger.logMessage("HEM4 Modeling Completed. Finished modeling all" +
                          " facilities. Check the log tab for error messages."+
                          " Modeling results are located in the Output"+
                          " subfolder of the HEM4 folder.")
            
            messagebox.showinfo('Modeling Completed', "HEM4 Modeling Completed. Finished modeling all" +
                          " facilities. Check the log tab for error messages."+
                          " Modeling results are located in the Output"+
                          " subfolder of the HEM4 folder.")

        else:

#            self.model.save.remove_folder()
            
            Logger.logMessage("HEM4 completed " + str(len(self.completed)) + 
                              " facilities and skipped " + str(len(self.skipped))+ 
                              " facilities. Modeling not completed for: " + "\n ".join(self.skipped))
            messagebox.showinfo('Modeling Completed', "HEM4 completed " + str(len(self.completed)) + 
                              " facilities and skipped " + str(len(self.skipped))+ 
                              " facilities. Modeling not completed for: " + "\n ".join(self.skipped))

            
            # output skipped facilities to csv
            skipped_path = self.model.rootoutput + 'Skipped_Facilities.xlsx'
            skipped_df = pd.DataFrame(self.skipped, columns=['Facility'])
            print(skipped_df)
            
            skipped_df.to_excel(skipped_path, index=False)

       
        
        self.nav.reset_gui()

        
        

        return success

    def createSourceCategoryOutputs(self):
        
        # Create Facility Max Risk and HI file
        fac_max_risk = FacilityMaxRiskandHI(self.model.rootoutput, None, self.model, None, None)
        fac_max_risk.write()
        
        # Create Facility Cancer Risk Exposure file
        fac_canexp = FacilityCancerRiskExp(self.model.rootoutput, None, self.model, None)
        fac_canexp.write()
        
        # Create Facility TOSHI Exposure file
        fac_hiexp = FacilityTOSHIExp(self.model.rootoutput, None, self.model, None)
        fac_hiexp.write()
