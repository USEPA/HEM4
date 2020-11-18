import os
import shutil
import threading
from datetime import datetime
import pdb

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

threadLocal = threading.local()

class resumeProcessor():

    abort = None
    def __init__ (self, model, remaining_facs, abort):
        self.model = model
        self.facs = remaining_facs
        self.abort = abort
        self.exception = None
        print("processor starting")

    def abortProcessing(self):
        self.abort.set()

    def process(self):


        # create Inputs folder
        inputspkgr = InputsPackager(self.model.rootoutput, self.model)
        inputspkgr.createInputs()

       
        Logger.logMessage("RUN GROUP: " + self.model.group_name)
        

        threadLocal.abort = False


        Logger.logMessage("Preparing Inputs for " + str(self.model.facids.count()) + " facilities\n")
        
        
       
        fac_list = []
        for index, row in self.model.faclist.dataframe.iterrows():
            
            facid = row[0]
            #print(facid)
            fac_list.append(facid)
            num = 1

        Logger.logMessage("The facility ids being modeled: " + ", ".join(fac_list))
        print("The facility ids being modeled: " + ", ".join(fac_list))

        success = False

        # Create output files with headers for any source-category outputs that will be appended
        # to facility by facility. These won't have any data for now.
        self.createSourceCategoryOutputs()
        
        for facid in fac_list:
            print(facid)
            if self.abort.is_set():
                Logger.logMessage("Aborting processing...")
                print("abort")
                return
            
            
            
            #save version of this gui as is? constantly overwrite it once each facility is done?
            Logger.logMessage("Running facility " + str(num) + " of " +
                              str(len(fac_list)))
            
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
                
                
            else:
                ## if the try is successful this is where we would update the 
                # dataframes or cache the last processed facility so that when 
                # restart we know which faciltiy we want to start on
                # increment facility count
            
              

                num += 1
                success = True
                

                #reset model options aftr facility
                self.model.model_optns = defaultdict()
                
#                try:  
#                    self.model.save.remove_folder()
#                except:
#                    pass
#                
            

        Logger.logMessage("HEM4 Modeling Completed. Finished modeling all" +
                      " facilities. Check the log tab for error messages."+
                      " Modeling results are located in the Output"+
                      " subfolder of the HEM4 folder.")

    
         #remove save folder after a completed run

        
        

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
