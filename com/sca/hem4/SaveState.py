# -*- coding: utf-8 -*-
"""
Created on Sat Jan 19 23:10:19 2019

@author: David Lindsey

"""
import traceback
import os
import shutil
import pickle

from com.sca.hem4.log import Logger
from com.sca.hem4.model.Model import fac_id


class SaveState():
    
    def __init__(self, runid, model):
        
        self.runid = runid
        self.model = model
        
        #create folder to save files in 
       #set output folder
        save_folder = "save/"+self.runid + "/"
        print('created save folder')
        self.save_folder = save_folder
       
        try:
            
            if os.path.exists(save_folder):
                shutil.rmtree(save_folder)
                
            os.makedirs(save_folder)
            
        except BaseException as ex:

            exception = ex
            fullStackInfo=''.join(traceback.format_exception(
                etype=type(ex), value=ex, tb=ex.__traceback__))

            message = "An error occurred while running a facility:\n" + fullStackInfo
            Logger.logMessage(message)
           
        else:
            message = "created " + self.save_folder
            Logger.logMessage(message)

    def save_model(self, facid):
        
        Logger.logMessage("removing fac id:" + facid)
        

        #get list of attributes and write them 
        remaining_facs = self.model.facids.tolist()[1:]
        #print("model attributes", self.model.__dict__)
        remaining_loc = self.save_folder + "/remaining_facs.pkl" 
        
        fachandler = open(remaining_loc, 'wb') 
       
        pickle.dump(remaining_facs, fachandler)
        
        print("Saved facs")
#        model_loc = self.save_folder + "/model.pkl"
#        
#        modelHandler = open(remaining_loc, 'w') 
#       
#        pickle.dump(remaining_facs, filehandler)
#   
#        
#        #if faclist is greater than 1 fac_id then save
#        if len(self.model.faclist.dataframe[fac_id]) > 1:
#            #loop through dataframes
#        
#            for k, v in self.model.__dict__.items():
#            
#                if k in attr_list and v is not None:
#
#                   #remove the faclist row then pickle
#                   try:
#                       
#                       remaining = v.dataframe[v.dataframe['fac_id'] != facid]
#                       
#                   except:
#                       
#                       #means its does response or some other df
#                       pass
#                       
#                   else:
#                   
#                   #pikle
#                       remaining.to_pickle(f"{self.save_folder}/{k}.pkl")
#                       print(k, 'pickled')
#                       
#      
#            
                
    def remove_folder(self):
        
        #remove save folder and everythign else
        shutil.rmtree(self.save_folder)
        
