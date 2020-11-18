# -*- coding: utf-8 -*-
"""
Created on Thu Nov  1 12:35:08 2018
@author: 
"""
import pandas as pd

from com.sca.hem4.log import Logger
from com.sca.hem4.upload.DependentInputFile import DependentInputFile
from tkinter import messagebox
from com.sca.hem4.model.Model import *

class EmisVar(DependentInputFile):

    def __init__(self, path, dependency):
        self.model = dependency
        self.faclist_df = self.model.faclist.dataframe
        self.emisloc_df = self.model.emisloc.dataframe
        self.path = path
        DependentInputFile.__init__(self, path, dependency)

    def createDataframe(self):
        
        #not calling standard read_file because length of columns is variable
        #depending on varation type
        
        #checking to see if variaiton file is excel or txt.
        #NEED TO MAKE SURE THE LINKED FILE HAS A .txt in it or append
        
        if self.path[-3:] == 'txt': 
            
            self.dataframe = self.path #save linked file path in place of dataframe
        
        else: #excel file used
        
            emisvar_df = pd.read_excel(self.path, skiprows=0, dtype=str)
            
            emisvar_df.columns = map(str.lower, emisvar_df.columns)
            
            # rename first three columns
            emisvar_df.rename(columns={"facility id": fac_id,
                                       "source id": source_id,
                                       emisvar_df.columns[2]: "variation"}, inplace=True)
            
            # convert all columns to float64 except first three
            float_cols=[i for i in emisvar_df.columns if i not in ["fac_id","source_id","variation"]]
            for col in float_cols:
                emisvar_df[col]=pd.to_numeric(emisvar_df[col], errors="coerce")

            self.dataframe = emisvar_df

        cleaned = self.clean(self.dataframe)
        validated = self.validate(cleaned)
        
        
        if validated is None:
                
            self.dataframe = pd.DataFrame()


    def clean(self, df):
        cleaned = df
        cleaned.replace(to_replace={fac_id:{"nan":""}, source_id:{"nan":""}}, inplace=True)
        cleaned = cleaned.reset_index(drop = True)

        # upper case of selected fields
        cleaned['variation'] = cleaned['variation'].str.upper()
        cleaned['variation'] = cleaned['variation'].str.strip()
        cleaned[source_id] = cleaned[source_id].str.upper()

        return cleaned

    def validate(self, df):
        # ----------------------------------------------------------------------------------
        # Strict: Invalid values in these columns will cause the upload to fail immediately.
        # ----------------------------------------------------------------------------------
        if len(df.loc[(df[fac_id] == '')]) > 0:
            Logger.logMessage("One or more facility IDs are missing in the Emissions Variations List.")
            messagebox.showinfo("Missing facility IDs", "One or more facility IDs are missing in the Emissions Variations List.")
            return None

        if len(df.loc[(df[source_id] == '')]) > 0:
            Logger.logMessage("One or more source IDs are missing in the Emissions Variations List.")
            messagebox.showinfo("Missing source IDs", "One or more source IDs are missing in the Emissions Variations List.")
            return None
                
        val_list = []
        for index, row in df.iterrows():
            facility = row[fac_id]

            valid = ['SEASON', 'MONTH', 'HROFDY', 'WSPEED', 'SEASHR', 'HRDOW',
                     'HRDOW7', 'SHRDOW', 'SHRDOW7', 'MHRDOW', 'MHRDOW7']
            if row['variation'] not in valid:
                Logger.logMessage("Facility " + facility + ": variation value invalid.")
                messagebox.showinfo("Variation invalid", "Facility " + facility + ": variation value invalid.")
                
                return None
        #-----------------------------------------------------------------------------------------------------
        # Confirm that all facilities needing emission variation according to the Facility List
        # are in the emission variation file.
        
        print("still going?")
        # facilities in emission variation file
        var_facs = set(df[fac_id])
        
        # facilities needing emission variation
        faclist_facs = set(self.faclist_df[self.faclist_df['emis_var']=='Y'][fac_id])
        
        if faclist_facs.issubset(var_facs) == False:
            missing = faclist_facs - var_facs
            Logger.logMessage("One or more facilities in the Facility List file that need " +
                              "emission variation are not in the emission variation file. These facilities are: " +
                              ", ".join(missing) + ". Please edit the emission variation file or Facility List file.")
            messagebox.showinfo("Missing facilities in Emission Variation", "One or more facilities in the Facility List file that need " +
                              "emission variation are not in the emission variation file. These facilities are: " +
                              ", ".join(missing) + ". Please edit the emission variation file or Facility List file.")
            return None


        #-----------------------------------------------------------------------------------------------------
        # Make sure all facility/source ids from emission variation file are also in
        # the emission location file

        # facility/source ids from emission variation file
        var_ids = set(df[[fac_id, source_id]].apply(lambda x: ','.join(x), axis=1).tolist())

        # facility/source ids from emission location file
        model_ids = set(self.emisloc_df[[fac_id, source_id]].apply(lambda x: ','.join(x), axis=1).tolist())

        if len(set(var_ids).difference(set(model_ids))) > 0:
            missing = set(var_ids).difference(set(model_ids))

            messagebox.showinfo("Missing Emission Location", "The emission " +
                                "variation file indicates variation for facility/source ids " +
                                ", ".join(missing) + " which are not in the " +
                                "emissions location file. Please edit " +
                                "the emissions variation or emissions location "+
                                " file.")
            return None


        vtype = df['variation'].tolist()

        if 'SEASON' in vtype:
            
            # check that seasonal variaton only has 4 values
            seasons = df[df['variation'].str.upper() == 'SEASON']
            print(seasons)
            s_wrong = []
            for row in seasons.iterrows():
                if len(row[1].dropna().values[3:]) != 4:
                    s_wrong.append(row[1][source_id])

            if len(s_wrong) > 0:
                messagebox.showinfo("Seasonal Emissions Variation",
                                    "Seasonal emissions variations require 4 "+
                                    "values. Sources: " + ", ".join(s_wrong) +
                                    " do not have the correct number of values. " +
                                    "Please update your Emission Variation File.")
                return None

        # check wind speed is only 6 values
        if 'WSPEED' in vtype:
            wspeed = df[df['variation'].str.upper() == 'WSPEED']
            w_wrong = []
            for row in wspeed.iterrows():
                if len(row[1].dropna().values[3:]) != 6:
                    w_wrong.append(row[1][source_id])

            if len(w_wrong) > 0:
                messagebox.showinfo("Wind Speed Emissions Variation",
                                    "Wind speed emissions variations require 6 "+
                                    "values. Sources: " + ", ".join(w_wrong) +
                                    " do not have the correct number of values. " +
                                    "Please update your Emission Variation File.")
                return None

        # make sure the monthly emissions variation has 12 values
        if 'MONTH' in vtype:
            month = df[df['variation'].str.upper() == 'MONTH']
            m_wrong = []
            for row in month.iterrows():
                if len(row[1].dropna().values[3:]) != 12:
                    m_wrong.append(row[1][source_id])

            if len(m_wrong) > 0:
                messagebox.showinfo("Monthly Emissions Variation",
                                    "Monthly emissions variations require 12 "+
                                    "values. Sources: " + ", ".join(m_wrong) +
                                    " do not have the correct number of values. " +
                                    "Please update your Emission Variation File.")
                return None

        if 'HROFDY' in vtype or 'SEASHR' in vtype or 'SHRDOW' in vtype or 'SHRDOW7' in vtype:
            other = df[~df['variation'].isin(['MONTH', 'WSPEED', 'SEASON'])]
            variation = other[other.columns[3:]].values

            o_wrong = 0
            for row in variation:
                if len(row) != 12:
                    o_wrong += 1

            if o_wrong > 0:
                messagebox.showinfo("Emissions Variation Error",
                                    "One of the emissions variations type does "+
                                    "not have the correct number of values. "+
                                    "Please check your input file to make all "+
                                    "values are either a multiple or factor "+
                                    "of 12.")
                return None

        Logger.logMessage("Uploaded emissions variations for [" + ",".join(var_ids) + "]\n")
        return df
