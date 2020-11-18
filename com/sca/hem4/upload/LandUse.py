#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 31 20:13:09 2018
@author: d
"""
from com.sca.hem4.log import Logger
from com.sca.hem4.model.Model import fac_id
from com.sca.hem4.upload.DependentInputFile import DependentInputFile
from tkinter import messagebox

class LandUse(DependentInputFile):

    def __init__(self, path, dependency):
        
        self.gasDryFacs = dependency
        DependentInputFile.__init__(self, path, dependency)

    def createDataframe(self):

        # Specify dtypes for all fields
        self.numericColumns = ["D01", "D02","D03", "D04", "D05","D06", "D07","D08","D09","D10","D11","D12","D13","D14",
                               "D15", "D16", "D17","D18", "D19", "D20","D21", "D22", "D23","D24", "D25", "D26",
                               "D27", "D28", "D29","D30", "D31", "D32","D33", "D34", "D35","D36"]
        self.strColumns = [fac_id]
        
        landuse_df = self.readFromPath((fac_id, "D01", "D02",
                                        "D03", "D04", "D05",
                                        "D06", "D07", "D08",
                                        "D09", "D10", "D11",
                                        "D12", "D13", "D14",
                                        "D15", "D16", "D17",
                                        "D18", "D19", "D20",
                                        "D21", "D22", "D23",
                                        "D24", "D25", "D26",
                                        "D27", "D28", "D29",
                                        "D30", "D31", "D32",
                                        "D33", "D34", "D35",
                                        "D36"))

        self.dataframe = landuse_df
        

    def clean(self, df):
        cleaned = df
        cleaned.replace(to_replace={fac_id:{"nan":""}}, inplace=True)
        cleaned = cleaned.reset_index(drop = True)

        return cleaned

    def validate(self, df):
        # ----------------------------------------------------------------------------------
        # Strict: Invalid values in these columns will cause the upload to fail immediately.
        # ----------------------------------------------------------------------------------
        if len(df.loc[(df[fac_id] == '')]) > 0:
            Logger.logMessage("One or more facility IDs are missing in the Land Use List.")
            messagebox.showinfo("Missing facility IDs", "One or more facility IDs are missing in the Land Use List.")
            return None

        landfids = set(df[fac_id])
        faclistfids = set(self.gasDryFacs)
        if faclistfids.intersection(landfids) != faclistfids:
            Logger.logMessage("Based on your Facility List Options file, the Land Use List is missing " +
                              "one or more facilities. Please correct one or both files and upload again.")
            messagebox.showinfo("Land use list missing", "Based on your Facility List Options file, the Land Use List is missing " +
                              "one or more facilities. Please correct one or both files and upload again.")
            return None

        duplicates = self.duplicates(df, [fac_id])
        if len(duplicates) > 0:
            Logger.logMessage("One or more records are duplicated in the Land Use List (key=fac_id):")
            messagebox.showinfo("Duplicate records", "One or more records are duplicated in the Land Use List (key=fac_id)")
            for d in duplicates:
                Logger.logMessage(d)
            return None

        for index, row in df.iterrows():

            facility = row[fac_id]

            for num in range(1, 37):
                number = str(num)
                number = "0"+number if num < 10 else number
                field = "D" + number
                if row[field] not in [1,2,3,4,5,6,7,8,9]:
                    Logger.logMessage("Facility " + facility + ": Field " + field + " contains invalid value.")
                    messagebox.showinfo("Invalid value", "Facility " + facility + ": Field " + field + " contains invalid value.")
                    return None

        # figure out how to get fac ids that have landuse based on flag or index
        # TODO

        # check for unassigned landuse
        check_landuse_assignment = set(df[fac_id])

        Logger.logMessage("Uploaded land use data for [" + ",".join(check_landuse_assignment) + "]\n")
        return df
