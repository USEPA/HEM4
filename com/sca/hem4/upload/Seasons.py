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

class Seasons(DependentInputFile):

    def __init__(self, path, dependency):
        self.gasDryFacs = dependency
        DependentInputFile.__init__(self, path, dependency)

    def createDataframe(self):

        # Specify dtypes for all fields
        self.numericColumns = ["M01", "M02", "M03", "M04","M05", "M06", "M07", "M08", "M09","M10","M11", "M12"]
        self.strColumns = [fac_id]
        seasons_df = self.readFromPath((fac_id, "M01", "M02", "M03", "M04",
                                        "M05", "M06", "M07", "M08", "M09","M10",
                                        "M11", "M12"))

        self.dataframe = seasons_df

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
            Logger.logMessage("One or more facility IDs are missing in the Months-to-Seasons List.")
            messagebox.showinfo("Missing facility IDs", "One or more facility IDs are missing in the Months-to-Seasons List.")
            return None

        seasonfids = set(df[fac_id])
        faclistfids = set(self.gasDryFacs)
        if faclistfids.intersection(seasonfids) != faclistfids:
            Logger.logMessage("Based on your Facility List Options file, the Months-to-Seasons List is missing " +
                              "one or more facilities. Please correct one or both files and upload again.")
            messagebox.showinfo("Missing facilities", "Based on your Facility List Options file, the Months-to-Seasons List is missing " +
                              "one or more facilities. Please correct one or both files and upload again.")
            return None

        duplicates = self.duplicates(df, [fac_id])
        if len(duplicates) > 0:
            Logger.logMessage("One or more records are duplicated in the Months-to-Seasons List (key=fac_id):")
            messagebox.showinfo("Duplicate records", "One or more records are duplicated in the Months-to-Seasons List (key=fac_id).")
            for d in duplicates:
                Logger.logMessage(d)
            return None

        for index, row in df.iterrows():

            facility = row[fac_id]

            for num in range(1, 13):
                number = str(num)
                number = "0"+number if num < 10 else number
                field = "M" + number
                if row[field] not in [1,2,3,4,5]:
                    Logger.logMessage("Facility " + facility + ": Field " + field + " contains invalid value.")
                    messagebox.showinfo("Invalid values", "Facility " + facility + ": Field " + field + " contains invalid value.")
                    return None

        # figure out how to get fac ids that have particle based on flag or index
        # TODO

        # check for unassigned seasons
        check_seasons_assignment = set(df[fac_id])

        Logger.logMessage("Uploaded seasonal variation data for [" + ",".join(check_seasons_assignment) + "]\n")
        return df
