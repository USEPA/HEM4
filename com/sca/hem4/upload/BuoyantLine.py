#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  3 20:34:16 2018

@author: d
"""
from com.sca.hem4.log import Logger
from com.sca.hem4.model.Model import fac_id
from com.sca.hem4.upload.DependentInputFile import DependentInputFile
from tkinter import messagebox

from com.sca.hem4.upload.EmissionsLocations import source_type

avgbld_len = 'avgbld_len';
avgbld_hgt = 'avgbld_hgt';
avgbld_wid = 'avgbld_wid';
avglin_wid = 'avglin_wid';
avgbld_sep = 'avgbld_sep';
avgbuoy = 'avgbuoy';

class BuoyantLine(DependentInputFile):

    def __init__(self, path, dependency):
        self.emisloc_df = dependency
        DependentInputFile.__init__(self, path, dependency)
        
    def createDataframe(self):

        # Specify dtypes for all fields
        self.numericColumns = [avgbld_len,avgbld_hgt,avgbld_wid,avglin_wid,avgbld_sep,avgbuoy]
        self.strColumns = [fac_id]

        multibuoy_df = self.readFromPath(
            (fac_id, avgbld_len, avgbld_hgt, avgbld_wid, avglin_wid, avgbld_sep, avgbuoy))
            
        self.dataframe = multibuoy_df

    def clean(self, df):
        cleaned = df.fillna({avgbld_len:0, avgbld_hgt:0, avgbld_wid:0, avglin_wid:0, avgbld_sep:0, avgbuoy:0})        
        cleaned.replace(to_replace={fac_id:{"nan":""}}, inplace=True)
        cleaned = cleaned.reset_index(drop = True)

        return cleaned

    def validate(self, df):
        
        # ----------------------------------------------------------------------------------
        # Strict: Invalid values in these columns will cause the upload to fail immediately.
        # ----------------------------------------------------------------------------------
        if len(df.loc[(df[fac_id] == '')]) > 0:
            Logger.logMessage("One or more facility IDs are missing in the Buoyant Line List.")
            messagebox.showinfo("Missing facility IDs", "One or more facility IDs are missing in the Buoyant Line List.")
            return None

        duplicates = self.duplicates(df, [fac_id])
        if len(duplicates) > 0:
            Logger.logMessage("One or more records are duplicated in the Buoyant Line Parameters List (key=fac_id):")
            messagebox.showinfo("Duplicate records", "One or more records are duplicated in the Buoyant Line Parameters List (key=fac_id):")
            for d in duplicates:
                Logger.logMessage(d)
            return None

        for index, row in df.iterrows():
            facility = row[fac_id]

            if row[avgbld_len] <= 0:
                Logger.logMessage("Facility " + facility + ": avg building length " + str(row[avgbld_len]) +
                                  " out of range.")
                messagebox.showinfo("Out of Range", "Facility " + facility + ": avg building length " + str(row[avgbld_len]) +
                                  " out of range.")
                return None
            if row[avgbld_hgt] <= 0:
                Logger.logMessage("Facility " + facility + ": avg building height " + str(row[avgbld_hgt]) +
                                  " out of range.")
                messagebox.showinfo("Out of Range", "Facility " + facility + ": avg building height " + str(row[avgbld_hgt]) +
                                  " out of range.")
                return None
            if row[avgbld_wid] <= 0:
                Logger.logMessage("Facility " + facility + ": avg building width " + str(row[avgbld_wid]) +
                                  " out of range.")
                messagebox.showinfo("Out of Range", "Facility " + facility + ": avg building width " + str(row[avgbld_wid]) +
                                  " out of range.")
                return None
            if row[avglin_wid] <= 0:
                Logger.logMessage("Facility " + facility + ": avg line width " + str(row[avglin_wid]) +
                                  " out of range.")
                messagebox.showinfo("Out of Range", "Facility " + facility + ": avg line width " + str(row[avglin_wid]) +
                                  " out of range.")
                return None
            if row[avgbld_sep] < 0:
                Logger.logMessage("Facility " + facility + ": avg building separation " + str(row[avgbld_sep]) +
                                  " out of range.")
                messagebox.showinfo("Out of Range", "Facility " + facility + ": avg building separation " + str(row[avgbld_sep]) +
                                  " out of range.")
                return None
            if row[avgbuoy] <= 0:
                Logger.logMessage("Facility " + facility + ": avg buoyancy " + str(row[avgbuoy]) +
                                  " out of range.")
                messagebox.showinfo("Out of Range", "Facility " + facility + ": avg buoyancy " + str(row[avgbuoy]) +
                                  " out of range.")
                return None

        # check for unassigned buoyant line
        check_buoyant_assignment = set(df[fac_id])

        # get buoyant line facility list
        find_b = self.emisloc_df[self.emisloc_df[source_type] == 'B']
        buoyant_fac = set(find_b[fac_id])

        if check_buoyant_assignment != buoyant_fac:
            buoyant_unassigned = set(check_buoyant_assignment - buoyant_fac)

            messagebox.showinfo("Unassigned buoyant Line parameters", "buoyant" +
                                " Line parameters for " +
                                ", ".join(buoyant_unassigned) + " have not been" +
                                " assigned. Please edit the 'source_type' column" +
                                " in the Emissions Locations file.")
            return None

        else:
            Logger.logMessage("Uploaded buoyant line parameters for [" + ",".join(check_buoyant_assignment) + "]\n")

        return df
