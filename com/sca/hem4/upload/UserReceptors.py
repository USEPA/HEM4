from com.sca.hem4.CensusBlocks import population
from com.sca.hem4.upload.DependentInputFile import DependentInputFile
from tkinter import messagebox
from com.sca.hem4.upload.EmissionsLocations import *
from com.sca.hem4.upload.FacilityList import *
import math
import pandas as pd

rec_type = 'rec_type';
rec_id = 'rec_id';

class UserReceptors(DependentInputFile):

    def __init__(self, path, dependency, csvFormat):
        self.faclist_df = dependency
        DependentInputFile.__init__(self, path, dependency, csvFormat=csvFormat)

    def createDataframe(self):
        
        # Specify dtypes for all fields
        self.numericColumns = [lon, lat, elev, hill]
        self.strColumns = [fac_id,location_type, utmzone, rec_type, rec_id]

        if self.csvFormat:
            ureceptor_df = self.readFromPathCsv(
                (fac_id, location_type, lon, lat, utmzone, elev, rec_type, rec_id, hill))
        else:
            ureceptor_df = self.readFromPath(
                (fac_id, location_type, lon, lat, utmzone, elev, rec_type, rec_id, hill))

        self.dataframe = ureceptor_df

    def clean(self, df):
        cleaned = df
        cleaned.replace(to_replace={fac_id:{"nan":""}}, inplace=True)
        cleaned = cleaned.reset_index(drop = True)

        # upper case of selected fields
        cleaned[location_type] = cleaned[location_type].str.upper()
        cleaned[rec_type] = cleaned[rec_type].str.upper()

        return cleaned

    def validate(self, df):
        # ----------------------------------------------------------------------------------
        # Strict: Invalid values in these columns will cause the upload to fail immediately.
        # ----------------------------------------------------------------------------------
        if len(df.loc[(df[fac_id] == '')]) > 0:
            Logger.logMessage("One or more facility IDs are missing in the User Receptors List.")
            messagebox.showinfo("Missing facility IDs", "One or more facility IDs are missing in the User Receptors List.")
            
            return None

        if len(df.loc[(df[location_type] != 'L') & (df[location_type] != 'U')]) > 0:
            Logger.logMessage("One or more locations are missing a coordinate system in the User Receptors List.")
            messagebox.showinfo("Missing location", "One or more locations are missing a coordinate system in the User Receptors List.")
            return None

        duplicates = self.duplicates(df, [fac_id, lon, lat])
        if len(duplicates) > 0:
            Logger.logMessage("One or more records are duplicated in the User Receptors List (key=fac_id, lon, lat):")
            messagebox.showinfo("Duplicate records", "One or more records are duplicated in the User Receptors List (key=fac_id, lon, lat).")
            for d in duplicates:
                Logger.logMessage(d)
            return None

        
        for index, row in df.iterrows():

            facility = row[fac_id]
            loc_type = row[location_type]

            maxlon = 180 if loc_type == 'L' else 850000
            minlon = -180 if loc_type == 'L' else 160000
            maxlat = 85 if loc_type == 'L' else 10000000
            minlat = -80 if loc_type == 'L' else 0

            if row[lon] > maxlon or row[lon] < minlon or math.isnan(row[lon]):
                Logger.logMessage("Facility " + facility + ": lon value " + str(row[lon]) + " out of range " +
                                  "in the User Receptors List.")
                messagebox.showinfo("Lon value out of range", "Facility " + facility + ": lon value " + str(row[lon]) + " out of range " +
                                  "in the User Receptors List.")
                return None
            if row[lat] > maxlat or row[lat] < minlat or math.isnan(row[lat]):
                Logger.logMessage("Facility " + facility + ": lat value " + str(row[lat]) + " out of range " +
                                  "in the User Receptors List.")
                messagebox.showinfo("Lat value out of range", "Facility " + facility + ": lat value " + str(row[lat]) + " out of range " +
                                  "in the User Receptors List.")
                return None

            if loc_type == 'U':
                zone = row[utmzone]
                if zone.endswith('N') or zone.endswith('S'):
                    zone = zone[:-1]

                try:
                    zonenum = int(zone)
                except ValueError as v:
                    Logger.logMessage("Facility " + facility + ": UTM zone value " + str(row[utmzone]) + " malformed " +
                                      "in the User Receptors List.")
                    messagebox.showinfo("UTM zone malformed", "Facility " + facility + ": UTM zone value " + str(row[utmzone]) + " malformed " +
                                      "in the User Receptors List.")
                    return None

                if zonenum < 1 or zonenum > 60:
                    Logger.logMessage("Facility " + facility + ": UTM zone value " + str(row[utmzone]) + " invalid " +
                                      "in the User Receptors List.")
                    messagebox.showinfo("UTM invalid", "Facility " + facility + ": UTM zone value " + str(row[utmzone]) + " invalid " +
                                      "in the User Receptors List.")
                    return None

            valid = ['P', 'B', 'M']
            if row[rec_type] not in valid:
                Logger.logMessage("Facility " + facility + ": Receptor type value " + str(row[rec_type]) + " invalid " +
                                  "in the User Receptors List.")
                messagebox.showinfo("Invalid receptor", "Facility " + facility + ": Receptor type value " + str(row[rec_type]) + " invalid " +
                                  "in the User Receptors List.")
                return None

            if row[rec_id] == 'nan':
                Logger.logMessage("Facility " + facility + ": Receptor ID is blank in the User Receptors List.")
                messagebox.showinfo("Blank Receptor ID", "Facility " + facility + ": Receptor ID is blank in the User Receptors List.")
                return None
                
                
        # check for unassigned user receptors        
        check_receptor_assignment = set(self.faclist_df[fac_id].loc[self.faclist_df[user_rcpt]=='Y'])
        user_rec_facs = set(df[fac_id])
        
        receptor_unassigned = []
        for fac in check_receptor_assignment:
            if fac not in user_rec_facs:
                receptor_unassigned.append(str(fac))

        if len(receptor_unassigned) > 0:
            facilities = set(receptor_unassigned)
            messagebox.showinfo("Unassigned User Receptors", "User receptors for " + ", ".join(facilities) +
                        " have not been assigned. Please edit the 'user_rcpt' column in the facility options file" +
                        " or add receptors for these facilities into the User Receptor file.")
            return None
        else:
            Logger.logMessage("Uploaded user receptors for [" + ",".join(check_receptor_assignment) + "]\n")
            return df
