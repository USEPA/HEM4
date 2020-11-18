from com.sca.hem4.CensusBlocks import population
from com.sca.hem4.upload.InputFile import InputFile
from tkinter import messagebox
from com.sca.hem4.upload.EmissionsLocations import *
from com.sca.hem4.upload.FacilityList import *

from tkinter import messagebox
import math


rec_type = 'rec_type';
rec_id = 'rec_id';

class AltReceptors(InputFile):

    def __init__(self, path):
        InputFile.__init__(self, path)

    def createDataframe(self):
        
        # Specify dtypes for all fields
        self.numericColumns = [lon, lat, elev, hill, population]
        self.strColumns = [location_type, utmzone, rec_type, rec_id]

        altreceptor_df = self.readFromPathCsv(
                (rec_id, rec_type, location_type, lon, lat, utmzone, elev, hill, population))

        self.dataframe = altreceptor_df

    def clean(self, df):
        cleaned = df
        cleaned.replace(to_replace={rec_id:{"nan":""}}, inplace=True)
        cleaned = cleaned.reset_index(drop = True)

        # upper case of selected fields
        cleaned[location_type] = cleaned[location_type].str.upper()
        cleaned[rec_type] = cleaned[rec_type].str.upper()

        return cleaned

    def validate(self, df):
        # ----------------------------------------------------------------------------------
        # Strict: Invalid values in these columns will cause the upload to fail immediately.
        # ----------------------------------------------------------------------------------
        if len(df.loc[(df[rec_id] == '')]) > 0:
            Logger.logMessage("One or more Receptor IDs are missing in the Alternate User Receptors List.")
            messagebox.showinfo("Missing Receptor IDs", "One or more Receptor IDs are missing in the Alternate User Receptors List.")
            return None

        if len(df.loc[(df[location_type] != 'L') & (df[location_type] != 'U')]) > 0:
            Logger.logMessage("One or more locations are missing a coordinate system in the Alternate User Receptors List.")
            return None

        duplicates = self.duplicates(df, [rec_id, lon, lat])
        if len(duplicates) > 0:
            Logger.logMessage("One or more records are duplicated in the Alternate User Receptors List (key=rec_id, lon, lat):")
            messagebox.showinfo("Duplicates", "One or more records are duplicated in the Alternate User Receptors List (key=rec_id, lon, lat):")
            for d in duplicates:
                Logger.logMessage(d)
            return None

        if len(df.loc[(df[population].isnull()) & (df[rec_type] == 'P')]) > 0:
            Logger.logMessage("Some 'P' receptors are missing population values in Alternate User Receptor List.")
            messagebox.showinfo("Missing Population Values", "Some 'P' receptors are missing population values in Alternate User Receptor List.")
            return None

        for index, row in df.iterrows():

            receptor = row[rec_id]
            loc_type = row[location_type]

            maxlon = 180 if loc_type == 'L' else 850000
            minlon = -180 if loc_type == 'L' else 160000
            maxlat = 85 if loc_type == 'L' else 10000000
            minlat = -80 if loc_type == 'L' else 0

            if row[lon] > maxlon or row[lon] < minlon or math.isnan(row[lon]):
                Logger.logMessage("Receptor " + receptor + ": lon value " + str(row[lon]) + " out of range " +
                                  "in the Alternate User Receptors List.")
                messagebox.showinfo("Longitude out of Range", "Receptor " + receptor + ": lon value " + str(row[lon]) + " out of range " +
                                  "in the Alternate User Receptors List.")
                return None
            
            if row[lat] > maxlat or row[lat] < minlat or math.isnan(row[lat]):
                Logger.logMessage("Receptor " + receptor + ": lat value " + str(row[lat]) + " out of range " +
                                  "in the Alternate User Receptors List.")
                messagebox.showinfo("Latitude out of Range", "Receptor " + receptor + ": lat value " + str(row[lat]) + " out of range " +
                                  "in the Alternate User Receptors List.")
                
                return None

            if loc_type == 'U':
                zone = row[utmzone]
                if zone.endswith('N') or zone.endswith('S'):
                    zone = zone[:-1]

                try:
                    zonenum = int(zone)
                except ValueError as v:
                    Logger.logMessage("Receptor " + receptor + ": UTM zone value " + str(row[utmzone]) + " malformed " +
                                      "in the Alternate User Receptors List.")
                    messagebox.showinfo("UTM value malformed", "Receptor " + receptor + ": UTM zone value " + str(row[utmzone]) + " malformed " +
                                      "in the Alternate User Receptors List.")
                    return None

                if zonenum < 1 or zonenum > 60:
                    Logger.logMessage("Receptor " + receptor + ": UTM zone value " + str(row[utmzone]) + " invalid " +
                                      "in the Alternate User Receptors List.")
                    messagebox.showinfo("UTM zone value invalid", "Receptor " + receptor + ": UTM zone value " + str(row[utmzone]) + " invalid " +
                                      "in the Alternate User Receptors List.")
                    
                    return None

            valid = ['P', 'B', 'M']
            if row[rec_type] not in valid:
                Logger.logMessage("Receptor " + receptor + ": Receptor type value " + str(row[rec_type]) + " invalid " +
                                  "in the Alternate User Receptors List.")
                messagebox.showinfo("Receptor type invalid", "Receptor " + receptor + ": Receptor type value " + str(row[rec_type]) + " invalid " +
                                  "in the Alternate User Receptors List.")
                return None

        Logger.logMessage("Uploaded alternate user receptors.\n")
        return df
