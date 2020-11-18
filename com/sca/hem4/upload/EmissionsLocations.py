from com.sca.hem4.upload.InputFile import InputFile
from com.sca.hem4.model.Model import *
from com.sca.hem4.support.UTM import *
from tkinter import messagebox
import math


location_type = 'location_type'
source_type = 'source_type'
lengthx = 'lengthx'
lengthy = 'lengthy'
angle = 'angle'
horzdim = 'horzdim'
vertdim = 'vertdim'
areavolrelhgt = 'areavolrelhgt'
stkht = 'stkht'
stkdia = 'stkdia'
stkvel = 'stkvel'
stktemp = 'stktemp'
x2 = 'x2'
y2 = 'y2'
method = 'method'
massfrac = 'massfrac'
partdiam = 'partdiam'
fastall = 'fastall'

class EmissionsLocations(InputFile):

    def __init__(self, path, hapemis, faclist, fac_ids):
        self.fac_ids = fac_ids
        self.hapemis = hapemis
        self.faclist = faclist
        InputFile.__init__(self, path)

    def createDataframe(self):

        # two row header
        self.skiprows = 1

        # Specify dtypes for all fields
        self.numericColumns = [lon,lat,lengthx,lengthy,angle,horzdim,vertdim,areavolrelhgt,
                               stkht,stkdia,stkvel,stktemp,elev,x2,y2,method,massfrac,partdiam]
        self.strColumns = [fac_id,source_id,location_type,source_type,utmzone]

        emisloc_df = self.readFromPath(
            (fac_id,source_id,location_type,lon,lat,utmzone,source_type,lengthx,lengthy,angle,
             horzdim,vertdim,areavolrelhgt,stkht,stkdia,stkvel,stktemp,elev,x2,y2,method,massfrac,partdiam))

        self.dataframe = emisloc_df

    def clean(self, df):

        cleaned = df.fillna({utmzone:'0N', source_type:'', lengthx:1, lengthy:1, angle:0,
                                    horzdim:0, vertdim:0, areavolrelhgt:0, stkht:0, stkdia:0,
                                    stkvel:0, stktemp:0, elev:0, x2:0, y2:0, method:1, massfrac:1, partdiam:1})
        cleaned.replace(to_replace={fac_id:{"nan":""}, source_id:{"nan":""}}, inplace=True)
        cleaned = cleaned.reset_index(drop = True)

        # upper case of selected fields
        cleaned[source_type] = cleaned[source_type].str.upper()
        cleaned[location_type] = cleaned[location_type].str.upper()
        cleaned[source_id] = cleaned[source_id].str.upper()

        return cleaned

    def validate(self, df):
        # ----------------------------------------------------------------------------------
        # Strict: Invalid values in these columns will cause the upload to fail immediately.
        # ----------------------------------------------------------------------------------
        efac = set(df[fac_id])
        
        if len(df.loc[(df[fac_id] == '')]) > 0:
            Logger.logMessage("One or more facility IDs are missing in the Emissions Locations List.")
            messagebox.showinfo("Missing facility IDs", "One or more facility IDs are missing in the Emissions Locations List.")
            
            return None

        duplicates = self.duplicates(df, [fac_id, source_id])
        if len(duplicates) > 0:
            Logger.logMessage("One or more records are duplicated in the Emissions Location List (key=fac_id, source_id):")
            messagebox.showinfo("Duplicate Records", "One or more records are duplicated in the Emissions Location List (key=fac_id, source_id)")
            for d in duplicates:
                Logger.logMessage(d)
            return None

        if self.fac_ids is not None:
            if self.fac_ids.intersection(efac) != self.fac_ids:
                Logger.logMessage("Based on your Facility List Options file, the Emissions Location List is missing " +
                                  "one or more facilities. Please correct one or both files and upload again.")
                messagebox.showinfo("Missing facilities",  "Based on your Facility List Options file, the Emissions Location List is missing " +
                                  "one or more facilities. Please correct one or both files and upload again.")
                return None

        if len(df.loc[(df[source_id] == '')]) > 0:
            Logger.logMessage("One or more source IDs are missing in the Emissions Locations List.")
            messagebox.showinfo("Missing source IDs", "One or more source IDs are missing in the Emissions Locations List.")
            return None
       
        # make sure source ids match in hap emissions and emissions location
        # for facilities in faclist file
        if self.fac_ids is not None and self.hapemis is not None:
            hfac = set(self.hapemis.dataframe[fac_id])

            in_hap = list(self.fac_ids.intersection(hfac))
            in_emis = list(self.fac_ids.intersection(efac))

            hsource = self.hapemis.dataframe[self.hapemis.dataframe[fac_id].isin(in_hap)][[fac_id,source_id]]
            hsource['facsrc'] = hsource[fac_id] + hsource[source_id]
            esource = df[df[fac_id].isin(in_emis)][[fac_id,source_id]]            
            esource['facsrc'] = esource[fac_id] + esource[source_id]
            
            hfacsrc = set(hsource['facsrc'])
            efacsrc = set(esource['facsrc'])
            
            hsource = set(self.hapemis.dataframe[self.hapemis.dataframe[fac_id].isin(in_hap)][source_id])
            esource = set(df[df[fac_id].isin(in_emis)][source_id])

            if hfacsrc != efacsrc:
                Logger.logMessage("Your Emissions Location and HAP Emissions file have mismatched source IDs. " +
                                  "Please correct one or both files with matching sources and upload again.")
                messagebox.showinfo("Mismatch source IDs", "Your Emissions Location and HAP Emissions file have mismatched source IDs. " +
                                  "Please correct one or both files with matching sources and upload again.")
                return None

        if len(df.loc[(df[location_type] != 'L') & (df[location_type] != 'U')]) > 0:
            Logger.logMessage("One or more locations are missing a coordinate system in the Emissions Locations List.")
            messagebox.showinfo("Missing coordinates", "One or more locations are missing a coordinate system in the Emissions Locations List.")
            return None


        if len(df.loc[(df[source_type] != 'P') & (df[source_type] != 'C') &
                      (df[source_type] != 'H') & (df[source_type] != 'A') &
                      (df[source_type] != 'V') & (df[source_type] != 'N') &
                      (df[source_type] != 'B') & (df[source_type] != 'I')]) > 0:
            Logger.logMessage("One or more source types are missing a valid value in the Emissions Locations List.")
            messagebox.showinfo("Missing valid value", "One or more source types are missing a valid value in the Emissions Locations List.")
            return None

        # Cannot model deposition or depletion of buoyant line sources
        depfacs = set(self.faclist.dataframe[fac_id].loc[(self.faclist.dataframe['dep']=='Y') |
                                                    (self.faclist.dataframe['depl']=='Y')])
        buoyfacs = set(df[fac_id].loc[df[source_type]=='B'])
        if len(depfacs.intersection(buoyfacs)) > 0:
            Logger.logMessage("AERMOD cannot currently model deposition or depletion of emissions from " +
                              "buoyant line sources, and the Emissions Location file includes a buoyant line " +
                              "source for one or more facilities. Please disable deposition and depletion for " +
                              "each of these facilities, or remove the buoyant line source(s).")
            
            messagebox.showinfo("Incompatible source", "AERMOD cannot currently model deposition or depletion of emissions from " +
                              "buoyant line sources, and the Emissions Location file includes a buoyant line " +
                              "source for one or more facilities. Please disable deposition and depletion for " +
                              "each of these facilities, or remove the buoyant line source(s).")
            return None

        # Cannot use the Aermod FASTALL option on a facility with a buoyant line source
        fastfacs = set(self.faclist.dataframe[fac_id].loc[(self.faclist.dataframe[fastall]=='Y')])
        buoyfacs = set(df[fac_id].loc[df[source_type]=='B'])
        if len(fastfacs.intersection(buoyfacs)) > 0:
            Logger.logMessage("AERMOD's FASTALL option cannot be used with buoyant line sources, and the " +
                              "Emissions Location file includes a buoyant line source for one or more facilities. " +
                              "Please disable FASTALL for each of these facilities, or remove the buoyant line source(s).")
            messagebox.showinfo("Incompatible Sources", "AERMOD's FASTALL option cannot be used with buoyant line sources, and the " +
                              "Emissions Location file includes a buoyant line source for one or more facilities. " +
                              "Please disable FASTALL for each of these facilities, or remove the buoyant line source(s).")
            return None
        
        # Check coordinates
        for index, row in df.iterrows():

            facility = row[fac_id]
            type = row[location_type]
                
            maxlon = 180 if type == 'L' else 850000
            minlon = -180 if type == 'L' else 160000
            maxlat = 85 if type == 'L' else 10000000
            minlat = -80 if type == 'L' else 0

            if row[lon] > maxlon or row[lon] < minlon or math.isnan(row[lon]):
                Logger.logMessage("Facility " + facility + ": lon value " + str(row[lon]) + " out of range " +
                                  "in the Emissions Locations List.")
                messagebox.showinfo("Lon value out of range", "Facility " + facility + ": lon value " + str(row[lon]) + " out of range " +
                                  "in the Emissions Locations List.")
                
                return None
            
            if row[lat] > maxlat or row[lat] < minlat or math.isnan(row[lat]):
                Logger.logMessage("Facility " + facility + ": lat value " + str(row[lat]) + " out of range " +
                                  "in the Emissions Locations List.")
                messagebox.showinfo("Lat value our of range", "Facility " + facility + ": lat value " + str(row[lat]) + " out of range " +
                                  "in the Emissions Locations List.")
                return None

            if type == 'U':
                zone = row[utmzone]
                if zone.endswith('N') or zone.endswith('S'):
                    zone = zone[:-1]

                try:
                    zonenum = int(zone)
                except ValueError as v:
                    Logger.logMessage("Facility " + facility + ": UTM zone value " + str(row[utmzone]) + " malformed " +
                                      "in the Emissions Locations List.")
                    messagebox.showinfo("UTM zone malformed", "Facility " + facility + ": UTM zone value " + str(row[utmzone]) + " malformed " +
                                      "in the Emissions Locations List.")
                    return None

                if zonenum < 1 or zonenum > 60:
                    Logger.logMessage("Facility " + facility + ": UTM zone value " + str(row[utmzone]) + " invalid " +
                                      "in the Emissions Locations List.")
                    messagebox.showinfo("UTM zone invalid", "Facility " + facility + ": UTM zone value " + str(row[utmzone]) + " invalid " +
                                      "in the Emissions Locations List.")
                    return None
                
            if row[source_type] == 'V':
                if row[horzdim] == 0 and row[vertdim] == 0:
                    Logger.logMessage("Facility " + facility + " Source ID " + row[source_id] + ": must supply non-zero initial " +
                                      "lateral and vertical dimensions for volume source in the Emissions Locations List.")
                    messagebox.showinfo("Invalid source IDs", "Facility " + facility + " Source ID " + row[source_id] + ": must supply non-zero initial " +
                                      "lateral and vertical dimensions for volume source in the Emissions Locations List.")
                    return None
            
            # Compare starting and ending coordinates to the 5th decimal place (approximately 1 meter)
            if round(row[lon], 5) == round(row[x2], 5) and round(row[lat], 5) == round(row[y2], 5):
                    Logger.logMessage("Facility/Source: " + facility + "/" + row[source_id]  + " has identical starting and ending coordinates " +
                                      "(within 5 decimal places) in the Emissions Locations List. Please change.")
                    messagebox.showinfo("Invalid source coordinates", "Facility/Source: " + facility + "/" + row[source_id]  + " has identical starting and ending coordinates " +
                                      "(within 5 decimal places) in the Emissions Locations List. Please change.")
                    return None

            if row[source_type] in ['B', 'N']:
                if row[x2] > maxlon or row[x2] < minlon or row[x2] == 0:
                    Logger.logMessage("Facility " + facility + ": ending lon value " + str(row[x2]) + " out of range " +
                                      "in the Emissions Locations List.")
                    messagebox.showinfo("Ending lon value out of range", "Facility " + facility + ": ending lon value " + str(row[x2]) + " out of range " +
                                      "in the Emissions Locations List.")
                    return None

                if row[y2] > maxlat or row[y2] < minlat or row[y2] == 0:
                    Logger.logMessage("Facility " + facility + ": ending lat value " + str(row[y2]) + " out of range " +
                                      "in the Emissions Locations List.")
                    messagebox.showinfo("Ending lat value our of range", "Facility " + facility + ": lat value " + str(row[y2]) + " out of range " +
                                      "in the Emissions Locations List.")
                    return None

                
        # ----------------------------------------------------------------------------------
        # Defaulted: Invalid values in these columns will be replaced with a default.
        # ----------------------------------------------------------------------------------
        for index, row in df.iterrows():

            facility = row[fac_id]

            if row[source_type] == 'N':
                # Line source
                if row[lengthx] < 1:
                    Logger.logMessage("Facility " + facility + ": Length X value " + str(row[lengthx]) +
                                      " out of range. Defaulting to 1.")
                    row[lengthx] = 1
            else:
                # Not a line source
                if row[lengthx] <= 0:
                    Logger.logMessage("Facility " + facility + ": Length X value " + str(row[lengthx]) +
                                  " out of range. Defaulting to 1.")
                    row[lengthx] = 1
                
            if row[lengthy] <= 0:
                Logger.logMessage("Facility " + facility + ": Length Y value " + str(row[lengthy]) +
                                  " out of range. Defaulting to 1.")
                row[lengthy] = 1
            if row[angle] < 0 or row[angle] >= 90:
                Logger.logMessage("Facility " + facility + ": angle value " + str(row[angle]) +
                                  " out of range. Defaulting to 0.")
                row[angle] = 0
            if row[horzdim] < 0:
                Logger.logMessage("Facility " + facility + ": Horizontal dim value " + str(row[horzdim]) +
                                  " out of range. Defaulting to 0.")
                row[horzdim] = 0
            if row[vertdim] < 0:
                Logger.logMessage("Facility " + facility + ": Vertical dim value " + str(row[vertdim]) +
                                  " out of range. Defaulting to 0.")
                row[vertdim] = 0
            if row[areavolrelhgt] < 0:
                Logger.logMessage("Facility " + facility + ": Release height value " + str(row[areavolrelhgt]) +
                                  " out of range. Defaulting to 0.")
                row[areavolrelhgt] = 0
            if row[stkht] < 0:
                Logger.logMessage("Facility " + facility + ": Stack height value " + str(row[stkht]) +
                                  " out of range. Defaulting to 0.")
                row[stkht] = 0
            if (row[source_type] in ['P', 'C', 'H']) and (row[stkdia] <= 0):
                Logger.logMessage("Facility " + facility + ": Stack diameter value " + str(row[stkdia]) +
                                  " out of range.")
                return None
            if row[stkvel] < 0:
                Logger.logMessage("Facility " + facility + ": Exit velocity value " + str(row[stkvel]) +
                                  " out of range. Defaulting to 0.")
                row[stkvel] = 0
            if row[method] not in [1, 2]:
                Logger.logMessage("Facility " + facility + ": Method value " + str(row[method]) +
                                  " invalid. Defaulting to 1.")
                row[method] = 1
            if row[massfrac] < 0 or row[massfrac] > 1:
                Logger.logMessage("Facility " + facility + ": Mass fraction value " + str(row[massfrac]) +
                                  " invalid. Defaulting to 1.")
                row[massfrac] = 1
            if row[partdiam] <= 0:
                Logger.logMessage("Facility " + facility + ": Particle diameter value " + str(row[partdiam]) +
                                  " invalid. Defaulting to 1.")
                row[partdiam] = 1

            df.loc[index] = row

        Logger.logMessage("Uploaded emissions location file for " + str(len(df)) + " facility-source combinations.\n")
        return df
