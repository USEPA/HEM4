from com.sca.hem4.log import Logger
from com.sca.hem4.upload.InputFile import InputFile
from tkinter import messagebox
import os
from datetime import datetime
from com.sca.hem4.model.Model import *

emis_tpy = 'emis_tpy';
part_frac = 'part_frac';
particle = 'particle';
gas = 'gas';

class HAPEmissions(InputFile):

    def __init__(self, path, haplib, fac_ids):
        self.haplib = haplib
        self.fac_ids = fac_ids
        InputFile.__init__(self, path)

    def clean(self, df):

        cleaned = df.fillna({emis_tpy:0, part_frac:0})
        cleaned.replace(to_replace={fac_id:{"nan":""}, source_id:{"nan":""}, pollutant:{"nan":""}}, inplace=True)
        cleaned = cleaned.reset_index(drop = True)

        # Upper case source id to match with Aermod
        cleaned[source_id] = cleaned[source_id].str.upper()
        
        # lower case the pollutant names for better merging
        cleaned[pollutant] = cleaned[pollutant].str.lower()

        # turn part_frac into a decimal
        cleaned[part_frac] = cleaned[part_frac] / 100

        # create additional columns, one for particle mass and the other for gas/vapor mass...
        cleaned[particle] = cleaned[emis_tpy] * cleaned[part_frac]
        cleaned[gas] = cleaned[emis_tpy] * (1 - cleaned[part_frac])
        
        return cleaned

    def validate(self, df):
        # ----------------------------------------------------------------------------------
        # Strict: Invalid values in these columns will cause the upload to fail immediately.
        # ----------------------------------------------------------------------------------
        if len(df.loc[(df[fac_id] == '')]) > 0:
            Logger.logMessage("One or more facility IDs are missing in the HAP Emissions List.")
            messagebox.showinfo("Missing facility IDs", "One or more facility IDs are missing in the HAP Emissions List.")
            return None

        duplicates = self.duplicates(df, [fac_id, source_id, pollutant])
        if len(duplicates) > 0:
            Logger.logMessage("One or more records are duplicated in the HAP Emissions List (key=fac_id, source_id, pollutant):")
            messagebox.showinfo("Duplicate records", "One or more records are duplicated in the HAP Emissions List (key=fac_id, source_id, pollutant):")
            for d in duplicates:
                Logger.logMessage(d)
            return None

        hapfids = set(df[fac_id])
        if self.fac_ids.intersection(hapfids) != self.fac_ids:
            Logger.logMessage("Based on your Facility List Options file, the HAP Emissions List is missing " +
                              "one or more facilities. Please correct one or both files and upload again.")
            messagebox.showinfo("Missing facilities", "Based on your Facility List Options file, the HAP Emissions List is missing " +
                              "one or more facilities. Please correct one or both files and upload again.")
            return None

        if len(df.loc[(df[source_id] == '')]) > 0:
            Logger.logMessage("One or more source IDs are missing in the HAP Emissions List.")
            messagebox.showinfo("Missing source IDs", "One or more source IDs are missing in the HAP Emissions List.")
            return None

        if len(df.loc[(df[pollutant] == '')]) > 0:
            Logger.logMessage("One or more pollutants are missing in the HAP Emissions List.")
            messagebox.showinfo("Missing pollutants", "One or more pollutants are missing in the HAP Emissions List.")
            return None

        # ----------------------------------------------------------------------------------
        # Defaulted: Invalid values in these columns will be replaced with a default.
        # ----------------------------------------------------------------------------------
        for index, row in df.iterrows():

            facility = row[fac_id]

            if row[emis_tpy] < 0:
                Logger.logMessage("Facility " + facility + ": emissions value " + str(row[emis_tpy]) +
                                  " out of range. Defaulting to 0.")
                row[emis_tpy] = 0

            if row[part_frac] < 0 or row[part_frac] > 1:
                Logger.logMessage("Facility " + facility + ": particulate fraction value " + str(row[part_frac]*100) +
                                  " out of range. Defaulting to 0.")
                row[part_frac] = 0

            df.loc[index] = row

        # verify pollutants are present in dose library
        master_list = list(self.haplib.dataframe[pollutant])
        lower = [x.lower() for x in master_list]

        user_haps = set(df[pollutant])
        missing_pollutants = []

        for hap in user_haps:
            if hap.lower() not in lower:
                missing_pollutants.append(hap)

        self.log = []
        # if there are any missing pollutants...
        if len(missing_pollutants) > 0:
            fix_pollutants = messagebox.askyesno("Missing Pollutants in Dose "+
                                                 "Response Library", "The "+
                                                 "following pollutants were "+
                                                 "not found in HEM4's Dose "+
                                                 "Response Library: " +
                                                 ', '.join(missing_pollutants) +
                                                 ".\n Would you like to amend "+
                                                 "your HAP Emissions file?"+
                                                 "(they will be removed "+
                                                 "otherwise). ")

            if fix_pollutants:
                Logger.logMessage("Aborting upload of HAP emissions pending resolution of missing pollutants.")
                messagebox.showinfo("Aborting upload", "Aborting upload of HAP emissions pending resolution of missing pollutants.")
                return None
            else:
                missing = missing_pollutants
                remove = set(missing)
                Logger.logMessage("Removing these pollutants, which were not found: " +
                                  "[{0}]".format(", ".join(str(i) for i in missing_pollutants)))

                # remove them from data frame
                # to separate log file the non-modeled HAP Emissions
                fileDir = os.path.dirname(os.path.realpath('__file__'))
                filename = os.path.join(fileDir, "output\DR_HAP_ignored.log")
                logfile = open(filename, 'w')

                logfile.write(str(datetime.now()) + ":\n")

                for p in remove:

                    df = df[df[pollutant] != str(p)]

                    # record upload in log
                    # add another essage to say the following pollutants were assigned a generic value...
                    self.log.append("Removed " + p + " from hap emissions file\n")

                    # get row so we can write facility and other info
                    ignored = df[df[pollutant] == p]

                    logfile.write("Removed: " + str(ignored))

                logfile.close()


        Logger.logMessage("Uploaded HAP emissions file for " + str(len(df)) + " source-HAP combinations.\n")
        return df

    def createDataframe(self):

        # Specify dtypes for all fields
        self.numericColumns = [emis_tpy,part_frac]
        self.strColumns = [fac_id,source_id,pollutant]

        hapemis_df = self.readFromPath((fac_id,source_id,pollutant,emis_tpy,part_frac))
        self.dataframe = hapemis_df
