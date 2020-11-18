from tkinter import messagebox

from com.sca.hem4.log import Logger
from com.sca.hem4.upload.InputFile import InputFile
from com.sca.hem4.model.Model import *

ure = 'ure';
rfc = 'rfc';
aegl_1_1h = 'aegl_1_1h';
aegl_1_8h = 'aegl_1_8h';
aegl_2_1h = 'aegl_2_1h';
aegl_2_8h = 'aegl_2_8h';
erpg_1 = 'erpg_1';
erpg_2 = 'erpg_2';
mrl = 'mrl';
rel = 'rel';
idlh_10 = 'idlh_10';
teel_0 = 'teel_0';
teel_1 = 'teel_1';
group_ure = 'group_ure';
tef = 'tef';
cas_no = 'cas_no';
comments = 'comments';
drvalues = 'drvalues';
acute_con = 'acute_con';

class DoseResponse(InputFile):

    def __init__(self):
        InputFile.__init__(self, "resources/Dose_Response_Library.xlsx")

    def clean(self, df):

        cleaned = df.fillna(0)

        # lower case the pollutant name for easier merging later
        cleaned[pollutant] = cleaned[pollutant].str.lower()

        return cleaned

    def validate(self, df):

        # ----------------------------------------------------------------------------------
        # Strict: Invalid values in these columns will cause the upload to fail immediately.
        # ----------------------------------------------------------------------------------
        duplicates = self.duplicates(df, [pollutant])
        if len(duplicates) > 0:
            Logger.logMessage("One or more records are duplicated in the Dose Response file (key=pollutant):")
            for d in duplicates:
                Logger.logMessage(d)

            Logger.logMessage("Please remove the duplicate records and restart HEM4.")
            return None
        else:
            return df

    def createDataframe(self):

        # Specify dtypes for all fields
        self.numericColumns = [ure, rfc,aegl_1_1h,aegl_1_8h,aegl_2_1h,aegl_2_8h,erpg_1,erpg_2,mrl,
                               rel,idlh_10,teel_0,teel_1,group_ure,tef]
        self.strColumns = [pollutant,group,cas_no,comments,drvalues,acute_con]

        # DOSE RESPONSE excel to dataframe
        # HEADER----------------------
        # pollutant|pollutant group|cas no|URE 1/(µg/m3)|RFC (mg/m3)|aegl_1 (1-hr) (mg/m3)|aegl_1 (8-hr) (mg/m3)|
        # aegl_2 (1 hr) (mg/m3)|aegl_2 (8 hr) (mg/m3)|erpg_1 (mg/m3)|erpg_2 (mg/m3)|mrl (mg/m3)|rel  (mg/m3)|
        # idlh_10 (mg/m3)|teel_0 (mg/m3)|teel_1 (mg/m3)|comments|comments_on_D/R_values|group URE|Tef|minimum acute reference conc (mg/m3)
        self.dataframe = self.readFromPath(
            (pollutant,group,cas_no,ure,rfc,aegl_1_1h,aegl_1_8h,aegl_2_1h,aegl_2_8h,erpg_1,erpg_2,
             mrl,rel,idlh_10,teel_0,teel_1,comments,drvalues,group_ure,tef,acute_con))
