import time
from datetime import datetime
from math import isnan

from com.sca.hem4.log import Logger
from com.sca.hem4.model.Model import *
from com.sca.hem4.upload.InputFile import InputFile
from tkinter import messagebox

from com.sca.hem4.upload.MetLib import surffile

met_station = 'met_station'
rural_urban = 'rural_urban'
max_dist = 'max_dist'
model_dist = 'model_dist'
radial = 'radial'
circles = 'circles'
overlap_dist = 'overlap_dist'
acute = 'acute'
hours = 'hours'
multiplier = 'multiplier'
ring1 = 'ring1'
dep = 'dep'
depl = 'depl'
phase = 'phase'
pdep = 'pdep'
pdepl = 'pdepl'
vdep = 'vdep'
vdepl = 'vdepl'
user_rcpt = 'user_rcpt'
bldg_dw = 'bldg_dw'
urban_pop = 'urban_pop'
fastall = 'fastall'
hivalu = 'hivalu'
fac_center = 'fac_center'
ring_distances = 'ring_distances'
emis_var = 'emis_var'
annual = 'annual'
period_start = 'period_start'
period_end = 'period_end'
class FacilityList(InputFile):
    
    def __init__(self, path, metlib):
        self.skiprows = 1
        self.metlib = metlib
        InputFile.__init__(self, path)

    def createDataframe(self):

        self.skiprows = 1

        # Specify dtypes for all fields
        self.numericColumns = [max_dist,model_dist,radial,circles,overlap_dist,hours,multiplier,
                               ring1,urban_pop,hivalu]
        self.strColumns = [fac_id,met_station,rural_urban,acute,elev,dep,depl,pdep,pdepl,
                           vdep,vdepl,user_rcpt,bldg_dw,fastall,fac_center,ring_distances,emis_var,
                           annual,period_start,period_end]

        faclist_df = self.readFromPath(
            (fac_id,met_station,rural_urban,urban_pop,max_dist,model_dist,radial,circles,overlap_dist, ring1,
             fac_center,ring_distances, acute,
             hours,multiplier,hivalu,dep,depl,pdep,pdepl,vdep,vdepl,elev,
             user_rcpt,bldg_dw,fastall,emis_var,annual,period_start,period_end)
        )
        self.dataframe = faclist_df

    def clean(self, df):

        # Replace NaN with blank, No or 0
        # Note: use of elevations or all receptors are defaulted to Y, acute hours is defaulted to 1,
        # acute multiplier is defaulted to 10, and emission variation is defaulted to N
        cleaned = df.fillna({radial:16, circles:13, overlap_dist:30, hours:1, multiplier:10, max_dist: 50000, model_dist: 3000,
                                                      ring1:100, urban_pop:0, hivalu:1})

        cleaned.replace(to_replace={rural_urban:{"nan":""}, elev:{"nan":"Y"}, met_station:{"nan":""},
                                      dep:{"nan":"N"}, depl:{"nan":"N"}, phase:{"nan":""}, pdep:{"nan":"NO"},
                                      pdepl:{"nan":"NO"}, vdep:{"nan":"NO"}, vdepl:{"nan":"NO"},
                                      user_rcpt:{"nan":"N"}, bldg_dw:{"nan":"N"},
                                      fastall:{"nan":"N"}, acute:{"nan":"N"}, fac_center:{"nan":""},
                                      'ring_distances':{"nan":""}, emis_var:{"nan":"N"}, annual:{"nan":"Y"},
                                      period_start:{"nan":""}, period_end:{"nan":""}}, inplace=True)

        cleaned = cleaned.reset_index(drop = True)

        # upper case for selected fields
        cleaned[rural_urban] = cleaned[rural_urban].str.upper()
        cleaned[acute] = cleaned[acute].str.upper()
        cleaned[vdep] = cleaned[vdep].str.upper()
        cleaned[vdepl] = cleaned[vdepl].str.upper()
        cleaned[pdep] = cleaned[pdep].str.upper()
        cleaned[pdepl] = cleaned[pdepl].str.upper()
        cleaned[user_rcpt] = cleaned[user_rcpt].str.upper()
        cleaned[bldg_dw] = cleaned[bldg_dw].str.upper()
        cleaned[fastall] = cleaned[fastall].str.upper()
        cleaned[emis_var] = cleaned[emis_var].str.upper()
        cleaned[annual] = cleaned[annual].str.upper()
        cleaned[elev] = cleaned[elev].str.upper()

        return cleaned

    def validate(self, df):

        # ----------------------------------------------------------------------------------
        # Strict: Invalid values in these columns will cause the upload to fail immediately.
        # ----------------------------------------------------------------------------------
        if len(df.loc[(df[fac_id] == '')]) > 0:
            Logger.logMessage("One or more facility IDs are missing in the Facility List.")
            messagebox.showinfo("Missing facility IDs", "One or more facility IDs are missing in the Facility List.")
            return None

        files = df[met_station].values.tolist()
        files = [file.upper() for file in files if file != '']
        if not set(files).issubset(set(self.metlib.dataframe[surffile].str.upper())):
            Logger.logMessage("One or more met stations referenced in the Facility List are invalid.")
            messagebox.showinfo("Invalid met station", "One or more met stations referenced in the Facility List are invalid.")
            return None

        duplicates = self.duplicates(df, [fac_id])
        if len(duplicates) > 0:
            Logger.logMessage("One or more records are duplicated in the Facility List (key=fac_id):")
            messagebox.showinfo("Duplicates", "One or more records are duplicated in the Facility List (key=fac_id)")
            for d in duplicates:
                Logger.logMessage(d)
            return None

        # ----------------------------------------------------------------------------------
        # Defaulted: Invalid values in these columns will be replaced with a default.
        # ----------------------------------------------------------------------------------
        for index, row in df.iterrows():

            facility = row[fac_id]
            
            # urban and urban_pop..note that the rural_urban value can be blank, and in this case we will
            # leave it blank here (it will be defaulted in Runstream based on census data)
            valid = ['U', 'R', '']
            if not row[rural_urban] in valid:
                Logger.logMessage("Facility " + facility +
                                  ": rural_urban value invalid. Will be defaulted based on census data.")
                row[rural_urban] = ""

            if row[rural_urban] == 'U':
                if row[urban_pop] <= 0:
                    Logger.logMessage("Facility " + facility + ": Invalid value (urban_pop): Defaulting to 50,000.")
                    row[urban_pop] = 50000

            # Modeled Distance of Receptors
            if row[model_dist] > 50000 or row[model_dist] <= 0:
                Logger.logMessage("Facility " + facility + ": model distance value " + str(row[model_dist]) +
                                  " out of range. Defaulting to 3000.")
                row[model_dist] = 3000

            # maximum distance and modeled distance are related...
            if row[max_dist] > 50000 or row[max_dist] <= 0:
                Logger.logMessage("Facility " + facility + ": max distance value " + str(row[max_dist]) +
                                  " out of range. Defaulting to 50000.")
                row[max_dist] = 50000
            elif row[model_dist] > row[max_dist]:
                Logger.logMessage("Facility " + facility + ": model distance value " + str(row[model_dist]) +
                                  " is larger than maximum distance. Defaulting max_dist to 50000.")
                row[max_dist] = 50000

            # Radials: default is 16, minimum number is 4
            if row[radial] == 0:
                Logger.logMessage("Facility " + facility + ": radial value " + str(row[radial]) +
                                  " out of range. Defaulting to 16.")
                row[radial] = 16

            if row[radial] < 4:
                Logger.logMessage("Facility " + facility + ": radial value " + str(row[radial]) +
                                  " out of range. Defaulting to 4.")
                row[radial] = 4

            # Circles: default is 13, minimum number is 3
            if row[circles] == 0:
                Logger.logMessage("Facility " + facility + ": circles value " + str(row[circles]) +
                                  " out of range. Defaulting to 13.")
                row[circles] = 13

            if row[circles] < 3:
                Logger.logMessage("Facility " + facility + ": circles value " + str(row[circles]) +
                                  " out of range. Defaulting to 3.")
                row[circles] = 3

            # Overlap Distance
            if row[overlap_dist] == 0:
                Logger.logMessage("Facility " + facility + ": overlap distance value " + str(row[overlap_dist]) +
                                  " out of range. Defaulting to 30.")
                row[overlap_dist] = 30
            elif row[overlap_dist] < 1:
                Logger.logMessage("Facility " + facility + ": overlap distance value " + str(row[overlap_dist]) +
                                  " out of range. Defaulting to 30.")
                row[overlap_dist] = 30
            elif row[overlap_dist] > 500:
                Logger.logMessage("Facility " + facility + ": overlap distance value " + str(row[overlap_dist]) +
                                  " out of range. Defaulting to 30.")
                row[overlap_dist] = 30

            # ring1
            if row[ring1] < 100 or row[ring1] > row[max_dist]:
                Logger.logMessage("Facility " + facility + ": ring1 value " + str(row[ring1]) +
                                  " out of range. Defaulting to 100.")
                row[ring1] = 100

            # Facility center...comma separated list that should start with either "U" (meaning UTM coords) or "L"
            # (meaning lat/lon) and contain two values if lat/lon (lat,lon) or three values if UTM
            # (northing,easting,zone)
            center_spec = row[fac_center]
            spec_valid = True
            if center_spec.upper().startswith("U"):
                components = center_spec.split(',')
                if len(components) != 4:
                    spec_valid = False
            elif center_spec.upper().startswith("L"):
                components = center_spec.split(',')
                if len(components) != 3:
                    spec_valid = False
            else:
                spec_valid = False

            if center_spec != "" and not spec_valid:
                Logger.logMessage("Facility " + facility + ": Invalid facility center specified: " + center_spec)
                Logger.logMessage("Facility " + facility + ": Using default (calculated) center instead.")
                row[fac_center] = ""

            # Ring distances...comma separated list that contains at least 3 values, all must be > 0 and <= 50000, and
            # values must be increasing
            distance_spec = row['ring_distances']
            spec_valid = True
            distances = distance_spec.split(',')
            if len(distances) < 3:
                spec_valid = False
            else:
                ring_distance = int(float(distances[0]))
                if row[model_dist] < ring_distance:
                    Logger.logMessage("Facility " + facility + ": Error: First ring is greater than modeling distance!")
                    messagebox.showinfo("Modeling distance error", "Facility " + facility + ": Error: First ring is greater than modeling distance!")
                    spec_valid = False
                prev = 0
                for d in distances[1:]:
                    ring_distance = int(float(d))
                    if ring_distance <= prev or ring_distance > 50000:
                        spec_valid = False
                    prev = ring_distance

            if distance_spec != "" and not spec_valid:
                Logger.logMessage("Facility " + facility + ": Invalid ring distances specified: " + distance_spec)
                Logger.logMessage("Facility " + facility + ": Using default (calculated) distances instead.")
                row['ring_distances'] = ""

            # If there are user supplied ring distances then the last one must equal max distance
            # for correct outer block interpolation
            if row['ring_distances'] != "":
                distlist = row['ring_distances'].split(",")
                if float(distlist[-1]) != row[max_dist]:
                    maxdist_str = "," + str(row[max_dist])
                    row['ring_distances'] += maxdist_str

            # Acute
            valid = ['Y', 'N']
            if row[acute] not in valid:
                Logger.logMessage("Facility " + facility + ": Invalid value for acute. Defaulting to 'N'.")
                row[acute] = 'N'

            # Hours
            valid = [1,2,3,4,6,8,12,24]
            if row[hours] not in valid:
                Logger.logMessage("Facility " + facility + ": Invalid value for hours. Defaulting to 1.")
                row[hours] = 1

            if row[acute] == 'Y':
                if row[multiplier] <= 0:
                    Logger.logMessage("Facility " + facility + ": Invalid value for multiplier. Defaulting to 10.")
                    row[multiplier] = 10
                if row[hivalu] <= 0:
                    Logger.logMessage("Facility " + facility + ": Invalid value for high value. Defaulting to 1.")
                    row[multiplier] = 1

            # pdep, pdepl, vdep, vdepl
            valid = ['NO', 'WO', 'DO', 'WD']
            row[vdep] = row[vdep].upper()
            row[vdepl] = row[vdepl].upper()
            row[pdep] = row[pdep].upper()
            row[pdepl] = row[pdepl].upper()
            if row[vdep] not in valid:
                Logger.logMessage("Facility " + facility + ": Invalid value for vdep. Defaulting to 'NO'.")
                row[vdep] = 'NO'
            if row[vdepl] not in valid:
                Logger.logMessage("Facility " + facility + ": Invalid value for vdepl. Defaulting to 'NO'.")
                row[vdepl] = 'NO'
            if row[pdep] not in valid:
                Logger.logMessage("Facility " + facility + ": Invalid value for pdep. Defaulting to 'NO'.")
                row[pdep] = 'NO'
            if row[pdepl] not in valid:
                Logger.logMessage("Facility " + facility + ": Invalid value for pdepl. Defaulting to 'NO'.")
                row[pdepl] = 'NO'

            # elev, user_rcpt, bldg_dw, fastall, emis_var
            valid = ['Y', 'N']
            if row[elev] not in valid:
                Logger.logMessage("Facility " + facility + ": Invalid value for elev. Defaulting to 'Y'.")
                row[elev] = 'Y'
            if row[user_rcpt] not in valid:
                Logger.logMessage("Facility " + facility + ": Invalid value for user_rcpt. Defaulting to 'N'.")
                row[user_rcpt] = 'N'
            if row[bldg_dw] not in valid:
                Logger.logMessage("Facility " + facility + ": Invalid value for bldg_dw. Defaulting to 'N'.")
                row[bldg_dw] = 'N'
            if row[fastall] not in valid:
                Logger.logMessage("Facility " + facility + ": Invalid value for fastall. Defaulting to 'N'.")
                row[fastall] = 'N'
            if row[emis_var] not in valid:
                Logger.logMessage("Facility " + facility + ": Invalid value for emis_var. Defaulting to 'N'.")
                row[emis_var] = 'N'

            # Annual and period start/end
            met_annual = row[annual]
            start_spec_valid = True
            self.period_start_components = ""
            period_start_spec = row[period_start].replace(" ","")
            if met_annual == "Y":
                if period_start_spec != "":
                    Logger.logMessage("Facility " + facility + ": Period start specified but ignored because annual = 'Y'")
                    row[period_start] = ""
                    start_spec_valid = False
                else:
                    Logger.logMessage("Facility " + facility + ": Using annual met option.")
            else:
                starts = period_start_spec.split(',')
                for s in starts:
                    if not s.isdigit():
                        start_spec_valid = False

                if len(starts) < 3 or len(starts) > 4:
                    start_spec_valid = False
                else:
                    for c in starts:
                        self.period_start_components += c + " "

                if period_start_spec != "" and not start_spec_valid:
                    Logger.logMessage("Facility " + facility + ": Invalid period start specified: " + period_start_spec)
                    Logger.logMessage("Facility " + facility + ": Aermod will use default.")
                    row[period_start] = ""
                else:
                    Logger.logMessage("Facility " + facility + ": Using period start = " + self.period_start_components)
                    if period_start_spec == '':
                        Logger.logMessage("Aermod will use default in place of blank period start value.")
                    row[period_start] = self.period_start_components

            end_spec_valid = True
            self.period_end_components = ""
            period_end_spec = row[period_end].replace(" ","")
            if met_annual == "Y":
                if period_end_spec != "":
                    Logger.logMessage("Facility " + facility + ": Period end specified but ignored because annual = 'Y'")
                    row[period_end] = ""
                    end_spec_valid = False
            else:
                ends = period_end_spec.split(',')
                for e in ends:
                    if not e.isdigit():
                        end_spec_valid = False

                if len(ends) < 3 or len(ends) > 4:
                    end_spec_valid = False
                else:
                    for c in ends:
                        self.period_end_components += c + " "

                if period_end_spec != "" and not end_spec_valid:
                    Logger.logMessage("Facility " + facility + ": Invalid period end specified: " + period_end_spec)
                    Logger.logMessage("Facility " + facility + ": Aermod will use default.")
                    row[period_end] = ""
                else:
                    Logger.logMessage("Facility " + facility + ": Using period end = " + self.period_end_components)
                    if period_end_spec == '':
                        Logger.logMessage("Aermod will use default in place of blank period end value.")
                    row[period_end] = self.period_end_components

            if period_start_spec != "" and start_spec_valid and period_end_spec != "" and end_spec_valid:
                if len(starts) != len(ends):
                    Logger.logMessage("Facility " + facility +
                          ": Inconsistent period start and end specified (both must include hours, or neither): " +
                          period_start_spec + " : " + period_end_spec)
                    Logger.logMessage("Facility " + facility + ": Aermod will use defaults.")
                    row[period_start] = ""
                    row[period_end] = ""

                start_time = self.get_timestamp(starts)
                end_time = self.get_timestamp(ends)
                if start_time >= end_time:
                    Logger.logMessage("Facility " + facility +
                                      ": Inconsistent period start and end specified (start must be before end): " +
                                      period_start_spec + " : " + period_end_spec)
                    Logger.logMessage("Facility " + facility + ": Aermod will use defaults.")
                    row[period_start] = ""
                    row[period_end] = ""

            df.loc[index] = row

        Logger.logMessage("Uploaded facilities options list file for " + str(len(df)) + " facilities.\n")
        return df

    def get_timestamp(self, components):
        component_dt = datetime(year=int(components[0]), month=int(components[1]), day=int(components[2]))
        return time.mktime(component_dt.timetuple())
