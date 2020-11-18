import os, fnmatch
import pandas as pd
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter
from com.sca.hem4.upload.HAPEmissions import *
from com.sca.hem4.upload.FacilityList import *
from com.sca.hem4.upload.DoseResponse import *
from com.sca.hem4.upload.UserReceptors import *
from com.sca.hem4.model.Model import *
from com.sca.hem4.support.UTM import *
from com.sca.hem4.FacilityPrep import *
from com.sca.hem4.writer.csv.AllInnerReceptors import *

notes = 'notes';
aconc_pop = 'aconc_pop';
aconc_all = 'aconc_all';
pop_interp = 'pop_interp';
all_interp = 'all_interp';


class Hem3AcuteBreakdown(ExcelWriter, InputFile):
    """
    Provides the contribution of each emission source to the receptors (both populated and unpopulated) of maximum acute
    impact for each pollutant.
    """

    def __init__(self, targetDir=None, facilityId=None, model=None, plot_df=None, filenameOverride=None,
                 createDataframe=False, achempop_df=None, achemmax_df=None):
        # Initialization for file reading/writing. If no file name override, use the
        # default construction.
        filename = facilityId + "_acute_bkdn.xlsx" if filenameOverride is None else filenameOverride
        path = os.path.join(targetDir, filename)

        ExcelWriter.__init__(self, model, plot_df)
        InputFile.__init__(self, path, createDataframe)

        self.filename = path
        self.targetDir = targetDir
        self.achempop_df = achempop_df
        self.achemmax_df = achemmax_df

    def getHeader(self):
        return ['Pollutant', 'Source ID', 'Emission type', 'Max conc at populated receptor (ug/m3)',
                'Is max populated receptor interpolated? (Y/N)', 'Max conc at any receptor (ug/m3)',
                'Is max conc at any receptor interpolated? (Y/N)']

    def getColumns(self):
        return [pollutant, source_id, emis_type, aconc_pop, pop_interp, aconc_all, all_interp]

    def generateOutputs(self):

        # First get breakdown info of max acute at a populated receptor
        popinfo_list = []
        for index, row in self.achempop_df.iterrows():
            if row[notes] == 'Discrete':
                # max acute is at an inner block
                flag1 = 'N'
                pol_list = self.model.all_inner_receptors_df[
                    (self.model.all_inner_receptors_df[lon] == row[lon]) &
                    (self.model.all_inner_receptors_df[lat] == row[lat]) &
                    (self.model.all_inner_receptors_df[pollutant].str.lower() == row[pollutant].lower())][[pollutant,
                                                                                                           source_id, emis_type, aconc]].values.tolist()
            else:
                # max acute is at an outer block
                flag1 = 'Y'
                pol_list = self.model.all_outer_receptors_df[
                    (self.model.all_outer_receptors_df[lon] == row[lon]) &
                    (self.model.all_outer_receptors_df[lat] == row[lat]) &
                    (self.model.all_outer_receptors_df[pollutant].str.lower() == row[pollutant].lower())][[pollutant,
                                                                                                           source_id, emis_type, aconc]].values.tolist()
            popinfo_list.extend(pol_list)
        #            popinfo_df.rename(columns={aconc:aconc_pop}, inplace=True)

        # Next get breakdown info of max acute at any receptor
        maxinfo_list = []
        for index, row in self.achemmax_df.iterrows():
            if row[notes] == 'Discrete':
                # max acute is at an inner block
                flag2 = 'N'
                maxpol_list = self.model.all_inner_receptors_df[
                    (self.model.all_inner_receptors_df[lon] == row[lon]) &
                    (self.model.all_inner_receptors_df[lat] == row[lat]) &
                    (self.model.all_inner_receptors_df[pollutant].str.lower() == row[pollutant].lower())][[pollutant,
                                                                                                           source_id, emis_type, aconc]].values.tolist()
            elif row[notes] == 'Interpolated':
                # max acute is at an outer block
                flag2 = 'Y'
                maxpol_liist = self.model.all_outer_receptors_df[
                    (self.model.all_outer_receptors_df[lon] == row[lon]) &
                    (self.model.all_outer_receptors_df[lat] == row[lat]) &
                    (self.model.all_outer_receptors_df[pollutant].str.lower() == row[pollutant].lower())][[pollutant,
                                                                                                           source_id, emis_type, aconc]].values.tolist()
            else:
                # max acute is at a polar receptor
                flag2 = 'N'
                maxpol_list = self.model.all_polar_receptors_df[
                    (self.model.all_polar_receptors_df[lon] == row[lon]) &
                    (self.model.all_polar_receptors_df[lat] == row[lat]) &
                    (self.model.all_polar_receptors_df[pollutant].str.lower() == row[pollutant].lower())][[pollutant,
                                                                                                           source_id, emis_type, aconc]].values.tolist()
            maxinfo_list.extend(maxpol_list)
        #            unpopinfo_df.rename(columns={aconc:aconc_all}, inplace=True)

        # Combine pop and all breakdown dataframes into one

        popinfo_df = pd.DataFrame(popinfo_list, columns=['pollutant','source_id','emis_type','aconc_pop'])
        maxinfo_df = pd.DataFrame(maxinfo_list, columns=['pollutant','source_id','emis_type','aconc_all'])

        temp_df = pd.merge(popinfo_df, maxinfo_df, how='inner', on=[pollutant, source_id, emis_type])
        temp_df[pop_interp] = flag1
        temp_df[all_interp] = flag2

        # Reorder columns for output purpose, reset the index, and sort by pollutant and source_id
        cols = self.getColumns()
        abkdn_df = temp_df.reindex(columns = cols)
        abkdn_df.reset_index(drop=True, inplace=True)
        abkdn_df.sort_values(by=[pollutant, source_id], inplace=True)


        # Return results
        self.dataframe = abkdn_df
        self.data = self.dataframe.values
        yield self.dataframe

    def createDataframe(self):

        [pollutant, source_id, emis_type, aconc_pop, pop_interp, aconc_all, all_interp]

        # Type setting for XLS reading
        self.numericColumns = [aconc_pop, aconc_all]
        self.strColumns = [pollutant, source_id, emis_type, pop_interp, all_interp]

        df = self.readFromPath(self.getColumns())
        return df.fillna("")