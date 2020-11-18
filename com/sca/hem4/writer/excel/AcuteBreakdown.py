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


class AcuteBreakdown(ExcelWriter, InputFile):
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
                pol_list = self.model.all_inner_receptors_df[
                        (self.model.all_inner_receptors_df[lon] == row[lon]) & 
                        (self.model.all_inner_receptors_df[lat] == row[lat]) &
                        (self.model.all_inner_receptors_df[pollutant].str.lower() == row[pollutant].lower())][[pollutant,
                        source_id, emis_type, aconc]].values.tolist()
                # N indicates this is not an interpolated block
                for j in range(len(pol_list)):
                    pol_list[j].append('N')
            else:
                # max acute is at an outer block
                pol_list = self.model.all_outer_receptors_df[
                        (self.model.all_outer_receptors_df[lon] == row[lon]) & 
                        (self.model.all_outer_receptors_df[lat] == row[lat]) &
                        (self.model.all_outer_receptors_df[pollutant].str.lower() == row[pollutant].lower())][[pollutant,
                        source_id, emis_type, aconc]].values.tolist()
                # Y indicates this is an interpolated block
                for j in range(len(pol_list)):
                    pol_list[j].append('Y')

            popinfo_list.extend(pol_list)
                            
        # Next get breakdown info of max acute at any receptor
        maxinfo_list = []
        for index, row in self.achemmax_df.iterrows():
            if row[notes] == 'Discrete':
                # max acute is at an inner block
                maxpol_list = self.model.all_inner_receptors_df[
                        (self.model.all_inner_receptors_df[lon] == row[lon]) & 
                        (self.model.all_inner_receptors_df[lat] == row[lat]) &
                        (self.model.all_inner_receptors_df[pollutant].str.lower() == row[pollutant].lower())][[pollutant,
                        source_id, emis_type, aconc]].values.tolist()
                for j in range(len(maxpol_list)):
                    maxpol_list[j].append('N')
            elif row[notes] == 'Interpolated':
                # max acute is at an outer block
                maxpol_list = self.model.all_outer_receptors_df[
                        (self.model.all_outer_receptors_df[lon] == row[lon]) & 
                        (self.model.all_outer_receptors_df[lat] == row[lat]) &
                        (self.model.all_outer_receptors_df[pollutant].str.lower() == row[pollutant].lower())][[pollutant,
                        source_id, emis_type, aconc]].values.tolist()
                for j in range(len(maxpol_list)):
                    maxpol_list[j].append('Y')
            else:
                # max acute is at a polar receptor
                maxpol_list = self.model.all_polar_receptors_df[
                        (self.model.all_polar_receptors_df[lon] == row[lon]) & 
                        (self.model.all_polar_receptors_df[lat] == row[lat]) &
                        (self.model.all_polar_receptors_df[pollutant].str.lower() == row[pollutant].lower())][[pollutant,
                        source_id, emis_type, aconc]].values.tolist()
                for j in range(len(maxpol_list)):
                    maxpol_list[j].append('N')

            maxinfo_list.extend(maxpol_list)
        
        # Combine pop and all breakdown dataframes into one
                
        popinfo_df = pd.DataFrame(popinfo_list, columns=['pollutant','source_id','emis_type','aconc_pop',pop_interp])
        maxinfo_df = pd.DataFrame(maxinfo_list, columns=['pollutant','source_id','emis_type','aconc_all',all_interp])
        
        temp_df = pd.merge(popinfo_df, maxinfo_df, how='inner', on=[pollutant, source_id, emis_type])
        
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
        # Type setting for XLS reading
        self.numericColumns = [aconc_pop, aconc_all]
        self.strColumns = [pollutant, source_id, emis_type, pop_interp, all_interp]

        df = self.readFromPath(self.getColumns())
        return df.fillna("")