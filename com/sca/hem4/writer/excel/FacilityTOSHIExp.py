import pandas as pd
from math import log10, floor, isnan
import os
from com.sca.hem4.FacilityPrep import *
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter

parameter = 'parameter';
value = 'value';
value_rnd = 'value_rnd';
value_sci = 'value_sci';
notes = 'notes';
population = 'population';
mir = 'mir';
overlap = 'overlap';

class FacilityTOSHIExp(ExcelWriter):
    """
    Provides a listing of the facilities by ID and the number of people with a 
    TOSHI greater than 1 for each facility and for each of the 14 TOSHIs.
    """

    def __init__(self, targetDir, facilityId, model, plot_df):
        ExcelWriter.__init__(self, model, plot_df)

        if self.model.group_name != None:
            outfile = self.model.group_name + "_facility_toshi_exp.xlsx"
        else:
            outfile = "facility_toshi_exp.xlsx"
        self.filename = os.path.join(targetDir, outfile)
        self.facilityId = facilityId
        self.header = None

    def getHeader(self):
        self.header = ['Facility ID', 'Number people exposed to > 1 Respiratory HI',
                       'Number people exposed to > 1 Liver HI', 'Number people exposed to > 1 Neurological HI',
                       'Number people exposed to > 1 Developmental HI', 'Number people exposed to > 1 Reproductive HI',
                       'Number people exposed to > 1 Kidney HI', 'Number people exposed to > 1 Ocular HI',
                       'Number people exposed to > 1 Endocrinological HI', 'Number people exposed to > 1 Hematological HI',
                       'Number people exposed to > 1 Immunological HI', 'Number people exposed to > 1 Skeletal HI',
                       'Number people exposed to > 1 Spleen HI', 'Number people exposed to > 1 Thyroid HI',
                       'Number people exposed to > 1 Whole Body HI']
        return self.header

    def writeWithoutHeader(self):
        for data in self.generateOutputs():
            if data is not None:
                self.appendToFile(data)

    def generateOutputs(self):
        
        if self.model.block_summary_chronic_df is not None:
            
            dfcols = ['facid', 'hi_resp', 'hi_live', 'hi_neur', 'hi_deve', 'hi_repr',
                      'hi_kidn', 'hi_ocul', 'hi_endo', 'hi_hema', 'hi_immu', 'hi_skel',
                      'hi_sple', 'hi_thyr', 'hi_whol']
                    
            bsc_df = self.model.block_summary_chronic_df
            hi_cols = [col for col in bsc_df.columns if 'hi_' in col]
            popcnt = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

            # Count people exposed to HI > 1 for all 14 endpoints
            for index, row in bsc_df.iterrows():
    
                # Exclude overlapped receptors
                if row[population] > 0 and row[overlap] == 'N':
                    
                    # round all 14 HI values to 1 significant figure
                    hi_vals = row[hi_cols].tolist()
                    rounded_vals = [self.round_to_sigfig(v) for v in hi_vals]
                    
                    # count population for any rounded HIs > 1
                    for idx, val in enumerate(rounded_vals):
                        if val > 1:
                            popcnt[idx] = popcnt[idx] + row[population]
                
            exp_list = [self.facilityId]
            exp_list.extend(popcnt)
    
            exp_df = pd.DataFrame([exp_list], columns = dfcols)
    
            # Put final df into array
            self.dataframe = exp_df
            self.data = self.dataframe.values
            yield self.dataframe

    def round_to_sigfig(self, x, sig=1):
        if x == 0:
            return 0;

        if isnan(x):
            return float('NaN')

        rounded = round(x, sig-int(floor(log10(abs(x))))-1)
        return rounded