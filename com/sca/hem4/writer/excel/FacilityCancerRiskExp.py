import math

import pandas as pd
from math import log10, floor, isnan
import os
from com.sca.hem4.FacilityPrep import FacilityPrep
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter

parameter = 'parameter';
value = 'value';
value_rnd = 'value_rnd';
value_sci = 'value_sci';
notes = 'notes';
population = 'population';
mir = 'mir';
overlap = 'overlap';

class FacilityCancerRiskExp(ExcelWriter):
    """
    Provides a listing of the facilities by ID, their lat/lons and the population
    exposed to different cancer risk levels at each facility.
    """

    def __init__(self, targetDir, facilityId, model, plot_df):
        ExcelWriter.__init__(self, model, plot_df)

        if self.model.group_name != None:
            outfile = self.model.group_name + "_facility_cancer_risk_exp.xlsx"
        else:
            outfile = "facility_cancer_risk_exp.xlsx"
        self.filename = os.path.join(targetDir, outfile)
        self.facilityId = facilityId
        self.header = None

    def getHeader(self):
        self.header = ['Facil_id', 'latitude', 'longitude', 'Number people exposed to >= 1 in 1,000 risk',
                       'Number people exposed to >= 1 in 10,000 risk', 'Number people exposed to >= 1 in 100,000 risk',
                       'Number people exposed to >= 1 in 1,000,000 risk', 'Number people exposed to >= 1 in 10,000,000 risk']
        return self.header

    def writeWithoutHeader(self):
        for data in self.generateOutputs():
            if data is not None:
                self.appendToFile(data)

    def generateOutputs(self):
        
        if self.model.block_summary_chronic_df is not None:
            
            # facility center lat/lon
            faclat = self.model.computedValues['cenlat']
            faclon = self.model.computedValues['cenlon']
    
            # There are 5 population count buckets.
            bucket1 = bucket2 = bucket3 = bucket4 = bucket5 = 0
    
            bsc_df = self.model.block_summary_chronic_df
    
            for index, row in bsc_df.iterrows():
    
                if row[population] > 0 and row[overlap] == 'N':
                    
                    # round cancer risk to 1 significant figure
                    rounded = self.round_to_sigfig(row[mir])
    
                    if rounded >= 1e-3:
                        bucket1 = bucket1 + row[population]
                    if rounded >= 1e-4:
                        bucket2 = bucket2 + row[population]
                    if rounded >= 1e-5:
                        bucket3 = bucket3 + row[population]
                    if rounded >= 1e-6:
                        bucket4 = bucket4 + row[population]
                    if rounded >= 1e-7:
                        bucket5 = bucket5 + row[population]
                
            exp_list = [self.facilityId, faclat, faclon, bucket1, bucket2,
                       bucket3, bucket4, bucket5]
    
            exp_df = pd.DataFrame([exp_list], columns=['facid', 'lat', 'lon', 'bucket1', 'bucket2',
                                                    'bucket3', 'bucket4', 'bucket5'])
    
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