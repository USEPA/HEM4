import os
import re

from math import log10

from com.sca.hem4.CensusBlocks import population
from com.sca.hem4.upload.DoseResponse import ure
from com.sca.hem4.upload.InputFile import InputFile
from com.sca.hem4.writer.csv.AllInnerReceptors import emis_type
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter
from com.sca.hem4.model.Model import *

inc = 'inc';
inc_rnd = 'inc_rnd';

class Incidence(ExcelWriter, InputFile):
    """
    Provides the incidence value for the total of all sources and all modeled pollutants as well
    as the incidence value for each source and each pollutant.
    """

    def __init__(self, targetDir=None, facilityId=None, model=None, plot_df=None, outerInc=None,
                 filenameOverride=None, createDataframe=False):
        # Initialization for file reading/writing. If no file name override, use the
        # default construction.
        filename = facilityId + "_incidence.xlsx" if filenameOverride is None else filenameOverride
        path = os.path.join(targetDir, filename)

        ExcelWriter.__init__(self, model, plot_df)
        InputFile.__init__(self, path, createDataframe)

        self.filename = path
        self.targetDir = targetDir

        # Local cache for URE values
        self.riskCache = {}

        self.outerInc = outerInc

    def getHeader(self):
        return ['Source ID', 'Pollutant', 'Emission type', 'Incidence', 'Incidence rounded']

    def getColumns(self):
        return [source_id, pollutant, emis_type, inc, inc_rnd]

    def generateOutputs(self):

        allinner_df = self.model.all_inner_receptors_df.copy()
        
        if allinner_df.empty == False:
            # compute incidence for each Inner rececptor row and then sum incidence by source_id and pollutant
            allinner_df[inc] = allinner_df.apply(lambda row: self.calculateRisk(row[pollutant],
                                                 row[conc]) * row[population]/70, axis=1)
            inner_inc = allinner_df.groupby([source_id, pollutant, emis_type], as_index=False)[[inc]].sum()
        else:
            inner_inc = allinner_df
            inner_inc[inc] = None

        # append inner_inc and outer_inc, and re-sum by source_id and pollutant
        all_inc = inner_inc.append(self.outerInc, ignore_index=True).groupby(
            [source_id, pollutant, emis_type], as_index=False)[[inc]].sum()

        # sum incidence by pollutant
        poll_inc = all_inc.groupby([pollutant, emis_type], as_index=False)[[inc]].sum()
        poll_inc[source_id] = "Total"

        # sum incidence by source id
        sourceid_inc = all_inc.groupby([source_id, emis_type], as_index=False)[[inc]].sum()
        sourceid_inc[pollutant] = "All modeled pollutants"

        # sum incidence by emission type
        emistype_inc = all_inc.groupby([emis_type], as_index=False)[[inc]].sum()
        emistype_inc[source_id] = "Total"
        emistype_inc[pollutant] = "All modeled pollutants"

        all_inc = all_inc[all_inc['inc'] != 0]

        # combine all, poll, sourceid, and emistype incidence dfs into one and store in data
        combined_inc = emistype_inc.append([all_inc, poll_inc, sourceid_inc], ignore_index=True)
        combined_inc = combined_inc[[source_id, pollutant, emis_type, inc]]

        # compute a rounded incidence value
        combined_inc[inc_rnd] = combined_inc[inc].apply(self.roundIncidence)

        # Put final df into array
        self.dataframe = combined_inc
        self.data = self.dataframe.values
        yield self.dataframe


    def roundIncidence(self, inc):
        # Round incidence to two significant figures
        if inc > 0:
            exp = int(log10(inc)+99)-99
            rndinc = round(inc, 1 - exp)
        else:
            rndinc = 0
        return rndinc


    def calculateRisk(self, pollutant_name, conc):
        URE = None

        # In order to get a case-insensitive exact match (i.e. matches exactly except for casing)
        # we are using a regex that is specified to be the entire value. Since pollutant names can
        # contain parentheses, escape them before constructing the pattern.
        pattern = '^' + re.escape(pollutant_name) + '$'

        # Since it's relatively expensive to get this from the dose response library, cache them locally.
        if pollutant_name in self.riskCache:
            URE = self.riskCache[pollutant_name][ure]
        else:
            row = self.model.haplib.dataframe.loc[
                self.model.haplib.dataframe[pollutant].str.contains(pattern, case=False, regex=True)]

            if row.size == 0:
                URE = 0
            else:
                URE = row.iloc[0][ure]

            self.riskCache[pollutant_name] = {ure : URE}


        mir = conc * URE
        return mir

    def createDataframe(self):
        # Type setting for XLS reading
        self.numericColumns = [inc, inc_rnd]
        self.strColumns = [source_id, pollutant, emis_type]

        df = self.readFromPath(self.getColumns())
        return df.fillna("")