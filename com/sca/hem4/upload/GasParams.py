# -*- coding: utf-8 -*-
"""
Created on Mon Oct 15 13:55:46 2018
@author: dlindsey
"""
from com.sca.hem4.model.Model import pollutant
from com.sca.hem4.upload.InputFile import InputFile

da = 'da';
dw = 'dw';
rcl = 'rcl';
henry = 'henry';
valid = 'valid';
source = 'source';
dw_da_source = 'dw_da_source';
rcl_source = 'rcl_source';
notes = 'notes';

class GasParams(InputFile):

    def __init__(self):
        InputFile.__init__(self, "resources/Gas_Param.xlsx")

    def createDataframe(self):

        # Specify dtypes for all fields
        self.numericColumns = [da, dw, rcl, henry, valid]
        self.strColumns = [pollutant,notes, source, dw_da_source, rcl_source]

        self.dataframe = self.readFromPath(
            (pollutant, da, dw, rcl, henry, valid, source, dw_da_source, rcl_source, notes))

        self.dataframe[pollutant] = self.dataframe[pollutant].str.lower()
