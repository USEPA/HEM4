import os

from com.sca.hem4.upload.InputFile import InputFile

designation = 'designation'
pollutant_name = 'pollutant_name'
pollutant_code = 'pollutant_code'
casNumber = 'casNumber'
pollutant_desc = 'pollutant_desc'
hapCategory = 'hapCategory'
tier2ChemName = 'tier2ChemName'
pbHap = 'pbHap'
shortPbHap = 'shortPbHap'
fullPbHap = 'fullPbHap'
note = 'note'

class PollutantCrosswalk(InputFile):

    def __init__(self, createDataframe=False):

        InputFile.__init__(self, "resources/Pollutant_CrossWalk.xlsx")

    def createDataframe(self):

        # Specify dtypes for all fields
        self.numericColumns = []
        self.strColumns = [designation, pollutant_name, pollutant_code, casNumber, pollutant_desc, hapCategory,
                            tier2ChemName, pbHap, shortPbHap, fullPbHap, note]

        crosswalk_df = self.readFromPath(
            (designation, pollutant_name, pollutant_code, casNumber, pollutant_desc, hapCategory,
             tier2ChemName, pbHap, shortPbHap, fullPbHap, note))

        self.dataframe = crosswalk_df