from com.sca.hem4.writer.excel.AcuteChemicalMax import AcuteChemicalMax
from com.sca.hem4.writer.excel.AcuteChemicalMaxNonCensus import AcuteChemicalMaxNonCensus
from com.sca.hem4.writer.excel.RiskBreakdown import *
from com.sca.hem4.writer.excel.summary.AltRecAwareSummary import AltRecAwareSummary

hq_rel = 'hq_rel'
hq_aegl1 = 'hq_aegl1'
hq_erpg1 = 'hq_erpg1'
hq_idlh = 'hq_idlh'
hq_aegl2 = 'hq_aegl2'
hq_erpg2 = 'hq_erpg2'

class AcuteImpacts(ExcelWriter, InputFile, AltRecAwareSummary):

    def __init__(self, targetDir, facilityIds, parameters=None):
        self.name = "Acute Impacts Summary"
        self.categoryName = parameters[0]
        self.categoryFolder = targetDir
        self.facilityIds = facilityIds

        path = os.path.join(targetDir, self.categoryName + "_acute_impacts.xlsx")

        firstFacility = facilityIds[0]

        InputFile.__init__(self, path, False)

        self.filename = path
        self.targetDir = targetDir
        self.altrec = self.determineAltRec(self.categoryFolder)

    def getHeader(self):
        if self.altrec == 'Y':
            return ['Facility ID', 'Pollutant', 'CONC_MG/M3',	'REL', 'AEGL_1_1H', 'ERPG_1', 'IDLH_10', 'AEGL_2_1H', 'ERPG_2',
                    'HQ_REL	', 'HQ_AEGL1', 'HQ_ERPG1', 'HQ_IDLH', 'HQ_AEGL2', 'HQ_ERPG2', 'Receptor ID', 'Distance', 'Angle']
        else:
            return ['Facility ID', 'Pollutant', 'CONC_MG',	'REL', 'AEGL_1_1H', 'ERPG_1', 'IDLH_10', 'AEGL_2_1H', 'ERPG_2',
                'HQ_REL	', 'HQ_AEGL1', 'HQ_ERPG1', 'HQ_IDLH', 'HQ_AEGL2', 'HQ_ERPG2', 'FIPS', 'Block', 'Distance', 'Angle']


    def getColumns(self):
        if self.altrec == 'Y':
            return[fac_id, pollutant, aconc, rel, aegl_1_1h, erpg_1, idlh_10, aegl_2_1h, erpg_2,
               hq_rel, hq_aegl1, hq_erpg1, hq_idlh, hq_aegl2, hq_erpg2, rec_id, distance, angle]
        else:
            return [fac_id, pollutant, aconc, rel, aegl_1_1h, erpg_1, idlh_10, aegl_2_1h, erpg_2,
               hq_rel, hq_aegl1, hq_erpg1, hq_idlh, hq_aegl2, hq_erpg2, fips, block, distance, angle]

    def generateOutputs(self):
        Logger.log("Creating " + self.name + " report...", None, False)

        anyAcute = "N"
        
        # Load the acute chemical max output for each facility
        allAcute_df = pd.DataFrame()
        for facilityId in self.facilityIds:
            targetDir = self.categoryFolder + "/" + facilityId


            acute = AcuteChemicalMaxNonCensus(targetDir=targetDir, facilityId=facilityId) if self.altrec == 'Y' else \
                AcuteChemicalMax(targetDir=targetDir, facilityId=facilityId)

            try:
                acute_df = acute.createDataframe()

                acute_df[fac_id] = facilityId

                allAcute_df = allAcute_df.append(acute_df)
                if not allAcute_df.empty:
                    anyAcute = "Y"
                    
            except FileNotFoundError as e:
                Logger.logMessage("Skipped facility " + facilityId + ". Couldn't find acute information.")

        if anyAcute == "Y":
            
            # Unit conversion for acute concentration
            allAcute_df[aconc] = allAcute_df.apply(lambda x: (x[aconc] / 1000), axis=1)
    
            # The hazard quotients are calculated by dividing the acute concentration by the benchmark value
            allAcute_df[hq_rel] = allAcute_df.apply(lambda x: (x[aconc] / x[rel]) if x[rel] > 0 else 0, axis=1)
            allAcute_df[hq_aegl1] = allAcute_df.apply(lambda x: (x[aconc] / x[aegl_1_1h]) if x[aegl_1_1h] > 0 else 0, axis=1)
            allAcute_df[hq_erpg1] = allAcute_df.apply(lambda x: (x[aconc] / x[erpg_1]) if x[erpg_1] > 0 else 0, axis=1)
            allAcute_df[hq_idlh] = allAcute_df.apply(lambda x: (x[aconc] / x[idlh_10]) if x[idlh_10] > 0 else 0, axis=1)
            allAcute_df[hq_aegl2] = allAcute_df.apply(lambda x: (x[aconc] / x[aegl_2_1h]) if x[aegl_2_1h] > 0 else 0, axis=1)
            allAcute_df[hq_erpg2] = allAcute_df.apply(lambda x: (x[aconc] / x[erpg_2]) if x[erpg_2] > 0 else 0, axis=1)
    
            if self.altrec == 'Y':
                allAcute_df = allAcute_df[[fac_id, pollutant, aconc, rel, aegl_1_1h, erpg_1, idlh_10, aegl_2_1h, erpg_2,
                       hq_rel, hq_aegl1, hq_erpg1, hq_idlh, hq_aegl2, hq_erpg2, rec_id, distance, angle]]
            else:
                allAcute_df = allAcute_df[[fac_id, pollutant, aconc, rel, aegl_1_1h, erpg_1, idlh_10, aegl_2_1h, erpg_2,
                       hq_rel, hq_aegl1, hq_erpg1, hq_idlh, hq_aegl2, hq_erpg2, fips, block, distance, angle]]
    
            allAcute_df.sort_values(by=[fac_id, pollutant], ascending=True, inplace=True)
            allAcute_df.reset_index(inplace=True, drop=True)
    
            # Put final df into array
            self.dataframe = allAcute_df
            self.data = self.dataframe.values
            yield self.dataframe
            
        else:
            
            Logger.logMessage("There was no acute data to generate the Acute Impacts summary.")

    def createDataframe(self):
        # Type setting for XLS reading
        self.numericColumns = [aconc, rel, aegl_1_1h, erpg_1, idlh_10, aegl_2_1h, erpg_2,
                               hq_rel, hq_aegl1, hq_erpg1, hq_idlh, hq_aegl2, hq_erpg2, distance, angle]

        if self.altrec == 'Y':
            self.strColumns = [fac_id, pollutant, rec_id]
        else:
            self.strColumns = [fac_id, pollutant, fips, block]

        self.skiprows = 0
        df = self.readFromPath(self.getColumns())
        return df.fillna("")