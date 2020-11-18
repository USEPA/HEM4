from com.sca.hem4.writer.excel.Incidence import Incidence, inc
from com.sca.hem4.writer.excel.RiskBreakdown import *

inc_contrib = 'inc_contrib'

class IncidenceDrivers(ExcelWriter):

    def __init__(self, targetDir, facilityIds, parameters=None):
        self.name = "Incidence Drivers Summary"
        self.categoryName = parameters[0]
        self.categoryFolder = targetDir
        self.facilityIds = facilityIds

        self.filename = os.path.join(targetDir, self.categoryName + "_incidence_drivers.xlsx")

    def getHeader(self):
        return ['Pollutant', 'Incidence', '% of Total Incidence']

    def generateOutputs(self):
        Logger.log("Creating " + self.name + " report...", None, False)

        # The first step is to load the incidence output for each facility so that we
        # can recover the risk for each pollutant.
        allinc_df = pd.DataFrame()
        for facilityId in self.facilityIds:
            targetDir = self.categoryFolder + "/" + facilityId

            incidence = Incidence(targetDir=targetDir, facilityId=facilityId)
            incidence_df = incidence.createDataframe()
            incidence_df = incidence_df.loc[(incidence_df[source_id].str.contains('Total')) &
                                            (~incidence_df[pollutant].str.contains('All modeled pollutants'))]

            incidence_df[fac_id] = facilityId

            allinc_df = allinc_df.append(incidence_df)

        allinc_df.drop_duplicates().reset_index(drop=True)

        summed = allinc_df.groupby([pollutant], as_index=False)[inc].sum()

        # Sort by incidence descending, and then pollutant name ascending
        summed.sort_values(by=[inc, pollutant], ascending=[False, True], inplace=True)
        totalIncidence = summed.sum()[inc]

        summed[inc_contrib] = summed.apply(
            lambda x: str(round(100*(x[inc] / totalIncidence), 2)) + "%" if totalIncidence > 0 else "0.00%", axis=1)

        summed = summed.append({pollutant : 'Total incidence', inc : totalIncidence, inc_contrib : '100%'}, ignore_index=True)

        # Put final df into array
        self.dataframe = summed
        self.data = self.dataframe.values
        yield self.dataframe