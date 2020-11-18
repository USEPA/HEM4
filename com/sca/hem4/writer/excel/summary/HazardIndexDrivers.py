import os
import pandas as pd

from com.sca.hem4.log.Logger import Logger
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter
from com.sca.hem4.writer.excel.RiskBreakdown import *
from com.sca.hem4.writer.csv.AllOuterReceptors import *
from com.sca.hem4.writer.excel.summary.CancerDrivers import risk_contrib
from com.sca.hem4.writer.excel.summary.AltRecAwareSummary import AltRecAwareSummary
from com.sca.hem4.writer.excel.MaximumIndividualRisksNonCensus import MaximumIndividualRisksNonCensus


class HazardIndexDrivers(ExcelWriter, AltRecAwareSummary):

    def __init__(self, targetDir, facilityIds, parameters=None):
        self.name = "Hazard Index Drivers Summary"
        self.categoryName = parameters[0]
        self.categoryFolder = targetDir
        self.facilityIds = facilityIds

        self.filename = os.path.join(targetDir, self.categoryName + "_hazard_index_drivers.xlsx")
        self.altrec = self.determineAltRec(self.categoryFolder)
        
    def getHeader(self):
        return ['Facility ID', 'HI Type', 'HI Total', 'Source ID', 'Pollutant', 'Hazard Index', 'Percentage']

    def generateOutputs(self):
        Logger.log("Creating " + self.name + " report...", None, False)

        # The first step is to load the risk breakdown output for each facility so that we
        # can recover the risk for each pollutant.
        allrisk_df = pd.DataFrame()
        for facilityId in self.facilityIds:
            targetDir = self.categoryFolder + "/" + facilityId

            riskBreakdown = RiskBreakdown(targetDir=targetDir, facilityId=facilityId)
            bkdn_df = riskBreakdown.createDataframe()
            bkdn_df = bkdn_df.loc[(bkdn_df[site_type] == 'Max indiv risk') & (bkdn_df[parameter] != 'Cancer risk')
                                  & (~bkdn_df[source_id].str.contains('Total')) & (~bkdn_df[pollutant].str.contains('All '))]

            bkdn_df[fac_id] = facilityId

            # Load the max indiv risks file for this facility to get the total value
            if self.altrec == 'N':
                maxIndivRisks = MaximumIndividualRisks(targetDir=targetDir, facilityId=facilityId)
            else:
                maxIndivRisks = MaximumIndividualRisksNonCensus(targetDir=targetDir, facilityId=facilityId)
            risks_df = maxIndivRisks.createDataframe()
            risks_df = risks_df.loc[risks_df[parameter] != 'Cancer risk']

            # Rename the 'value' column so that we don't collide with the value in bkdn_df...
            risks_df.rename(index=str, columns={value: "total"}, inplace=True)
            bkdn_df = bkdn_df.merge(risks_df, on=[parameter], how='inner')

            allrisk_df = allrisk_df.append(bkdn_df)

        allrisk_df.drop_duplicates().reset_index(drop=True)
        hidrivers_df = pd.DataFrame(allrisk_df, columns=[fac_id, parameter, "total", source_id, pollutant, value])

        # The risk contribution is the risk divided by the MIR.
        hidrivers_df[risk_contrib] = hidrivers_df.apply(
            lambda x: round(100*(x[value] / x['total']), 2) if x['total'] > 0 else 0, axis=1)

        hidrivers_df = hidrivers_df[[fac_id, parameter, "total", source_id, pollutant, value, risk_contrib]]

        hidrivers_df.sort_values(by=[fac_id, parameter, risk_contrib], ascending=False, inplace=True)
        hidrivers_df.reset_index(inplace=True, drop=True)

        # For a given parameter, start adding contribs until you get to 90%. Once that
        # threshold has been reached, drop all subsequent rows from that facility/param and
        # then start over when the first record of the next parameter appears.
        drops = []
        currentParameter = ''
        totalContrib = 0
        for index, row in hidrivers_df.iterrows():
            param = row.loc[parameter]
            totalValue = row.loc["total"]

            # Start over if new parameter
            if param != currentParameter:
                currentParameter = param
                totalContrib = 0

            if totalContrib >= 90:
                drops.append(index)
            else:
                # Special case for a total value of 0...drop it!
                if totalValue == 0:
                    drops.append(index)
                    totalContrib = 100
                else:
                    totalContrib = totalContrib + row[risk_contrib]

        # Remove records that are not in the top 90% of contributors or have total = 0
        hidrivers_df.drop(hidrivers_df.index[drops], inplace=True)

        # This sort is make the final output consistent with what HEM3 produces.
        hidrivers_df.sort_values(by=["total", risk_contrib], ascending=False, inplace=True)

        # Put final df into array
        self.dataframe = hidrivers_df
        self.data = self.dataframe.values
        yield self.dataframe