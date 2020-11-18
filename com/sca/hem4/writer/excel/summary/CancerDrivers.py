from com.sca.hem4.writer.excel.RiskBreakdown import *
from com.sca.hem4.writer.excel.MaximumIndividualRisksNonCensus import MaximumIndividualRisksNonCensus
from com.sca.hem4.writer.excel.summary.AltRecAwareSummary import AltRecAwareSummary

risk_contrib = 'risk_contrib'

class CancerDrivers(ExcelWriter, AltRecAwareSummary):

    def __init__(self, targetDir, facilityIds, parameters=None):
        self.name = "Cancer Drivers Summary"
        self.categoryName = parameters[0]
        self.categoryFolder = targetDir
        self.facilityIds = facilityIds

        self.filename = os.path.join(targetDir, self.categoryName + "_cancer_drivers.xlsx")
        self.altrec = self.determineAltRec(self.categoryFolder)

    def getHeader(self):
        return ['Facility ID', 'MIR', 'Pollutant', 'Cancer Risk', 'Risk Contribution', 'Source ID']

    def generateOutputs(self):
        Logger.log("Creating " + self.name + " report...", None, False)

        # The first step is to load the risk breakdown output for each facility so that we
        # can recover the risk for each pollutant.
        allrisk_df = pd.DataFrame()
        for facilityId in self.facilityIds:
            targetDir = self.categoryFolder + "/" + facilityId

            riskBreakdown = RiskBreakdown(targetDir=targetDir, facilityId=facilityId)
            bkdn_df = riskBreakdown.createDataframe()
            bkdn_df = bkdn_df.loc[(bkdn_df[site_type] == 'Max indiv risk') & (bkdn_df[parameter] == 'Cancer risk')
                        & (~bkdn_df[source_id].str.contains('Total')) & (~bkdn_df[pollutant].str.contains('All '))]

            bkdn_df[fac_id] = facilityId

            # Load the max indiv risks file for this facility to get the MIR value
            if self.altrec == 'N':
                maxIndivRisks = MaximumIndividualRisks(targetDir=targetDir, facilityId=facilityId)
            else:
                maxIndivRisks = MaximumIndividualRisksNonCensus(targetDir=targetDir, facilityId=facilityId)
            risks_df = maxIndivRisks.createDataframe()
            risks_df = risks_df.loc[risks_df[parameter] == 'Cancer risk']
            bkdn_df[mir] = risks_df[value].iloc[0]

            allrisk_df = allrisk_df.append(bkdn_df)

        allrisk_df.drop_duplicates().reset_index(drop=True)
        cancerdrivers_df = pd.DataFrame(allrisk_df, columns=[fac_id, mir, pollutant, value, source_id])

        # Sort by descending mir
        cancerdrivers_df.sort_values(by=[mir], inplace=True, ascending=False)
        
        
        # The risk contribution is the risk divided by the MIR.
        cancerdrivers_df[risk_contrib] = cancerdrivers_df.apply(
            lambda x: round(100*(x[value] / x[mir]), 2) if x[mir] > 0 else 0, axis=1)

        cancerdrivers_df = cancerdrivers_df[[fac_id, mir, pollutant, value, risk_contrib, source_id]]
        cancerdrivers_df.sort_values(by=[fac_id, risk_contrib], ascending=False, inplace=True)
        cancerdrivers_df.reset_index(inplace=True, drop=True)

        # For a given facility, start adding contribs until you get to 90%. Once that
        # threshold has been reached, drop all subsequent rows from that facility and
        # then start over when the first record of the next facility appears.
        drops = []
        updates = []
        currentFacility = ''
        totalContrib = 0
        for index, row in cancerdrivers_df.iterrows():
            facility = row.loc[fac_id]
            mirValue = row.loc[mir]

            # Start over if new facility
            if facility != currentFacility:
                currentFacility = facility
                totalContrib = 0

            if totalContrib >= 90:
                drops.append(index)
            else:
                # Special case for a mir value of 0...roll up into a single
                # record.
                if mirValue == 0:
                    updates.append(facility)
                    totalContrib = 100
                else:
                    totalContrib = totalContrib + row[risk_contrib]

        # Remove records that are not in the top 90% of contributors
        cancerdrivers_df.drop(cancerdrivers_df.index[drops], inplace=True)

        # Update records that need to be rolled up because their mir = 0
        for facid in updates:
            self.rollup(facid, cancerdrivers_df)

        # This sort is make the final output consistent with what HEM3 produces.
        cancerdrivers_df.sort_values(by=[mir, risk_contrib], ascending=False, inplace=True)

        # for (index, row) in cancerdrivers_df.iterrows():
        #     print(str(row.loc[fac_id]) + ", " + str(row.loc[mir]) + ", " +
        #           str(row.loc[pollutant]) + ", " + str(row.loc[value]) + ", " +
        #           str(row.loc[risk_contrib]) + ", " + str(row.loc[source_id]))

        # Put final df into array
        self.dataframe = cancerdrivers_df
        self.data = self.dataframe.values
        yield self.dataframe

    def rollup(self, facid, df):
        df.loc[df.fac_id == facid, pollutant] = 'None'
        df.loc[df.fac_id == facid, source_id] = 'None'
        df.loc[df.fac_id == facid, value] = 0
        df.loc[df.fac_id == facid, risk_contrib] = 0