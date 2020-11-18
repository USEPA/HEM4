from math import log10, floor
from com.sca.hem4.writer.csv.BlockSummaryChronic import *
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter
from com.sca.hem4.FacilityPrep import *
from com.sca.hem4.writer.excel.summary.AltRecAwareSummary import AltRecAwareSummary
from com.sca.hem4.writer.csv.BlockSummaryChronicNonCensus import BlockSummaryChronicNonCensus

risklevel = 'risklevel'
facilitycount = 'facilitycount'
class Histogram(ExcelWriter, AltRecAwareSummary):

    def __init__(self, targetDir, facilityIds, parameters=None):
        self.name = "Cancer Histogram"
        self.categoryName = parameters[0]
        self.categoryFolder = targetDir
        self.facilityIds = facilityIds

        self.filename = os.path.join(targetDir, self.categoryName + "_histogram_risk.xlsx")
        self.altrec = self.determineAltRec(self.categoryFolder)

    def getHeader(self):
        return ['Risk level', 'Population', 'Facility count']

    def generateOutputs(self):
        Logger.log("Creating " + self.name + " report...", None, False)

        self.facilityMap = {}

        # Data structure to keep track of the needed histogram values.
        # There are 5 sub lists corresponding to the five buckets.
        counts = [[0,0], [0,0], [0,0], [0,0], [0,0]]

        blocksummary_df = pd.DataFrame()
        for facilityId in self.facilityIds:
            targetDir = self.categoryFolder + "/" + facilityId

            blockSummaryChronic = BlockSummaryChronicNonCensus(targetDir=targetDir, facilityId=facilityId) if self.altrec == 'Y' else\
                BlockSummaryChronic(targetDir=targetDir, facilityId=facilityId)

            bsc_df = blockSummaryChronic.createDataframe()

            bsc_df.sort_values(by=[mir], ascending=False, inplace=True)
            foundMax = False
            for index, row in bsc_df.iterrows():

                if not foundMax and row[population] > 0:
                    foundMax = True
                    rounded = self.round_to_sigfig(row[mir])

                    if rounded < 1e-6:
                        counts[0][1] = counts[0][1] + 1
                    if rounded >= 1e-6:
                        counts[1][1] = counts[1][1] + 1
                    if rounded >= 1e-5:
                        counts[2][1] = counts[2][1] + 1
                    if rounded >= 1e-4:
                        counts[3][1] = counts[3][1] + 1
                    if rounded >= 1e-3:
                        counts[4][1] = counts[4][1] + 1

            blocksummary_df = blocksummary_df.append(bsc_df)


        blocksummary_df.reset_index(inplace=True, drop=True)
        
        
#        blocksummary_all0 = blocksummary_df[blocksummary_df.population == 0]
#        blocksummary_no0 = blocksummary_df.drop(blocksummary_df[blocksummary_df.population == 0].index, inplace=False)
        
        
        if self.altrec == 'N':

            # Census
            
            # Drop records that (are not user receptors AND have population = 0)
            blocksummary_df.drop(blocksummary_df[(blocksummary_df.population == 0) & 
                                                 (~blocksummary_df.block.str.contains('U', case=False))].index,
                                                 inplace=True)
            
            aggs = {lat:'first', lon:'first', overlap:'first', elev:'first', utme:'first', blk_type:'first',
                    utmn:'first', hill:'first', fips:'first', block:'first', population:'first',
                    mir:'sum', hi_resp:'sum', hi_live:'sum', hi_neur:'sum', hi_deve:'sum',
                    hi_repr:'sum', hi_kidn:'sum', hi_ocul:'sum', hi_endo:'sum', hi_hema:'sum',
                    hi_immu:'sum', hi_skel:'sum', hi_sple:'sum', hi_thyr:'sum', hi_whol:'sum'}
    
            # Aggregate concentration, grouped by FIPS/block
            risk_summed = blocksummary_df.groupby([fips, block]).agg(aggs)[blockSummaryChronic.getColumns()]
                        
        else:

            # Alternate receptors
            
            # Drop records that (are not user receptors AND have population = 0)
            blocksummary_df.drop(blocksummary_df[(blocksummary_df.population == 0) & 
                                                 (~blocksummary_df.rec_id.str.contains('U', case=False))].index,
                                                 inplace=True)
            
            aggs = {lat:'first', lon:'first', overlap:'first', elev:'first', utme:'first', blk_type:'first',
                    utmn:'first', hill:'first', rec_id: 'first', population:'first',
                    mir:'sum', hi_resp:'sum', hi_live:'sum', hi_neur:'sum', hi_deve:'sum',
                    hi_repr:'sum', hi_kidn:'sum', hi_ocul:'sum', hi_endo:'sum', hi_hema:'sum',
                    hi_immu:'sum', hi_skel:'sum', hi_sple:'sum', hi_thyr:'sum', hi_whol:'sum'}
    
            # Aggregate concentration, grouped by rec_id
            risk_summed = blocksummary_df.groupby([rec_id]).agg(aggs)[blockSummaryChronic.getColumns()]
            
      
        for index, row in risk_summed.iterrows():
            rounded = self.round_to_sigfig(row[mir])
            if rounded < 1e-6:
                counts[0][0] = counts[0][0] + row[population]
            if rounded >= 1e-6:
                counts[1][0] = counts[1][0] + row[population]
            if rounded >= 1e-5:
                counts[2][0] = counts[2][0] + row[population]
            if rounded >= 1e-4:
                counts[3][0] = counts[3][0] + row[population]
            if rounded >= 1e-3:
                counts[4][0] = counts[4][0] + row[population]
        
#        risks = [
#            ['<1e-6', counts[0][0], counts[0][1]] if counts[0][1] > 0 else ['<1e-6', '', 0],
#            ['>=1e-6', counts[1][0], counts[1][1]] if counts[1][1] > 0 else ['>=1e-6', '', 0],
#            ['>=1e-5', counts[2][0], counts[2][1]] if counts[2][1] > 0 else ['>=1e-5', '', 0],
#            ['>=1e-4', counts[3][0], counts[3][1]] if counts[3][1] > 0 else ['>=1e-4', '', 0],
#            ['>=1e-3', counts[4][0], counts[4][1]] if counts[4][1] > 0 else ['>=1e-3', '', 0],
#        ]
                
        risks = [
            ['<1e-6', counts[0][0], counts[0][1]],
            ['>=1e-6', counts[1][0], counts[1][1]],
            ['>=1e-5', counts[2][0], counts[2][1]],
            ['>=1e-4', counts[3][0], counts[3][1]],
            ['>=1e-3', counts[4][0], counts[4][1]],
        ]
        histogram_df = pd.DataFrame(risks, columns=[risklevel, population, facilitycount]).astype(
            dtype=int, errors='ignore')

        # Put final df into array
        self.dataframe = histogram_df
        self.data = self.dataframe.values
        yield self.dataframe

    def round_to_sigfig(self, x, sig=1):
        if x == 0:
            return 0;

        if math.isnan(x):
            return float('NaN')

        rounded = round(x, sig-int(floor(log10(abs(x))))-1)
        return rounded