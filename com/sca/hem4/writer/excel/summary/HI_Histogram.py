from math import log10, floor
from com.sca.hem4.writer.csv.BlockSummaryChronic import *
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter
from com.sca.hem4.FacilityPrep import *
from com.sca.hem4.writer.excel.summary.AltRecAwareSummary import AltRecAwareSummary
from com.sca.hem4.writer.csv.BlockSummaryChronicNonCensus import BlockSummaryChronicNonCensus

risklevel = 'risklevel'
resp_population = 'resp_population'
resp_facilitycount = 'resp_facilitycount'
live_population = 'live_population'
live_facilitycount = 'live_facilitycount'
neuro_population = 'neuro_population'
neuro_facilitycount = 'neuro_facilitycount'
deve_population = 'deve_population'
deve_facilitycount = 'deve_facilitycount'
repr_population = 'repr_population'
repr_facilitycount = 'repr_facilitycount'
kidn_population = 'kidn_population'
kidn_facilitycount = 'kidn_facilitycount'
ocul_population = 'ocul_population'
ocul_facilitycount = 'ocul_facilitycount'
endo_population = 'endo_population'
endo_facilitycount = 'endo_facilitycount'
hema_population = 'hema_population'
hema_facilitycount = 'hema_facilitycount'
immu_population = 'immu_population'
immu_facilitycount = 'immu_facilitycount'
skel_population = 'skel_population'
skel_facilitycount = 'skel_facilitycount'
sple_population = 'sple_population'
sple_facilitycount = 'sple_facilitycount'
thyr_population = 'thyr_population'
thyr_facilitycount = 'thyr_facilitycount'
whol_population = 'whol_population'
whol_facilitycount = 'whol_facilitycount'

class HI_Histogram(ExcelWriter, AltRecAwareSummary):

    def __init__(self, targetDir, facilityIds, parameters=None):
        self.name = "Non-cancer Histogram"
        self.categoryName = parameters[0]
        self.categoryFolder = targetDir
        self.facilityIds = facilityIds

        self.filename = os.path.join(targetDir, self.categoryName + "_hi_histogram.xlsx")
        self.altrec = self.determineAltRec(self.categoryFolder)

    def getHeader(self):
        return ['HI Level',	 'Respiratory Pop',	'Respiratory Facilities',
                'Liver Pop',	'Liver Facilities',
                'Neurological Pop',	'Neurological Facilities',
                'Developmental Pop', 'Developmental Facilities',
                'Reproductive Pop',	'Reproductive Facilities',
                'Kidney Pop', 'Kidney Facilities',
                'Ocular Pop', 'Ocular Facilities',
                'Endocrine Pop', 'Endocrine Facilities',
                'Hematological Pop', 'Hematological Facilities',
                'Immunological Pop', 'Immunological Facilities',
                'Skeletal Pop', 'Skeletal Facilities',
                'Spleen Pop', 'Spleen Facilities',
                'Thyroid Pop', 'Thyroid Facilities',
                'Whole Body Pop', 'Whole Body Facilities'
                ]

    def generateOutputs(self):
        Logger.log("Creating " + self.name + " report...", None, False)

        self.facilityMap = {}

        # Data structure to keep track of the needed histogram values.
        # There are 5 sub lists corresponding to the five buckets.
        counts = [[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0], 
                  [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                  [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                  [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],
                  [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]]

        blocksummary_df = pd.DataFrame()
        for facilityId in self.facilityIds:
            targetDir = self.categoryFolder + "/" + facilityId

            blockSummaryChronic = BlockSummaryChronicNonCensus(targetDir=targetDir, facilityId=facilityId) if self.altrec == 'Y' else\
                BlockSummaryChronic(targetDir=targetDir, facilityId=facilityId)

            bsc_df = blockSummaryChronic.createDataframe()

            # Get max resp value that has a population > 0
            respMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_resp].idxmax()]
            rounded = self.round_to_sigfig(respMax[hi_resp])
            if rounded > 1000:
                counts[0][1] = counts[0][1] + 1
            if rounded > 100:
                counts[1][1] = counts[1][1] + 1
            if rounded > 10:
                counts[2][1] = counts[2][1] + 1
            if rounded > 1:
                counts[3][1] = counts[3][1] + 1
            if rounded <= 1:
                counts[4][1] = counts[4][1] + 1

            liveMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_live].idxmax()]
            rounded = self.round_to_sigfig(liveMax[hi_live])
            if rounded > 1000:
                counts[0][3] = counts[0][3] + 1
            if rounded > 100:
                counts[1][3] = counts[1][3] + 1
            if rounded > 10:
                counts[2][3] = counts[2][3] + 1
            if rounded > 1:
                counts[3][3] = counts[3][3] + 1
            if rounded <= 1:
                counts[4][3] = counts[4][3] + 1

            neuroMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_neur].idxmax()]
            rounded = self.round_to_sigfig(neuroMax[hi_neur])
            if rounded > 1000:
                counts[0][5] = counts[0][5] + 1
            if rounded > 100:
                counts[1][5] = counts[1][5] + 1
            if rounded > 10:
                counts[2][5] = counts[2][5] + 1
            if rounded > 1:
                counts[3][5] = counts[3][5] + 1
            if rounded <= 1:
                counts[4][5] = counts[4][5] + 1

            deveMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_deve].idxmax()]
            rounded = self.round_to_sigfig(deveMax[hi_deve])
            if rounded > 1000:
                counts[0][7] = counts[0][7] + 1
            if rounded > 100:
                counts[1][7] = counts[1][7] + 1
            if rounded > 10:
                counts[2][7] = counts[2][7] + 1
            if rounded > 1:
                counts[3][7] = counts[3][7] + 1
            if rounded <= 1:
                counts[4][7] = counts[4][7] + 1

            reproMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_repr].idxmax()]
            rounded = self.round_to_sigfig(reproMax[hi_repr])
            if rounded > 1000:
                counts[0][9] = counts[0][9] + 1
            if rounded > 100:
                counts[1][9] = counts[1][9] + 1
            if rounded > 10:
                counts[2][9] = counts[2][9] + 1
            if rounded > 1:
                counts[3][9] = counts[3][9] + 1
            if rounded <= 1:
                counts[4][9] = counts[4][9] + 1

            kidnMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_kidn].idxmax()]
            rounded = self.round_to_sigfig(kidnMax[hi_kidn])
            if rounded > 1000:
                counts[0][11] = counts[0][11] + 1
            if rounded > 100:
                counts[1][11] = counts[1][11] + 1
            if rounded > 10:
                counts[2][11] = counts[2][11] + 1
            if rounded > 1:
                counts[3][11] = counts[3][11] + 1
            if rounded <= 1:
                counts[4][11] = counts[4][11] + 1

            oculMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_ocul].idxmax()]
            rounded = self.round_to_sigfig(oculMax[hi_ocul])
            if rounded > 1000:
                counts[0][13] = counts[0][13] + 1
            if rounded > 100:
                counts[1][13] = counts[1][13] + 1
            if rounded > 10:
                counts[2][13] = counts[2][13] + 1
            if rounded > 1:
                counts[3][13] = counts[3][13] + 1
            if rounded <= 1:
                counts[4][13] = counts[4][13] + 1

            endoMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_endo].idxmax()]
            rounded = self.round_to_sigfig(endoMax[hi_endo])
            if rounded > 1000:
                counts[0][15] = counts[0][15] + 1
            if rounded > 100:
                counts[1][15] = counts[1][15] + 1
            if rounded > 10:
                counts[2][15] = counts[2][15] + 1
            if rounded > 1:
                counts[3][15] = counts[3][15] + 1
            if rounded <= 1:
                counts[4][15] = counts[4][15] + 1

            hemaMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_hema].idxmax()]
            rounded = self.round_to_sigfig(hemaMax[hi_hema])
            if rounded > 1000:
                counts[0][17] = counts[0][17] + 1
            if rounded > 100:
                counts[1][17] = counts[1][17] + 1
            if rounded > 10:
                counts[2][17] = counts[2][17] + 1
            if rounded > 1:
                counts[3][17] = counts[3][17] + 1
            if rounded <= 1:
                counts[4][17] = counts[4][17] + 1

            immuMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_immu].idxmax()]
            rounded = self.round_to_sigfig(immuMax[hi_immu])
            if rounded > 1000:
                counts[0][19] = counts[0][19] + 1
            if rounded > 100:
                counts[1][19] = counts[1][19] + 1
            if rounded > 10:
                counts[2][19] = counts[2][19] + 1
            if rounded > 1:
                counts[3][19] = counts[3][19] + 1
            if rounded <= 1:
                counts[4][19] = counts[4][19] + 1

            skelMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_skel].idxmax()]
            rounded = self.round_to_sigfig(skelMax[hi_skel])
            if rounded > 1000:
                counts[0][21] = counts[0][21] + 1
            if rounded > 100:
                counts[1][21] = counts[1][21] + 1
            if rounded > 10:
                counts[2][21] = counts[2][21] + 1
            if rounded > 1:
                counts[3][21] = counts[3][21] + 1
            if rounded <= 1:
                counts[4][21] = counts[4][21] + 1

            spleMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_sple].idxmax()]
            rounded = self.round_to_sigfig(spleMax[hi_sple])
            if rounded > 1000:
                counts[0][23] = counts[0][23] + 1
            if rounded > 100:
                counts[1][23] = counts[1][23] + 1
            if rounded > 10:
                counts[2][23] = counts[2][23] + 1
            if rounded > 1:
                counts[3][23] = counts[3][23] + 1
            if rounded <= 1:
                counts[4][23] = counts[4][23] + 1

            thyrMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_thyr].idxmax()]
            rounded = self.round_to_sigfig(thyrMax[hi_thyr])
            if rounded > 1000:
                counts[0][25] = counts[0][25] + 1
            if rounded > 100:
                counts[1][25] = counts[1][25] + 1
            if rounded > 10:
                counts[2][25] = counts[2][25] + 1
            if rounded > 1:
                counts[3][25] = counts[3][25] + 1
            if rounded <= 1:
                counts[4][25] = counts[4][25] + 1

            wholMax = bsc_df.loc[bsc_df[(bsc_df[population] > 0)][hi_whol].idxmax()]
            rounded = self.round_to_sigfig(wholMax[hi_whol])
            if rounded > 1000:
                counts[0][27] = counts[0][27] + 1
            if rounded > 100:
                counts[1][27] = counts[1][27] + 1
            if rounded > 10:
                counts[2][27] = counts[2][27] + 1
            if rounded > 1:
                counts[3][27] = counts[3][27] + 1
            if rounded <= 1:
                counts[4][27] = counts[4][27] + 1

            blocksummary_df = blocksummary_df.append(bsc_df)

        blocksummary_df.reset_index(inplace=True, drop=True)

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
    
            # Aggregate concentration, grouped by FIPS/block
            risk_summed = blocksummary_df.groupby([rec_id]).agg(aggs)[blockSummaryChronic.getColumns()]
           

        for index, row in risk_summed.iterrows():
            roundedResp = self.round_to_sigfig(row[hi_resp])
            roundedLive = self.round_to_sigfig(row[hi_live])
            roundedNeuro = self.round_to_sigfig(row[hi_neur])
            roundedDeve = self.round_to_sigfig(row[hi_deve])
            roundedRepr = self.round_to_sigfig(row[hi_repr])
            roundedKidn = self.round_to_sigfig(row[hi_kidn])
            roundedOcul = self.round_to_sigfig(row[hi_ocul])
            roundedEndo = self.round_to_sigfig(row[hi_endo])
            roundedHema = self.round_to_sigfig(row[hi_hema])
            roundedImmu = self.round_to_sigfig(row[hi_immu])
            roundedSkel = self.round_to_sigfig(row[hi_skel])
            roundedSple = self.round_to_sigfig(row[hi_sple])
            roundedThyr = self.round_to_sigfig(row[hi_thyr])
            roundedWhol = self.round_to_sigfig(row[hi_whol])

            if roundedResp > 1000:
                counts[0][0] = counts[0][0] + row[population]
            if roundedResp > 100:
                counts[1][0] = counts[1][0] + row[population]
            if roundedResp > 10:
                counts[2][0] = counts[2][0] + row[population]
            if roundedResp > 1:
                counts[3][0] = counts[3][0] + row[population]
            if roundedResp <= 1:
                counts[4][0] = counts[4][0] + row[population]

            if roundedLive > 1000:
                counts[0][2] = counts[0][2] + row[population]
            if roundedLive > 100:
                counts[1][2] = counts[1][2] + row[population]
            if roundedLive > 10:
                counts[2][2] = counts[2][2] + row[population]
            if roundedLive > 1:
                counts[3][2] = counts[3][2] + row[population]
            if roundedLive <= 1:
                counts[4][2] = counts[4][2] + row[population]

            if roundedNeuro > 1000:
                counts[0][4] = counts[0][4] + row[population]
            if roundedNeuro > 100:
                counts[1][4] = counts[1][4] + row[population]
            if roundedNeuro > 10:
                counts[2][4] = counts[2][4] + row[population]
            if roundedNeuro > 1:
                counts[3][4] = counts[3][4] + row[population]
            if roundedNeuro <= 1:
                counts[4][4] = counts[4][4] + row[population]

            if roundedDeve > 1000:
                counts[0][6] = counts[0][6] + row[population]
            if roundedDeve > 100:
                counts[1][6] = counts[1][6] + row[population]
            if roundedDeve > 10:
                counts[2][6] = counts[2][6] + row[population]
            if roundedDeve > 1:
                counts[3][6] = counts[3][6] + row[population]
            if roundedDeve <= 1:
                counts[4][6] = counts[4][6] + row[population]

            if roundedRepr > 1000:
                counts[0][8] = counts[0][8] + row[population]
            if roundedRepr > 100:
                counts[1][8] = counts[1][8] + row[population]
            if roundedRepr > 10:
                counts[2][8] = counts[2][8] + row[population]
            if roundedRepr > 1:
                counts[3][8] = counts[3][8] + row[population]
            if roundedRepr <= 1:
                counts[4][8] = counts[4][8] + row[population]

            if roundedKidn > 1000:
                counts[0][10] = counts[0][10] + row[population]
            if roundedKidn > 100:
                counts[1][10] = counts[1][10] + row[population]
            if roundedKidn > 10:
                counts[2][10] = counts[2][10] + row[population]
            if roundedKidn > 1:
                counts[3][10] = counts[3][10] + row[population]
            if roundedKidn <= 1:
                counts[4][10] = counts[4][10] + row[population]

            if roundedOcul > 1000:
                counts[0][12] = counts[0][12] + row[population]
            if roundedOcul > 100:
                counts[1][12] = counts[1][12] + row[population]
            if roundedOcul > 10:
                counts[2][12] = counts[2][12] + row[population]
            if roundedOcul > 1:
                counts[3][12] = counts[3][12] + row[population]
            if roundedOcul <= 1:
                counts[4][12] = counts[4][12] + row[population]

            if roundedEndo > 1000:
                counts[0][14] = counts[0][14] + row[population]
            if roundedEndo > 100:
                counts[1][14] = counts[1][14] + row[population]
            if roundedEndo > 10:
                counts[2][14] = counts[2][14] + row[population]
            if roundedEndo > 1:
                counts[3][14] = counts[3][14] + row[population]
            if roundedEndo <= 1:
                counts[4][14] = counts[4][14] + row[population]

            if roundedHema > 1000:
                counts[0][16] = counts[0][16] + row[population]
            if roundedHema > 100:
                counts[1][16] = counts[1][16] + row[population]
            if roundedHema > 10:
                counts[2][16] = counts[2][16] + row[population]
            if roundedHema > 1:
                counts[3][16] = counts[3][16] + row[population]
            if roundedHema <= 1:
                counts[4][16] = counts[4][16] + row[population]

            if roundedImmu > 1000:
                counts[0][18] = counts[0][18] + row[population]
            if roundedImmu > 100:
                counts[1][18] = counts[1][18] + row[population]
            if roundedImmu > 10:
                counts[2][18] = counts[2][18] + row[population]
            if roundedImmu > 1:
                counts[3][18] = counts[3][18] + row[population]
            if roundedImmu <= 1:
                counts[4][18] = counts[4][18] + row[population]

            if roundedSkel > 1000:
                counts[0][20] = counts[0][20] + row[population]
            if roundedSkel > 100:
                counts[1][20] = counts[1][20] + row[population]
            if roundedSkel > 10:
                counts[2][20] = counts[2][20] + row[population]
            if roundedSkel > 1:
                counts[3][20] = counts[3][20] + row[population]
            if roundedSkel <= 1:
                counts[4][20] = counts[4][20] + row[population]

            if roundedSple > 1000:
                counts[0][22] = counts[0][22] + row[population]
            if roundedSple > 100:
                counts[1][22] = counts[1][22] + row[population]
            if roundedSple > 10:
                counts[2][22] = counts[2][22] + row[population]
            if roundedSple > 1:
                counts[3][22] = counts[3][22] + row[population]
            if roundedSple <= 1:
                counts[4][22] = counts[4][22] + row[population]

            if roundedThyr > 1000:
                counts[0][24] = counts[0][24] + row[population]
            if roundedThyr > 100:
                counts[1][24] = counts[1][24] + row[population]
            if roundedThyr > 10:
                counts[2][24] = counts[2][24] + row[population]
            if roundedThyr > 1:
                counts[3][24] = counts[3][24] + row[population]
            if roundedThyr <= 1:
                counts[4][24] = counts[4][24] + row[population]

            if roundedWhol > 1000:
                counts[0][26] = counts[0][26] + row[population]
            if roundedWhol > 100:
                counts[1][26] = counts[1][26] + row[population]
            if roundedWhol > 10:
                counts[2][26] = counts[2][26] + row[population]
            if roundedWhol > 1:
                counts[3][26] = counts[3][26] + row[population]
            if roundedWhol <= 1:
                counts[4][26] = counts[4][26] + row[population]

        risks = [
            ['> 1000', counts[0][0], counts[0][1], counts[0][2], counts[0][3], counts[0][4], counts[0][5],
             counts[0][6], counts[0][7], counts[0][8], counts[0][9], counts[0][10], counts[0][11],
             counts[0][12], counts[0][13], counts[0][14], counts[0][15], counts[0][16], counts[0][17],
             counts[0][18], counts[0][19], counts[0][20], counts[0][21], counts[0][22], counts[0][23],
             counts[0][24], counts[0][25], counts[0][26], counts[0][27]
            ],
            ['> 100', counts[1][0], counts[1][1], counts[1][2], counts[1][3], counts[1][4], counts[1][5],
             counts[1][6], counts[1][7], counts[1][8], counts[1][9], counts[1][10], counts[1][11],
             counts[1][12], counts[1][13], counts[1][14], counts[1][15], counts[1][16], counts[1][17],
             counts[1][18], counts[1][19], counts[1][20], counts[1][21], counts[1][22], counts[1][23],
             counts[1][24], counts[1][25], counts[1][26], counts[1][27],            
            ],
            ['> 10', counts[2][0], counts[2][1], counts[2][2], counts[2][3], counts[2][4], counts[2][5],
             counts[2][6], counts[2][7], counts[2][8], counts[2][9], counts[2][10], counts[2][11],
             counts[2][12], counts[2][13], counts[2][14], counts[2][15], counts[2][16], counts[2][17],
             counts[2][18], counts[2][19], counts[2][20], counts[2][21], counts[2][22], counts[2][23],
             counts[2][24], counts[2][25], counts[2][26], counts[2][27],             
            ],
            ['> 1', counts[3][0], counts[3][1], counts[3][2], counts[3][3], counts[3][4], counts[3][5],
             counts[3][6], counts[3][7], counts[3][8], counts[3][9], counts[3][10], counts[3][11],
             counts[3][12], counts[3][13], counts[3][14], counts[3][15], counts[3][16], counts[3][17],
             counts[3][18], counts[3][19], counts[3][20], counts[3][21], counts[3][22], counts[3][23],
             counts[3][24], counts[3][25], counts[3][26], counts[3][27],             
            ],
            ['<= 1', counts[4][0], counts[4][1], counts[4][2], counts[4][3], counts[4][4], counts[4][5],
             counts[4][6], counts[4][7], counts[4][8], counts[4][9], counts[4][10], counts[4][11],
             counts[4][12], counts[4][13], counts[4][14], counts[4][15], counts[4][16], counts[4][17],
             counts[4][18], counts[4][19], counts[4][20], counts[4][21], counts[4][22], counts[4][23],
             counts[4][24], counts[4][25], counts[4][26], counts[4][27],            
             ]
        ]
        histogram_df = pd.DataFrame(risks, columns=[risklevel, resp_population, resp_facilitycount, 
                                    live_population, live_facilitycount,
                                    neuro_population, neuro_facilitycount, 
                                    deve_population, deve_facilitycount, 
                                    repr_population, repr_facilitycount,
                                    kidn_population, kidn_facilitycount,                                     
                                    ocul_population, ocul_facilitycount,                                     
                                    endo_population, endo_facilitycount,                                     
                                    hema_population, hema_facilitycount,                                     
                                    immu_population, immu_facilitycount,                                     
                                    skel_population, skel_facilitycount,                                     
                                    sple_population, sple_facilitycount,                                     
                                    thyr_population, thyr_facilitycount,                                     
                                    whol_population, whol_facilitycount,                                     
                                    ]).astype(dtype=int, errors='ignore')

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