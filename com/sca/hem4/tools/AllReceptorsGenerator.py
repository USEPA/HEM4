import os
from math import floor, log10

import pandas as pd
from com.sca.hem4.writer.csv.BlockSummaryChronic import *
from com.sca.hem4.writer.csv.BlockSummaryChronicNonCensus import BlockSummaryChronicNonCensus
from com.sca.hem4.writer.excel.FacilityMaxRiskandHI import FacilityMaxRiskandHI

output_dir = "/Users/chris/Downloads/PrimCop"
radius = 5000

class AllReceptorsGenerator(InputFile):

    def __init__(self, hem4_output_dir, radius):
        self.output_dir = hem4_output_dir
        self.radius = radius

        self.basepath = os.path.basename(os.path.normpath(self.output_dir))
        files = os.listdir(self.output_dir)
        rootpath = self.output_dir + '/'
        self.facilityIds = [item for item in files if os.path.isdir(os.path.join(rootpath, item))
                            and 'inputs' not in item.lower() and 'acute maps' not in item.lower()]

        self.altrec = self.determineAltRec(self.output_dir)

    def getHeader(self):
        return ['FIPS', 'Block', 'Lon', 'Lat', 'Population', 'MIR', 'MIR (rounded)', hi_resp, hi_live,
                hi_neur, hi_deve, hi_repr, hi_kidn, hi_ocul, hi_endo, hi_hema, hi_immu, hi_skel, hi_sple,
                hi_thyr, hi_whol, 'Facility count']

    def getColumns(self):
        return [fips, block, lon, lat, population, mir, 'mir_rounded', hi_resp, hi_live,
                hi_neur, hi_deve, hi_repr, hi_kidn, hi_ocul, hi_endo, hi_hema, hi_immu, hi_skel, hi_sple,
                hi_thyr, hi_whol, 'fac_count']
    def generate(self):

        blocksummary_df = pd.DataFrame()

        # Used for finding the fac center
        maxRiskAndHI = FacilityMaxRiskandHI(targetDir=self.output_dir, filenameOverride="PrimCop_Actuals2_facility_max_risk_and_hi.xlsx")
        maxRiskAndHI_df = maxRiskAndHI.createDataframe()

        for facilityId in self.facilityIds:
            print("Inspecting facility folder " + facilityId + " for output files...")

            try:
                targetDir = self.output_dir + "/" + facilityId

                maxrisk_df = maxRiskAndHI_df.loc[maxRiskAndHI_df['Facil_id'] == facilityId]
                center_lat = maxrisk_df.iloc[0]['fac_center_latitude']
                center_lon = maxrisk_df.iloc[0]['fac_center_longitude']
                ceny, cenx, zone, hemi, epsg = UTM.ll2utm(center_lat, center_lon)

                blockSummaryChronic = BlockSummaryChronicNonCensus(targetDir=targetDir, facilityId=facilityId) if self.altrec == 'Y' else \
                    BlockSummaryChronic(targetDir=targetDir, facilityId=facilityId)

                bsc_df = blockSummaryChronic.createDataframe()
                bsc_df['fac_count'] = 1

                bsc_df[distance] = np.sqrt((cenx - bsc_df.utme)**2 + (ceny - bsc_df.utmn)**2)
                maxdist = self.radius
                bsc_df = bsc_df.query('distance <= @maxdist').copy()
                blocksummary_df = blocksummary_df.append(bsc_df)

                bsc_df['fac_count']

            except BaseException as e:
                print("Error gathering output information: " + repr(e))
                print("Skipping facility " + facilityId)
                continue

        blocksummary_df.drop_duplicates().reset_index(drop=True)

        columns = [fips, block, lon, lat, population, mir, hi_resp, hi_live,
                   hi_neur, hi_deve, hi_repr, hi_kidn, hi_ocul, hi_endo, hi_hema, hi_immu, hi_skel, hi_sple,
                   hi_thyr, hi_whol, 'fac_count']

        if self.altrec == 'N':

            aggs = {lat:'first', lon:'first', overlap:'first', elev:'first', utme:'first', blk_type:'first',
                    utmn:'first', hill:'first', fips:'first', block:'first', population:'first',
                    mir:'sum', hi_resp:'sum', hi_live:'sum', hi_neur:'sum', hi_deve:'sum',
                    hi_repr:'sum', hi_kidn:'sum', hi_ocul:'sum', hi_endo:'sum', hi_hema:'sum',
                    hi_immu:'sum', hi_skel:'sum', hi_sple:'sum', hi_thyr:'sum', hi_whol:'sum', 'fac_count':'sum'}

            # Aggregate concentration, grouped by FIPS/block
            risk_summed = blocksummary_df.groupby([fips, block]).agg(aggs)[columns]

        else:

            aggs = {lat:'first', lon:'first', overlap:'first', elev:'first', utme:'first', blk_type:'first',
                    utmn:'first', hill:'first', rec_id: 'first', population:'first',
                    mir:'sum', hi_resp:'sum', hi_live:'sum', hi_neur:'sum', hi_deve:'sum',
                    hi_repr:'sum', hi_kidn:'sum', hi_ocul:'sum', hi_endo:'sum', hi_hema:'sum',
                    hi_immu:'sum', hi_skel:'sum', hi_sple:'sum', hi_thyr:'sum', hi_whol:'sum'}

            # Aggregate concentration, grouped by rec_id
            risk_summed = blocksummary_df.groupby([rec_id]).agg(aggs)[columns]

        risk_summed['mir_rounded'] = risk_summed[mir].apply(self.round_to_sigfig, 1)

        path = os.path.join(self.output_dir, 'MIR_HI_allreceptors.xlsx')
        risk_summed.to_excel(path, index=False, columns=self.getColumns(), header=self.getHeader())

    def determineAltRec(self, targetDir):

        # Check the Inputs folder for the existence of alt_receptors.csv
        fpath = os.path.join(targetDir, "Inputs", "alt_receptors.csv")
        if os.path.exists(fpath):
            altrecUsed = 'Y'
        else:
            altrecUsed = 'N'

        return altrecUsed

    def round_to_sigfig(self, x, sig=1):
        if x == 0:
            return 0;

        if math.isnan(x):
            return float('NaN')

        rounded = round(x, sig-int(floor(log10(abs(x))))-1)
        return rounded

    def createDataframe(self):
        self.numericColumns = [lat, lon, mir, 'mir_rounded', population, hi_resp, hi_live,
                               hi_neur, hi_deve, hi_repr, hi_kidn, hi_ocul, hi_endo, hi_hema, hi_immu, hi_skel, hi_sple,
                               hi_thyr, hi_whol, 'fac_count']
        self.strColumns = [fips, block]
        df = self.readFromPathCsv(self.getColumns())
        return df.fillna("")


generator = AllReceptorsGenerator(hem4_output_dir=output_dir, radius=radius)
generator.generate()
