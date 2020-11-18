from math import floor, log10

from pandas.core.dtypes.common import is_string_dtype

from com.sca.hem4.writer.csv.AllInnerReceptors import *
from com.sca.hem4.writer.csv.AllOuterReceptors import *
from com.sca.hem4.writer.csv.AllPolarReceptors import AllPolarReceptors, sector, ring
from com.sca.hem4.writer.csv.BlockSummaryChronic import BlockSummaryChronic
from com.sca.hem4.writer.csv.RingSummaryChronic import RingSummaryChronic
from com.sca.hem4.writer.csv.Temporal import Temporal
from com.sca.hem4.writer.csv.hem3.Hem3AllInnerReceptors import Hem3AllInnerReceptors
from com.sca.hem4.writer.csv.hem3.Hem3AllOuterReceptors import Hem3AllOuterReceptors
from com.sca.hem4.writer.csv.hem3.Hem3AllPolarReceptors import Hem3AllPolarReceptors
from com.sca.hem4.writer.csv.hem3.Hem3BlockSummaryChronic import Hem3BlockSummaryChronic
from com.sca.hem4.writer.csv.hem3.Hem3RingSummaryChronic import Hem3RingSummaryChronic
from com.sca.hem4.writer.csv.hem3.Hem3Temporal import Hem3Temporal
from com.sca.hem4.writer.excel.AcuteBreakdown import aconc_pop, aconc_all, AcuteBreakdown
from com.sca.hem4.writer.excel.AcuteChemicalMax import AcuteChemicalMax
from com.sca.hem4.writer.excel.AcuteChemicalPopulated import AcuteChemicalPopulated
from com.sca.hem4.writer.excel.MaximumIndividualRisks import MaximumIndividualRisks, value, parameter
from com.sca.hem4.writer.excel.RiskBreakdown import RiskBreakdown, site_type, mir
from com.sca.hem4.writer.excel.hem3.Hem3AcuteBreakdown import Hem3AcuteBreakdown
from com.sca.hem4.writer.excel.hem3.Hem3AcuteChemicalMax import Hem3AcuteChemicalMax
from com.sca.hem4.writer.excel.hem3.Hem3AcuteChemicalPopulated import Hem3AcuteChemicalPopulated
from com.sca.hem4.writer.excel.hem3.Hem3MaximumIndividualRisks import Hem3MaximumIndividualRisks
from com.sca.hem4.writer.excel.hem3.Hem3RiskBreakdown import Hem3RiskBreakdown

facid = "fac2-IL"
hem3Dirname = r"C:\Temp\hem3files\hem3_output_qa_all_points\FAC2-IL"
hem4Dirname = r"C:\Temp\hem4files\QA_Test_Run_2_AllPoints\Fac2-IL"
acute = 'N'
temporal = 'N'
temporal_cols = 8

class Hem3Comparer():

    def __init__(self, hem3Dir, hem4Dir, acute, temporal, temporal_cols):
        self.hem3Dir = hem3Dir
        self.hem4Dir = hem4Dir
        self.diff_target = self.hem4Dir + "\diff"
        self.acute = acute
        self.temporal = temporal
        self.temporal_cols = temporal_cols

        if not (os.path.exists(self.diff_target) or os.path.isdir(self.diff_target)):
            print("Creating diff directory for results...")
            os.mkdir(self.diff_target)

    def compare(self):

        #---------- All inner receptors -----------#
        hem3File = facid + "_all_inner_receptors.csv"
        hem4File = facid + "_all_inner_receptors.csv"
        diffFile = "diff_all_inner_receptors.csv"
        joinColumns = [fips, block, source_id, pollutant]
        diffColumns = [conc]
        if self.acute == 'Y':
            diffColumns.append(aconc)

        #------------------------------------------#
        hem4allinner = AllInnerReceptors(targetDir=self.hem4Dir, facilityId=None, model=None, plot_df=None,
                                         acuteyn=self.acute, filenameOverride=hem4File)
        hem3allinner = Hem3AllInnerReceptors(targetDir=self.hem3Dir, facilityId=None, model=None, plot_df=None,
                                             acuteyn=self.acute, filenameOverride=hem3File)
        allinner_diff = AllInnerReceptors(targetDir=self.diff_target, facilityId=None, model=None, plot_df=None,
                                          acuteyn=self.acute, filenameOverride=diffFile)
        allinner_diff.writeHeader()
        diff_df = self.calculateNumericDiffs(hem3allinner, hem4allinner, joinColumns, diffColumns)
        allinner_diff.appendToFile(diff_df)

        #---------- All polar receptors -----------#
        hem3File = facid + "_all_polar_receptors.csv"
        hem4File = facid + "_all_polar_receptors.csv"
        diffFile = "diff_all_polar_receptors.csv"
        joinColumns = [sector, ring, source_id, pollutant]
        diffColumns = [conc]
        if self.acute == 'Y':
            diffColumns.append(aconc)


        #------------------------------------------#
        hem4allpolar = AllPolarReceptors(targetDir=self.hem4Dir, facilityId=None, model=None, plot_df=None,
                                         acuteyn=self.acute, filenameOverride=hem4File)
        hem3allpolar = Hem3AllPolarReceptors(targetDir=self.hem3Dir, facilityId=None, model=None, plot_df=None,
                                             acuteyn=self.acute, filenameOverride=hem3File)
        allpolar_diff = AllPolarReceptors(targetDir=self.diff_target, facilityId=None, model=None, plot_df=None,
                                          acuteyn=self.acute, filenameOverride=diffFile)
        allpolar_diff.writeHeader()
        diff_df = self.calculateNumericDiffs(hem3allpolar, hem4allpolar, joinColumns, diffColumns)
        allpolar_diff.appendToFile(diff_df)

        if self.temporal == 'Y':
            #---------- Temporal output -----------#
            hem3File = facid + "_temporal.csv"
            hem4File = facid + "_temporal.csv"
            diffFile = "diff_temporal.csv"
            joinColumns = [fips, block, pollutant]
            diffColumns = ['C_01', 'C_02', 'C_03', 'C_04', 'C_05', 'C_06', 'C_07', 'C_08']

            model = Model()
            model.tempvar = 12
            model.model_optns = {}
            model.model_optns['runtype'] = 0
            model.seasonvar = True
            #------------------------------------------#
            hem4temporal = Temporal(targetDir=self.hem4Dir, facilityId=None,
                                    model=model, plot_df=None,
                                    filenameOverride=hem4File)
            hem3temporal = Hem3Temporal(targetDir=self.hem3Dir, facilityId=None,
                                        model=model, plot_df=None,
                                        filenameOverride=hem3File)
            temporal_diff = Temporal(targetDir=self.diff_target, facilityId=None,
                                     model=model, plot_df=None,
                                     filenameOverride=diffFile)
            temporal_diff.writeHeader()
            diff_df = self.calculateNumericDiffs(hem3temporal, hem4temporal, joinColumns, diffColumns)
            temporal_diff.appendToFile(diff_df)

        #---------- Maximum individual risks -----------#
        hem3File = facid + "_maximum_indiv_risks.xlsx"
        hem4File = facid + "_maximum_indiv_risks.xlsx"

        diffFile = "diff_maximum_indiv_risks.xlsx"
        joinColumns = [parameter]
        diffColumns = [value]
        #------------------------------------------#
        hem4risks = MaximumIndividualRisks(targetDir=self.hem4Dir, facilityId=None, model=None, plot_df=None,
                                           filenameOverride=hem4File)
        hem3risks = Hem3MaximumIndividualRisks(targetDir=self.hem3Dir, facilityId=None, model=None, plot_df=None,
                                               filenameOverride=hem3File)
        risks_diff = MaximumIndividualRisks(targetDir=self.diff_target, facilityId=None, model=None, plot_df=None,
                                            filenameOverride=diffFile)
        risks_diff.writeHeader()
        diff_df = self.calculateNumericDiffs(hem3risks, hem4risks, joinColumns, diffColumns)
        risks_diff.appendToFile(diff_df)

        #---------- Risk breakdown -----------#
        hem3File = facid + "_risk_breakdown.xlsx"
        hem4File = facid + "_risk_breakdown.xlsx"
        diffFile = "diff_risk_breakdown.xlsx"
        joinColumns = [site_type, parameter, source_id, pollutant]
        diffColumns = [value]
        #------------------------------------------#
        hem4risks = RiskBreakdown(targetDir=self.hem4Dir, facilityId=None, model=None, plot_df=None,
                                  filenameOverride=hem4File)
        hem3risks = Hem3RiskBreakdown(targetDir=self.hem3Dir, facilityId=None, model=None, plot_df=None,
                                      filenameOverride=hem3File)
        risks_diff = RiskBreakdown(targetDir=self.diff_target, facilityId=None, model=None, plot_df=None,
                                   filenameOverride=diffFile)
        risks_diff.writeHeader()
        diff_df = self.calculateNumericDiffs(hem3risks, hem4risks, joinColumns, diffColumns)
        risks_diff.appendToFile(diff_df)

        #---------- Block Summary Chronic -----------#
        hem3File = facid + "_block_summary_chronic.csv"
        hem4File = facid + "_block_summary_chronic.csv"
        diffFile = "diff_block_summary_chronic.csv"
        joinColumns = [fips, block]
        diffColumns = [mir, hi_resp, hi_live, hi_neur, hi_deve,
                       hi_repr, hi_kidn, hi_ocul, hi_endo, hi_hema, hi_immu, hi_skel, hi_sple, hi_thyr, hi_whol]
        #------------------------------------------#
        hem4summary = BlockSummaryChronic(targetDir=self.hem4Dir, facilityId=None, model=None, plot_df=None,
                                          filenameOverride=hem4File)
        hem3summary = Hem3BlockSummaryChronic(targetDir=self.hem3Dir, facilityId=None, model=None, plot_df=None,
                                              filenameOverride=hem3File)
        summary_diff = BlockSummaryChronic(targetDir=self.diff_target, facilityId=None, model=None, plot_df=None,
                                           filenameOverride=diffFile)
        summary_diff.writeHeader()
        diff_df = self.calculateNumericDiffs(hem3summary, hem4summary, joinColumns, diffColumns)
        summary_diff.appendToFile(diff_df)

        #---------- Ring Summary Chronic -----------#
        hem3File = facid + "_ring_summary_chronic.csv"
        hem4File = facid + "_ring_summary_chronic.csv"
        diffFile = "diff_ring_summary_chronic.csv"
        joinColumns = [utme, utmn]
        diffColumns = [mir, hi_resp, hi_live, hi_neur, hi_deve,
                       hi_repr, hi_kidn, hi_ocul, hi_endo, hi_hema, hi_immu, hi_skel, hi_sple, hi_thyr, hi_whol]
        #------------------------------------------#
        hem4summary = RingSummaryChronic(targetDir=self.hem4Dir, facilityId=None, model=None, plot_df=None,
                                         filenameOverride=hem4File)
        hem3summary = Hem3RingSummaryChronic(targetDir=self.hem3Dir, facilityId=None, model=None, plot_df=None,
                                             filenameOverride=hem3File)
        summary_diff = RingSummaryChronic(targetDir=self.diff_target, facilityId=None, model=None, plot_df=None,
                                          filenameOverride=diffFile)
        summary_diff.writeHeader()
        diff_df = self.calculateNumericDiffs(hem3summary, hem4summary, joinColumns, diffColumns)
        summary_diff.appendToFile(diff_df)

        if self.acute == 'Y':
            #---------- Acute Chemical Max -----------#
            hem3File = facid + "_acute_chem_unpop.xlsx"
            hem4File = facid + "_acute_chem_max.xlsx"
            diffFile = "diff_acute_chem_max.xlsx"
            joinColumns = [pollutant]
            diffColumns = [aconc]
            #------------------------------------------#
            hem4max = AcuteChemicalMax(targetDir=self.hem4Dir, facilityId=None, model=None, plot_df=None,
                                       filenameOverride=hem4File)
            hem3max = Hem3AcuteChemicalMax(targetDir=self.hem3Dir, facilityId=None, model=None, plot_df=None,
                                           filenameOverride=hem3File)
            max_diff = AcuteChemicalMax(targetDir=self.diff_target, facilityId=None, model=None, plot_df=None,
                                        filenameOverride=diffFile)
            max_diff.writeHeader()
            diff_df = self.calculateNumericDiffs(hem3max, hem4max, joinColumns, diffColumns)
            max_diff.appendToFile(diff_df)

            #---------- Acute Chemical Pop -----------#
            hem3File = facid + "_acute_chem_pop.xlsx"
            hem4File = facid + "_acute_chem_pop.xlsx"
            diffFile = "diff_acute_chem_pop.xlsx"
            joinColumns = [pollutant]
            diffColumns = [aconc]
            #------------------------------------------#
            hem4pop = AcuteChemicalPopulated(targetDir=self.hem4Dir, facilityId=None, model=None, plot_df=None,
                                             filenameOverride=hem4File)
            hem3pop = Hem3AcuteChemicalPopulated(targetDir=self.hem3Dir, facilityId=None, model=None, plot_df=None,
                                                 filenameOverride=hem3File)
            pop_diff = AcuteChemicalPopulated(targetDir=self.diff_target, facilityId=None, model=None, plot_df=None,
                                              filenameOverride=diffFile)
            pop_diff.writeHeader()
            diff_df = self.calculateNumericDiffs(hem3pop, hem4pop, joinColumns, diffColumns)
            pop_diff.appendToFile(diff_df)

            #---------- Acute Breakdown -----------#
            hem3File = facid + "_acute_bkdn.xlsx"
            hem4File = facid + "_acute_bkdn.xlsx"
            diffFile = "diff_acute_bkdn.xlsx"
            joinColumns = [source_id, pollutant]
            diffColumns = [aconc_pop, aconc_all]
            #------------------------------------------#
            hem4bkdn = AcuteBreakdown(targetDir=self.hem4Dir, facilityId=None, model=None, plot_df=None,
                                      filenameOverride=hem4File)
            hem3bkdn = Hem3AcuteBreakdown(targetDir=self.hem3Dir, facilityId=None, model=None, plot_df=None,
                                          filenameOverride=hem3File)
            bkdn_diff = AcuteBreakdown(targetDir=self.diff_target, facilityId=None, model=None, plot_df=None,
                                       filenameOverride=diffFile)
            bkdn_diff.writeHeader()
            diff_df = self.calculateNumericDiffs(hem3bkdn, hem4bkdn, joinColumns, diffColumns)
            bkdn_diff.appendToFile(diff_df)

        #---------- All outer receptors -----------#
        hem3File = facid + "_all_outer_receptors.csv"
        hem4File = facid + "_all_outer_receptors.csv"
        diffFile = "diff_all_outer_receptors.csv"
        joinColumns = [fips, block, source_id, pollutant]
        diffColumns = [conc]
        if self.acute == 'Y':
            diffColumns.append(aconc)

        #------------------------------------------#
        hem4allouter = AllOuterReceptors(targetDir=self.hem4Dir, facilityId=None, model=None, plot_df=None,
                                         acuteyn=self.acute, filenameOverride=hem4File)
        hem3allouter = Hem3AllOuterReceptors(targetDir=self.hem3Dir, facilityId=None, model=None, plot_df=None,
                                             acuteyn=self.acute, filenameOverride=hem3File)
        allouter_diff = AllOuterReceptors(targetDir=self.diff_target, facilityId=None, model=None, plot_df=None,
                                          acuteyn=self.acute, filenameOverride=diffFile)
        allouter_diff.writeHeader()
        diff_df = self.calculateNumericDiffs(hem3allouter, hem4allouter, joinColumns, diffColumns)
        allouter_diff.appendToFile(diff_df)


    # Note: for this method to work correctly, none of the columns in diffColumns can be
    # present in joinColumns
    def calculateNumericDiffs(self, hem3_entity, hem4_entity, joinColumns, diffColumns):

        # Percent Change = ((HEM4- HEM3)/HEM3) * 100

        hem4_df = hem4_entity.createDataframe()
        hem3_df = hem3_entity.createDataframe()

        for col in joinColumns:
            if is_string_dtype(hem4_df[col]):
                hem4_df[col] = hem4_df[col].str.lower()
                hem3_df[col] = hem3_df[col].str.lower()

        merged_df = hem4_df.merge(hem3_df, on=joinColumns, suffixes=('', '_y'))
        for numericCol in diffColumns:

            merged_df[numericCol] = merged_df[numericCol].apply(self.scrub_zero)
            merged_df[numericCol+"_y"] = merged_df[numericCol+"_y"].apply(self.scrub_zero)

            hem3Value = merged_df[numericCol+"_y"]
            hem4Value = merged_df[numericCol]

            merged_df[numericCol] = 100*(hem4Value - hem3Value) / hem3Value
            merged_df[numericCol] = merged_df[numericCol].apply(self.round_to_sigfig, args=[3])

        merged_df.drop(list(merged_df.filter(regex='_y$')), axis=1, inplace=True)
        return merged_df

    # Change 0 to "very small" so we can compute a percentage change...
    def scrub_zero(self, x):
        if x == 0:
            return 0.000001
        else:
            return x

    def round_to_sigfig(self, x, sig=1):
        if x == 0:
            return 0

        if math.isnan(x):
            return float('NaN')

        rounded = round(x, sig-int(floor(log10(abs(x))))-1)
        return rounded

comparer = Hem3Comparer(hem3Dirname, hem4Dirname, acute, temporal, temporal_cols)
comparer.compare()
print("Done!")
