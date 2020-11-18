from math import floor, log10

from com.sca.hem4.runner.FacilityRunner import FacilityRunner
from com.sca.hem4.writer.csv.AllOuterReceptors import *
from com.sca.hem4.writer.csv.AllPolarReceptors import AllPolarReceptors
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter

facilityID = "Fac1-NC"
sourceID = "RW000001"
pollutantName = "Acetaldehyde"
rundir = r"C:/Users/ccook/Documents/HEM4_git_repository/HEM4/output/GUIv3_DepDeplComparision_Test8"
hapemis_path = rundir + "/Inputs/hapemis.xlsx"
output_dir = rundir + "/" + facilityID
acute = 'Y'

# Runtype: 0 == no deposition, 1 == both, 2 == dry only, 3 == wet only...see FacilityRunner#set_runtype()
runtype = 1

# emistype: P == particle, V == gaseous
emistype = 'V'
# deptype: D == dry, W == wet
deptype = 'W'

class ConcentrationComparer(ExcelWriter):

    def __init__(self, fac_id, source_id, pollutant, hapemisPath, output_dir):

        filename = fac_id + "_dep_compared.xlsx"
        path = os.path.join(output_dir, filename)

        ExcelWriter.__init__(self, None, None)

        self.filename = path
        self.targetDir = output_dir
        self.fac_id = fac_id
        self.source_id = source_id
        self.pollutant = pollutant
        self.hapemisPath = hapemisPath
        self.output_dir = output_dir

        if not (os.path.exists(self.hapemisPath) or
                (os.path.exists(self.output_dir) and os.path.isdir(self.output_dir))):
            raise Exception("Please specify valid HAP emission and output directory paths.")

    def getHeader(self):

        return ['Facility ID', 'Source ID', 'Pollutant', 'Receptor Type',
                'Lat', 'Lon', 'UTME', 'UTMN', 'Retrieved Dep', 'Computed Dep', '% Change']

    def getColumns(self):
        return [fac_id, source_id, pollutant, rec_type, lat, lon, utme, utmn, 'r_dep', 'c_dep', 'change']

    def generateOutputs(self):
        rows = []

        # HAP emis...load file and filter by fac id, source id, and pollutant
        haplib = DoseResponse()
        hapemis = HAPEmissions(hapemis_path, haplib, {self.fac_id})
        hapemis_df = hapemis.dataframe
        hapemis_df = hapemis_df.loc[(hapemis_df[fac_id] == self.fac_id) & (hapemis_df[source_id] == self.source_id) &
                                    (hapemis_df[pollutant].str.lower() == self.pollutant.lower())]

        # Aermod plotfile
        plotfile_name = "plotfile_p" if emistype == 'P' else "plotfile_v"
        plotfile_name += ".plt"

        facilityRunner = FacilityRunner(self.fac_id, None, False)
        ppfile = open(self.output_dir + "/" + plotfile_name, "r")
        plot_df = facilityRunner.readplotf(ppfile, runtype)
        plot_df = plot_df.loc[plot_df[source_id] == self.source_id]

        plotcolumn = wdp if deptype == 'W' else ddp
        column = wetdep if deptype == 'W' else drydep

        # multiply by the hapemis value and the conversion factor
        plot_df[plotcolumn] = plot_df[plotcolumn].multiply(hapemis_df.iloc[0][emis_tpy])
        plot_df[plotcolumn] = plot_df[plotcolumn].multiply(2000*0.4536/3600/8760)
        plot_df[plotcolumn] = plot_df[plotcolumn].multiply(hapemis_df.iloc[0][part_frac]) if emistype == 'P' else \
                        plot_df[plotcolumn].multiply(1 - hapemis_df.iloc[0][part_frac])

        # Compare plot values to polar dep values produced by HEM4
        plot_polar_df = plot_df.loc[plot_df[net_id] == 'POLGRID1']
        allpolar = AllPolarReceptors(targetDir=self.output_dir, facilityId=self.fac_id, model=None, plot_df=plot_df,
                                     acuteyn=acute)
        allpolar_df = allpolar.createDataframe()
        allpolar_df = allpolar_df.loc[(allpolar_df[source_id] == self.source_id) 
                                      & (allpolar_df[pollutant].str.lower() == self.pollutant.lower())
                                      & (allpolar_df[emis_type] == emistype)]
        for index,row in allpolar_df.iterrows():

            utm_n, utm_e, zone, hemi, epsg = UTM.ll2utm(row[lat], row[lon])
            plot_row = plot_polar_df.loc[(plot_polar_df[utmn] == utm_n) & (plot_polar_df[utme] == utm_e)]
            plot_row = plot_row.iloc[0]

            computed = self.round_to_sigfig(plot_row[plotcolumn], 8)
            retrieved = self.round_to_sigfig(row[column], 8)
            diff = self.round_to_sigfig(100*((retrieved - computed) / retrieved), 3)
            record = {fac_id:self.fac_id, source_id:self.source_id, pollutant:self.pollutant, rec_type:"polar",
                      lat:row[lat], lon:row[lon], utme:utm_e, utmn:utm_n, 'r_dep':retrieved, 'c_dep':computed,
                      'change':diff}
            rows.append(record)

        # Compare plot values to inner concentrations produced by HEM4
        plot_inner_df = plot_df.loc[plot_df[net_id] != 'POLGRID1']
        allinner = AllInnerReceptors(targetDir=self.output_dir, facilityId=self.fac_id, model=None, plot_df=plot_df,
                                     acuteyn=acute)
        allinner_df = allinner.createDataframe()
        allinner_df = allinner_df.loc[(allinner_df[source_id] == self.source_id) 
                                      & (allinner_df[pollutant].str.lower() == self.pollutant.lower())
                                      & (allinner_df[emis_type] == emistype)]
        for index,row in allinner_df.iterrows():

            utm_n, utm_e, zone, hemi, epsg = UTM.ll2utm(row[lat], row[lon])
            plot_row = plot_inner_df.loc[(plot_inner_df[utmn] == utm_n) & (plot_inner_df[utme] == utm_e)]
            plot_row = plot_row.iloc[0]

            computed = self.round_to_sigfig(plot_row[plotcolumn], 8)
            retrieved = self.round_to_sigfig(row[column], 8)
            diff = self.round_to_sigfig(100*((retrieved - computed) / retrieved), 3)
            record = {fac_id:self.fac_id, source_id:self.source_id, pollutant:self.pollutant, rec_type:"block",
                      lat:row[lat], lon:row[lon], utme:utm_e, utmn:utm_n, 'r_dep':retrieved, 'c_dep':computed,
                      'change':diff}
            rows.append(record)

        # Return results
        self.dataframe = pd.DataFrame(rows, columns=self.getColumns())
        self.data = self.dataframe.values
        yield self.dataframe

    def round_to_sigfig(self, x, sig=1):
        if x == 0:
            return 0;

        if math.isnan(x):
            return float('NaN')

        rounded = round(x, sig-int(floor(log10(abs(x))))-1)
        return rounded

comparer = ConcentrationComparer(facilityID, sourceID, pollutantName, hapemis_path, output_dir)
comparer.write()
