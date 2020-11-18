import os, fnmatch
import pandas as pd

from com.sca.hem4.writer.csv.AllOuterReceptors import AllOuterReceptors
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter
from com.sca.hem4.upload.HAPEmissions import *
from com.sca.hem4.upload.FacilityList import *
from com.sca.hem4.upload.DoseResponse import *
from com.sca.hem4.upload.UserReceptors import *
from com.sca.hem4.model.Model import *
from com.sca.hem4.support.UTM import *
from com.sca.hem4.FacilityPrep import *
from com.sca.hem4.writer.csv.AllInnerReceptors import *

notes = 'notes';
aconc_sci = 'aconc_sci';


class Hem3AcuteChemicalMax(ExcelWriter, InputFile):
    """
    Provides the maximum acute concentration for each modeled pollutant occurring anywhere offsite, whether at a
    populated (census block or user-defined) receptor or an unpopulated (polar grid) receptor, the acute benchmarks
    associated with each pollutant, and other max receptor information.
    """

    def __init__(self, targetDir=None, facilityId=None, model=None, plot_df=None, filenameOverride=None,
                 createDataframe=False):
        # Initialization for file reading/writing. If no file name override, use the
        # default construction.
        filename = facilityId + "_acute_chem_max.xlsx" if filenameOverride is None else filenameOverride
        path = os.path.join(targetDir, filename)

        ExcelWriter.__init__(self, model, plot_df)
        InputFile.__init__(self, path, createDataframe)

        self.filename = path
        self.targetDir = targetDir

    def getHeader(self):
        return ['Pollutant', 'Conc (µg/m3)', 'Conc sci (µg/m3)', 'Aegl_1 1hr (mg/m3)', 'Aegl_1 8hr (mg/m3)',
                'Aegl_2 1hr (mg/m3)', 'Aegl_2 8hr (mg/m3)', 'Erpg_1 (mg/m3)', 'Erpg_2 (mg/m3)', 'Idlh_10 (mg/m3)',
                'Mrl (mg/m3)', 'Rel (mg/m3)', 'Teel_0 (mg/m3)', 'Teel_1 (mg/m3)', 'Population',
                'Distance (in meters)', 'Angle (from north)', 'Elevation (in meters)', 'Hill Height (in meters)',
                'Fips', 'Block', 'Utm easting', 'Utm northing', 'Latitude', 'Longitude', 'Receptor type', 'Notes']

    def getColumns(self):
        return [pollutant, aconc, aconc_sci, aegl_1_1h,aegl_1_8h,aegl_2_1h,aegl_2_8h,erpg_1,erpg_2,
                mrl,rel,idlh_10,teel_0,teel_1, population, distance, angle, elev, hill, fips, block,
                utme, utmn, lat, lon, rec_type, notes]

    def generateOutputs(self):

        # Set-up a dataframe to hold the running max conc for each pollutant along with location of the receptor
        pols = self.model.runstream_hapemis[pollutant].str.lower().unique().tolist()
        cols = [aconc, lon, lat, notes]
        fillval = [0, 0, 0, '']
        filler = [fillval for p in pols]
        maxconc_df = pd.DataFrame(data=filler, index=pols, columns=cols)

        # dataframe of pollutants with their acute benchmarks
        polinfo_cols = [pollutant, aegl_1_1h, aegl_1_8h, aegl_2_1h, aegl_2_8h, erpg_1, erpg_2,
                        mrl, rel, idlh_10, teel_0, teel_1]
        polinfo = self.model.haplib.dataframe[polinfo_cols][
            self.model.haplib.dataframe[pollutant].str.lower().isin([x.lower() for x in pols])]
        polinfo[pollutant] = polinfo.apply(lambda x: x[pollutant].lower(), axis=1)
        polinfo.set_index([pollutant], inplace=True, drop=False)

        # 1) First search the polar receptors for the max acute conc per pollutant

        polarconcs = self.model.all_polar_receptors_df.copy()
        # Sum acute conc to unique lat/lons
        aggs = {pollutant:'first', lat:'first', lon:'first', aconc:'sum'}
        newcolumns = [pollutant, lat, lon, aconc]
        polarsum = polarconcs.groupby([pollutant, lat, lon]).agg(aggs)[newcolumns]

        # loop over each pollutant and find the polar receptor with the max acute conc
        for x in pols:
            max_idx = polarsum[polarsum[pollutant].str.lower() == x][aconc].idxmax()
            maxconc_df.loc[x, aconc] = polarsum[aconc].loc[max_idx]
            maxconc_df.loc[x, lon] = polarsum[lon].loc[max_idx]
            maxconc_df.loc[x, lat] = polarsum[lat].loc[max_idx]
            maxconc_df.loc[x, notes] = 'Polar'


        # 2) Next search the discrete (inner) receptors for the max acute conc per pollutant.

        if self.model.all_inner_receptors_df.empty == False:
            innconcs = self.model.all_inner_receptors_df.copy()
            # Sum acute conc to unique lat/lons
            innsum = innconcs.groupby([pollutant, lat, lon]).agg(aggs)[newcolumns]

            # loop over each pollutant and find the discrete receptor with the max acute conc
            #    If inner acute conc is larger than stored value, replace stored value.
            for p in pols:
                max_idx = innsum[innsum[pollutant].str.lower() == p][aconc].idxmax()
                if innsum[aconc].loc[max_idx] > maxconc_df[aconc].loc[p]:
                    maxconc_df.loc[p, aconc] = innsum[aconc].loc[max_idx]
                    maxconc_df.loc[p, lon] = innsum[lon].loc[max_idx]
                    maxconc_df.loc[p, lat] = innsum[lat].loc[max_idx]
                    maxconc_df.loc[p, notes] = 'Discrete'

        # 3) Finally, search the outer receptors

        outercolumns = [fips, block, lat, lon, source_id, emis_type, pollutant, conc,
                        aconc, elev, population, overlap]

        # Get a list of the all_outer_receptor files (could be more than one)
        listOuter = []
        listDirfiles = os.listdir(self.targetDir)
        pattern = "*_all_outer_receptors*.csv"
        for entry in listDirfiles:
            if fnmatch.fnmatch(entry, pattern):
                listOuter.append(entry)

        # Loop over each pollutant and outer receptor file and see if max outer acute conc
        # is larger than the stored value
        for p in pols:
            for f in listOuter:
                allouter = AllOuterReceptors(targetDir=self.targetDir, filenameOverride=f)
                outconcs = allouter.createDataframe()

                # Sum acute conc to unique lat/lons
                outsum = outconcs.groupby([pollutant, lat, lon]).agg(aggs)[newcolumns]
                max_idx = outsum[outsum[pollutant].str.lower() == p][aconc].idxmax()
                if outsum[aconc].loc[max_idx] > maxconc_df[aconc].loc[p]:
                    maxconc_df.loc[p, aconc] = outsum[aconc].loc[max_idx]
                    maxconc_df.loc[p, lon] = outsum[lon].loc[max_idx]
                    maxconc_df.loc[p, lat] = outsum[lat].loc[max_idx]
                    maxconc_df.loc[p, notes] = 'Interpolated'

        # 4) Build output dataframe
        acute_df = polinfo.join(maxconc_df, how='inner')
        acute_df[aconc_sci] = acute_df.apply(lambda x: format(x[aconc], ".1e"), axis=1)
        acute_df[elev] = 0
        acute_df[hill] = 0
        acute_df[distance] = 0
        acute_df[angle] = 0
        acute_df[population] = 0
        acute_df[utme] = 0
        acute_df[utmn] = 0

        for index, row in acute_df.iterrows():
            if row[notes] == 'Polar':
                acute_df.at[index,elev] = self.model.polargrid.loc[(self.model.polargrid[lon] == row[lon]) &
                                                                   (self.model.polargrid[lat] == row[lat]), elev].values[0]
                acute_df.at[index,hill] = self.model.polargrid.loc[(self.model.polargrid[lon] == row[lon]) &
                                                                   (self.model.polargrid[lat] == row[lat]), hill].values[0]
                acute_df.at[index,population] = 0
                acute_df.at[index,distance] = self.model.polargrid.loc[(self.model.polargrid[lon] == row[lon]) &
                                                                       (self.model.polargrid[lat] == row[lat]), distance].values[0]
                acute_df.at[index,angle] = self.model.polargrid.loc[(self.model.polargrid[lon] == row[lon]) &
                                                                    (self.model.polargrid[lat] == row[lat]), angle].values[0]
                acute_df.at[index,utmn] = self.model.polargrid.loc[(self.model.polargrid[lon] == row[lon]) &
                                                                   (self.model.polargrid[lon] == row[lon]), utmn].values[0]
                acute_df.at[index,utme] = self.model.polargrid.loc[(self.model.polargrid[lon] == row[lon]) &
                                                                   (self.model.polargrid[lat] == row[lat]), utme].values[0]
                acute_df.at[index,fips] = 'na'
                acute_df.at[index,block] = 'na'
                acute_df.at[index,rec_type] = self.model.polargrid.loc[(self.model.polargrid[lon] == row[lon]) &
                                                                       (self.model.polargrid[lat] == row[lat]), rec_type].values[0]
            elif row[notes] == 'Discrete':
                acute_df.at[index,elev] = self.model.innerblks_df.loc[(self.model.innerblks_df[lon] == row[lon]) &
                                                                      (self.model.innerblks_df[lat] == row[lat]), elev].values[0]
                acute_df.at[index,hill] = self.model.innerblks_df.loc[(self.model.innerblks_df[lon] == row[lon]) &
                                                                      (self.model.innerblks_df[lat] == row[lat]), hill].values[0]
                acute_df.at[index,population] = self.model.innerblks_df.loc[(self.model.innerblks_df[lon] == row[lon]) &
                                                                            (self.model.innerblks_df[lat] == row[lat]), population].values[0]
                acute_df.at[index,distance] = self.model.innerblks_df.loc[(self.model.innerblks_df[lon] == row[lon]) &
                                                                          (self.model.innerblks_df[lat] == row[lat]), distance].values[0]
                acute_df.at[index,angle] = self.model.innerblks_df.loc[(self.model.innerblks_df[lon] == row[lon]) &
                                                                       (self.model.innerblks_df[lat] == row[lat]), angle].values[0]
                acute_df.at[index,utmn] = self.model.innerblks_df.loc[(self.model.innerblks_df[lon] == row[lon]) &
                                                                      (self.model.innerblks_df[lon] == row[lon]), utmn].values[0]
                acute_df.at[index,utme] = self.model.innerblks_df.loc[(self.model.innerblks_df[lon] == row[lon]) &
                                                                      (self.model.innerblks_df[lat] == row[lat]), lat].values[0]
                acute_df.at[index,fips] = self.model.innerblks_df.loc[(self.model.innerblks_df[lon] == row[lon]) &
                                                                      (self.model.innerblks_df[lat] == row[lat]), fips].values[0]
                acute_df.at[index,block] = self.model.innerblks_df[(self.model.innerblks_df[lon] == row[lon]) &
                                                                   (self.model.innerblks_df[lat] == row[lat])][idmarplot].values[0][-10:]
                acute_df.at[index,rec_type] = self.model.innerblks_df.loc[(self.model.innerblks_df[lon] == row[lon]) &
                                                                          (self.model.innerblks_df[lat] == row[lat]), rec_type].values[0]
            else:
                acute_df.at[index,elev] = self.model.outerblks_df.loc[(self.model.outerblks_df[lon] == row[lon]) &
                                                                      (self.model.outerblks_df[lat] == row[lat]), elev].values[0]
                acute_df.at[index,hill] = self.model.outerblks_df.loc[(self.model.outerblks_df[lon] == row[lon]) &
                                                                      (self.model.outerblks_df[lat] == row[lat]), hill].values[0]
                acute_df.at[index,population] = self.model.outerblks_df.loc[(self.model.outerblks_df[lon] == row[lon]) &
                                                                            (self.model.outerblks_df[lat] == row[lat]), population].values[0]
                acute_df.at[index,distance] = self.model.outerblks_df.loc[(self.model.outerblks_df[lon] == row[lon]) &
                                                                          (self.model.outerblks_df[lat] == row[lat]), distance].values[0]
                acute_df.at[index,angle] = self.model.outerblks_df.loc[(self.model.outerblks_df[lon] == row[lon]) &
                                                                       (self.model.outerblks_df[lat] == row[lat]), angle].values[0]
                acute_df.at[index,utmn] = self.model.outerblks_df.loc[(self.model.outerblks_df[lon] == row[lon]) &
                                                                      (self.model.outerblks_df[lon] == row[lon]), utmn].values[0]
                acute_df.at[index,utme] = self.model.outerblks_df.loc[(self.model.outerblks_df[lon] == row[lon]) &
                                                                      (self.model.outerblks_df[lat] == row[lat]), lat].values[0]
                acute_df.at[index,fips] = self.model.outerblks_df.loc[(self.model.outerblks_df[lon] == row[lon]) &
                                                                      (self.model.outerblks_df[lat] == row[lat]), fips].values[0]
                acute_df.at[index,block] = self.model.outerblks_df[(self.model.outerblks_df[lon] == row[lon]) &
                                                                   (self.model.outerblks_df[lat] == row[lat])][idmarplot].values[0][-10:]
                acute_df.at[index,rec_type] = self.model.outerblks_df.loc[(self.model.outerblks_df[lon] == row[lon]) &
                                                                          (self.model.outerblks_df[lat] == row[lat]), rec_type].values[0]

        cols = [pollutant, aconc, aconc_sci, aegl_1_1h,aegl_1_8h,aegl_2_1h,aegl_2_8h,erpg_1,erpg_2,
                mrl,rel,idlh_10,teel_0,teel_1, population, distance, angle, elev, hill, fips, block,
                utme, utmn, lat, lon, rec_type, notes]
        acute_df = acute_df[cols]

        self.dataframe = acute_df
        self.data = self.dataframe.values
        yield self.dataframe

    def createDataframe(self):
        # Type setting for XLS reading
        self.numericColumns = [aconc, aconc_sci, aegl_1_1h,aegl_1_8h,aegl_2_1h,aegl_2_8h,erpg_1,erpg_2,mrl,rel,idlh_10,
                               teel_0,teel_1, population, distance, angle, elev, hill, utme, utmn, lat, lon]
        self.strColumns = [pollutant, fips, block, rec_type, notes]

        df = self.readFromPath(self.getColumns())
        return df.fillna("")