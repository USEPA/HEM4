import fnmatch
import os
import pandas as pd
from com.sca.hem4.writer.csv.AllInnerReceptors import AllInnerReceptors, block, fips, lat, lon, pollutant, overlap, \
    conc, fac_id
from com.sca.hem4.writer.csv.AllOuterReceptors import AllOuterReceptors
from com.sca.hem4.writer.csv.AllPolarReceptors import AllPolarReceptors
from com.sca.hem4.writer.excel.InputSelectionOptions import InputSelectionOptions

output_dir = "C:\\Users\\Chris Stolte\\IdeaProjects\\HEM4\\output\\CEN"
pollutant_name = "compounds"

class MaxConcentrationLocator():

    def __init__(self, hem4_output_dir, pollutant_name):
        self.output_dir = hem4_output_dir
        self.pollutant = pollutant_name.lower()

        self.basepath = os.path.basename(os.path.normpath(self.output_dir))
        files = os.listdir(self.output_dir)
        rootpath = self.output_dir + '/'
        self.facilityIds = [item for item in files if os.path.isdir(os.path.join(rootpath, item))
                             and 'inputs' not in item.lower() and 'acute maps' not in item.lower()]

    def locate(self):

        max_concentrations = pd.DataFrame()

        for facilityId in self.facilityIds:
            print("Inspecting facility folder " + facilityId + " for output files...")

            try:
                targetDir = self.output_dir + "/" + facilityId

                # Determine if this facility was run with acute or not
                inputops = InputSelectionOptions(targetDir=targetDir, facilityId=facilityId)
                inputops_df = inputops.createDataframe()
                acute_yn = inputops_df['acute_yn'].iloc[0]

                # Open the polar file and create df
                allpolar = AllPolarReceptors(targetDir=targetDir, facilityId=facilityId, acuteyn=acute_yn)
                allpolar_df = allpolar.createDataframe()

                allpolar_df = allpolar_df.loc[(allpolar_df[pollutant].str.contains("(?i)" + self.pollutant)) &
                                              (allpolar_df[overlap] == 'N')]

                print(str(len(allpolar_df)) + " polar rows.")
                polar_summed = allpolar_df.groupby(by=[lat, lon, pollutant], as_index=False) \
                    .sum().reset_index(drop=True)

                # Open the inner file and create df
                allinner = AllInnerReceptors(targetDir=targetDir, facilityId=facilityId, acuteyn=acute_yn)
                allinner_df = allinner.createDataframe()

                print(str(len(allinner_df)) + " inner rows before filter.")

                # Don't consider schools and monitors
                allinner_df = allinner_df.loc[(~allinner_df[block].str.contains('S')) &
                                              (~allinner_df[block].str.contains('M')) &
                                              (allinner_df[pollutant].str.contains("(?i)" + self.pollutant)) &
                                              (allinner_df[overlap] == 'N')]

                print(str(len(allinner_df)) + " inner rows after filter.")
                inner_summed = allinner_df.groupby(by=[fips, block, lat, lon, pollutant], as_index=False) \
                    .sum().reset_index(drop=True)

                # Open any/all outer files and create df
                allouter_combined = pd.DataFrame()
                listOuter = []
                listDirfiles = os.listdir(targetDir)
                pattern = "*_all_outer_receptors*.csv"
                for entry in listDirfiles:
                    if fnmatch.fnmatch(entry, pattern):
                        listOuter.append(entry)

                for f in listOuter:
                    allouter = AllOuterReceptors(targetDir=targetDir, acuteyn=acute_yn, filenameOverride=f)
                    allouter_df = allouter.createDataframe()

                    if not allouter_df.empty:

                        # Don't consider schools and monitors
                        allouter_df = allouter_df.loc[(~allouter_df[block].str.contains('S')) &
                                                      (~allouter_df[block].str.contains('M')) &
                                                      (allouter_df[pollutant].str.contains("(?i)" + self.pollutant)) &
                                                      (allouter_df[overlap] == 'N')]

                        allouter_combined = allouter_combined.append(allouter_df)

                print(str(len(allouter_combined)) + " outer rows.")
                outer_summed = allouter_combined.groupby(by=[fips, block, lat, lon, pollutant], as_index=False) \
                    .sum().reset_index(drop=True)

            except BaseException as e:
                print("Error gathering output information: " + repr(e))
                print("Skipping facility " + facilityId)
                continue

            # For each pollutant matched by the search term, find a max concentration among all three dfs and then
            # the overall max
            pollutants = list(polar_summed[pollutant].unique())
            for p in pollutants:
                polar_sliced = polar_summed.loc[polar_summed[pollutant] == p].reset_index()
                row = polar_sliced.loc[polar_sliced[conc].idxmax()]
                winning_row = row
                winning_row[fips] = None
                winning_row[block] = None
                winning_type = 'P'

                inner_sliced = inner_summed.loc[inner_summed[pollutant] == p].reset_index()
                row = inner_sliced.loc[inner_sliced[conc].idxmax()]
                if row[conc] > winning_row[conc]:
                    winning_row = row
                    winning_type = 'U' if row[block].str.contains('U') else 'C'

                outer_sliced = outer_summed.loc[outer_summed[pollutant] == p].reset_index()
                row = outer_sliced.loc[outer_sliced[conc].idxmax()]
                if row[conc] > winning_row[conc]:
                    winning_row = row
                    winning_type = 'U' if row[block].str.contains('U') else 'C'

                winning_row[fac_id] = facilityId
                winning_row['type'] = winning_type
                #print("Winning row for " + p + ": " + str(winning_row[lat]) + ", " + str(winning_row[lon]) + ", " + str(winning_row[conc]))
                max_concentrations = max_concentrations.append(winning_row)

        path = os.path.join(self.output_dir, 'max-conc-locations.xlsx')
        max_concentrations.to_excel(path, index=False,
            columns=[fac_id, pollutant, conc, lat, lon, fips, block, 'type'],
            header=['Facility ID', 'Pollutant', 'Max Concentration', 'Lat', 'Lon', 'FIPS', 'Block', 'Receptor Type'])


locator = MaxConcentrationLocator(output_dir, pollutant_name)
locator.locate()
