from com.sca.hem4.upload.PollutantCrosswalk import PollutantCrosswalk, pollutant_name, designation
from com.sca.hem4.writer.csv.AllInnerReceptorsNonCensus import AllInnerReceptorsNonCensus
from com.sca.hem4.writer.csv.AllOuterReceptorsNonCensus import AllOuterReceptorsNonCensus
from com.sca.hem4.writer.excel.FacilityMaxRiskandHI import FacilityMaxRiskandHI
from com.sca.hem4.writer.excel.MaximumIndividualRisksNonCensus import MaximumIndividualRisksNonCensus
from com.sca.hem4.writer.excel.RiskBreakdown import *
from com.sca.hem4.writer.excel.InputSelectionOptions import InputSelectionOptions
from com.sca.hem4.log.Logger import Logger

risk_contrib = 'risk_contrib'
category = 'category'
octant = 'octant'
centroid = 'centroid'
total_risk = 'totalrisk'
total_as_risk = 'total_as_risk'
total_pah_risk = 'total_pah_risk'
total_df_risk = 'total_df_risk'

class MultiPathwayNonCensus(ExcelWriter):

    def __init__(self, targetDir, facilityIds, parameters=None):
        self.name = "Multipathway Summary"
        self.categoryName = parameters[0]
        self.categoryFolder = targetDir
        self.facilityIds = facilityIds

        self.filename = os.path.join(targetDir, self.categoryName + "_multi_pathway.xlsx")
        self.haplib_df = DoseResponse().dataframe

        self.riskCache = {}

    def getHeader(self):
        return ['Run Group', 'Facility ID', 'Rural/Urban', 'Octant or MIR', 'Chem, Centroid, or Discrete',
                'Receptor ID', 'Lat',	'Lon', 'Population', 'Total Inhalation Cancer Risk',
                'Total Inhalation As Cancer Risk',	 'Total Inhalation PAH Cancer Risk',
                'Total Inhalation D/F Cancer Risk'
                ]

    def generateOutputs(self):
        Logger.log("Creating " + self.name + " report...", None, False)

        # The first step is to load the risk breakdown output for each facility so that we
        # can recover the risk for each pollutant.
        filename = self.categoryName + "_facility_max_risk_and_hi.xlsx"
        facilityMaxRiskAndHI = FacilityMaxRiskandHI(targetDir=self.categoryFolder, filenameOverride=filename)
        facilityMaxRiskAndHI_df = facilityMaxRiskAndHI.createDataframe()

        pollutantCrosswalk = PollutantCrosswalk(createDataframe=True)
        pollutantCrosswalk_df = pollutantCrosswalk.dataframe
        # Lowercase the pollutant name column
        pollutantCrosswalk_df[pollutant_name] = pollutantCrosswalk_df[pollutant_name].str.lower()

        pathways = []
        for facilityId in self.facilityIds:
            targetDir = self.categoryFolder + "/" + facilityId

            # Determine if this facility was run with acute or not
            inputops = InputSelectionOptions(targetDir=targetDir, facilityId=facilityId)
            inputops_df = inputops.createDataframe()
            acute_yn = inputops_df['acute_yn'].iloc[0]

            # Steps a-f in Steve's summary
            maxIndivRisks = MaximumIndividualRisksNonCensus(targetDir=targetDir, facilityId=facilityId)
            maxIndivRisks_df = maxIndivRisks.createDataframe()
            # Replace nan with empty string
            maxIndivRisks_df.replace('nan', '', regex=True, inplace=True)

            riskBkdn = RiskBreakdown(targetDir=targetDir, facilityId=facilityId)
            riskBkdn_df = riskBkdn.createDataframe()
            riskBkdn_df = riskBkdn_df.loc[(riskBkdn_df[site_type] == 'Max indiv risk') &
                                          (riskBkdn_df[parameter] == 'Cancer risk') &
                                          (riskBkdn_df[source_id].str.contains('Total')) &
                                          (~riskBkdn_df[pollutant].str.contains('All '))]
            # Lowercase the pollutant name column
            riskBkdn_df[pollutant] = riskBkdn_df[pollutant].str.lower()

            # keep all records but give default designation of 'POL' to pollutants which are not in crosswalk
            rbkdn_df = riskBkdn_df.merge(pollutantCrosswalk_df, left_on=[pollutant], right_on=[pollutant_name], how="left")
            rbkdn_df[designation] = rbkdn_df[designation].fillna('POL')

            rbkdn_df = rbkdn_df.groupby(designation).sum().reset_index()

            maxRiskAndHI_df = facilityMaxRiskAndHI_df.loc[facilityMaxRiskAndHI_df['Facil_id'] == facilityId]
            maxIndivRisks_df = maxIndivRisks_df.loc[maxIndivRisks_df[parameter] == 'Cancer risk']

            facilityMaxRiskAndHI_df.reset_index()
            maxIndivRisks_df.reset_index()

            asRow = rbkdn_df.loc[rbkdn_df[designation] == 'As']
            asRisk = 0 if asRow.empty else asRow.iloc[0][value]

            pahRow = rbkdn_df.loc[rbkdn_df[designation] == 'PAH']
            pahRisk = 0 if pahRow.empty else pahRow.iloc[0][value]

            dfRow = rbkdn_df.loc[rbkdn_df[designation] == 'DF']
            dfRisk = 0 if dfRow.empty else dfRow.iloc[0][value]

            pathway = [self.categoryName, facilityId, maxRiskAndHI_df.iloc[0][rural_urban], 'MIR', 'All HAP',
                       maxIndivRisks_df.iloc[0][rec_id],
                       maxIndivRisks_df.iloc[0][lat], maxIndivRisks_df.iloc[0][lon],
                       maxIndivRisks_df.iloc[0][population], maxRiskAndHI_df.iloc[0]['mx_can_rsk'],
                       asRisk, pahRisk, dfRisk]

            pathways.append(pathway)

            # Steps g-j
            allinner = AllInnerReceptorsNonCensus(targetDir=targetDir, facilityId=facilityId, acuteyn=acute_yn)
            allinner_df = allinner.createDataframe()

            # Only keep records that have non-zero population or represent non-overlapped user receptors
            allinner_df = allinner_df.loc[(allinner_df[population] > 0) & (allinner_df[overlap] == 'N')]

            # group by and sum by fips, block, population, lat, lon, pollutant
            allinner_df = allinner_df.groupby(by=[rec_id, population, lat, lon, pollutant], as_index=False) \
                .sum().reset_index(drop=True)

            # compute risk with immediate above result
            allinner_df['risk'] = allinner_df.apply(lambda x: self.calculateRisk(x[pollutant], x[conc]), axis=1)

            # keep all records but give default designation of 'POL' to pollutants which are not in crosswalk
            allinnermerged_df = allinner_df.merge(pollutantCrosswalk_df, left_on=[pollutant], right_on=[pollutant_name], how="left")
            allinnermerged_df[designation] = allinnermerged_df[designation].fillna('POL')

            # Aggregate concentration, grouped by FIPS/block
            inner_summed = allinnermerged_df.groupby(by=[rec_id, population, lat, lon, designation], as_index=False) \
                .sum().reset_index(drop=True)

            # Steps k-n
            allouter_summed = pd.DataFrame()
            listOuter = []
            listDirfiles = os.listdir(targetDir)
            pattern = "*_all_outer_receptors*.csv"
            for entry in listDirfiles:
                if fnmatch.fnmatch(entry, pattern):
                    listOuter.append(entry)

            anyOuters = "N"
            for f in listOuter:
                allouter = AllOuterReceptorsNonCensus(targetDir=targetDir, acuteyn=acute_yn, filenameOverride=f)
                allouter_df = allouter.createDataframe()

                if not allouter_df.empty:
                    
                    anyOuters = "Y"
                    
                    # Only keep records that have non-zero population or represent non-overlapped user receptors
                    allouter_df = allouter_df.loc[(allouter_df[population] > 0) & (allouter_df[overlap] == 'N')]
    
                    allouter_df = allouter_df.groupby(by=[rec_id, population, lat, lon, pollutant], as_index=False) \
                        .sum().reset_index(drop=True)
    
                    allouter_df['risk'] = allouter_df.apply(lambda x: self.calculateRisk(x[pollutant], x[conc]), axis=1)
    
                    # keep all records but give default designation of 'POL' to pollutants which are not in crosswalk
                    alloutermerged_df = allouter_df.merge(pollutantCrosswalk_df, left_on=[pollutant], right_on=[pollutant_name], how="left")
                    alloutermerged_df[designation] = alloutermerged_df[designation].fillna('POL')
    
                    outer_summed = alloutermerged_df.groupby(by=[rec_id, population, lat, lon, designation], as_index=False) \
                        .sum().reset_index(drop=True)
                    allouter_summed = allouter_summed.append(outer_summed)
            
            if anyOuters == "Y":
                riskblocks_df = inner_summed.append(allouter_summed)
            else:
                riskblocks_df = inner_summed

            # Steps o-r
            asRisksPathway = self.getRisksPathway('As', riskblocks_df, facilityId, maxRiskAndHI_df, maxIndivRisks_df)
            pahRisksPathway = self.getRisksPathway('PAH', riskblocks_df, facilityId, maxRiskAndHI_df, maxIndivRisks_df)
            dfRisksPathway = self.getRisksPathway('DF', riskblocks_df, facilityId, maxRiskAndHI_df, maxIndivRisks_df)

            pathways.append(asRisksPathway)
            pathways.append(pahRisksPathway)
            pathways.append(dfRisksPathway)

            # Steps s-w
            octants = {'E': [], 'N': [], 'NE': [], 'NW': [], 'S': [], 'SE': [], 'SW': [], 'W': []}
            facCenterLat = maxRiskAndHI_df.iloc[0]['fac_center_latitude']
            facCenterLon = maxRiskAndHI_df.iloc[0]['fac_center_longitude']
            for index, row in riskblocks_df.iterrows():
                bearingValue, distanceValue = self.bearingDistance(facCenterLat, facCenterLon, row[lat], row[lon])
                row[distance] = distanceValue

                if bearingValue > 337.5 or bearingValue <= 22.5:
                    octants['N'].append(row)
                elif bearingValue > 22.5 and bearingValue <= 67.5:
                    octants['NE'].append(row)
                elif bearingValue > 67.5 and bearingValue <= 112.5:
                    octants['E'].append(row)
                elif bearingValue > 112.5 and bearingValue <= 157.5:
                    octants['SE'].append(row)
                elif bearingValue > 157.5 and bearingValue <= 202.5:
                    octants['S'].append(row)
                elif bearingValue > 202.5 and bearingValue <= 247.5:
                    octants['SW'].append(row)
                elif bearingValue > 247.5 and bearingValue <= 292.5:
                    octants['W'].append(row)
                elif bearingValue > 292.5 and bearingValue <= 337.5:
                    octants['NW'].append(row)

            for key in octants.keys():
                minDistanceRow = None
                rows = octants[key]

                print("Octant " + key + " has " + str(len(rows)) + " rows.")

                for row in rows:
                    if minDistanceRow is None or row[distance] < minDistanceRow[distance]:
                        minDistanceRow = row

                if minDistanceRow is not None:
                    centroidPathway = self.getCentroidPathway(key, minDistanceRow[rec_id],
                                                              riskblocks_df, facilityId, maxRiskAndHI_df)

                    pathways.append(centroidPathway)

        pathways_df = pd.DataFrame(pathways, columns=[category, fac_id, rural_urban, octant, centroid, rec_id,
                                                      lat, lon, population, total_risk, total_as_risk, total_pah_risk,
                                                      total_df_risk])

        # Put final df into array
        self.dataframe = pathways_df
        self.data = self.dataframe.values
        yield self.dataframe

    def getRisksPathway(self, designationValue, riskblocks_df, facilityId, maxRiskAndHI_df, maxIndivRisks_df):
        onlyThis = riskblocks_df[riskblocks_df[designation] == designationValue]
        if onlyThis.empty:
            maxPathway = [self.categoryName, facilityId, maxRiskAndHI_df.iloc[0][rural_urban], 'MIR', designationValue,'','', '',
                          '', '', 0, 0, 0]
        else:
            maxRisk = onlyThis.loc[onlyThis['risk'].idxmax()].iloc[0]
            quartet_df = riskblocks_df[(riskblocks_df[rec_id] == maxRisk[rec_id])]

            asRiskValue = 0 if quartet_df.loc[quartet_df[designation] == 'As'].empty else \
                quartet_df.loc[quartet_df[designation] == 'As'].iloc[0]['risk']
            pahRiskValue = 0 if quartet_df.loc[quartet_df[designation] == 'PAH'].empty else \
                quartet_df.loc[quartet_df[designation] == 'PAH'].iloc[0]['risk']
            dfRiskValue = 0 if quartet_df.loc[quartet_df[designation] == 'DF'].empty else \
                quartet_df.loc[quartet_df[designation] == 'DF'].iloc[0]['risk']

            maxPathway = [self.categoryName, facilityId, maxRiskAndHI_df.iloc[0][rural_urban], 'MIR', designationValue,
                          maxIndivRisks_df.iloc[0][rec_id],
                          maxIndivRisks_df.iloc[0][lat], maxIndivRisks_df.iloc[0][lon],
                          maxIndivRisks_df.iloc[0][population], quartet_df['risk'].sum(),
                          asRiskValue, pahRiskValue, dfRiskValue]

        return maxPathway

    def getCentroidPathway(self, octant, recIdValue, riskblocks_df, facilityId, maxRiskAndHI_df):
        quartet_df = riskblocks_df[(riskblocks_df[rec_id] == recIdValue)]

        asRiskValue = 0 if quartet_df.loc[quartet_df[designation] == 'As'].empty else \
            quartet_df.loc[quartet_df[designation] == 'As'].iloc[0]['risk']
        pahRiskValue = 0 if quartet_df.loc[quartet_df[designation] == 'PAH'].empty else \
            quartet_df.loc[quartet_df[designation] == 'PAH'].iloc[0]['risk']
        dfRiskValue = 0 if quartet_df.loc[quartet_df[designation] == 'DF'].empty else \
            quartet_df.loc[quartet_df[designation] == 'DF'].iloc[0]['risk']

        populationValue = quartet_df.iloc[0][population]
        receptorType = 'Centroid' if populationValue > 0 else 'Discrete'
        maxPathway = [self.categoryName, facilityId, maxRiskAndHI_df.iloc[0][rural_urban], octant, receptorType,
                      recIdValue, quartet_df.iloc[0][lat], quartet_df.iloc[0][lon],
                      populationValue, quartet_df['risk'].sum(), asRiskValue, pahRiskValue, dfRiskValue]

        return maxPathway


    def calculateRisk(self, pollutant_name, conc):
        URE = self.getRiskParams(pollutant_name)
        return conc * URE

    def getRiskParams(self, pollutant_name):
        URE = 0.0

        # In order to get a case-insensitive exact match (i.e. matches exactly except for casing)
        # we are using a regex that is specified to be the entire value. Since pollutant names can
        # contain parentheses, escape them before constructing the pattern.
        pattern = '^' + re.escape(pollutant_name) + '$'

        # Since it's relatively expensive to get these values from their respective libraries, cache them locally.
        # Note that they are cached as a pair (i.e. if one is in there, the other one will be too...)
        if pollutant_name in self.riskCache:
            URE = self.riskCache[pollutant_name][ure]
        else:
            row = self.haplib_df.loc[
                self.haplib_df[pollutant].str.contains(pattern, case=False, regex=True)]

            if row.size == 0:
                URE = 0.0
            else:
                URE = row.iloc[0][ure]

            self.riskCache[pollutant_name] = {ure : URE}

        return URE

    def bearingDistance(self, lat1, lon1, lat2, lon2):

        lat1_rad = math.radians(lat1)
        lon1_rad = math.radians(lon1)
        lat2_rad = math.radians(lat2)
        lon2_rad = math.radians(lon2)
        dLon = (lon2_rad - lon1_rad)
        dLat = (lat2_rad - lat1_rad)

        # Bearing (angle)
        y = math.sin(dLon) * math.cos(lat2_rad)
        x = math.cos(lat1_rad) * math.sin(lat2_rad) - math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(dLon)
        brng = math.atan2(y, x)
        brng = math.degrees(brng)
        brng = (brng + 360) % 360

        # Distance (m), uses Haversine formula
        R = 6371000 # Earth radius in meters

        a = math.sin(dLat/2)**2 + \
            math.cos(lat1_rad)*math.cos(lat2_rad)*math.sin(dLon/2)**2
        distance = 2*R*math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return brng, distance
