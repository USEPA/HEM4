import fnmatch
from math import log10, floor
from com.sca.hem4.upload.FacilityList import FacilityList
from com.sca.hem4.writer.csv.AllInnerReceptorsNonCensus import AllInnerReceptorsNonCensus
from com.sca.hem4.writer.csv.AllOuterReceptorsNonCensus import AllOuterReceptorsNonCensus
from com.sca.hem4.writer.csv.BlockSummaryChronic import *
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter
from com.sca.hem4.FacilityPrep import *
from com.sca.hem4.writer.excel.summary.AltRecAwareSummary import AltRecAwareSummary
from collections import OrderedDict
import numpy as np


class SourceTypeRiskHistogram(ExcelWriter, AltRecAwareSummary):

    def __init__(self, targetDir, facilityIds, parameters=None):
        self.name = "Source Type Risk Histogram"
        self.categoryName = parameters[0]
        self.categoryFolder = targetDir
        self.facilityIds = facilityIds

        # Parameters specify which part of the source id contains the code
        self.codePosition = parameters[1][0]
        self.codeLength = parameters[1][1]
        self.sourceTypes = None
        self.header = None
        self.haplib_df = DoseResponse().dataframe
        self.filename = os.path.join(targetDir, self.categoryName + "_source_type_risk.xlsx")

        self.riskCache = {}

    def getHeader(self):
        return [' ']

    def generateOutputs(self):
        Logger.log("Creating " + self.name + " report...", None, False)
                
        # Read the facility list options input to know which facilities were run with acute
        faclistFile = os.path.join(self.categoryFolder, "inputs/faclist.xlsx")
        cols = [fac_id,met_station,rural_urban,urban_pop,max_dist,model_dist,radial,circles,overlap_dist, ring1,
                fac_center,ring_distances, acute,
                hours,multiplier,hivalu,dep,depl,pdep,pdepl,vdep,vdepl,elev,
                user_rcpt,bldg_dw,fastall,emis_var,annual,period_start,period_end]
        faclist = pd.read_excel(faclistFile, skiprows=1, names=cols, dtype=str)
        faclist[acute] = faclist[acute].replace('nan', 'N')

        # Create a list to hold the values for each bucket
        maximum = []
        hundo = ['>= 100 in 1 million']
        ten = ['>= 10 in 1 million']
        one = ['>= 1 in 1 million']
        incidences = ['Incidence']

        codes = {}
        codes['overall'] = [0, 0, 0, 0, 0]

        # Initialize overall block risk DF (sector level)
        sector_blkrisk = pd.DataFrame()
        
        # Initialize set of source id's
        allsrcids = set()
        
        
        for facilityId in self.facilityIds:
                        
            targetDir = self.categoryFolder + "/" + facilityId

            altrec = self.determineAltRec(self.categoryFolder)

            acute_yn = faclist[faclist['fac_id']==facilityId]['acute'].iloc[0]
            allinner = AllInnerReceptorsNonCensus(targetDir=targetDir, facilityId=facilityId, acuteyn=acute_yn) if altrec=='Y' else \
                AllInnerReceptors(targetDir=targetDir, facilityId=facilityId, acuteyn=acute_yn)
            allinner_df = allinner.createDataframe()

            # Unique list of source ids from this facility
            uniqsrcs = set(allinner_df[source_id])
            allsrcids.update(uniqsrcs)
            
            # Merge ure column
            allinner2_df = pd.merge(allinner_df, self.haplib_df[['pollutant', 'ure']],
                                how='left', on='pollutant')
            
            allinner2_df['risk'] = allinner2_df[conc] * allinner2_df['ure']

#            allinner_df['risk'] = allinner_df.apply(lambda x: self.calculateRisk(x[pollutant], x[conc]), axis=1)

            # convert source ids to the code part only, and then group and sum
            allinner2_df[source_id] = allinner2_df[source_id].apply(lambda x: x[self.codePosition:self.codePosition+self.codeLength])
                         
            # Aggregate risk, grouped by FIPS/block (or receptor id if we're using alternates) and source
            aggs = {lat:'first', lon:'first', population:'first', 'risk':'sum'}                
            byCols = [rec_id, source_id] if altrec=='Y' else [fips, block, source_id]
            inner_summed = allinner2_df.groupby(by=byCols, as_index=False).agg(aggs).reset_index(drop=True)
            
            # Drop records that (are not user receptors AND have population = 0)       
            if altrec == 'N':
                inner_summed.drop(inner_summed[(inner_summed.population == 0) &
                                               (~inner_summed.block.str.contains('U', case=False))].index,
                                               inplace=True)
            else:
                inner_summed.drop(inner_summed[(inner_summed.population == 0) & 
                                               (~inner_summed.rec_id.str.contains('U_', case=False))].index,
                                               inplace=True)
                        
            # Append to sector block risk DF
            sector_blkrisk = sector_blkrisk.append(inner_summed)

            # Aggregate risk by block and source
            sector_summed = sector_blkrisk.groupby(by=byCols, as_index=False).agg(aggs).reset_index(drop=True)


            # Get a list of the all_outer_receptor files (could be more than one)
            listOuter = []
            listDirfiles = os.listdir(targetDir)
            pattern = "*_all_outer_receptors*.csv"
            for entry in listDirfiles:
                if fnmatch.fnmatch(entry, pattern):
                    listOuter.append(entry)

            for f in listOuter:

                allouter = AllOuterReceptorsNonCensus(targetDir=targetDir, acuteyn=acute_yn, filenameOverride=f) if altrec=='Y' else \
                    AllOuterReceptors(targetDir=targetDir, acuteyn=acute_yn, filenameOverride=f)
                allouter_df = allouter.createDataframe()
                
                if not allouter_df.empty:
                    
                    # Merge ure column
                    allouter2_df = pd.merge(allouter_df, self.haplib_df[['pollutant', 'ure']],
                                        how='left', on='pollutant')
                    
                    allouter2_df['risk'] = allouter2_df[conc] * allouter2_df['ure']
    
                    # convert source ids to the code part only, and then group and sum
                    allouter2_df[source_id] = allouter2_df[source_id].apply(lambda x: x[self.codePosition:self.codePosition+self.codeLength])
        
                    # Aggregate risk, grouped by FIPS/block (or receptor id if we're using alternates) and source
                    aggs = {lat:'first', lon:'first', population:'first', 'risk':'sum'}                
                    byCols = [rec_id, source_id] if altrec=='Y' else [fips, block, source_id]
                    outer_summed = allouter2_df.groupby(by=byCols, as_index=False).agg(aggs).reset_index(drop=True)
    
                    # Drop records that (are not user receptors AND have population = 0)
                    if altrec == 'N':
                        outer_summed.drop(outer_summed[(outer_summed.population == 0) & ("U" not in outer_summed.block)].index,
                                         inplace=True)
                    else:
                        outer_summed.drop(outer_summed[(outer_summed.population == 0) & ("U" not in outer_summed.rec_id)].index,
                                         inplace=True)
    
                    # Append to sector block risk DF
                    sector_blkrisk = sector_blkrisk.append(outer_summed)
    
                    # Aggregate risk by block and source
                    sector_summed = sector_blkrisk.groupby(by=byCols, as_index=False).agg(aggs).reset_index(drop=True)
                          
        # Aggregate sector source risk to just block (or rec_id)
        aggs = {lat:'first', lon:'first', population:'first', 'risk':'sum'}                
        byCols = [rec_id] if altrec=='Y' else [fips, block]
        sectortot_summed = sector_summed.groupby(by=byCols, as_index=False).agg(aggs).reset_index(drop=True)
                   
        # Round sector risk to 1 sig fig
        sector_summed['risk'] = sector_summed['risk'].apply(lambda x: self.round_to_sigfig(x, 1))
        sectortot_summed['risk'] = sectortot_summed['risk'].apply(lambda x: self.round_to_sigfig(x, 1))


        # Get population counts per risk level and source type         
        for index, row in sector_summed.iterrows():
            code = row[source_id]
            risk = row['risk']
            pop = row[population]

            if not code in codes:
                codes[code] = [0, 0, 0, 0, 0]

            # Update the code for this source
            codelist = codes[code]
            if risk > codelist[0]:
                codelist[0] = risk
            if risk >= 0.0001:
                codelist[1] += pop
            if risk >= 0.00001:
                codelist[2] += pop
            if risk >= 0.000001:
                codelist[3] += pop
            codelist[4] += (risk * pop) / 70

        
        # Get overall population counts per risk level
        for index, row in sectortot_summed.iterrows():
            risk = row['risk']
            pop = row[population]

            # Update the 'overall' code
            codelist = codes['overall']
            if risk > codelist[0]:
                codelist[0] = risk
            if risk >= 0.0001:
                codelist[1] += pop
            if risk >= 0.00001:
                codelist[2] += pop
            if risk >= 0.000001:
                codelist[3] += pop
            codelist[4] += (risk * pop) / 70

        # Maximum MIR for the entire sector
        self.sector_mir = round(sectortot_summed['risk'].max() * 1000000, 3)

        # Prepend the header, sorting by maximums...
        header = ['', 'Maximum Overall']
        
        # Get a list of all source types
        sourceTypes = [id[self.codePosition:self.codePosition+self.codeLength] for id in allsrcids]
        self.sourceTypes = list(set(sourceTypes))

        # Get maximum values only on the first pass...these will be used to sort the source types
        for code in self.sourceTypes:
            codelist = codes[code]
            maximum.append(self.round_to_sigfig(codelist[0]*1000000))

        # Re-sort source types based on maximum values (decending) and then compile values again.
        maximum, self.sourceTypes = (list(t) for t in zip(*sorted( zip(maximum, self.sourceTypes), reverse=True )))
        maximum.insert(0, 'Maximum (in 1 million)')
        # The max value is the maximum MIR of the entire sector
        maximum.insert(1, self.sector_mir)

        header = ['', 'Maximum Overall']
        header.extend(self.sourceTypes)

        # Finally, re-create the histogram using the sorted version of the source types!
        self.sourceTypes.insert(0, 'overall')
        for code in self.sourceTypes:
            codelist = codes[code]
            hundo.append(codelist[1])
            ten.append(codelist[2])
            one.append(codelist[3])
            incidences.append(self.round_to_sigfig(codelist[4]))

        allvalues = [['Cancer Risk'], maximum, ['Number of people'], hundo, ten, one, [''], incidences]
        allvalues.insert(0, header)

        histogram_df = pd.DataFrame(allvalues, columns=self.header).astype(dtype=int, errors='ignore')

        # Put final df into array
        self.dataframe = histogram_df
        self.data = self.dataframe.values
        yield self.dataframe

    def calculateRisk(self, pollutant_name, conc):
        risk = []
        for i in range(len(pollutant_name)):
            URE = self.getRiskParams(pollutant_name[i])
            risk.append(conc[i] * URE)
        return risk
    
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

    # Override the default write() method in order to add bottom section of report
    def writeWithTimestamp(self):
        super(SourceTypeRiskHistogram, self).writeWithTimestamp()

        sector_mir_txt = ["Run Group MIR (in a million) = " + str(self.sector_mir)]
        
        notes = ["Note: The Maximum Overall column lists the population at various risk levels attributable to all\n" + \
        "source types/emission process groups combined, while the other columns list the population at various risk\n" + \
        "levels attributable to each individual source type in isolation. The sum of the population tallies across\n" + \
        "the individual source types may not necessarily equal the corresponding value in the maximum overall column,\n" + \
        "at a given risk level, because: (a) two or more source types' impact in combination may be required to cause\n" + \
        "a census block population to exceed a given risk level; or conversely (b) an individual source type's impact\n" + \
        "in isolation may be enough to cause a census block population to exceed a given risk level, while other\n" + \
        "source types may similarly impact the same census block population and also (in isolation) cause that\n" + \
        "population to exceed the given risk level."]
        
        self.appendHeaderAtLocation(headers=sector_mir_txt, startingrow=13, startingcol=0)
        self.appendHeaderAtLocation(headers=notes, startingrow=15, startingcol=0)

    def round_to_sigfig(self, x, sig=2):
        if x == 0:
            return 0;

        if math.isnan(x):
            return float('NaN')

        rounded = round(x, sig-int(floor(log10(abs(x))))-1)
        return rounded