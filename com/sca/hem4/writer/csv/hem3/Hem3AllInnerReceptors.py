from com.sca.hem4.CensusBlocks import *
from com.sca.hem4.writer.csv.CsvWriter import CsvWriter
from com.sca.hem4.upload.HAPEmissions import *
from com.sca.hem4.upload.FacilityList import *
import os

emis_type = 'ems_type';
block = 'block';
drydep = 'drydep';
wetdep = 'wetdep';
aconc = 'aconc';
aresult = 'aresult';

class Hem3AllInnerReceptors(CsvWriter, InputFile):
    """
    Provides the annual average concentration modeled at every census block within the modeling cutoff distance,
    specific to each source ID and pollutant, along with receptor information, and acute concentration (if modeled) and
    wet and dry deposition flux (if modeled). This class can act as both a writer and a reader of the csv file that holds
    inner receptor information.
    """

    def __init__(self, targetDir=None, facilityId=None, model=None, plot_df=None, acuteyn=None,
                 filenameOverride=None, createDataframe=False):
        # Initialization for CSV reading/writing. If no file name override, use the
        # default construction.
        self.targetDir = targetDir
        filename = facilityId + "_all_inner_receptors.csv" if filenameOverride is None else filenameOverride
        path = os.path.join(self.targetDir, filename)

        CsvWriter.__init__(self, model, plot_df)
        InputFile.__init__(self, path, createDataframe)

        # initialize cache for inner census block data
        self.innblkCache = {}
        self.filename = path
        self.acute_yn = acuteyn

    def getHeader(self):
        if self.acute_yn:
            return ['Latitude', 'Longitude', 'Source ID', 'Emission type', 'Pollutant',
                'Conc (µg/m3)', 'Population', 'FIPs', 'Block',
                'Wet deposition (g/m2/yr)', 'Dry deposition (g/m2/yr)', 'Acute Conc (ug/m3)',
                'Elevation (m)', 'Overlap']
        else:
            return ['Latitude', 'Longitude', 'Source ID', 'Emission type', 'Pollutant',
                    'Conc (µg/m3)', 'Population', 'FIPs', 'Block',
                    'Wet deposition (g/m2/yr)', 'Dry deposition (g/m2/yr)',
                    'Elevation (m)', 'Overlap']

    def getColumns(self):
        if self.acute_yn:
            return [lat, lon, source_id, emis_type, pollutant, conc, population,
                fips, block, wetdep, drydep, elev, aconc, overlap]
        else:
            return [lat, lon, source_id, emis_type, pollutant, conc, population,
                    fips, block, wetdep, drydep, elev, overlap]
    def generateOutputs(self):
        """
        Compute source and pollutant specific air concentrations at inner receptors.
        """

        # Units conversion factor
        self.cf = 2000*0.4536/3600/8760

        # Aermod runtype (with or without deposition) determines what columns are in the aermod plotfile.
        # Set accordingly in a dictionary.
        self.rtype = self.model.model_optns['runtype']
        self.plotcols = {0: [utme,utmn,source_id,result,aresult,'emis_type']}
        self.plotcols[1] = [utme,utmn,source_id,result,ddp,wdp,aresult,'emis_type']
        self.plotcols[2] = [utme,utmn,source_id,result,ddp,aresult,'emis_type']
        self.plotcols[3] = [utme,utmn,source_id,result,wdp,aresult,'emis_type']


        # If acute was run for this facility, read the acute plotfile
        if self.model.facops.iloc[0][acute] == 'Y':
            apfile = open(self.targetDir + "maxhour.plt", "r")
            self.aplot_df = pd.read_table(apfile, delim_whitespace=True, header=None, 
                names=[utme,utmn,aresult,elev,hill,flag,avg_time,source_id,num_yrs,net_id],
                usecols=[0,1,2,3,4,5,6,7,8,9], 
                converters={utme:np.float64,utmn:np.float64,aresult:np.float64,elev:np.float64,hill:np.float64
                       ,flag:np.float64,avg_time:np.str,source_id:np.str,rank:np.str,net_id:np.str
                       ,concdate:np.str},
                comment='*')          

        # Extract Chronic inner concs from Chronic plotfile and round the utm coordinates
        innercplot_df = self.plot_df.query("net_id != 'POLGRID1'").copy()
        innercplot_df.utme = innercplot_df.utme.round()
        innercplot_df.utmn = innercplot_df.utmn.round()

        # If acute was run for this facility, extract inner concs from acute plotfile and join to
        # Chronic inner concs.
        # Otherwise, add column of 0's for acute result
        if self.model.facops.iloc[0][acute] == 'Y':
            inneraplot_df = self.aplot_df.query("net_id != 'POLGRID1'").copy()
            inneraplot_df.utme = inneraplot_df.utme.round()
            inneraplot_df.utmn = inneraplot_df.utmn.round()
            innerplot_df = pd.merge(innercplot_df, inneraplot_df[[source_id, utme, utmn, aresult]], 
                                    how='inner', on = [source_id, utme, utmn])
        else:
            innerplot_df = innercplot_df.copy()
            innerplot_df[aresult] = 0
  

        # create array of unique source_id's
        srcids = innerplot_df[source_id].unique().tolist()

        dlist = []
        columns = self.getColumns()


        # process inner concs one source_id at a time
        for x in srcids:
            innerplot_onesrcid = innerplot_df[self.plotcols[self.rtype]].loc[innerplot_df[source_id] == x]
            hapemis_onesrcid = self.model.runstream_hapemis[[source_id,pollutant,emis_tpy]].loc[self.model.runstream_hapemis[source_id] == x]
            for row1 in innerplot_onesrcid.itertuples():
                for row2 in hapemis_onesrcid.itertuples():
                   
                    record = None
                    key = (row1.utme, row1.utmn)
                    if key in self.innblkCache:
                        record = self.innblkCache.get(key)
                    else:
                        record = self.model.innerblks_df.loc[(self.model.innerblks_df[utme] == row1[1]) & (self.model.innerblks_df[utmn] == row1[2])]
                        self.innblkCache[key] = record

                    d_fips = record[fips].values[0]
                    d_idmarplot = record[idmarplot].values[0]
                    d_block = d_idmarplot[-10:]
                    d_lat = record[lat].values[0]
                    d_lon = record[lon].values[0]
                    d_sourceid = row1.source_id
                    d_emistype = row1.emis_type
                    d_pollutant = row2.pollutant
                    d_conc = row1.result * row2.emis_tpy * self.cf
                    d_aconc = row1.aresult * row2.emis_tpy * self.cf * self.model.facops.iloc[0][multiplier]
                    d_elev = record[elev].values[0]
                    d_drydep = "" if self.rtype in [0,3] else row1.ddp * row2.emis_tpy * self.cf
                    d_wetdep = "" if self.rtype in [0,2] else row1.wdp * row2.emis_tpy * self.cf
                    d_population = record[population].values[0]
                    d_overlap = record[overlap].values[0]
                    datalist = [d_fips, d_block, d_lat, d_lon, d_sourceid, d_emistype, d_pollutant, d_conc,
                                d_aconc, d_elev, d_drydep, d_wetdep, d_population, d_overlap]
                    dlist.append(dict(zip(columns, datalist)))

        innerconc_df = pd.DataFrame(dlist, columns=columns)


        # dataframe to array
        self.dataframe = innerconc_df
        self.data = self.dataframe.values

        yield self.dataframe

    def createDataframe(self):
        # Type setting for CSV reading
        if self.acute_yn == 'Y':
            self.numericColumns = [lat, lon, conc, aconc, elev, drydep, wetdep, population]
        else:
            self.numericColumns = [lat, lon, conc, elev, drydep, wetdep, population]

        self.strColumns = [fips, block, source_id, emis_type, pollutant, overlap]

        df = self.readFromPathCsv(self.getColumns())
        return df.fillna("")