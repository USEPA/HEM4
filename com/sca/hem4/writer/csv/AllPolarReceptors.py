from com.sca.hem4.FacilityPrep import *
from com.sca.hem4.writer.csv.AllInnerReceptors import *
import traceback
from com.sca.hem4.support.NormalRounding import *


class AllPolarReceptors(CsvWriter, InputFile):
    """
    Provides the annual average concentration modeled at every census block within the modeling cutoff distance,
    specific to each source ID and pollutant, along with receptor information, and acute concentration (if modeled) and
    wet and dry deposition flux (if modeled).
    """
    def __init__(self, targetDir=None, facilityId=None, model=None, plot_df=None, acuteyn=None,
                 filenameOverride=None, createDataframe=False):

        # Initialization for CSV reading/writing. If no file name override, use the
        # default construction.
        self.targetDir = targetDir
        filename = facilityId + "_all_polar_receptors.csv" if filenameOverride is None else filenameOverride
        path = os.path.join(self.targetDir, filename)

        CsvWriter.__init__(self, model, plot_df)
        InputFile.__init__(self, path, createDataframe)

        # initialize cache for inner census block data
        self.polarCache = {}
        self.acute_yn = acuteyn
        self.filename = path

    def getHeader(self):
        if self.acute_yn == 'N':
            return ['Source ID', 'Emission type', 'Pollutant', 'Conc (ug/m3)',
                    'Distance (m)', 'Angle (from north)', 'Sector', 'Ring number', 'Elevation (m)',
                    'Latitude', 'Longitude', 'Overlap', 'Wet deposition (g/m2/yr)', 'Dry deposition (g/m2/yr)']
        else:
            return ['Source ID', 'Emission type', 'Pollutant', 'Conc (ug/m3)', 'Acute conc (ug/m3)',
                    'Distance (m)', 'Angle (from north)', 'Sector', 'Ring number', 'Elevation (m)',
                    'Latitude', 'Longitude', 'Overlap', 'Wet deposition (g/m2/yr)', 'Dry deposition (g/m2/yr)']
            


    def getColumns(self):
        if self.acute_yn == 'N':
            return [source_id, emis_type, pollutant, conc, distance, angle, sector, ring, elev, lat, lon, 
                    overlap, wetdep, drydep]
        else:
            return [source_id, emis_type, pollutant, conc, aconc, distance, angle, sector, ring, elev, lat, lon, 
                    overlap, wetdep, drydep]

    def generateOutputs(self):
        """
        Create the facility specific All Polar Receptor output file.
        This is a CSV formatted file with the following fields:
            source_id
            emis_type
            pollutant
            conc_ug_m3
            distance (m)
            angle
            sector number
            ring number
            elevation (m)
            latitude
            longitude
            overlap (Y/N)
        """
            
        # Units conversion factor
        self.cf = 2000*0.4536/3600/8760
                
        # Aermod runtype (with or without deposition) determines what columns are in the aermod plotfile.
        # Set accordingly in a dictionary.
        self.rtype = self.model.model_optns['runtype']
        self.plotcols = {0: [utme,utmn,source_id,result,aresult,emis_type]}
        self.plotcols[1] = [utme,utmn,source_id,result,ddp,wdp,aresult,emis_type]
        self.plotcols[2] = [utme,utmn,source_id,result,ddp,aresult,emis_type]
        self.plotcols[3] = [utme,utmn,source_id,result,wdp,aresult,emis_type]

                    
        #extract Chronic polar concs from the Chronic plotfile
        polarcplot_df = self.plot_df.query("net_id == 'POLGRID1'").copy()

        # If acute was run for this facility, extract polar concs from Acute plotfile and join to
        # chronic polar concs, otherwise, add column of 0's for acute result
        if self.acute_yn == 'Y':
            polaraplot_df = self.model.acuteplot_df.query("net_id == 'POLGRID1'").copy()
            polarplot_df = pd.merge(polarcplot_df, polaraplot_df[[emis_type, source_id, utme, utmn, aresult]], 
                                    how='inner', on = [emis_type, source_id, utme, utmn])
        else:
            polarplot_df = polarcplot_df.copy()
            polarplot_df[aresult] = 0.0
        
        # array of unique source_id's
        srcids = polarplot_df[source_id].unique().tolist()

        dlist = []
        col_list = self.getColumns()
            
        # process polar concs one source_id at a time
        for x in srcids:
            polarplot_onesrcid = polarplot_df[self.plotcols[self.rtype]].loc[polarplot_df[source_id] == x]
            hapemis_onesrcid = self.model.runstream_hapemis[[source_id,pollutant,emis_tpy,part_frac]] \
                               .loc[self.model.runstream_hapemis[source_id] == x]
                        
            for row1 in polarplot_onesrcid.itertuples():
                for row2 in hapemis_onesrcid.itertuples():
                                        
                    d_sourceid = row1.source_id
                    d_emistype = row1.emis_type
                    d_pollutant = row2.pollutant
                    
                    if row1.emis_type == 'C':
                        d_conc = row1.result * row2.emis_tpy * self.cf
                        d_aconc = row1.aresult * row2.emis_tpy * self.cf * self.model.facops.iloc[0][multiplier]
                        d_drydep = "" if self.rtype in [0,3] else row1.ddp * row2.emis_tpy * self.cf
                        d_wetdep = "" if self.rtype in [0,2] else row1.wdp * row2.emis_tpy * self.cf
                    elif row1.emis_type == 'P':
                        d_conc = row1.result * row2.emis_tpy * self.cf * row2.part_frac
                        d_aconc = row1.aresult * row2.emis_tpy * self.cf * self.model.facops.iloc[0][multiplier] \
                                  * row2.part_frac
                        d_drydep = "" if self.rtype in [0,3] else row1.ddp * row2.emis_tpy * self.cf * row2.part_frac
                        d_wetdep = "" if self.rtype in [0,2] else row1.wdp * row2.emis_tpy * self.cf * row2.part_frac
                    else:
                        d_conc = row1.result * row2.emis_tpy * self.cf * (1 - row2.part_frac)
                        d_aconc = row1.aresult * row2.emis_tpy * self.cf * self.model.facops.iloc[0][multiplier] \
                                  * (1 - row2.part_frac)
                        d_drydep = "" if self.rtype in [0,3] else row1.ddp * row2.emis_tpy * self.cf * (1 - row2.part_frac)
                        d_wetdep = "" if self.rtype in [0,2] else row1.wdp * row2.emis_tpy * self.cf * (1 - row2.part_frac)

                    record = None
                    key = (row1.utme, row1.utmn)
                    if key in self.polarCache:
                        record = self.polarCache.get(key)
                    else:
                        record = self.model.polargrid.loc[(self.model.polargrid[utme] == row1.utme) & (self.model.polargrid[utmn] == row1.utmn)]
                        self.polarCache[key] = record

                    d_distance = record[distance].values[0]
                    d_angle = record[angle].values[0]
                    d_sector = record[sector].values[0]
                    d_ring_no = record[ring].values[0]
                    d_elev = record[elev].values[0]
                    d_lat = record[lat].values[0]
                    d_lon = record[lon].values[0]
                    d_overlap = record[overlap].values[0]
                    
                    if self.acute_yn == 'N':
                        datalist = [d_sourceid, d_emistype, d_pollutant, d_conc,
                                    d_distance, d_angle, d_sector, d_ring_no,
                                    d_elev, d_lat, d_lon, d_overlap, d_wetdep, d_drydep]
                    else:
                        datalist = [d_sourceid, d_emistype, d_pollutant, d_conc, d_aconc,
                                    d_distance, d_angle, d_sector, d_ring_no,
                                    d_elev, d_lat, d_lon, d_overlap, d_wetdep, d_drydep]
                        

                    dlist.append(dict(zip(col_list, datalist)))
                                        
                        
        all_polar_receptors_df = pd.DataFrame(dlist, columns=col_list)
        values = {wetdep:'', drydep:''}
        all_polar_receptors_df.fillna(value=values, inplace=True)

        #dataframe to array
        self.dataframe = all_polar_receptors_df
        self.data = self.dataframe.values

        yield self.dataframe

    def createDataframe(self):
        # Type setting for CSV reading
        if self.acute_yn == 'N':
            self.numericColumns = [distance, angle, sector, ring, lat, lon, conc, elev, drydep, wetdep]
        else:
            self.numericColumns = [distance, angle, sector, ring, lat, lon, conc, aconc, elev, drydep, wetdep]
            
        self.strColumns = [source_id, emis_type, pollutant, overlap]

        df = self.readFromPathCsv(self.getColumns())
        return df.fillna("")

