import os
import pandas as pd

from com.sca.hem4.CensusBlocks import *
from com.sca.hem4.writer.csv.AllInnerReceptors import *
from com.sca.hem4.writer.csv.CsvWriter import CsvWriter

modeled = 'modeled'
nhrs = 'nhrs'
seas = 'seas'
hour = 'hour'
timeblk = 'timeblk'
class Temporal(CsvWriter, InputFile):
    """
    Provides the annual average pollutant concentrations at different time periods (depending on the temporal option
    chosen) for all pollutants and receptors.
    """

    def __init__(self, targetDir=None, facilityId=None, model=None, plot_df=None,
                 filenameOverride=None, createDataframe=False):
        # Initialization for CSV reading/writing. If no file name override, use the
        # default construction.
        self.targetDir = targetDir
        filename = facilityId + "_temporal.csv" if filenameOverride is None else filenameOverride
        path = os.path.join(self.targetDir, filename)

        CsvWriter.__init__(self, model, plot_df)
        InputFile.__init__(self, path, createDataframe)

        self.filename = path

        # initialize cache for outer/outer census block data
        self.innblkCache = {}
        self.outblkCache = {}

        self.rtype = model.model_optns['runtype']
        self.resolution = model.tempvar
        self.seasonal = model.seasonvar

        self.temporal_outer_df = None

    def getHeader(self):
        defaultHeader = ['Fips', 'Block', 'Population', 'Lat', 'Lon', 'Pollutant', 'Emis_type', 'Overlap', 'Modeled']
        return self.extendCols(defaultHeader)

    def getColumns(self):
        defaulsCols = [fips, block, population, lat, lon, pollutant, emis_type, overlap, modeled]
        return self.extendCols(defaulsCols)

    def extendCols(self, default):
        n_seas = 4 if self.seasonal else 1
        n_hrblk = round(24/self.resolution)
        concCols = n_seas * n_hrblk

        self.conc_cols = []
        for c in range(1, concCols+1):
            colnum = str(c)
            if len(colnum) < 2:
                colnum = "0" + colnum
            colName = "C_" + colnum
            self.conc_cols.append(colName)

        default.extend(self.conc_cols)
        return default

    def generateOutputs(self):

        # Units conversion factor
        self.cf = 2000*0.4536/3600/8760

        # Aermod runtype (with or without deposition) determines what columns are in the aermod plotfile.
        # Set accordingly in a dictionary.
        self.plotcols = {0: [utme,utmn,source_id,conc, emis_type],
                         1: [utme,utmn,source_id,conc,ddp,wdp, emis_type],
                         2: [utme,utmn,source_id,conc,ddp, emis_type],
                         3: [utme,utmn,source_id,conc,wdp, emis_type]}

        self.plotoutercols = {0: [lat,lon,source_id,conc, emis_type],
                         1: [lat,lon,source_id,conc,ddp,wdp, emis_type],
                         2: [lat,lon,source_id,conc,ddp, emis_type],
                         3: [lat,lon,source_id,conc,wdp, emis_type]}

        # Open up the temporal plot file and set up both outer and outer slices
        pfile = open(self.targetDir + '/seasonhr.plt', "r")
        self.readPlotfile(pfile)

        # sort by receptor, season?, hour
        if self.seasonal:
            self.temporal_inner_df.sort_values(by=[source_id, utme, utmn, seas, hour], inplace=True)
        else:
            self.temporal_inner_df.sort_values(by=[source_id, utme, utmn, hour], inplace=True)

        # copy the emission types from the regular plot_df
        self.temporal_inner_df = self.temporal_inner_df.merge(right=self.plot_df, on=[source_id, utme, utmn, elev, hill])

        new_cols = [utme, utmn, source_id, conc, emis_type, seas, hour]
        innerplot_df = self.temporal_inner_df[new_cols].copy()

        # add the new concentration columns, based on user selections
        innerplot_df = pd.concat([innerplot_df, pd.DataFrame(columns=self.conc_cols)])
        innerplot_df.reset_index()

        # create array of unique source_id's
        srcids = innerplot_df[source_id].unique().tolist()

        # --------------------------------------------------------------------------------------------------
        # Start the algorithm for inner receptors...this is a slight variant of the logic found in AllInnerReceptors.
        # --------------------------------------------------------------------------------------------------
        dlist = []
        col_list = self.getColumns()

        # process inner concs one source_id at a time
        for x in srcids:
            innerplot_onesrcid = innerplot_df[self.plotcols[self.rtype]].loc[innerplot_df[source_id] == x]
            hapemis_onesrcid = self.model.runstream_hapemis[[source_id,pollutant,emis_tpy,part_frac]] \
                .loc[self.model.runstream_hapemis[source_id] == x]
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
                    d_pollutant = row2.pollutant
                    d_population = record[population].values[0]
                    d_overlap = record[overlap].values[0]
                    d_emistype = row1.emis_type
                    d_modeled = 'D'

                    d_concs = []
                    records_to_avg = int(96 / len(self.conc_cols))
                    recs = innerplot_df.loc[(innerplot_df[utme] == row1[1]) & (innerplot_df[utmn] == row1[2])]

                    column = 0
                    for c in self.conc_cols:
                        offset = int(column * records_to_avg)
                        values = recs[conc].values[offset:offset+records_to_avg]
                        average = sum(values) / records_to_avg

                        if d_emistype == 'C':
                            d_concs.append(average * row2.emis_tpy * self.cf)
                        elif d_emistype == 'P':
                            d_concs.append(average * row2.emis_tpy * self.cf * row2.part_frac)
                        else:
                            d_concs.append(average * row2.emis_tpy * self.cf * (1 - row2.part_frac))
                        column+= 1

                    datalist = [d_fips, d_block, d_population, d_lat, d_lon, d_pollutant, d_emistype, d_overlap, d_modeled]
                    datalist.extend(d_concs)

                    dlist.append(dict(zip(col_list, datalist)))

        innerconc_df = pd.DataFrame(dlist, columns=col_list)

        # --------------------------------------------------------------------------------------------------
        # Now do outer receptors...
        # --------------------------------------------------------------------------------------------------
        if self.seasonal:
            self.temporal_outer_df.sort_values(by=[source_id, utme, utmn, seas, hour], inplace=True)
        else:
            self.temporal_outer_df.sort_values(by=[source_id, utme, utmn, hour], inplace=True)

        # copy the emission types from the regular plot_df
        self.temporal_outer_df = self.temporal_outer_df.merge(right=self.plot_df, on=[source_id, utme, utmn, elev, hill],
              how='left')

        polarplot_df = self.temporal_outer_df[new_cols].copy()
        
        # add the new concentration columns, based on user selections
        polarplot_df = pd.concat([polarplot_df, pd.DataFrame(columns=self.conc_cols)])
        polarplot_df.reset_index()

        dlist = []

        # outer blocks
        outblks = self.model.outerblks_df[[fips, idmarplot, lat, lon, elev, 'distance', 'angle', population, overlap,
               's', 'ring_loc']].copy()
        
        # polar receptors
        plrrecp = self.model.polargrid[['utme', 'utmn', 'sector', 'ring']].copy()

        # Merge Seasonhr polar concs with polar receptors
        polarconcs = polarplot_df.merge(right=plrrecp, how='outer', on=['utme', 'utmn'])

        d_list = []
        for index, row in outblks.iterrows():
            d_fips = row[fips]
            d_block = row[idmarplot][5:]
            d_lat = row[lat]
            d_lon = row[lon]
            d_population = row[population]
            d_overlap = row[overlap]

            # Determine the 4 surrounding polar sector/ring combinations (s1,r1), (s1,r2), (s2,r1), and (s2,r2)
            if int(row['s']) == self.model.numsectors:
                s1 = self.model.numsectors
                s2 = 1
            else:
                s1 = int(row['s'])
                s2 = int(row['s']) + 1
        
            r1 = int(row['ring_loc'])
            if r1 == self.model.numrings:
                r1 = r1 - 1
            r2 = int(row['ring_loc']) + 1
            if r2 > self.model.numrings:
                r2 = self.model.numrings
        
            # Retrieve the 4 surrounding concs from the polarconcs DF
            conc_s1r1 = polarconcs.loc[(polarconcs['sector'] == s1) & (polarconcs['ring'] == r1)]
            conc_s1r2 = polarconcs.loc[(polarconcs['sector'] == s1) & (polarconcs['ring'] == r2)]
            conc_s2r1 = polarconcs.loc[(polarconcs['sector'] == s2) & (polarconcs['ring'] == r1)]
            conc_s2r2 = polarconcs.loc[(polarconcs['sector'] == s2) & (polarconcs['ring'] == r2)]
        
            for i in range(0, len(conc_s1r1)):
                d_emistype = conc_s1r1[emis_type].values[i]
                d_seas = conc_s1r1[seas].values[i]
                d_hour = conc_s1r1[hour].values[i]
                d_sourceid = conc_s1r1[source_id].values[i]
                
                s1r1 = conc_s1r1[conc].values[i]
                s1r2 = conc_s1r2[conc].values[i]
                s2r1 = conc_s2r1[conc].values[i]
                s2r2 = conc_s2r2[conc].values[i]
                
                # Interpolate
                if s1r1 == 0 or s1r2 == 0:
                    R_s12 = max(s1r1, s1r2)
                else:
                    Lnr_s12 = ((math.log(s1r1) * (int(row['ring_loc'])+1-row['ring_loc'])) +
                               (math.log(s1r2) * (row['ring_loc']-int(row['ring_loc']))))
                    R_s12 = math.exp(Lnr_s12)
            
                if s2r1 == 0 or s2r2 == 0:
                    R_s34 = max(s2r1, s2r2)
                else:
                    Lnr_s34 = ((math.log(s2r1) * (int(row['ring_loc'])+1-row['ring_loc'])) +
                               (math.log(s2r2) * (row['ring_loc']-int(row['ring_loc']))))
                    R_s34 = math.exp(Lnr_s34)
            
                d_iconc = R_s12*(int(row['s'])+1-row['s']) + R_s34*(row['s']-int(row['s']))

                columns = [fips, block, source_id, population, lat, lon, emis_type, overlap, conc, seas, hour]
                datalist = [d_fips, d_block, d_sourceid, d_population, d_lat, d_lon, d_emistype, d_overlap, d_iconc, d_seas, d_hour]
                dlist.append(dict(zip(columns, datalist)))

        # Append to outer concs list
        outerplot_df = pd.DataFrame(dlist, columns=columns)

        # create array of unique source_id's
        srcids = outerplot_df[source_id].unique().tolist()

        # process outer concs one source_id at a time
        for x in srcids:
            outerplot_onesrcid = outerplot_df[self.plotoutercols[self.rtype]].loc[outerplot_df[source_id] == x]
            hapemis_onesrcid = self.model.runstream_hapemis[[source_id,pollutant,emis_tpy,part_frac]] \
                .loc[self.model.runstream_hapemis[source_id] == x]
            for row1 in outerplot_onesrcid.itertuples():
                for row2 in hapemis_onesrcid.itertuples():

                    record = None
                    key = (row1.lat, row1.lon)
                    if key in self.outblkCache:
                        record = self.outblkCache.get(key)
                    else:
                        record = self.model.outerblks_df.loc[(self.model.outerblks_df[lat] == row1[1]) & (self.model.outerblks_df[lon] == row1[2])]
                        self.outblkCache[key] = record

                    d_fips = record[fips].values[0]
                    d_idmarplot = record[idmarplot].values[0]
                    d_block = d_idmarplot[-10:]
                    d_lat = record[lat].values[0]
                    d_lon = record[lon].values[0]
                    d_pollutant = row2.pollutant
                    d_population = record[population].values[0]
                    d_overlap = record[overlap].values[0]
                    d_emistype = row1.emis_type
                    d_modeled = 'I'

                    d_concs = []
                    records_to_avg = int(96 / len(self.conc_cols))
                    recs = outerplot_df.loc[(outerplot_df[lat] == row1[1]) & (outerplot_df[lon] == row1[2])]

                    column = 0
                    for c in self.conc_cols:
                        offset = int(column * records_to_avg)
                        values = recs[conc].values[offset:offset+records_to_avg]
                        average = sum(values) / records_to_avg

                        if d_emistype == 'C':
                            d_concs.append(average * row2.emis_tpy * self.cf)
                        elif d_emistype == 'P':
                            d_concs.append(average * row2.emis_tpy * self.cf * row2.part_frac)
                        else:
                            d_concs.append(average * row2.emis_tpy * self.cf * (1 - row2.part_frac))
                        column+= 1

                    datalist = [d_fips, d_block, d_population, d_lat, d_lon, d_pollutant, d_emistype, d_overlap, d_modeled]
                    datalist.extend(d_concs)

                    dlist.append(dict(zip(col_list, datalist)))

        outerconc_df = pd.DataFrame(dlist, columns=col_list)

        self.dataframe = innerconc_df.append(outerconc_df)
        self.data = self.dataframe.values
        yield self.dataframe

    def readPlotfile(self, pfile):
        columns = [0,1,2,3,4,5,6,7,8,9,10]
        colnames = [utme,utmn,conc,elev,hill,flag,source_id,nhrs,seas,hour,net_id]
        type_converters = {utme:np.float64,utmn:np.float64,conc:np.float64,elev:np.float64,hill:np.float64,
            flag:np.float64,source_id:np.str,nhrs:np.int64,seas:np.int64,hour:np.int64,net_id:np.str}

        if self.rtype == 1:
            # Wet and dry dep
            columns.append(11)
            columns.append(12)
            colnames.insert(3, drydep)
            colnames.insert(4, wetdep)
            type_converters[drydep] = np.float64
            type_converters[wetdep] = np.float64
        elif self.rtype == 2:
            # Dry only
            columns.append(11)
            colnames.insert(3, drydep)
            type_converters[drydep] = np.float64
        elif self.rtype == 3:
            # Wet only
            columns.append(11)
            colnames.insert(3, wetdep)
            type_converters[wetdep] = np.float64

        plot_df = pd.read_table(pfile, delim_whitespace=True, header=None, names=colnames, usecols=columns,
             converters=type_converters, comment='*')

        plot_df.utme = plot_df.utme.round()
        plot_df.utmn = plot_df.utmn.round()

        # Extract Chronic outer concs from temporal plotfile and round the utm coordinates
        self.temporal_inner_df = plot_df.query("net_id != 'POLGRID1'").copy()
        self.temporal_outer_df = plot_df.query("net_id == 'POLGRID1'").copy()

    def createDataframe(self):
        # Type setting for CSV reading
        self.numericColumns = [lat, lon, population]
        self.numericColumns = self.extendCols(self.numericColumns)
        self.strColumns = [fips, block, emis_type, pollutant, overlap, modeled]
        df = self.readFromPathCsv(self.getColumns())
        return df.fillna("")
