import re
import operator

from pandas import Series

from com.sca.hem4.FacilityPrep import sector, ring
from com.sca.hem4.upload.DoseResponse import *
from com.sca.hem4.writer.csv.AllInnerReceptors import *
from com.sca.hem4.writer.excel.Incidence import inc
from com.sca.hem4.FacilityPrep import *

mir = 'mir';
hi_resp = 'hi_resp';
hi_live = 'hi_live';
hi_neur = 'hi_neur';
hi_deve = 'hi_deve';
hi_repr = 'hi_repr';
hi_kidn = 'hi_kidn';
hi_ocul = 'hi_ocul';
hi_endo = 'hi_endo';
hi_hema = 'hi_hema';
hi_immu = 'hi_immu';
hi_skel = 'hi_skel';
hi_sple = 'hi_sple';
hi_thyr = 'hi_thyr';
hi_whol = 'hi_whol';

class AllOuterReceptorsNonCensus(CsvWriter, InputFile):
    """
    Provides the annual average concentration interpolated at every receptor beyond the modeling cutoff distance but
    within the modeling domain, specific to each source ID and pollutant, along with receptor information, and acute
    concentration (if modeled) and wet and dry deposition flux (if modeled).
    """

    def __init__(self, targetDir=None, facilityId=None, model=None, plot_df=None, acuteyn=None, 
                 filenameOverride=None, createDataframe=False):
        # Initialization for CSV reading/writing. If no file name override, use the
        # default construction.
        filename = facilityId + "_all_outer_receptors.csv" if filenameOverride is None else filenameOverride
        path = os.path.join(targetDir, filename)

        CsvWriter.__init__(self, model, plot_df)
        InputFile.__init__(self, path, createDataframe)

        self.filename = path
        self.acute_yn = acuteyn


        # No need to go further if we are instantiating this class to read in a CSV file...
        if self.model is None:
            return


        self.outerblocks = self.model.outerblks_df[[lat, lon, utme, utmn, hill]]
        self.outerAgg = None
        self.outerInc = None

        # AllOuterReceptor DF columns
        self.columns = self.getColumns()

        # Initialize max_riskhi dictionary. Keys are mir, and HIs. Values are
        # lat, lon, and risk value. This dictionary identifies the lat/lon of the max receptor for
        # the mir and each HI.
        self.max_riskhi = {}
        self.riskhi_parms = [mir, hi_resp, hi_live, hi_neur, hi_deve, hi_repr, hi_kidn, hi_ocul,
                             hi_endo, hi_hema, hi_immu, hi_skel, hi_sple, hi_thyr, hi_whol]
        for icol in self.riskhi_parms:
            self.max_riskhi[icol] = [0, 0, 0]

        # Initialize max_riskhi_bkdn dictionary. This dictionary identifies the Lat/lon of the max receptor for
        # the mir and each HI, and has source/pollutant specific risk at that lat/lon.
        # Keys are: parameter, source_id, pollutant, and emis_type.
        # Values are: lat, lon, and risk value.
        self.srcpols = self.model.all_polar_receptors_df[[source_id, pollutant, emis_type]].drop_duplicates().values.tolist()
        self.max_riskhi_bkdn = {}
        self.outerInc = {}
        for jparm in self.riskhi_parms:
            for jsrcpol in self.srcpols:
                self.max_riskhi_bkdn[(jparm, jsrcpol[0], jsrcpol[1], jsrcpol[2])] = [0, 0, 0]

        # Initialize the outerInc dictionary. This dictionary contains cancer incidence by source, pollutant,
        # and emis_type.
        # Keys are: source_id, pollutant, and emis_type
        # Value is: incidence
        for jsrcpol in self.srcpols:
            self.outerInc[(jsrcpol[0], jsrcpol[1], jsrcpol[2])] = 0

        # Compute a recipricol of the rfc for easier computation of HIs
        self.haplib_df = self.model.haplib.dataframe
        self.haplib_df['invrfc'] = self.haplib_df.apply(lambda x: 1/x['rfc'] if x['rfc']>0 else 0.0, axis=1)

        # Local copy of target organs and combine target organ columns into one list column
        self.organs_df = self.model.organs.dataframe
        self.organs_df['organ_list'] = (self.organs_df[['resp','liver','neuro','dev','reprod','kidney',
                        'ocular','endoc','hemato','immune','skeletal','spleen','thyroid','wholebod']]
                        .values.tolist())


    def getHeader(self):
        return ['Receptor ID', 'Latitude', 'Longitude', 'Source ID', 'Emission type', 'Pollutant',
                'Conc (ug/m3)', 'Acute Conc (ug/m3)', 'Elevation (m)', 'Population', 'Overlap']

    def getColumns(self):
        if self.acute_yn == 'N':
            return [rec_id, lat, lon, source_id, emis_type, pollutant, conc, elev, population, overlap]
        else:
            return [rec_id, lat, lon, source_id, emis_type, pollutant, conc, aconc, elev, population, overlap]
            

    def generateOutputs(self):
        """
        Interpolate polar pollutant concs to outer receptors.
        """

        if not self.outerblocks.empty:
            
            # Units conversion factor
            self.cf = 2000*0.4536/3600/8760
    
            # Runtype (with or without deposition) determines what columns are in the aermod plotfile.
            self.rtype = self.model.model_optns['runtype']
    
            # Was acute run? If not, this is chronic only.
            if self.acute_yn == 'N':
            
                #-------- Chronic only ------------------------------------
    
                #extract Chronic polar concs from the Chronic plotfile and round the utm coordinates
                polarplot_df = self.plot_df.query("net_id == 'POLGRID1'").copy()
                polarplot_df.utme = polarplot_df.utme.round()
                polarplot_df.utmn = polarplot_df.utmn.round()
    
                # Assign sector and ring to polar concs from polarplot_df and set an index
                # of source_id + sector + ring
                self.polarconcs = pd.merge(polarplot_df, self.model.polargrid[['utme', 'utmn', 'sector', 'ring']], 
                                     how='inner', on=['utme', 'utmn'])
                self.polarconcs['newindex'] = self.polarconcs['source_id'] \
                                                + 's' + self.polarconcs['sector'].apply(str) \
                                                + 'r' + self.polarconcs['ring'].apply(str)
                self.polarconcs.set_index(['newindex'], inplace=True)
                
                # QA - make sure merge retained all rows
                if self.polarconcs.shape[0] != polarplot_df.shape[0]:
                    print("Error! self.polarconcs has wrong number of rows in AllOuterReceptors")
                    #TODO stop this facility
    
                #subset outer blocks DF to needed columns and sort by increasing distance
                outerblks_subset = self.model.outerblks_df[[rec_id, lat, lon, elev,
                                                            'distance', 'angle', population, overlap,
                                                            's', 'ring_loc']].copy()
                outerblks_subset.sort_values(by=['distance'], axis=0, inplace=True)
           
                # Define sector/ring of 4 surrounding polar receptors of each outer receptor
                a_s = outerblks_subset['s'].values
                a_ringloc = outerblks_subset['ring_loc'].values
                as1, as2, ar1, ar2 = self.compute_s1s2r1r2(a_s, a_ringloc)
                outerblks_subset['s1'] = as1.tolist()
                outerblks_subset['s2'] = as2.tolist()
                outerblks_subset['r1'] = ar1.tolist()
                outerblks_subset['r2'] = ar2.tolist()
        
                # Assign each source_id to every outer receptor
                srcids = self.polarconcs['source_id'].unique().tolist()
                srcid_df = pd.DataFrame(srcids, columns=['source_id'])
                srcid_df['key'] = 1
                outerblks_subset['key'] = 1
                outerblks_subset2 = pd.merge(outerblks_subset, srcid_df, on=['key'])
                
                # Get the 4 surrounding polar Aermod concs of each outer receptor
                cs1r1 = pd.merge(outerblks_subset2, self.polarconcs[['sector','ring','source_id','result','emis_type']],
                                 how='left', left_on=['s1','r1','source_id'],
                                 right_on=['sector','ring','source_id'])
                cs1r1.rename(columns={"result":"result_s1r1"}, inplace=True)
                cs1r2 = pd.merge(outerblks_subset2, self.polarconcs[['sector','ring','source_id','result']],
                                 how='left', left_on=['s1','r2','source_id'],
                                 right_on=['sector','ring','source_id'])
                cs1r2.rename(columns={"result":"result_s1r2"}, inplace=True)
                cs2r1 = pd.merge(outerblks_subset2, self.polarconcs[['sector','ring','source_id','result']],
                                 how='left', left_on=['s2','r1','source_id'],
                                 right_on=['sector','ring','source_id'])
                cs2r1.rename(columns={"result":"result_s2r1"}, inplace=True)
                cs2r2 = pd.merge(outerblks_subset2, self.polarconcs[['sector','ring','source_id','result']],
                                 how='left', left_on=['s2','r2','source_id'],
                                 right_on=['sector','ring','source_id'])
                cs2r2.rename(columns={"result":"result_s2r2"}, inplace=True)
        
                outer4interp = cs1r1.copy()
                outer4interp['result_s1r2'] = cs1r2['result_s1r2']
                outer4interp['result_s2r1'] = cs2r1['result_s2r1']
                outer4interp['result_s2r2'] = cs2r2['result_s2r2']
    
                # Interpolate polar Aermod concs to each outer receptor; store results in arrays
                a_ccs1r1 = outer4interp['result_s1r1'].values
                a_ccs1r2 = outer4interp['result_s1r2'].values
                a_ccs2r1 = outer4interp['result_s2r1'].values
                a_ccs2r2 = outer4interp['result_s2r2'].values
                a_sectfrac = outer4interp['s'].values
                a_ringfrac = outer4interp['ring_loc'].values
                a_intconc = self.interpolate(a_ccs1r1, a_ccs1r2, a_ccs2r1, a_ccs2r2, a_sectfrac, a_ringfrac)
    
                #   Apply emissions to interpolated outer concs and write
                
                outerconcs = outer4interp[['rec_id', 'lat', 'lon', 'elev', 'population', 'overlap',
                                        'emis_type', 'source_id']]
                outerconcs['intconc'] = a_intconc
                
                num_rows_outer_recs = outerblks_subset.shape[0]
                num_polls_in_hapemis = self.model.runstream_hapemis[pollutant].nunique()
                num_rows_hapemis = self.model.runstream_hapemis.shape[0]
                num_rows_output = num_rows_outer_recs * num_rows_hapemis
                num_srcids = len(srcids)
         
                col_list = self.getColumns()
         
               
                #  Write no more than 10,000,000 rows to a given CSV output file
                
                if num_rows_output <= self.batchSize:
                    
                    # One output file
                                    
                    outer_polconcs = pd.merge(outerconcs, self.model.runstream_hapemis[[source_id,pollutant,emis_tpy,part_frac]],
                                        on=[source_id])
                    
                    if 'C' in outer_polconcs['emis_type'].values:
                        outer_polconcs['conc'] = outer_polconcs['intconc'] * outer_polconcs['emis_tpy'] * self.cf
    
                    else:
                        outer_polconcs_p = outer_polconcs[outer_polconcs['emis_type']=='P']
                        outer_polconcs_v = outer_polconcs[outer_polconcs['emis_type']=='V']
                        outer_polconcs_p['conc'] = outer_polconcs_p['intconc'] * outer_polconcs_p['emis_tpy'] \
                                                       * outer_polconcs_p['part_frac'] * self.cf
                        outer_polconcs_v['conc'] = outer_polconcs_v['intconc'] * outer_polconcs_v['emis_tpy'] \
                                                       * ( 1 - outer_polconcs_v['part_frac']) * self.cf
                        outer_polconcs = outer_polconcs_p.append(outer_polconcs_v, ignore_index=True)
         
                    self.dataframe = outer_polconcs[col_list]
                    self.data = self.dataframe.values
                    yield self.dataframe
                    
                else:
         
                    # Multiple output files
                    
                    # compute the number of CSV files (batches) to output and number of rows from outerconcs to use in 
                    # each batch.
                    num_batches = int(round(num_rows_output/self.batchSize))
                    num_outerconc_rows_per_batch = int(round(self.batchSize / num_rows_hapemis)) * num_srcids
                                    
                    for k in range(num_batches):
                        start = k * num_outerconc_rows_per_batch
                        end = start + num_outerconc_rows_per_batch
                        outerconcs_batch = outerconcs[start:end]
                        outer_polconcs = pd.merge(outerconcs_batch, self.model.runstream_hapemis[[source_id,pollutant,emis_tpy,part_frac]],
                                            on=[source_id])
    
                        if 'C' in outer_polconcs['emis_type'].values:
                            outer_polconcs['conc'] = outer_polconcs['intconc'] * outer_polconcs['emis_tpy'] * self.cf
        
                        else:
                            outer_polconcs_p = outer_polconcs[outer_polconcs['emis_type']=='P']
                            outer_polconcs_v = outer_polconcs[outer_polconcs['emis_type']=='V']
                            outer_polconcs_p['conc'] = outer_polconcs_p['intconc'] * outer_polconcs_p['emis_tpy'] \
                                                           * outer_polconcs_p['part_frac'] * self.cf
                            outer_polconcs_v['conc'] = outer_polconcs_v['intconc'] * outer_polconcs_v['emis_tpy'] \
                                                           * ( 1 - outer_polconcs_v['part_frac']) * self.cf
                            outer_polconcs = outer_polconcs_p.append(outer_polconcs_v, ignore_index=True)
    
                        self.dataframe = outer_polconcs[col_list]
                        self.data = self.dataframe.values
                        yield self.dataframe
                    
                    # Last batch
                    outerconcs_batch = outerconcs[end:]
                    outer_polconcs = pd.merge(outerconcs_batch, self.model.runstream_hapemis[[source_id,pollutant,emis_tpy,part_frac]],
                                        on=[source_id])
    
                    if 'C' in outer_polconcs['emis_type'].values:
                        outer_polconcs['conc'] = outer_polconcs['intconc'] * outer_polconcs['emis_tpy'] * self.cf
    
                    else:
                        outer_polconcs_p = outer_polconcs[outer_polconcs['emis_type']=='P']
                        outer_polconcs_v = outer_polconcs[outer_polconcs['emis_type']=='V']
                        outer_polconcs_p['conc'] = outer_polconcs_p['intconc'] * outer_polconcs_p['emis_tpy'] \
                                                       * outer_polconcs_p['part_frac'] * self.cf
                        outer_polconcs_v['conc'] = outer_polconcs_v['intconc'] * outer_polconcs_v['emis_tpy'] \
                                                       * ( 1 - outer_polconcs_v['part_frac']) * self.cf
                        outer_polconcs = outer_polconcs_p.append(outer_polconcs_v, ignore_index=True)
    
                    self.dataframe = outer_polconcs[col_list]
                    self.data = self.dataframe.values
                    yield self.dataframe
                
                
            else:
                        
                #extract Chronic polar concs from the Chronic plotfile and round the utm coordinates
                polarcplot_df = self.plot_df.query("net_id == 'POLGRID1'").copy()
                polarcplot_df.utme = polarcplot_df.utme.round()
                polarcplot_df.utmn = polarcplot_df.utmn.round()
    
                # extract polar concs from Acute plotfile and join to chronic polar concs
                polaraplot_df = self.model.acuteplot_df.query("net_id == 'POLGRID1'").copy()
                polaraplot_df.utme = polaraplot_df.utme.round()
                polaraplot_df.utmn = polaraplot_df.utmn.round()
                polarplot_df = pd.merge(polarcplot_df, polaraplot_df[['emis_type', source_id, utme, utmn, aresult]], 
                                        how='inner', on = ['emis_type', source_id, utme, utmn])
    
                # Assign sector and ring to polar concs from polarplot_df and set an index
                # of source_id + sector + ring
                self.polarconcs = pd.merge(polarplot_df, self.model.polargrid[['utme', 'utmn', 'sector', 'ring']], 
                                     how='inner', on=['utme', 'utmn'])
                self.polarconcs['newindex'] = self.polarconcs['source_id'] \
                                                + 's' + self.polarconcs['sector'].apply(str) \
                                                + 'r' + self.polarconcs['ring'].apply(str)
                self.polarconcs.set_index(['newindex'], inplace=True)
                
                # QA - make sure merge retained all rows
                if self.polarconcs.shape[0] != polarplot_df.shape[0]:
                    print("Error! self.polarconcs has wrong number of rows in AllOuterReceptors")
                    #TODO stop this facility
        
                #subset outer blocks DF to needed columns and sort by increasing distance
                outerblks_subset = self.model.outerblks_df[[rec_id, lat, lon, elev,
                                                            'distance', 'angle', population, overlap,
                                                            's', 'ring_loc']].copy()
                outerblks_subset.sort_values(by=['distance'], axis=0, inplace=True)
    
    
                # Define sector/ring of 4 surrounding polar receptors of each outer receptor
                a_s = outerblks_subset['s'].values
                a_ringloc = outerblks_subset['ring_loc'].values
                as1, as2, ar1, ar2 = self.compute_s1s2r1r2(a_s, a_ringloc)
                outerblks_subset['s1'] = as1.tolist()
                outerblks_subset['s2'] = as2.tolist()
                outerblks_subset['r1'] = ar1.tolist()
                outerblks_subset['r2'] = ar2.tolist()
        
                # Assign each source_id to every outer receptor
                srcids = self.polarconcs['source_id'].unique().tolist()
                srcid_df = pd.DataFrame(srcids, columns=['source_id'])
                srcid_df['key'] = 1
                outerblks_subset['key'] = 1
                outerblks_subset2 = pd.merge(outerblks_subset, srcid_df, on=['key'])
                    
                # Get the 4 surrounding polar Aermod concs of each outer receptor
                cs1r1 = pd.merge(outerblks_subset2, self.polarconcs[['sector','ring','source_id','result','aresult','emis_type']],
                                 how='left', left_on=['s1','r1','source_id'],
                                 right_on=['sector','ring','source_id'])
                cs1r1.rename(columns={"result":"result_s1r1", "aresult":"aresult_s1r1"}, inplace=True)
                cs1r2 = pd.merge(outerblks_subset2, self.polarconcs[['sector','ring','source_id','result','aresult']],
                                 how='left', left_on=['s1','r2','source_id'],
                                 right_on=['sector','ring','source_id'])
                cs1r2.rename(columns={"result":"result_s1r2", "aresult":"aresult_s1r2"}, inplace=True)
                cs2r1 = pd.merge(outerblks_subset2, self.polarconcs[['sector','ring','source_id','result','aresult']],
                                 how='left', left_on=['s2','r1','source_id'],
                                 right_on=['sector','ring','source_id'])
                cs2r1.rename(columns={"result":"result_s2r1", "aresult":"aresult_s2r1"}, inplace=True)
                cs2r2 = pd.merge(outerblks_subset2, self.polarconcs[['sector','ring','source_id','result','aresult']],
                                 how='left', left_on=['s2','r2','source_id'],
                                 right_on=['sector','ring','source_id'])
                cs2r2.rename(columns={"result":"result_s2r2", "aresult":"aresult_s2r2"}, inplace=True)
    
                outer4interp = cs1r1.copy()
                outer4interp['result_s1r2'] = cs1r2['result_s1r2']
                outer4interp['result_s2r1'] = cs2r1['result_s2r1']
                outer4interp['result_s2r2'] = cs2r2['result_s2r2']
                outer4interp['aresult_s1r2'] = cs1r2['aresult_s1r2']
                outer4interp['aresult_s2r1'] = cs2r1['aresult_s2r1']
                outer4interp['aresult_s2r2'] = cs2r2['aresult_s2r2']
            
                #   Interpolate polar Aermod concs to each outer receptor; store results in arrays
                
                # Chronic
                a_ccs1r1 = outer4interp['result_s1r1'].values
                a_ccs1r2 = outer4interp['result_s1r2'].values
                a_ccs2r1 = outer4interp['result_s2r1'].values
                a_ccs2r2 = outer4interp['result_s2r2'].values
                a_sectfrac = outer4interp['s'].values
                a_ringfrac = outer4interp['ring_loc'].values
                a_intcconc = self.interpolate(a_ccs1r1, a_ccs1r2, a_ccs2r1, a_ccs2r2, a_sectfrac, a_ringfrac)
                
                # Acute
                a_acs1r1 = outer4interp['aresult_s1r1'].values
                a_acs1r2 = outer4interp['aresult_s1r2'].values
                a_acs2r1 = outer4interp['aresult_s2r1'].values
                a_acs2r2 = outer4interp['aresult_s2r2'].values
                a_intaconc = self.interpolate(a_acs1r1, a_acs1r2, a_acs2r1, a_acs2r2, a_sectfrac, a_ringfrac)
                
                #   Apply emissions to interpolated outer concs and write
                
                outerconcs = outer4interp[['rec_id', 'lat', 'lon', 'elev', 'population', 'overlap',
                                        'emis_type', 'source_id']]
                outerconcs['intcconc'] = a_intcconc
                outerconcs['intaconc'] = a_intaconc
                
                num_rows_outer_recs = outerblks_subset.shape[0]
                num_polls_in_hapemis = self.model.runstream_hapemis[pollutant].nunique()
                num_rows_hapemis = self.model.runstream_hapemis.shape[0]
                num_rows_output = num_rows_outer_recs * num_rows_hapemis
                num_srcids = len(srcids)
         
                col_list = self.getColumns()
     
           
                # Write no more than 10,000,000 rows to a given CSV output file
                
                if num_rows_output <= self.batchSize:
                    
                    # One output file
                    
                    outer_polconcs = pd.merge(outerconcs, self.model.runstream_hapemis[[source_id,pollutant,emis_tpy,part_frac]],
                                        on=[source_id])
                
                    if 'C' in outer_polconcs['emis_type'].values:
                        outer_polconcs['conc'] = outer_polconcs['intcconc'] * outer_polconcs['emis_tpy'] * self.cf
                        outer_polconcs['aconc'] = outer_polconcs['intaconc'] * outer_polconcs['emis_tpy'] \
                                                  * self.cf * self.model.facops.iloc[0][multiplier]
     
                    else:
                        outer_polconcs_p = outer_polconcs[outer_polconcs['emis_type']=='P']
                        outer_polconcs_v = outer_polconcs[outer_polconcs['emis_type']=='V']
                        outer_polconcs_p['conc'] = outer_polconcs_p['intcconc'] * outer_polconcs_p['emis_tpy'] \
                                                       * outer_polconcs_p['part_frac'] * self.cf
                        outer_polconcs_p['aconc'] = outer_polconcs_p['intaconc'] * outer_polconcs_p['emis_tpy'] \
                                                       * outer_polconcs_p['part_frac'] * self.cf
                        outer_polconcs_v['conc'] = outer_polconcs_v['intcconc'] * outer_polconcs_v['emis_tpy'] \
                                                       * ( 1 - outer_polconcs_v['part_frac']) * self.cf
                        outer_polconcs_v['aconc'] = outer_polconcs_v['intaconc'] * outer_polconcs_v['emis_tpy'] \
                                                       * ( 1 - outer_polconcs_v['part_frac']) * self.cf
                        outer_polconcs = outer_polconcs_p.append(outer_polconcs_v, ignore_index=True)
    
                    self.dataframe = outer_polconcs[col_list]
                    self.data = self.dataframe.values
                    yield self.dataframe
                
                else:
                    
                    # Multiple output files
     
                    # compute the number of CSV files (batches) to output and number of rows from outerconcs to use in 
                    # each batch.
                    num_batches = int(round(num_rows_output/self.batchSize))
                    num_outerconc_rows_per_batch = int(round(self.batchSize / num_rows_hapemis)) * num_srcids
                                    
                    for k in range(num_batches):
                        start = k * num_outerconc_rows_per_batch
                        end = start + num_outerconc_rows_per_batch
                        outerconcs_batch = outerconcs[start:end]
                        outer_polconcs = pd.merge(outerconcs_batch, self.model.runstream_hapemis[[source_id,pollutant,emis_tpy,part_frac]],
                                            on=[source_id])
                        if 'C' in outer_polconcs['emis_type'].values:
                            outer_polconcs['conc'] = outer_polconcs['intcconc'] * outer_polconcs['emis_tpy'] * self.cf
                            outer_polconcs['aconc'] = outer_polconcs['intaconc'] * outer_polconcs['emis_tpy'] \
                                                      * self.cf * self.model.facops.iloc[0][multiplier]
         
                        else:
                            outer_polconcs_p = outer_polconcs[outer_polconcs['emis_type']=='P']
                            outer_polconcs_v = outer_polconcs[outer_polconcs['emis_type']=='V']
                            outer_polconcs_p['conc'] = outer_polconcs_p['intcconc'] * outer_polconcs_p['emis_tpy'] \
                                                           * outer_polconcs_p['part_frac'] * self.cf
                            outer_polconcs_p['aconc'] = outer_polconcs_p['intaconc'] * outer_polconcs_p['emis_tpy'] \
                                                           * outer_polconcs_p['part_frac'] * self.cf
                            outer_polconcs_v['conc'] = outer_polconcs_v['intcconc'] * outer_polconcs_v['emis_tpy'] \
                                                           * ( 1 - outer_polconcs_v['part_frac']) * self.cf
                            outer_polconcs_v['aconc'] = outer_polconcs_v['intaconc'] * outer_polconcs_v['emis_tpy'] \
                                                           * ( 1 - outer_polconcs_v['part_frac']) * self.cf
                            outer_polconcs = outer_polconcs_p.append(outer_polconcs_v, ignore_index=True)
    
                        self.dataframe = outer_polconcs[col_list]
                        self.data = self.dataframe.values
                        yield self.dataframe
                    
                    # Last batch
                    outerconcs_batch = outerconcs[end:]
                    outer_polconcs = pd.merge(outerconcs_batch, self.model.runstream_hapemis[[source_id,pollutant,emis_tpy,part_frac]],
                                        on=[source_id])
    
                    if 'C' in outer_polconcs['emis_type'].values:
                        outer_polconcs['conc'] = outer_polconcs['intcconc'] * outer_polconcs['emis_tpy'] * self.cf
                        outer_polconcs['aconc'] = outer_polconcs['intaconc'] * outer_polconcs['emis_tpy'] \
                                                  * self.cf * self.model.facops.iloc[0][multiplier]
     
                    else:
                        outer_polconcs_p = outer_polconcs[outer_polconcs['emis_type']=='P']
                        outer_polconcs_v = outer_polconcs[outer_polconcs['emis_type']=='V']
                        outer_polconcs_p['conc'] = outer_polconcs_p['intcconc'] * outer_polconcs_p['emis_tpy'] \
                                                       * outer_polconcs_p['part_frac'] * self.cf
                        outer_polconcs_p['aconc'] = outer_polconcs_p['intaconc'] * outer_polconcs_p['emis_tpy'] \
                                                       * outer_polconcs_p['part_frac'] * self.cf
                        outer_polconcs_v['conc'] = outer_polconcs_v['intcconc'] * outer_polconcs_v['emis_tpy'] \
                                                       * ( 1 - outer_polconcs_v['part_frac']) * self.cf
                        outer_polconcs_v['aconc'] = outer_polconcs_v['intaconc'] * outer_polconcs_v['emis_tpy'] \
                                                       * ( 1 - outer_polconcs_v['part_frac']) * self.cf
                        outer_polconcs = outer_polconcs_p.append(outer_polconcs_v, ignore_index=True)
    
                    self.dataframe = outer_polconcs[col_list]
                    self.data = self.dataframe.values
                    yield self.dataframe

        else:
            
            # No outer blocks to process. Return empty dataframes

            blksumm_cols = [lat, lon, overlap, elev, rec_id, utme, utmn, hill, population,
                            mir, hi_resp, hi_live, hi_neur, hi_deve, hi_repr, hi_kidn, hi_ocul,
                            hi_endo, hi_hema, hi_immu, hi_skel, hi_sple, hi_thyr, hi_whol]
            self.outerAgg = pd.DataFrame(columns=blksumm_cols)
            
            col_list = self.getColumns()
            self.dataframe = pd.DataFrame(columns=col_list)
               


    def get4Corners(self, s1, s2, r1, r2, srcid):

        # Initialize output arrays
        cc_s1r1 = np.zeros(len(s1), dtype=float)
        cc_s1r2 = np.zeros(len(s1), dtype=float)
        cc_s2r1 = np.zeros(len(s1), dtype=float)
        cc_s2r2 = np.zeros(len(s1), dtype=float)
        cc_emistype = np.zeros(len(s1), dtype=str)
        
        for i in np.arange(len(s1)):
            cc_s1r1[i] = self.polarconcs['result'].loc[srcid[i]+'s'+str(s1[i])+'r'+str(r1[i])]
            cc_s1r2[i] = self.polarconcs['result'].loc[srcid[i]+'s'+str(s1[i])+'r'+str(r2[i])]
            cc_s2r1[i] = self.polarconcs['result'].loc[srcid[i]+'s'+str(s2[i])+'r'+str(r1[i])]
            cc_s2r2[i] = self.polarconcs['result'].loc[srcid[i]+'s'+str(s2[i])+'r'+str(r2[i])]
            cc_emistype[i] = self.polarconcs['emis_type'].loc[srcid[i]+'s'+str(s2[i])+'r'+str(r2[i])]
            i = i + 1
            
        return cc_s1r1, cc_s1r2, cc_s2r1, cc_s2r2, cc_emistype
    


    def interpolate(self, conc_s1r1, conc_s1r2, conc_s2r1, conc_s2r2, s, ring_loc):
        # Interpolate 4 concentrations to the point defined by (s, ring_loc)
        
        # initialize the output array
        ic = np.zeros(len(conc_s1r1), dtype=float)
        
        for i in np.arange(len(conc_s1r1)):
            
            if conc_s1r1[i] == 0 or conc_s1r2[i] == 0:
                R_s12 = max(conc_s1r1[i], conc_s1r2[i])
            else:
                Lnr_s12 = ((math.log(conc_s1r1[i]) * (int(ring_loc[i])+1-ring_loc[i])) +
                           (math.log(conc_s1r2[i]) * (ring_loc[i]-int(ring_loc[i]))))
                R_s12 = math.exp(Lnr_s12)
    
            if conc_s2r1[i] == 0 or conc_s2r2[i] == 0:
                R_s34 = max(conc_s2r1[i], conc_s2r2[i])
            else:
                Lnr_s34 = ((math.log(conc_s2r1[i]) * (int(ring_loc[i])+1-ring_loc[i])) +
                           (math.log(conc_s2r2[i]) * (ring_loc[i]-int(ring_loc[i]))))
                R_s34 = math.exp(Lnr_s34)
    
            ic[i] = R_s12*(int(s[i])+1-s[i]) + R_s34*(s[i]-int(s[i]))
        
        return ic


    def compute_s1s2r1r2(self, ar_s, ar_r):
        # Define the four surrounding polar sector/rings for each outer block
        
        # Initialize output arrays
        s1 = np.zeros(len(ar_s), dtype=int)
        s2 = np.zeros(len(ar_s), dtype=int)
        r1 = np.zeros(len(ar_s), dtype=int)
        r2 = np.zeros(len(ar_s), dtype=int)
        
        for i in np.arange(len(ar_s)):
            if int(ar_s[i]) == self.model.numsectors:
                s1[i] = self.model.numsectors
                s2[i] = 1
            else:
                s1[i] = int(ar_s[i])
                s2[i] = int(ar_s[i]) + 1
                
            r1[i] = int(ar_r[i])
            if r1[i] == self.model.numrings:
                r1[i] = r1[i] - 1
            r2[i] = int(ar_r[i]) + 1
            if r2[i] > self.model.numrings:
                r2[i] = self.model.numrings
            
        return s1, s2, r1, r2



    def analyze(self, data):

        # Skip if no data
        # Skip if no data in this batch
        if data.size > 0:
                       
            # DF of outer receptor concs
            outer_concs = pd.DataFrame(data, columns=self.columns)
            
            # Get utme, utmn, and hill columns
            outer_concs1 = pd.merge(outer_concs, self.model.outerblks_df[[lat, lon, 'utme', 'utmn', 'hill']],
                                    how='left', on=[lat, lon])

            # Confirm the merge did not grow or shrink the number of rows
            if len(outer_concs.index) != len(outer_concs1.index):
                emessage = "Error! Incorrect merging of outer blocks with outer_concs in AllOuterReceptors."
                Logger.logMessage(emessage)
                raise Exception(emessage)
            
            # Merge ure and inverted rfc
            outer_concs2 = pd.merge(outer_concs1, self.haplib_df[['pollutant', 'ure', 'invrfc']],
                                    how='left', on='pollutant')

            # Confirm the merge did not grow or shrink the number of rows
            if len(outer_concs.index) != len(outer_concs2.index):
                emessage = "Error! Incorrect merging of haplib with outer_concs1 in AllOuterReceptors."
                Logger.logMessage(emessage)
                raise Exception(emessage)
            
            # Merge target organ list
            outer_concs3 = pd.merge(outer_concs2, self.organs_df[['pollutant', 'organ_list']],
                                    how='left', on='pollutant')
            outer_concs3.sort_values(by=[lat, lon], inplace=True)

            # Confirm the merge did not grow or shrink the number of rows
            if len(outer_concs.index) != len(outer_concs3.index):
                emessage = "Error! Incorrect merging of target organs with outer_concs2 in AllOuterReceptors."
                Logger.logMessage(emessage)
                raise Exception(emessage)
            
            chk4null = outer_concs3[outer_concs3['organ_list'].isnull()]
            if not chk4null.empty:
                # Replace NaN with list of 0's
                outer_concs3['organ_list'] = outer_concs3['organ_list'].apply(
                        lambda x: x if isinstance(x, list) else [0,0,0,0,0,0,0,0,0,0,0,0,0,0])

            # Sort by lat/lon
            outer_concs3.sort_values(by=[lat, lon], inplace=True)

            
            # List of all lat/lons from outer_concs3. Not unique because of pollutant/source/emis_type.
            latlon_alllist = outer_concs3[[lat, lon]].values
            # List of unique lat/lons
            latlon_uniqlist = [list(item) for item in set(tuple(row) for row in latlon_alllist)]
            # Sort list of lists by lat/lon so it will link correctly with outer_concs3
            latlon_uniqlist.sort(key=operator.itemgetter(0,1))
            
            # Compute the number of unique groups of lat/lons
            # Also compute the number rows there are for each unique group in outer_concs3 (accounts for pollutant/source/emis_type)
            nouter = len(outer_concs3.index)
            ngroups = len(latlon_uniqlist)
            grouplen = int(nouter / ngroups)

            
            # Sum cancer risk by lat/lon group
            a_mir = self.calculateMir(outer_concs3['conc'].values, outer_concs3['ure'].values)
            sumMir = []
            for x in range(ngroups):
                idxstart = x * grouplen
                idxend = idxstart + grouplen
                s = 0
                for y in range(idxstart, idxend):
                    s = s + a_mir[y]
                sumMir.append(s)
                
            
            # Calculate resp HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[0])
            if np.sum(organval) == 0:
                sumResp = [0] * ngroups
            else:
                a_resp = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumResp = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_resp[y]
                    sumResp.append(s)

            # Calculate liver HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[1])
            if np.sum(organval) == 0:
                sumLive = [0] * ngroups
            else:
                a_live = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumLive = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_live[y]
                    sumLive.append(s)

            # Calculate neuro HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[2])
            if np.sum(organval) == 0:
                sumNeur = [0] * ngroups
            else:
                a_neur = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumNeur = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_neur[y]
                    sumNeur.append(s)

            # Calculate dev HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[3])
            if np.sum(organval) == 0:
                sumDeve = [0] * ngroups
            else:
                a_deve = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumDeve = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_deve[y]
                    sumDeve.append(s)

            # Calculate reprod HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[4])
            if np.sum(organval) == 0:
                sumRepr = [0] * ngroups
            else:
                a_repr = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumRepr = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_repr[y]
                    sumRepr.append(s)

            # Calculate kidney HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[5])
            if np.sum(organval) == 0:
                sumKidn = [0] * ngroups
            else:
                a_kidn = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumKidn = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_kidn[y]
                    sumKidn.append(s)

            # Calculate ocular HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[6])
            if np.sum(organval) == 0:
                sumOcul = [0] * ngroups
            else:
                a_ocul = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumOcul = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_ocul[y]
                    sumOcul.append(s)

            # Calculate endoc HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[7])
            if np.sum(organval) == 0:
                sumEndo = [0] * ngroups
            else:
                a_endo = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumEndo = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_endo[y]
                    sumEndo.append(s)

            # Calculate hemato HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[8])
            if np.sum(organval) == 0:
                sumHema = [0] * ngroups
            else:
                a_hema = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumHema = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_hema[y]
                    sumHema.append(s)

            # Calculate immune HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[9])
            if np.sum(organval) == 0:
                sumImmu = [0] * ngroups
            else:
                a_immu = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumImmu = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_immu[y]
                    sumImmu.append(s)

            # Calculate skeletal HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[10])
            if np.sum(organval) == 0:
                sumSkel = [0] * ngroups
            else:
                a_skel = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumSkel = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_skel[y]
                    sumSkel.append(s)

            # Calculate spleen HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[11])
            if np.sum(organval) == 0:
                sumSple = [0] * ngroups
            else:
                a_sple = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumSple = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_sple[y]
                    sumSple.append(s)

            # Calculate thyroid HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[12])
            if np.sum(organval) == 0:
                sumThyr = [0] * ngroups
            else:
                a_thyr = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumThyr = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_thyr[y]
                    sumThyr.append(s)

            # Calculate whole body HI
            organval = np.array(list(zip(*outer_concs3['organ_list']))[13])
            if np.sum(organval) == 0:
                sumWhol = [0] * ngroups
            else:
                a_whol = self.calculateHI(outer_concs3['conc'].values, outer_concs3['invrfc'].values, 
                                          organval)
                sumWhol = []
                for x in range(ngroups):
                    idxstart = x * grouplen
                    idxend = idxstart + grouplen
                    s = 0
                    for y in range(idxstart, idxend):
                        s = s + a_whol[y]
                    sumWhol.append(s)
                        

            #----------- Build Outer receptor DF risks by lat/lon for later use in BlockSummaryChronic ----------------
            
            tempagg0 = pd.DataFrame(latlon_uniqlist, columns=[lat, lon])
            uniqcoors = outer_concs3[[lat, lon, overlap, elev, rec_id, utme, utmn, hill, population]].drop_duplicates()
            
            tempagg = pd.merge(tempagg0, uniqcoors, on=[lat,lon])
            tempagg[mir] = sumMir
            tempagg[hi_resp] = sumResp
            tempagg[hi_live] = sumLive
            tempagg[hi_neur] = sumNeur
            tempagg[hi_deve] = sumDeve
            tempagg[hi_repr] = sumRepr
            tempagg[hi_kidn] = sumKidn
            tempagg[hi_ocul] = sumOcul
            tempagg[hi_endo] = sumEndo
            tempagg[hi_hema] = sumHema
            tempagg[hi_immu] = sumImmu
            tempagg[hi_skel] = sumSkel
            tempagg[hi_sple] = sumSple
            tempagg[hi_thyr] = sumThyr
            tempagg[hi_whol] = sumWhol
                        
            blksumm_cols = [lat, lon, overlap, elev, rec_id, utme, utmn, hill, population,
                            mir, hi_resp, hi_live, hi_neur, hi_deve, hi_repr, hi_kidn, hi_ocul,
                            hi_endo, hi_hema, hi_immu, hi_skel, hi_sple, hi_thyr, hi_whol]


            
            if self.outerAgg is None:
                self.outerAgg = pd.DataFrame(columns=blksumm_cols)
            self.outerAgg = self.outerAgg.append(tempagg, sort=False)


            #----------- Keep track of maximum risk and HI ---------------------------------------

            # Find max mir and each max HI for Outer receptors in this box. Update the max_riskhi and
            # max_riskhi_bkdn dictionaries.
                        
            for iparm in self.riskhi_parms:
                idx = tempagg[iparm].idxmax()
                if tempagg[iparm].loc[idx] > self.max_riskhi[iparm][2]:
                    # Update the  max_riskhi dictionary
                    maxlat = tempagg[lat].loc[idx]
                    maxlon = tempagg[lon].loc[idx]
                    self.max_riskhi[iparm] = [maxlat, maxlon, tempagg[iparm].loc[idx]]
                    # Update the max_riskhi_bkdn dictionary
                    parmvalue = tempagg[iparm].loc[idx]
                    batch_receptors_max = outer_concs3[(outer_concs3[lat]==maxlat) & (outer_concs3[lon]==maxlon)]
                    for index, row in batch_receptors_max.iterrows():
                        self.max_riskhi_bkdn[(iparm, row[source_id], row[pollutant], row['emis_type'])] = \
                            [maxlat, maxlon, parmvalue]


            #--------------- Keep track of incidence -----------------------------------------
            
            # Compute incidence for each Outer rececptor and then sum incidence by source_id/pollutant/emis_type
            
            outer_concs3.sort_values(by=[source_id, pollutant, 'emis_type'], inplace=True)
            a_mirbysrc = self.calculateMir(outer_concs3['conc'].values, outer_concs3['ure'].values)
            
            groups = outer_concs3[[source_id, pollutant, 'emis_type']].values
            unique_groups = [list(item) for item in set(tuple(row) for row in groups)]
            # sort unique_groups by source_id, pollutant, and emis_type
            unique_groups.sort(key=operator.itemgetter(0,1,2))
            ngroups = len(unique_groups)
            grouplen = int(nouter / ngroups)

            a_inc = a_mirbysrc * outer_concs3[population].values /70
            sumInc = []
            for x in range(ngroups):
                idxstart = x * grouplen
                idxend = idxstart + grouplen
                s = 0
                for y in range(idxstart, idxend):
                    s = s + a_inc[y]
                sumInc.append(s)
                
            batchInc = pd.DataFrame(unique_groups, columns=[source_id, pollutant, 'emis_type'])
            batchInc['inc'] = sumInc

            # Update the outerInc incidence dictionary
            for incdx, incrow in batchInc.iterrows():
                self.outerInc[(incrow[source_id], incrow[pollutant], incrow['emis_type'])] = \
                    self.outerInc[(incrow[source_id], incrow[pollutant], incrow['emis_type'])] + incrow['inc']


    def calculateMir(self, conc, ure):
        cancer_risk = conc * ure
        return cancer_risk
    
    def calculateHI(self, conc, invrfc, organ):
        aHI = conc * (invrfc/1000) * organ
        return aHI


    def createDataframe(self):
        # Type setting for CSV reading
        if self.acute_yn == 'N':
            self.numericColumns = [lat, lon, conc, elev, population]
        else:
            self.numericColumns = [lat, lon, conc, aconc, elev, population]

        self.strColumns = [rec_id, source_id, emis_type, pollutant, overlap]

        df = self.readFromPathCsv(self.getColumns())
        return df.fillna("")

