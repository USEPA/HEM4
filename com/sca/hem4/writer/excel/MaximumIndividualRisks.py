from com.sca.hem4.upload.UserReceptors import rec_type
from com.sca.hem4.writer.csv.BlockSummaryChronic import mir
from com.sca.hem4.writer.csv.AllInnerReceptors import *
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter
from com.sca.hem4.FacilityPrep import *
import pandas as pd
import numpy as np
from math import log10, floor
from com.sca.hem4.model.Model import *

parameter = 'parameter';
value = 'value';
value_rnd = 'value_rnd';
value_sci = 'value_sci';
notes = 'notes';
blk_type = 'blk_type';

class MaximumIndividualRisks(ExcelWriter, InputFile):
    """
    Provides the maximum cancer risk and all 14 TOSHIs at populated receptors, as well as additional receptor
    information.
    """

    def __init__(self, targetDir=None, facilityId=None, model=None, plot_df=None, filenameOverride=None,
                 createDataframe=False):

        # Initialization for file reading/writing. If no file name override, use the
        # default construction.
        filename = facilityId + "_maximum_indiv_risks.xlsx" if filenameOverride is None else filenameOverride
        path = os.path.join(targetDir, filename)

        ExcelWriter.__init__(self, model, plot_df)
        InputFile.__init__(self, path, createDataframe)

        self.filename = path

    def nodivby0(self, n, d):
        quotient = np.zeros(len(n))
        for i in np.arange(len(n)):
            if d[i] != 0:
                quotient[i] = n[i]/d[i]
            else:
                quotient[i] = 0
        return quotient

    def calcHI(self, hiname, hivar):
                
        mr_parameter = hiname
        io_idx = self.model.risk_by_latlon[((self.model.risk_by_latlon[rec_type] == "C") |
                                           (self.model.risk_by_latlon[rec_type] == "P")) &
                                           (self.model.risk_by_latlon['block'].str.contains('S')==False) &
                                           (self.model.risk_by_latlon['block'].str.contains('M')==False)][hivar].idxmax()
        mr_lat = float(self.model.risk_by_latlon[lat].loc[io_idx])
        mr_lon = float(self.model.risk_by_latlon[lon].loc[io_idx])
        if self.model.risk_by_latlon[overlap].loc[io_idx] == "N":
            #not overlapped
            mr_value = self.model.risk_by_latlon[hivar].loc[io_idx]
            mr_value_sci = format(mr_value, ".1e")
            # TODO keep 2 significant figures for rounded value
            #mr_value_rnd = round(mr_value, -int(floor(log10(mr_value))) + 1) if mr_value > 0 else 0
            mr_value_rnd = round(mr_value, -int(floor(log10(abs(mr_value))))) if mr_value > 0 else 0
            if self.model.risk_by_latlon[blk_type].loc[io_idx] == "D":
                mr_pop = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][population].values[0]
                mr_dist = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][distance].values[0]
                mr_angle = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][angle].values[0]
                mr_elev = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][elev].values[0]
                mr_hill = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][hill].values[0]
                mr_fips = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][fips].values[0]
                mr_block = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][idmarplot].values[0][-10:]
                mr_utme = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][utme].values[0]
                mr_utmn = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][utmn].values[0]
                if 'U' in self.model.risk_by_latlon['block'].loc[io_idx]:
                    mr_rectype = "User receptor"
                else:
                    mr_rectype = "Census block"
                mr_notes = "Discrete"
            else:
                mr_pop = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][population].values[0]
                mr_dist = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][distance].values[0]
                mr_angle = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][angle].values[0]
                mr_elev = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][elev].values[0]
                mr_hill = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][hill].values[0]
                mr_fips = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][fips].values[0]
                mr_block = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][idmarplot].values[0][-10:]
                mr_utme = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][utme].values[0]
                mr_utmn = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][utmn].values[0]
                mr_rectype = "Census block"
                mr_notes = "Interpolated"
        else:
            #overlapped
            iop_idx = self.model.risk_by_latlon[(self.model.risk_by_latlon[overlap] == "N") & 
                            ('S' not in self.model.risk_by_latlon[block]) & ('M' not in self.model.risk_by_latlon[block])][hivar].idxmax()
            mr_lat = float(self.model.risk_by_latlon[lat].loc[iop_idx])
            mr_lon = float(self.model.risk_by_latlon[lon].loc[iop_idx])
            mr_value = self.model.risk_by_latlon[hivar].loc[iop_idx]
            mr_value_sci = format(mr_value, ".1e")
            # TODO keep 2 significant figures for rounded value
            #mr_value_rnd = round(mr_value, -int(floor(log10(mr_value))) + 1) if mr_value > 0 else 0
            mr_value_rnd = round(mr_value, -int(floor(log10(abs(mr_value))))) if mr_value > 0 else 0
            if self.model.risk_by_latlon[blk_type].loc[iop_idx] == "PG":
                mr_pop = 0
                mr_dist = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][distance].values[0]
                mr_angle = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][angle].values[0]
                mr_elev = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][elev].values[0]
                mr_hill = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][hill].values[0]
                mr_fips = ""
                mr_block = ""
                mr_utme = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][utme].values[0]
                mr_utmn = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][utmn].values[0]
                mr_rectype = "Polar"
                mr_notes = "Overlapped source. Using polar receptor."
            elif self.model.risk_by_latlon[blk_type].loc[iop_idx] == "I":
                mr_pop = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][population].values[0]
                mr_dist = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][distance].values[0]
                mr_angle = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][angle].values[0]
                mr_elev = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][elev].values[0]
                mr_hill = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][hill].values[0]
                mr_fips = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][fips].values[0]
                mr_block = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][idmarplot].values[0][-10:]
                mr_utme = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][utme].values[0]
                mr_utmn = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][utmn].values[0]
                mr_rectype = "Census block"
                mr_notes = "Overlapped source. Using interpolated receptor."
            else:
                mr_pop = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][population].values[0]
                mr_dist = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][distance].values[0]
                mr_angle = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][angle].values[0]
                mr_elev = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][elev].values[0]
                mr_hill = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][hill].values[0]
                mr_fips = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][fips].values[0]
                mr_block = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][idmarplot].values[0][-10:]
                mr_utme = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][utme].values[0]
                mr_utmn = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][utmn].values[0]
                if 'U' in self.model.risk_by_latlon['block'].loc[iop_idx]:
                    mr_rectype = "User receptor"
                else:
                    mr_rectype = "Census block"
                mr_notes = "Overlapped source. Using discrete census or user receptor."

        return [mr_parameter, mr_value, mr_value_rnd, mr_value_sci, mr_pop, mr_dist, mr_angle, mr_elev,
                mr_hill, mr_fips, mr_block, mr_utme, mr_utmn, mr_lat, mr_lon, mr_rectype, mr_notes]


    def getHeader(self):
        return ['Parameter', 'Value', 'Value rounded', 'Value scientific notation', 'Population', 'Distance (m)',
                'Angle (from north)', 'Elevation (m)', 'Hill height (m)', 'FIPs', 'Block',
                'UTM easting', 'UTM northing', 'Latitude', 'Longitude', 'Receptor type', 'Notes']

    def getColumns(self):
        return [parameter, value, value_rnd, value_sci, population, distance, angle, elev,
                hill, fips, block, utme, utmn, lat, lon, rec_type, notes]

    def generateOutputs(self):
        """
        Find maximum risk/HI. First look at populated receptors then at polar receptors.
        Also set self.headers and self.data.
        """
        
        #construct dataframe that indicates if specific HI is present at the facility
        labels = ["Parmname", "Parmvar", "Status"]
        pollist = self.model.runstream_hapemis[pollutant].unique().tolist()
        pollist_df = pd.DataFrame({"pollutant":pollist})
        pollist_endpts = pd.merge(pollist_df,self.model.organs.dataframe[["pollutant","resp","liver","neuro","dev",
                                                                          "reprod","kidney","ocular","endoc","hemato",
                                                                          "immune","skeletal","spleen","thyroid",
                                                                          "wholebod"]],on="pollutant",how="left")
        pollist_endpts_cnt = pollist_endpts.sum(axis=0)
        histatus = []
        if pollist_endpts_cnt.loc["resp"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Respiratory HI", "hi_resp", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["liver"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Liver HI", "hi_live", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["neuro"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Neurological HI", "hi_neur", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["dev"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Developmental HI", "hi_deve", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["reprod"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Reproductive HI", "hi_repr", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["kidney"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Kidney HI", "hi_kidn", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["ocular"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Ocular HI", "hi_ocul", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["endoc"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Endocrine HI", "hi_endo", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["hemato"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Hematological HI", "hi_hema", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["immune"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Immunological HI", "hi_immu", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["skeletal"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Skeletal HI", "hi_skel", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["spleen"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Spleen HI", "hi_sple", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["thyroid"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Thyroid HI", "hi_thyr", status)
        histatus.append(statusdata)
        if pollist_endpts_cnt.loc["wholebod"] == 0:
            status = "N"
        else:
            status = "Y"
        statusdata = ("Whole body HI", "hi_whol", status)
        histatus.append(statusdata)

        histatus_df = pd.DataFrame.from_records(histatus, columns = labels)


        #initialize list that will hold max risk and HI info
        maxrisklist = []


        """
        Find the receptor with the max cancer risk and record info about that receptor.
        Algorithm is:
            1) Find max risk from populated receptors (inner or outer, but no school or monitor)
            2) If this receptor is not overlapped, use it
            3) If it is overlapped, find max from all receptors where overlap is N (no school or monitor)
            4) Get information about this receptor
        """
                
        mr_parameter = "Cancer risk"
        io_idx = self.model.risk_by_latlon[((self.model.risk_by_latlon[rec_type] == "C") |
                                           (self.model.risk_by_latlon[rec_type] == "P")) &
                                           (self.model.risk_by_latlon['block'].str.contains('S')==False) &
                                           (self.model.risk_by_latlon['block'].str.contains('M')==False)][mir].idxmax()
        if self.model.risk_by_latlon[mir].loc[io_idx] > 0:
            #max risk is > 0, do calculations
            mr_lat = float(self.model.risk_by_latlon[lat].loc[io_idx])
            mr_lon = float(self.model.risk_by_latlon[lon].loc[io_idx])
            if self.model.risk_by_latlon[overlap].loc[io_idx] == "N":
                #not overlapped
                mr_value = self.model.risk_by_latlon[mir].loc[io_idx]
                mr_value_sci = format(mr_value, ".1e")
                mr_value_rnd = round(mr_value, -int(floor(log10(abs(mr_value))))) if mr_value > 0 else 0
                if self.model.risk_by_latlon['blk_type'].loc[io_idx] == "D":
                    mr_pop = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][population].values[0]
                    mr_dist = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][distance].values[0]
                    mr_angle = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][angle].values[0]
                    mr_elev = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][elev].values[0]
                    mr_hill = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][hill].values[0]
                    mr_fips = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][fips].values[0]
                    mr_block = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][idmarplot].values[0][-10:]
                    mr_utme = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][utme].values[0]
                    mr_utmn = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][utmn].values[0]
                    if 'U' in self.model.risk_by_latlon['block'].loc[io_idx]:
                        mr_rectype = "User receptor"
                    else:
                        mr_rectype = "Census block"
                    mr_notes = "Discrete"
                else:
                    mr_pop = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][population].values[0]
                    mr_dist = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][distance].values[0]
                    mr_angle = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][angle].values[0]
                    mr_elev = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][elev].values[0]
                    mr_hill = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][hill].values[0]
                    mr_fips = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][fips].values[0]
                    mr_block = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][idmarplot].values[0][-10:]
                    mr_utme = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][utme].values[0]
                    mr_utmn = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][utmn].values[0]
                    if 'U' in self.model.risk_by_latlon['block'].loc[io_idx]:
                        mr_rectype = "User receptor"
                    else:
                        mr_rectype = "Census block"
                    mr_notes = "Interpolated"
            else:                
                #overlapped
                iop_idx = self.model.risk_by_latlon[(self.model.risk_by_latlon[overlap] == "N") & 
                                ('S' not in self.model.risk_by_latlon[block]) & ('M' not in self.model.risk_by_latlon[block])][mir].idxmax()
                mr_lat = float(self.model.risk_by_latlon[lat].loc[iop_idx])
                mr_lon = float(self.model.risk_by_latlon[lon].loc[iop_idx])
                mr_value = self.model.risk_by_latlon[mir].loc[iop_idx]
                mr_value_sci = format(mr_value, ".1e")
                mr_value_rnd = round(mr_value, -int(floor(log10(abs(mr_value)))))

                if self.model.risk_by_latlon[blk_type].loc[iop_idx] == "PG":
                    mr_pop = 0
                    mr_dist = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][distance].values[0]
                    mr_angle = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][angle].values[0]
                    mr_elev = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][elev].values[0]
                    mr_hill = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][hill].values[0]
                    mr_fips = ""
                    mr_block = ""
                    mr_utme = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][utme].values[0]
                    mr_utmn = self.model.polargrid[(self.model.polargrid[lon] == mr_lon) & (self.model.polargrid[lat] == mr_lat)][utmn].values[0]
                    mr_rectype = "Polar"
                    mr_notes = "Overlapped source. Using polar receptor."
                elif self.model.risk_by_latlon['blk_type'].loc[iop_idx] == "I":
                    mr_pop = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][population].values[0]
                    mr_dist = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][distance].values[0]
                    mr_angle = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][angle].values[0]
                    mr_elev = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][elev].values[0]
                    mr_hill = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][hill].values[0]
                    mr_fips = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][fips].values[0]
                    mr_block = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][idmarplot].values[0][-10:]
                    mr_utme = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][utme].values[0]
                    mr_utmn = self.model.outerblks_df[(self.model.outerblks_df[lon] == mr_lon) & (self.model.outerblks_df[lat] == mr_lat)][utmn].values[0]
                    if 'U' in self.model.risk_by_latlon['block'].loc[io_idx]:
                        mr_rectype = "User receptor"
                    else:
                        mr_rectype = "Census block"
                    mr_notes = "Overlapped source. Using interpolated receptor."
                else:
                    mr_pop = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][population].values[0]
                    mr_dist = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][distance].values[0]
                    mr_angle = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][angle].values[0]
                    mr_elev = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][elev].values[0]
                    mr_hill = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][hill].values[0]
                    mr_fips = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][fips].values[0]
                    mr_block = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][idmarplot].values[0][-10:]
                    mr_utme = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][utme].values[0]
                    mr_utmn = self.model.innerblks_df[(self.model.innerblks_df[lon] == mr_lon) & (self.model.innerblks_df[lat] == mr_lat)][utmn].values[0]
                    if 'U' in self.model.risk_by_latlon['block'].loc[io_idx]:
                        mr_rectype = "User receptor"
                    else:
                        mr_rectype = "Census block"
                    mr_notes = "Overlapped source. Using discrete census or user receptor."
        else:
            #max risk is 0, set all variables as empty
            mr_lat = 0
            mr_lon = 0
            mr_value = 0
            mr_value_rnd = 0
            mr_value_sci = 0
            mr_pop = 0
            mr_dist = 0
            mr_angle = 0
            mr_elev = 0
            mr_hill = 0
            mr_fips = ""
            mr_block = ""
            mr_utme = 0
            mr_utmn = 0
            mr_rectype = ""
            mr_notes = ""

        riskrow = [mr_parameter, mr_value, mr_value_rnd, mr_value_sci, mr_pop, mr_dist, mr_angle, mr_elev,
                   mr_hill, mr_fips, mr_block, mr_utme, mr_utmn, mr_lat, mr_lon, mr_rectype, mr_notes]
        maxrisklist.append(riskrow)


        """
        For each target organ HI, find the receptor with the highest HI and record
        info about that receptor. Not every target organ HI will exist, it depends
        on the pollutants emitted by this facility.
        """

        #set a default HI row
        defhirow = ["", 0, 0, 0, 0, 0, 0, 0, 0, "", "", 0, 0, 0, 0, "", ""]

        #iterate over histatus_df to see if a target organ HI exists
        for row in histatus_df.itertuples():
            if row.Status == "N":
                #no HI, use default info
                hirow = defhirow.copy()
                hirow[0] = row.Parmname
            else:
                #HI exists, find the receptor with max value
                hirow = self.calcHI(row.Parmname, row.Parmvar)

            maxrisklist.append(hirow)


        columns = self.getColumns()

        # Convert maxrisklist list to a dataframe and then output to self.data array
        maxrisk_df = pd.DataFrame(maxrisklist, columns=columns)

        self.dataframe = maxrisk_df
        self.data = self.dataframe.values
        yield self.dataframe

    def createDataframe(self):
        # Type setting for XLS reading
        self.numericColumns = [value, value_rnd, value_sci, population, distance, angle, elev, hill, utme, utmn, lat, lon]
        self.strColumns = [parameter, fips, block, rec_type, notes]

        df = self.readFromPath(self.getColumns())
        return df.fillna("")