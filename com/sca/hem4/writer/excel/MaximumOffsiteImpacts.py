from math import log10, floor
from com.sca.hem4.writer.csv.RingSummaryChronic import *
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter
from com.sca.hem4.FacilityPrep import *
from math import log10, floor

from com.sca.hem4.writer.csv.RingSummaryChronic import *
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter


class MaximumOffsiteImpacts(ExcelWriter):
    """
    Provides the maximum cancer risk and all 14 TOSHIs at any receptor, either a populated (census block, user defined)
    or an unpopulated (polar grid) receptor, as well as additional receptor information.
    """

    def __init__(self, targetDir, facilityId, model, plot_df, ring_summary_chronic_df, inner_recep_risk_df):
        ExcelWriter.__init__(self, model, plot_df)

        self.filename = os.path.join(targetDir, facilityId + "_maximum_offsite_impacts.xlsx")
        self.ring_summary_chronic_df = ring_summary_chronic_df
        self.inner_recep_risk_df = inner_recep_risk_df


    def getHeader(self):
        return ['Parameter', 'Value', 'Value_rnd', 'Value_sci', 'Population', 'Distance (in meters)',
                'Angle (from north)', 'Elevation (in meters)', 'Hill Height (in meters)', 'Fips', 'Block',
                'Utm_east', 'Utm_north', 'Latitude', 'Longitude', 'Rec_type', 'Notes']

    def generateOutputs(self):
        """
        Max offsite impacts occur at non-overlapped inner or polar receptors with the highest value. Outer
        receptors are not checked because they cannot be higher than any polar receptor.
        """
        
        # dictionary of receptor types and notes
        rectype_dict = {"PG":"Polar grid", "D":"Census block", "I":"Census block"}
        notes_dict = {"PG":"Polar", "D":"Discrete", "I":"Interpolated"}

        ring_risk = self.ring_summary_chronic_df.copy()
        inner_risk = self.inner_recep_risk_df.copy()

        # add population and recype columns into ring risk df
        ring_risk[population] = 0
        ring_risk[blk_type] = "PG"

        # add distance and angle from the inner blocks df to the inner risk df
        if self.model.innerblks_df.empty == False:
            innrisk = pd.merge(inner_risk, self.model.innerblks_df[[lat, lon, distance, angle]],
                               on=[lat, lon])
        else:
            innrisk = inner_risk
            innrisk[distance] = None
            innrisk[angle] = None

        # append ring risk to inner risk to make one risk df
        allrisk = innrisk.append(ring_risk, sort=True).reset_index(drop=True).infer_objects().fillna('')

        # find max offsite receptor info for mir and all 14 HIs
        moilist = []
        parmdict = {'mir':'Cancer risk', 'hi_resp':'Respiratory HI', 'hi_live':'Liver HI', 'hi_neur':'Neurological HI',
                    'hi_deve':'Developmental HI', 'hi_repr':'Reproductive HI', 'hi_kidn':'Kidney HI',
                    'hi_ocul':'Ocular HI', 'hi_endo':'Endorcrine HI', 'hi_hema':'Hematological HI',
                    'hi_immu':'Immunological HI', 'hi_skel':'Skeletal HI', 'hi_sple':'Spleen HI',
                    'hi_thyr':'Thyroid HI', 'hi_whol':'Whole body HI'}
        for parm in parmdict:
            io_idx = allrisk[allrisk[overlap] != "Y"][parm].idxmax()
            moi_parm = parmdict[parm]
            moi_value = float(allrisk[parm].loc[io_idx])
            if moi_value > 0:
                # TODO keep 2 significant figures for rounded value
                #moi_value_rnd = round(moi_value, -int(math.floor(math.log10(moi_value))) + 1) if moi_value > 0 else 0
                moi_value_rnd = round(moi_value, -int(floor(log10(abs(moi_value))))) if moi_value > 0 else 0
                moi_value_sci = format(moi_value, ".1e")
                moi_pop = allrisk[population].loc[io_idx]
                moi_dist = float(allrisk[distance].loc[io_idx])
                moi_angle = float(allrisk[angle].loc[io_idx])
                moi_elev = float(allrisk[elev].loc[io_idx])
                moi_hill = float(allrisk[hill].loc[io_idx])
                moi_fips = allrisk[fips].loc[io_idx]
                moi_block = allrisk[block].loc[io_idx]
                moi_utme = float(allrisk[utme].loc[io_idx])
                moi_utmn = float(allrisk[utmn].loc[io_idx])
                moi_lat = float(allrisk[lat].loc[io_idx])
                moi_lon = float(allrisk[lon].loc[io_idx])
                if "U" not in moi_block:
                    moi_rectype = rectype_dict[allrisk[blk_type].loc[io_idx]]
                    moi_notes = notes_dict[allrisk[blk_type].loc[io_idx]]
                else:
                    moi_rectype = "User receptor"
                    moi_notes = "Discrete"
            else:
                moi_value_rnd = 0
                moi_value_sci = 0
                moi_pop = 0
                moi_dist = 0
                moi_angle = 0
                moi_elev = 0
                moi_hill = 0
                moi_fips = ""
                moi_block = ""
                moi_utme = 0
                moi_utmn = 0
                moi_lat = 0
                moi_lon = 0
                moi_rectype = ""
                moi_notes = ""
            moi_row = [moi_parm, moi_value, moi_value_rnd, moi_value_sci, moi_pop, moi_dist, moi_angle,
                       moi_elev, moi_hill, moi_fips, moi_block, moi_utme, moi_utmn,
                       moi_lat, moi_lon, moi_rectype, moi_notes]
            moilist.append(moi_row)


        moilist_df = pd.DataFrame(moilist)

        self.dataframe = moilist_df
        self.data = self.dataframe.values
        yield self.dataframe