from com.sca.hem4.upload.UserReceptors import rec_type
from com.sca.hem4.writer.csv.AllOuterReceptors import *
from com.sca.hem4.FacilityPrep import *

blk_type = 'blk_type';


class BlockSummaryChronicNonCensus(CsvWriter, InputFile):
    """
    Provides the risk and each TOSHI for every census block modeled, as well as additional block information.
    """

    def __init__(self, targetDir=None, facilityId=None, model=None, plot_df=None,
                 filenameOverride=None, createDataframe=False, outerAgg=None):
        # Initialization for CSV reading/writing. If no file name override, use the
        # default construction.
        self.targetDir = targetDir
        filename = facilityId + "_block_summary_chronic.csv" if filenameOverride is None else filenameOverride
        path = os.path.join(self.targetDir, filename)

        CsvWriter.__init__(self, model, plot_df)
        InputFile.__init__(self, path, createDataframe)

        self.filename = path

        # Local cache for URE/RFC values
        self.riskCache = {}

        # Local cache for organ endpoint values
        self.organCache = {}

        self.outerAgg = outerAgg

    def getHeader(self):
        return ['Latitude', 'Longitude', 'Overlap', 'Elevation (m)', 'Receptor ID', 'X', 'Y', 'Hill',
                'Population', 'MIR', 'Respiratory HI', 'Liver HI', 'Neurological HI', 'Developmental HI',
                'Reproductive HI', 'Kidney HI', 'Ocular HI', 'Endocrine HI', 'Hematological HI',
                'Immunological HI', 'Skeletal HI', 'Spleen HI', 'Thyroid HI', 'Whole body HI', 'Discrete/Interpolated Receptor']

    def getColumns(self):
        return [lat, lon, overlap, elev, rec_id, utme, utmn, hill, population,
                mir, hi_resp, hi_live, hi_neur, hi_deve, hi_repr, hi_kidn, hi_ocul,
                hi_endo, hi_hema, hi_immu, hi_skel, hi_sple, hi_thyr, hi_whol, blk_type]

    def generateOutputs(self):
        """
        plot_df is not needed. Instead, the allinner and allouter receptor
        outputs are used to compute cancer risk and HI's at each block receptor.
        """
        
        allinner_df = self.model.all_inner_receptors_df.copy()

        innerblocks = self.model.innerblks_df[[lat, lon, utme, utmn, hill]]

        # join inner receptor df with the inner block df and then select columns
        columns = [pollutant, conc, lat, lon, rec_id, overlap, elev,
                   utme, utmn, population, hill]
        innermerged = allinner_df.merge(innerblocks, on=[lat, lon])[columns]

        # compute cancer and noncancer values for each Inner rececptor row
        innermerged[[mir, hi_resp, hi_live, hi_neur, hi_deve, hi_repr, hi_kidn, hi_ocul, hi_endo,
                     hi_hema, hi_immu, hi_skel, hi_sple, hi_thyr, hi_whol]] = \
            innermerged.apply(lambda row: self.calculateRisks(row[pollutant], row[conc]), axis=1)

        # For the Inner and Outer receptors, group by lat,lon and then aggregate each group by summing the mir and hazard index fields
        aggs = {pollutant:'first', lat:'first', lon:'first', overlap:'first', elev:'first', utme:'first',
                utmn:'first', hill:'first', conc:'first', rec_id:'first', population:'first',
                mir:'sum', hi_resp:'sum', hi_live:'sum', hi_neur:'sum', hi_deve:'sum',
                hi_repr:'sum', hi_kidn:'sum', hi_ocul:'sum', hi_endo:'sum', hi_hema:'sum',
                hi_immu:'sum', hi_skel:'sum', hi_sple:'sum', hi_thyr:'sum', hi_whol:'sum'}

        newcolumns = [lat, lon, overlap, elev, rec_id, utme, utmn, hill, population,
                      mir, hi_resp, hi_live, hi_neur, hi_deve, hi_repr, hi_kidn, hi_ocul,
                      hi_endo, hi_hema, hi_immu, hi_skel, hi_sple, hi_thyr, hi_whol]

        inneragg = innermerged.groupby([lat, lon]).agg(aggs)[newcolumns]

        # add a receptor type column to note if discrete or interpolated. D => discrete, I => interpolated
        inneragg[blk_type] = "D"
        if self.outerAgg is not None:
            self.outerAgg[blk_type] = "I"

        # append the inner and outer values and write
        if self.outerAgg is not None:
            self.dataframe = inneragg.append(self.outerAgg, ignore_index = True).sort_values(by=[rec_id])
        else:
            self.dataframe = inneragg
            
        self.data = self.dataframe.values
        yield self.dataframe

    def calculateRisks(self, pollutant_name, conc):
        URE = None
        RFC = None

        # In order to get a case-insensitive exact match (i.e. matches exactly except for casing)
        # we are using a regex that is specified to be the entire value. Since pollutant names can
        # contain parentheses, escape them before constructing the pattern.
        pattern = '^' + re.escape(pollutant_name) + '$'

        # Since it's relatively expensive to get these values from their respective libraries, cache them locally.
        # Note that they are cached as a pair (i.e. if one is in there, the other one will be too...)
        if pollutant_name in self.riskCache:
            URE = self.riskCache[pollutant_name][ure]
            RFC = self.riskCache[pollutant_name][rfc]
        else:
            row = self.model.haplib.dataframe.loc[
                self.model.haplib.dataframe[pollutant].str.contains(pattern, case=False, regex=True)]

            if row.size == 0:
                URE = 0
                RFC = 0
            else:
                URE = row.iloc[0][ure]
                RFC = row.iloc[0][rfc]

            self.riskCache[pollutant_name] = {ure : URE, rfc : RFC}

        organs = None
        if pollutant_name in self.organCache:
            organs = self.organCache[pollutant_name]
        else:
            row = self.model.organs.dataframe.loc[
                self.model.organs.dataframe[pollutant].str.contains(pattern, case=False, regex=True)]

            if row.size == 0:
                # Couldn't find the pollutant...set values to 0 and log message
                listed = []
            else:
                listed = row.values.tolist()

            # Note: sometimes there is a pollutant with no effect on any organ (RFC == 0). In this case it will
            # not appear in the organs library, and therefore 'listed' will be empty. We will just assign a
            # dummy list in this case...
            organs = listed[0] if len(listed) > 0 else [0]*16
            self.organCache[pollutant_name] = organs

        risks = []
        MIR = conc * URE
        risks.append(MIR)

        # Note: indices 2-15 correspond to the organ response value columns in the organs library...
        for i in range(2, 16):
            hazard_index = (0 if RFC == 0 else (conc/RFC/1000)*organs[i])
            risks.append(hazard_index)
        return Series(risks)

    def createDataframe(self):
        # Type setting for CSV reading
        self.numericColumns = [lat, lon, elev, utme, utmn, population, hill, mir, hi_resp, hi_live, hi_neur, hi_deve,
                               hi_repr, hi_kidn, hi_ocul, hi_endo, hi_hema, hi_immu, hi_skel, hi_sple, hi_thyr, hi_whol]
        self.strColumns = [rec_id, overlap, blk_type]

        df = self.readFromPathCsv(self.getColumns())
        return df.fillna("")
