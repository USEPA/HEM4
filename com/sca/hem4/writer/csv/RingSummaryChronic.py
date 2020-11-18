from com.sca.hem4.FacilityPrep import *
from com.sca.hem4.writer.csv.BlockSummaryChronic import *
from com.sca.hem4.writer.csv.CsvWriter import CsvWriter

class RingSummaryChronic(CsvWriter, InputFile):
    """
    Provides the risk and each TOSHI for every census block modeled, as well as additional block information.
    """

    def __init__(self, targetDir=None, facilityId=None, model=None, plot_df=None,
                 filenameOverride=None, createDataframe=False):
        # Initialization for CSV reading/writing. If no file name override, use the
        # default construction.
        self.targetDir = targetDir
        filename = facilityId + "_ring_summary_chronic.csv" if filenameOverride is None else filenameOverride
        path = os.path.join(self.targetDir, filename)

        CsvWriter.__init__(self, model, plot_df)
        InputFile.__init__(self, path, createDataframe)

        self.filename = path

        # Local cache for URE/RFC values
        self.riskCache = {}

        # Local cache for organ endpoint values
        self.organCache = {}

    def getHeader(self):
        return ['Latitude', 'Longitude', 'Overlap', 'Elevation (m)', 'X', 'Y', 'Hill', 'MIR',
                'Respiratory HI', 'Liver HI', 'Neurological HI', 'Developmental HI', 'Reproductive HI',
                'Kidney HI', 'Ocular HI', 'Endocrine HI', 'Hematological HI', 'Immunological HI', 'Skeletal HI',
                'Spleen HI', 'Thyroid HI', 'Whole body HI', 'Distance (m)', 'Angle (from north)', 'Sector']

    def getColumns(self):
        return [lat, lon, overlap, elev, utme, utmn, hill, mir, hi_resp, hi_live, hi_neur,
                hi_deve, hi_repr, hi_kidn, hi_ocul, hi_endo, hi_hema, hi_immu, hi_skel,
                hi_sple, hi_thyr, hi_whol, distance, angle, sector]

    def generateOutputs(self):
        """
        Note that this implementation does NOT use the plot file data, because the polar receptor concentrations have
        already been processed and stored in the model (see model.all_polar_receptors)
        """

        allpolar_df = self.model.all_polar_receptors_df.copy()

        # join with the polar grid df and then select columns
        columns = [pollutant, conc, lat, lon, overlap, elev, utme, utmn, hill, distance, angle, sector]
        merged = allpolar_df.merge(self.model.polargrid, on=[lat, lon, elev, distance, angle, sector, overlap])[columns]
        
        # get a URE value for each row, by joining with the dose response library (on pollutant)
        merged[[mir, hi_resp, hi_live, hi_neur, hi_deve, hi_repr, hi_kidn, hi_ocul
            , hi_endo, hi_hema, hi_immu, hi_skel, hi_sple, hi_thyr, hi_whol]] = \
            merged.apply(lambda row: self.calculateRisks(row[pollutant], row[conc]), axis=1)

        # last step: group by lat,lon and then aggregate each group by summing the mir and hazard index fields
        aggs = {pollutant:'first', lat:'first', lon:'first', overlap:'first', elev:'first', utme:'first',
                utmn:'first', hill:'first', conc:'first', distance:'first', angle:'first',
                sector:'first', mir:'sum', hi_resp:'sum', hi_live:'sum', hi_neur:'sum', hi_deve:'sum',
                hi_repr:'sum', hi_kidn:'sum', hi_ocul:'sum', hi_endo:'sum', hi_hema:'sum',
                hi_immu:'sum', hi_skel:'sum', hi_sple:'sum', hi_thyr:'sum', hi_whol:'sum'}

        newcolumns = self.getColumns()

        self.dataframe = merged.groupby([lat, lon]).agg(aggs)[newcolumns].sort_values(by=[sector, distance])
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
                # Couldn't find the pollutant...set values to 0
                listed = []
            else:
                listed = row.values.tolist()

            # Note: sometimes there is a pollutant with no effect on any organ (RFC == 0). In this case it will
            # not appear in the organs library, and therefore 'listed' will be empty. We will just assign a
            # dummy list in this case...
            organs = listed[0] if len(listed) > 0 else list(range(16))
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

        lat, lon, overlap, elev, utme, utmn, hill, mir, hi_resp, hi_live, hi_neur,
        hi_deve, hi_repr, hi_kidn, hi_ocul, hi_endo, hi_hema, hi_immu, hi_skel,
        hi_sple, hi_thyr, hi_whol, distance, angle, sector


        self.numericColumns = [lat, lon, elev, utme, utmn, hill, mir, hi_resp, hi_live, hi_neur,
                               hi_deve, hi_repr, hi_kidn, hi_ocul, hi_endo, hi_hema, hi_immu, hi_skel,
                               hi_sple, hi_thyr, hi_whol, distance, angle, sector]
        self.strColumns = [overlap]
        df = self.readFromPathCsv(self.getColumns())
        return df.fillna("")

