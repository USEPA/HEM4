from com.sca.hem4.FacilityPrep import *
from com.sca.hem4.upload.InputFile import InputFile
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter

parameter = 'parameter';
value = 'value';
value_rnd = 'value_rnd';
value_sci = 'value_sci';
notes = 'notes';

class FacilityMaxRiskandHI(ExcelWriter, InputFile):
    """
    Provides a listing of the facilities by ID, their lat/lons and the population 
    exposed to different cancer risk levels at each facility
    """

    def __init__(self, targetDir=None, facilityId=None, model=None, plot_df=None, filenameOverride=None,
                 createDataframe=False, incidence=None):
        ExcelWriter.__init__(self, model, plot_df)

        # Initialization for file reading/writing. If no file name override, use the
        # default construction.
        if self.model is not None and self.model.group_name is not None:
            outfile = self.model.group_name + "_facility_max_risk_and_hi.xlsx"
        else:
            outfile = "facility_max_risk_and_hi.xlsx"

        filename = outfile if filenameOverride is None else filenameOverride
        path = os.path.join(targetDir, filename)


        InputFile.__init__(self, path, createDataframe)

        self.filename = path
        self.targetDir = targetDir

        self.facilityId = facilityId
        self.header = None
        self.incidence = incidence

    def getHeader(self):
        self.header = ['Facil_id', 'mx_can_rsk', 'can_rsk_interpltd', 'can_rcpt_type', 'can_latitude', 'can_longitude',
                       'can_blk', 'respiratory_hi', 'resp_hi_interpltd', 'resp_rcpt_type', 'resp_latitude', 'resp_longitude',
                       'resp_blk', 'liver_hi', 'liver_hi_interpltd', 'liver_rcpt_type', 'liver_blk', 'neurological_hi',
                       'neuro_hi_interpltd', 'neuro_rcpt_type', 'neuro_latitude', 'neuro_longitude', 'neuro_blk',
                       'developmental_hi', 'devel_hi_interpltd', 'devel_rcpt_type', 'devel_blk', 'reproductive_hi',
                       'repro_hi_interptd', 'repro_rcpt_type', 'repro_blk', 'kidney_hi', 'kidney_hi_interptd',
                       'kidney_rcpt_type', 'kidney_blk', 'ocular_hi', 'ocular_hi_interptd', 'ocular_rcpt_type', 'ocular_blk',
                       'endocrine_hi', 'endo_hi_interptd', 'endo_rcpt_type', 'endo_blk', 'hematological_hi',
                       'hema_hi_interptd', 'hema_rcpt_type', 'hema_blk', 'immunological_hi', 'immun_hi_interptd',
                       'immun_rcpt_type', 'immun_blk', 'skeletal_hi', 'skel_hi_interptd', 'skel_rcpt_type', 'skel_blk',
                       'spleen_hi', 'spleen_hi_interptd', 'spleen_rcpt_type', 'spleen_blk', 'thyroid_hi',
                       'thyroid_hi_interptd', 'thyroid_rcpt_type', 'thyroid_blk', 'whole_body_hi', 'whole_hi_interptd',
                       'whole_rcpt_type', 'whole_blk', 'pop_overlp', 'incidence', 'metname', 'km_to_metstation',
                       'fac_center_latitude', 'fac_center_longitude', 'rural_urban']
        return self.header

    def getColumns(self):
        return self.header

    def writeWithoutHeader(self):
        for data in self.generateOutputs():
            if data is not None:
                self.appendToFile(data)

    def generateOutputs(self):

        # Simply take the indiv max risks data frame, do some gymnastics, and yield it as the next
        # record to be appended.
        facrisk_df = pd.DataFrame()

        if self.model.max_indiv_risk_df is not None:
            maxrisk = self.model.max_indiv_risk_df
            risklist = []
            riskrow = [self.facilityId]
            for i in range(0,15):
                row = maxrisk.iloc[i]
                interpolated = 'Y' if row['notes'] == 'Interpolated' else 'N'
                riskrow.extend([row['value'], interpolated, row['rec_type']])

                # Note: respiratory and neuro have additional lat/lon columns
                if i in (0, 1, 3):
                    riskrow.extend([row['lat'], row['lon']])

                riskrow.append(row['block'])

            # Population that is overlapped
            inncnt = self.model.innerblks_df['population'].loc[self.model.innerblks_df['overlap'] == 'Y'].sum()
            if not self.model.outerblks_df.empty:
                outcnt = self.model.outerblks_df['population'].loc[self.model.outerblks_df['overlap'] == 'Y'].sum()
                ovlpcnt = inncnt + outcnt
            else:
                ovlpcnt = inncnt
            riskrow.append(ovlpcnt)

            riskrow.append(self.incidence.iloc[0]['inc'])
            riskrow.append(self.model.computedValues['metfile'])
            riskrow.append(self.model.computedValues['distance'])
            riskrow.append(self.model.computedValues['cenlat'])
            riskrow.append(self.model.computedValues['cenlon'])

            # urban or rural
            if self.model.model_optns['urban'] == True:
                ur = "U"
            else:
                ur = "R"
            riskrow.append(ur)
            
            risklist.append(riskrow)
            facrisk_df = pd.DataFrame(risklist)

        self.dataframe = facrisk_df
        self.data = self.dataframe.values
        yield self.dataframe

    def createDataframe(self):
        # Type setting for XLS reading
        self.numericColumns = ['mx_can_rsk', 'can_latitude', 'can_longitude', 'respiratory_hi', 'resp_latitude',
                               'resp_longitude', 'liver_hi', 'neurological_hi', 'neuro_latitude', 'neuro_longitude',
                               'developmental_hi', 'reproductive_hi', 'kidney_hi', 'ocular_hi', 'endocrine_hi',
                               'hematological_hi', 'immunological_hi', 'skeletal_hi', 'spleen_hi', 'thyroid_hi',
                               'whole_body_hi', 'incidence', 'km_to_metstation', 'fac_center_latitude',
                               'fac_center_longitude', ]
        self.strColumns = ['Facil_id', 'can_rsk_interpltd', 'can_rcpt_type', 'can_blk', 'resp_hi_interpltd',
                           'resp_rcpt_type', 'resp_blk', 'liver_hi_interpltd', 'liver_rcpt_type', 'liver_blk',
                           'neuro_hi_interpltd', 'neuro_rcpt_type', 'neuro_blk', 'devel_hi_interpltd',
                           'devel_rcpt_type', 'devel_blk', 'repro_hi_interptd', 'repro_rcpt_type', 'repro_blk',
                           'kidney_hi_interptd', 'kidney_rcpt_type', 'kidney_blk', 'ocular_hi_interptd',
                           'ocular_rcpt_type', 'ocular_blk', 'endo_hi_interptd', 'endo_rcpt_type', 'endo_blk',
                           'hema_hi_interptd', 'hema_rcpt_type', 'hema_blk', 'immun_hi_interptd',
                           'immun_rcpt_type', 'immun_blk', 'skel_hi_interptd', 'skel_rcpt_type', 'skel_blk',
                           'spleen_hi_interptd', 'spleen_rcpt_type', 'spleen_blk', 'thyroid_hi_interptd',
                           'thyroid_rcpt_type', 'thyroid_blk', 'whole_hi_interptd', 'whole_rcpt_type', 'whole_blk',
                           'pop_overlp', 'metname', 'rural_urban']

        df = self.readFromPath(self.getColumns())
        return df.fillna("")