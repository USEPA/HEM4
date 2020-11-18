import pandas as pd

from com.sca.hem4.FacilityPrep import *
from com.sca.hem4.model.Model import *
from com.sca.hem4.writer.excel.ExcelWriter import ExcelWriter
from com.sca.hem4.writer.excel.Incidence import inc

parameter = 'parameter';
value = 'value';
value_rnd = 'value_rnd';
value_sci = 'value_sci';
notes = 'notes';

class FacilityMaxRiskandHINonCensus(ExcelWriter):
    """
    Provides a listing of the facilities by ID, their lat/lons and the population
    exposed to different cancer risk levels at each facility.
    """

    def __init__(self, targetDir, facilityId, model, plot_df, incidence):
        ExcelWriter.__init__(self, model, plot_df)

        if self.model.group_name != None:
            outfile = self.model.group_name + "_facility_max_risk_and_hi.xlsx"
        else:
            outfile = "facility_max_risk_and_hi.xlsx"
        self.filename = os.path.join(targetDir, outfile)
        self.facilityId = facilityId
        self.header = None
        self.incidence = incidence

    def getHeader(self):
        self.header = ['Facil_id', 'mx_can_rsk', 'can_rsk_interpltd', 'can_rcpt_type', 'can_latitude', 'can_longitude',
                       'can_recid', 'respiratory_hi', 'resp_hi_interpltd', 'resp_rcpt_type', 'resp_latitude', 'resp_longitude',
                       'resp_recid', 'liver_hi', 'liver_hi_interpltd', 'liver_rcpt_type', 'liver_recid', 'neurological_hi',
                       'neuro_hi_interpltd', 'neuro_rcpt_type', 'neuro_latitude', 'neuro_longitude', 'neuro_recid',
                       'developmental_hi', 'devel_hi_interpltd', 'devel_rcpt_type', 'devel_recid', 'reproductive_hi',
                       'repro_hi_interptd', 'repro_rcpt_type', 'repro_recid', 'kidney_hi', 'kidney_hi_interptd',
                       'kidney_rcpt_type', 'kidney_recid', 'ocular_hi', 'ocular_hi_interptd', 'ocular_rcpt_type', 'ocular_recid',
                       'endocrine_hi', 'endo_hi_interptd', 'endo_rcpt_type', 'endo_recid', 'hematological_hi',
                       'hema_hi_interptd', 'hema_rcpt_type', 'hema_recid', 'immunological_hi', 'immun_hi_interptd',
                       'immun_rcpt_type', 'immun_recid', 'skeletal_hi', 'skel_hi_interptd', 'skel_rcpt_type', 'skel_recid',
                       'spleen_hi', 'spleen_hi_interptd', 'spleen_rcpt_type', 'spleen_recid', 'thyroid_hi',
                       'thyroid_hi_interptd', 'thyroid_rcpt_type', 'thyroid_recid', 'whole_body_hi', 'whole_hi_interptd',
                       'whole_rcpt_type', 'whole_recid', 'pop_overlp', 'incidence', 'metname', 'km_to_metstation',
                       'fac_center_latitude', 'fac_center_longitude', 'rural_urban']
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
                    riskrow.extend([row[lat], row[lon]])

                riskrow.append(row[rec_id])

            # Population that is overlapped
            inncnt = self.model.innerblks_df['population'].loc[self.model.innerblks_df['overlap'] == 'Y'].sum()
            if not self.model.outerblks_df.empty:
                outcnt = self.model.outerblks_df['population'].loc[self.model.outerblks_df['overlap'] == 'Y'].sum()
                ovlpcnt = inncnt + outcnt
            else:
                ovlpcnt = inncnt
            riskrow.append(ovlpcnt)

            riskrow.append(self.incidence.iloc[0][inc])
            riskrow.append(self.model.computedValues['metfile'])
            riskrow.append(self.model.computedValues[distance])
            riskrow.append(self.model.computedValues['cenlat'])
            riskrow.append(self.model.computedValues['cenlon'])

            # rural or urban
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