from com.sca.hem4.log import Logger
from com.sca.hem4.model.Model import pollutant
from com.sca.hem4.upload.DoseResponse import rfc
from com.sca.hem4.upload.InputFile import InputFile

epa_woe = 'epa_woe';
resp = 'resp';
liver = 'liver';
neuro = 'neuro';
dev = 'dev';
reprod = 'reprod';
kidney = 'kidney';
ocular = 'ocular';
endoc = 'endoc';
hemato = 'hemato';
immune = 'immune';
skeletal = 'skeletal';
spleen = 'spleen';
thyroid = 'thyroid';
wholebod = 'wholebod';

class TargetOrganEndpoints(InputFile):

    def __init__(self, haplib):
        self.haplib_df = haplib.dataframe
        InputFile.__init__(self, "resources/Target_Organ_Endpoints.xlsx")

    def clean(self, df):

        cleaned = df.fillna(0)

        # lower case the pollutant name for easier merging later
        cleaned[pollutant] = cleaned[pollutant].str.lower()

        return cleaned

    def validate(self, df):

        # ----------------------------------------------------------------------------------
        # Strict: Invalid values in these columns will cause the upload to fail immediately.
        # ----------------------------------------------------------------------------------
        duplicates = self.duplicates(df, [pollutant])
        if len(duplicates) > 0:
            Logger.logMessage("One or more records are duplicated in the Target Organs file (key=pollutant):")
            for d in duplicates:
                Logger.logMessage(d)

            Logger.logMessage("Please remove the duplicate records and restart HEM4.")
            return None
        else:
            # Verify that no non-cancer causing pollutants are missing
            haplib = self.haplib_df.loc[self.haplib_df[rfc] > 0]
            pollutants = set(haplib[pollutant])
            organ_pollutants = set(df[pollutant].unique())

            if not pollutants.issubset(organ_pollutants):
                Logger.logMessage("There are non-cancer causing pollutants in the Dose Response file that are not " +
                                  "present in the Target Organs file:")
                diff = pollutants - organ_pollutants
                for d in diff:
                    Logger.logMessage(d)
                Logger.logMessage("Please augment the Target Organs file with these pollutants and restart HEM4.")
                return None

            return df

    def createDataframe(self):

        # Specify dtypes for all fields
        self.numericColumns = [resp,liver,neuro,dev,reprod,kidney,ocular,endoc,hemato,
                               immune,skeletal,spleen,thyroid,wholebod]
        self.strColumns = [pollutant,epa_woe]

        # TARGET ORGAN ENDPOINTS excel to dataframe
        # HEADER----------------------
        # pollutant|epa_woe	|resp|liver|neuro|dev|reprod|kidney|ocular|endoc|hemato|immune|skeletal|spleen|thyroid|wholebod
        self.dataframe = self.readFromPath(
            (pollutant,epa_woe,resp,liver,neuro,dev,reprod,kidney,ocular,endoc,hemato,
             immune,skeletal,spleen,thyroid,wholebod))

