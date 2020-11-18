from com.sca.hem4.upload.InputFile import InputFile
from com.sca.hem4.model.Model import *

surfcity = 'surfcity'
surffile = 'surffile'
surfwban = 'surfwban'
surfyear = 'surfyear'
surflat = 'surflat'
surflon = 'surflon'
uacity = 'uacity'
uawban = 'uawban'
ualat = 'ualat'
ualon = 'ualon'
elev = 'elev'
anemhgt = 'anemhgt'
commkey = 'commkey'
comment = 'comment'
desc = 'desc'
upperfile = 'upperfile'

class MetLib(InputFile):

    def __init__(self):
        InputFile.__init__(self, "resources/metlib_aermod.xlsx")

    def createDataframe(self):

        # Specify dtypes for all fields
        self.numericColumns = [surfyear, surflat, surflon, ualat, ualon, elev, anemhgt]
        self.strColumns = [surfwban, surfcity, surffile, uawban, uacity, upperfile]

        # HEADER----------------------
        self.dataframe = self.readFromPath(
            (surfcity,surffile,surfwban,surfyear,surflat, surflon,uacity,uawban,ualat,ualon,elev,anemhgt,upperfile))

        self.dataframe.fillna(0, inplace=True)
