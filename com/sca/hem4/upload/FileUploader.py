import os
import pandas as pd
from tkinter.filedialog import askopenfilename
from tkinter import messagebox

from com.sca.hem4.upload.DoseResponse import DoseResponse
from com.sca.hem4.upload.EmissionsLocations import EmissionsLocations
from com.sca.hem4.upload.FacilityList import FacilityList
from com.sca.hem4.upload.HAPEmissions import HAPEmissions
from com.sca.hem4.upload.MetLib import MetLib
from com.sca.hem4.upload.TargetOrganEndpoints import TargetOrganEndpoints
from com.sca.hem4.upload.UserReceptors import UserReceptors
from com.sca.hem4.upload.AltReceptors import AltReceptors
from com.sca.hem4.upload.Polyvertex import Polyvertex
from com.sca.hem4.upload.BuoyantLine import BuoyantLine
from com.sca.hem4.upload.Downwash import Downwash
from com.sca.hem4.upload.Particle import Particle
from com.sca.hem4.upload.LandUse import LandUse
from com.sca.hem4.upload.Seasons import Seasons
from com.sca.hem4.upload.GasParams import GasParams
from com.sca.hem4.upload.EmisVar import EmisVar


class FileUploader():

    def __init__(self, model):
        self.model = model

    def uploadLibrary(self, filetype):
        uploaded = None
        if filetype == "haplib":
            uploaded = DoseResponse()
            self.model.haplib = uploaded
        elif filetype == "organs":
            uploaded = TargetOrganEndpoints(self.model.haplib)
            self.model.organs = uploaded
        elif filetype == "metlib":
            uploaded = MetLib()
            self.model.metlib = uploaded
        elif filetype == "gas params":
            uploaded = GasParams()
            self.model.gasparams = uploaded

        return False if uploaded.dataframe.empty is True else True

    def upload(self, filetype, path):
        uploaded = None
        if filetype == "faclist":
            uploaded = FacilityList(path, self.model.metlib)
            self.model.faclist = uploaded
        elif filetype == "hapemis":
            uploaded = HAPEmissions(path, self.model.haplib, set(self.model.fac_ids))
            self.model.hapemis = uploaded
        elif filetype == "emisloc":
            uploaded = EmissionsLocations(path, self.model.hapemis, self.model.faclist, set(self.model.fac_ids))
            self.model.emisloc = uploaded
        elif filetype == "alt receptors":
            uploaded = AltReceptors(path)
            self.model.altreceptr = uploaded

        return False if uploaded.dataframe.empty is True else True

    def uploadDependent(self, filetype, path, dependency, facilities=None):
        uploaded = None

        if filetype == "polyvertex":
            uploaded = Polyvertex(path, dependency)
            self.model.multipoly = uploaded
        elif filetype == "buoyant line":
            uploaded = BuoyantLine(path, dependency)
            self.model.multibuoy = uploaded
        elif filetype == "user receptors":
            uploaded = UserReceptors(path, dependency, False)
            self.model.ureceptr = uploaded
        elif filetype ==  "building downwash":
            uploaded = Downwash(path, dependency)
            self.model.bldgdw = uploaded
        elif filetype == "particle depletion":
            uploaded = Particle(path, dependency, facilities)
            self.model.partdep = uploaded
        elif filetype == "land use":
            uploaded = LandUse(path, dependency)
            self.model.landuse = uploaded
        elif filetype == "seasons":
            uploaded = Seasons(path, dependency)
            self.model.seasons = uploaded
        elif filetype == "emissions variation":
            uploaded = EmisVar(path, dependency)
            self.model.emisvar = uploaded

        return False if uploaded.dataframe.empty is True else True

