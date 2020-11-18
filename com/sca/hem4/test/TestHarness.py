from threading import Event

from com.sca.hem4.Processor import Processor
from com.sca.hem4.log.Logger import Logger
from com.sca.hem4.model.Model import Model
from com.sca.hem4.upload.FileUploader import FileUploader
import pkg_resources

class TestHarness:
    """
    A class that simulates the GUI by setting up a model, uploading library
    files, and processing hard-coded inputs in the context of a unit test.
    """
    def __init__(self, altrec):
        self.success = False
        self.altrec = altrec

        pd_version = pkg_resources.get_distribution("pandas").version
        np_version = pkg_resources.get_distribution("numpy").version
        print("Pandas version:" + str(pd_version))
        print("Numpy version:" + str(np_version))

        self.model = Model()
        self.model.group_name = 'TST'

        if altrec:
            self.model.altRec_optns['altrec'] = True

        uploader = FileUploader(self.model)

        # set up the test fixture files...
        uploader.uploadLibrary("haplib")
        uploader.uploadLibrary("organs")
        uploader.uploadLibrary("gas params")

        uploader.upload("faclist", "fixtures/input/faclist.xlsx")
        self.model.facids = self.model.faclist.dataframe["fac_id"]
        uploader.upload("hapemis", "fixtures/input/hapemis.xlsx")
        uploader.upload("emisloc", "fixtures/input/emisloc.xlsx")

        #set phase column in faclist dataframe to None
        self.model.faclist.dataframe['phase'] = None
        
        uploader.uploadDependent("user receptors", "fixtures/input/urec.xlsx",
                                 self.model.faclist.dataframe)

        if altrec:
            uploader.uploadDependent("alt receptors", "fixtures/input/urec_only.csv",
                                     self.model.faclist.dataframe)

        processor = Processor(self.model, Event())
        self.success = processor.process()
        
        Logger.close(True)