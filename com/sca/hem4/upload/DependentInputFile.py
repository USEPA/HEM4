import abc
from com.sca.hem4.upload.InputFile import InputFile


class DependentInputFile(InputFile):
    __metaclass__ = abc.ABCMeta

    def __init__(self, path, dependency, facilities = None, csvFormat = False):
        self.dependency = dependency
        self.facilities = facilities
        self.csvFormat = csvFormat
        InputFile.__init__(self, path)


    
        
        
