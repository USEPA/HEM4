from abc import abstractmethod, ABC

class Writer(ABC):

    def __init__(self):
        self.outputs = None
        self.filename = None
        self.headers = None
        self.data = None
        self.dataframe = None
        self.batchSize = 10000000

    def write(self, generateOnly=False):

        if not generateOnly:
            self.writeHeader()

        for data in self.generateOutputs():
            if data is not None:

                if not generateOnly:
                    self.appendToFile(data)

                self.analyze(data)

    @abstractmethod
    def writeHeader(self):
        pass

    @abstractmethod
    def appendToFile(self):
        pass

    @abstractmethod
    def getHeader(self):
        pass

    @abstractmethod
    def generateOutputs(self):
        pass

    @abstractmethod
    def analyze(self, data):
        pass