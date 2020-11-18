import csv
import os
import re

from com.sca.hem4.writer.Writer import Writer
from com.sca.hem4.log.Logger import Logger


class CsvWriter(Writer):

    def __init__(self, model, plot_df):
        Writer.__init__(self)

        self.model = model
        self.plot_df = plot_df

    def appendToFile(self, dataframe):
        """
        Append the given data to a CSV file which is presumed to already exist. If the file has gotten too big,
        start a new one before writing this data.
        """
        statinfo = os.stat(self.filename)
        size = statinfo.st_size # file size in bytes
        if self.fileTooBig(size):
            self.startNewFile()

        if self.filename.find('all_outer_receptors') == -1:
            data = dataframe.values
            with open(self.filename, 'a', encoding='UTF-8-sig', newline='') as csvarchive:
                writer = csv.writer(csvarchive, quoting=csv.QUOTE_NONNUMERIC)
                self.writeFormatted(writer, data)
        else:
            self.writeBigCsv(dataframe)

    def writeHeader(self, headers=None):
        with open(self.filename, 'w', encoding='UTF-8-sig', newline='') as csvarchive:
            writer = csv.writer(csvarchive, quoting=csv.QUOTE_NONNUMERIC)

            if headers is None:
                headers = self.getHeader()

            writer.writerow(headers)

    def analyze(self, data):
        pass

    def writeFormatted(self, writer, data):
        """
        Write a row of data using preset formatting.
        """
        for row in data:
            writer.writerow([float('{:6.12}'.format(x)) if isinstance(x, float) else x for x in row])

    def writeBigCsv(self, dfname):
        """
        Write a chunk of a dataframe to a CSV file
        """
        dfname.to_csv(self.filename, header=False, mode="a", index=False, chunksize=1000)
        
    def fileTooBig(self, size):
        threshold = 1024 * 1024 * 1024 * 1.5
        return True if size >= threshold else False

    def startNewFile(self):
        filenameNoExtension = os.path.splitext(self.filename)[0]

        # Does the filename already end in a digit?
        m = re.search(r'(\d+)$', filenameNoExtension)
        if m is None:
            self.filename = filenameNoExtension + "_part2.csv"
        else:
            part = int(m.group(1)) + 1
            filenameNoExtension = re.sub(r"part\d+$", "part%s" % part, filenameNoExtension)
            self.filename = filenameNoExtension + ".csv"