import unittest

from com.sca.hem4.writer.csv.AllOuterReceptors import AllOuterReceptors

"""
This test verifies the correctness of the algorithm that appends to file names
for CSV outputs that are too big to fit in a single file (i.e. AllOuterReceptors)
"""
class CsvWriterMultipartTest(unittest.TestCase):

    def test_naming(self):

        allouter = AllOuterReceptors(targetDir="output", facilityId="12345678")
        print(allouter.filename)

        # Make sure the algorithm handles double digit parts
        for i in range(1,11):
            allouter.startNewFile()
            print(allouter.filename)

        self.assertEqual("output\\12345678_all_outer_receptors_part11.csv", allouter.filename)