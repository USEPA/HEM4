import unittest
import os
import shutil
from com.sca.hem4.test.SingleFacilityRun import SingleFacilityRun
from com.sca.hem4.test.TestHarness import TestHarness

class UserReceptorsOnlyRun(SingleFacilityRun):

    @classmethod
    def setUpClass(cls):

        # Remove all facility folders from the output directory before beginning
        folder = "output"
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isdir(file_path): shutil.rmtree(file_path)
            except BaseException as e:
                print(e)

        # Create the test harness, process the files, etc.
        cls.testHarness = TestHarness(True)
        if not cls.testHarness.success:
            raise RuntimeError("something went wrong creating the test harness")

        cls.outputFixturePrefix = "fixtures/output/ureponly/"

if __name__ == '__main__':
    unittest.main()