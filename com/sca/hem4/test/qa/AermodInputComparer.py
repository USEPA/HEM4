import os
import re


class AermodInputComparer:

    def __init__(self):

        self.pathToFiles = r"C:/temp/aaadumb"

    def compare(self):

        hem3File = os.path.join(self.pathToFiles, "aermod_Run_2_AreaOnly_fac1-nc_hem3.inp")
        with open(hem3File, "r") as hem3_file:
            # Read each line and standardize the whitespace before storing in a list
            hem3_lines = hem3_file.readlines()

        hem3_standard = [self.standardize(l) for l in hem3_lines]

        hem4File = os.path.join(self.pathToFiles, "aermod_Run_2_AreaOnly_fac1-nc_hem4.inp")
        with open(hem4File, "r") as hem4_file:
            # Read each line and standardize the whitespace before storing in a list
            hem4_lines = hem4_file.readlines()

        hem4_standard = [self.standardize(l) for l in hem4_lines]

        diffFile = os.path.join(self.pathToFiles, "diff_Run_2_AreaOnly_fac1-nc.txt")
        with open(diffFile, "w") as diff_file:
            diff_file.write("> [hem3]\n")
            for line in hem3_standard:
                if not line in hem4_standard:
                    diff_file.write("> " + line + "\n")
            diff_file.write("-------------------------------------------------------------------------\n")
            diff_file.write("< [hem4]\n")
            for line in hem4_standard:
                if not line in hem3_standard:
                    diff_file.write("< " + line + "\n")

    def standardize(self, line):
        standard = re.sub(r"\s+", " ", line)

        # standardize the sig figs
        standard = re.sub(r"\.0+ ", " ", standard)
        standard = re.sub(r"\.(\d*)([123456789])0+ ", r".\1\2 ", standard)
        standard = standard.strip()
        standard = standard.upper()
        return standard


comparer = AermodInputComparer()
comparer.compare()
