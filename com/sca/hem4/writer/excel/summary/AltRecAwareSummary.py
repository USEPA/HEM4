import os

class AltRecAwareSummary:

    def __init__(self):
        pass

    def determineAltRec(self, targetDir):

        # Check the Inputs folder for the existence of alt_receptors.csv
        fpath = os.path.join(targetDir, "Inputs", "alt_receptors.csv")
        if os.path.exists(fpath):
            altrecUsed = 'Y'
        else:
            altrecUsed = 'N'
            
        return altrecUsed

