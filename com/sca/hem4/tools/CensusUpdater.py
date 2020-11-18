import datetime
import json

import pandas as pd
from com.sca.hem4.log.Logger import Logger

class CensusUpdater():

    def __init__(self):

        self.previousValue = None
        self.pathToCensusFiles = 'census'

        self.stateCodeMap = {
            '01':'AL','05':'AR','04':'AZ','06':'CA','08':'CO','09':'CT','11':'DC','10':'DE','12':'FL','13':'GA',
            '19':'IA','16':'ID','17':'IL','18':'IN','20':'KS','21':'KY','22':'LA','25':'MA','24':'MD','23':'ME',
            '26':'MI','27':'MN','29':'MO','28':'MS','30':'MT','37':'NC','38':'ND','31':'NE','33':'NH','34':'NJ',
            '35':'NM','32':'NV','36':'NY','39':'OH','40':'OK','41':'OR','42':'PA','44':'RI','45':'SC','46':'SD',
            '47':'TN','48':'TX','49':'UT','51':'VA','50':'VT','53':'WA','55':'WI','54':'WV','56':'WY','02':'AK',
            '15':'HI','72':'PR','78':'VI'}

    def migrate(self):

        for state in self.stateCodeMap.values():

            Logger.logMessage("Opening " + state + " for migration...")
            pathToFile = r'census\Blks_' + state + '.json'

            with open(pathToFile, "r") as read_file:
                data = json.load(read_file)

                replaced = [self.migrateRecord(x) for x in data]

            # Open the file again and re-write it using the updated json.
            with open(pathToFile, "w") as write_file:
                json.dump(replaced, write_file, indent=4)

    def update(self, changesetFilepath):
        try:
            changeset_df = self.readFromPath(changesetFilepath)

            # Add two columns for posterity
            changeset_df['lastModified'] = None
            changeset_df['previous'] = None

            for index, row in changeset_df.iterrows():

                self.previousValue = ""

                blockid = row["blockid"]
                operation = row["change"].strip().upper()

                # Get the two-letter state abbreviation and construct the census file name.
                state = self.getStateForCode(blockid[0:2])
                Logger.logMessage("Opening " + state + " for updates...")
                pathToFile = self.pathToCensusFiles + '\\Blks_' + state + '.json'

                if operation == "DELETE":
                    Logger.logMessage("Deleting block " + row["blockid"])


                with open(pathToFile, "r") as read_file:
                    data = json.load(read_file)

                    # This is the crucial part! We are doing a list comprehension that is based on
                    # both a filter and a conditional. The filter is the last part - deletes won't
                    # make it into the resulting list at all. Moves and zeros will be handled by
                    # the mutate function. Any blockid that is not the one we care about will pass
                    # through unchanged.
                    replaced = [self.mutate(x, operation, row)
                        if x['IDMARPLOT']==blockid
                        else x for x in data if x['IDMARPLOT']!=blockid or (operation == 'MOVE' or operation == 'ZERO')]

                # Open the file again and re-write it using the updated json.
                with open(pathToFile, "w") as write_file:
                    json.dump(replaced, write_file, indent=4)

                # Update the changeset row
                row["lastModified"] = str(datetime.datetime.now())
                if operation == 'DELETE':
                    row["previous"] = row["blockid"]
                else:
                    row["previous"] = "Block id not found" if self.previousValue == "" else self.previousValue

            # Write out the updated changeset
            changeset_df.fillna("")
            changeset_df.to_excel(changesetFilepath, index=False)

            # Update the index
            self.updateIndex()

            Logger.logMessage("Census update complete!")
        except BaseException as e:
            Logger.logMessage("Error running census update: " + str(e))

    def updateIndex(self):
        # Update the census key file...build an index in-memory, and then use it
        # to create the file
        Logger.logMessage("Updating census key...")

        index = {}
        for key, value in self.stateCodeMap.items():
            pathToFile = self.pathToCensusFiles + '\\Blks_' + value + '.json'
            Logger.logMessage(pathToFile)
            with open(pathToFile, "r") as read_file:
                stateBlocks = json.load(read_file)
                minRec = 1
                for block in stateBlocks:
                    fips = block["FIPS"]

                    if not fips in index:
                        index[fips] = []
                        index[fips+"STATE"] = 'Blks_' + value
                        index[fips+"MIN_REC"] = minRec

                    index[fips].append(block)

                    minRec+= 1

        indexRecords = []
        pathToIndexFile = self.pathToCensusFiles + '\\Census_key.json'
        with open(pathToIndexFile, "w") as index_file:
            for key, value in index.items():
                if "STATE" in key or "MIN_REC" in key:
                    continue

                state = index[key+"STATE"]
                minRec = index[key+"MIN_REC"]
                indexRecords.append(self.index(state, minRec, key, value))

            indexRecords = sorted(indexRecords, key=lambda record: record['FIPS'])
            json.dump(indexRecords, index_file, indent=4)


    """
    Produces a record like this, given a list of blocks with the same FIPS number:
    {
        "FIPS": "01001",
        "MIN_REC": 1,
        "NO": 1159,
        "FILE_NAME": "Blks_AL",
        "LAT_MIN": 32.3616781,
        "LAT_MAX": 32.7075255,
        "LON_MIN": -86.91804,
        "LON_MAX": -86.4122427,
        "ELEV_MAX": 208,
        "YEAR": "1"
    }
    """
    def index(self, filename, minRec, fips, blocks):
        minLat = None
        maxLat = None
        minLon = None
        maxLon = None
        maxElev = None

        for block in blocks:
            if block["LAT"] is None or block["LON"] is None or block["ELEV"] is None:
                continue

            minLat = block["LAT"] if minLat is None else min(block["LAT"], minLat)
            maxLat = block["LAT"] if maxLat is None else max(block["LAT"], maxLat)
            minLon = block["LON"] if minLon is None else min(block["LON"], minLon)
            maxLon = block["LON"] if maxLon is None else max(block["LON"], maxLon)
            maxElev = block["ELEV"] if maxElev is None else max(block["ELEV"], maxElev)

        return {"FIPS" : fips, "MIN_REC" : minRec, "NO" : len(blocks), "FILE_NAME" : filename,
                "LAT_MIN" : minLat, "LAT_MAX" : maxLat, "LON_MIN" : minLon, "LON_MAX" : maxLon,
                "ELEV_MAX" : int(round(maxElev)), "YEAR" : "1",}

    def migrateRecord(self, record):
        record.pop('MOVED', None)

        id = record['IDMARPLOT']
        population = record['POPULATION']

        if population is None:
            print("Found null population for block id " + record['IDMARPLOT'])
            record['POPULATION'] = 0

        if len(id) > 15 and id.startswith('0'):
            record['IDMARPLOT'] = id[1:]
            print("Chopped leading zero: " + id)
        elif 'U' in id and len(id.rpartition("U")[0]) > 5:
            print("Chopped leading zero: " + id)
            record['IDMARPLOT'] = id[1:]

        return record

    def mutate(self, record, operation, row):
        if operation == 'MOVE':
            Logger.logMessage("Moving block " + record["IDMARPLOT"] + " to [" + str(row['lat']) + "," + str(row['lon']) + "]")
            self.previousValue = "[" + str(record['LAT']) + "," + str(record['LON']) + "]"
            record['LAT'] = float(row['lat'])
            record['LON'] = float(row['lon'])
            record['MOVED'] = 'Y'
        elif operation == 'ZERO':
            Logger.logMessage("Zeroing population for block " + record["IDMARPLOT"])
            self.previousValue = str(record['POPULATION'])
            record['POPULATION'] = 0

        return record

    def readFromPath(self, filepath):
        colnames = ["facid", "category", "blockid", "lat", "lon", "change"]
        with open(filepath, "rb") as f:
            df = pd.read_excel(f, skiprows=0, names=colnames, dtype=str, na_values=[''], keep_default_na=False)
            return df

    def getStateForCode(self, code):
        return self.stateCodeMap[code]
