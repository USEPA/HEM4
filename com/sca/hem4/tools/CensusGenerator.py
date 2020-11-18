import csv
import glob
import os

import pandas as pd
from decimal import *
import simplejson as json

nationalCensusFilepath = "C:\\Users\\Chris Stolte\\Hem4 Data\\us_blocks_2010_06082020.csv"
changesetFilepath = "C:\\Users\\Chris Stolte\\Hem4 Data\\census-additions.xlsx"
additionsFilepath = "C:\\Users\\Chris Stolte\\Hem4 Data\\census-additions.xlsx"

class CensusGenerator():

    def __init__(self):

        self.previousValue = None
        self.previousValue = None
        self.pathToCensusFiles = 'census'

        self.stateCodeMap = {
            '01':'AL','05':'AR','04':'AZ','06':'CA','08':'CO','09':'CT','11':'DC','10':'DE','12':'FL','13':'GA',
            '19':'IA','16':'ID','17':'IL','18':'IN','20':'KS','21':'KY','22':'LA','25':'MA','24':'MD','23':'ME',
            '26':'MI','27':'MN','29':'MO','28':'MS','30':'MT','37':'NC','38':'ND','31':'NE','33':'NH','34':'NJ',
            '35':'NM','32':'NV','36':'NY','39':'OH','40':'OK','41':'OR','42':'PA','44':'RI','45':'SC','46':'SD',
            '47':'TN','48':'TX','49':'UT','51':'VA','50':'VT','53':'WA','55':'WI','54':'WV','56':'WY','02':'AK',
            '15':'HI','72':'PR','78':'VI'}

    def generateChanges(self, censusFilepath, changesetFilepath):

        try:
            census_df = self.readCensusFromPath(censusFilepath)
            changeset_df = self.readChangesFromPath(changesetFilepath)

            for index,row in changeset_df.iterrows():
                pass

                blockid = row["blockid"]
                operation = row["change"].strip().upper()

                # find the row in the census data, if it exists
                census_idx = census_df.index[census_df['blkid'] == blockid].values[0]
                census_row = census_df.loc[census_df['blkid'] == blockid].iloc[0]

                if census_row is None:
                    print("Couldn't find block id = " + blockid + " in census data.")
                    continue

                if operation == "DELETE":
                    print("Deleting block " + blockid)
                    census_df = census_df[census_df['blkid'] != blockid]
                    continue

                # Mutate for 'MOVE' and 'ZERO' operations
                replaced = self.mutate(census_row, operation, row)
                census_df.loc[census_idx] = replaced

            self.writeCensusFile(censusFilepath, census_df)

        except BaseException as e:
            print("Error running census generate: " + str(e))

    def generateAdditions(self, censusFilepath, additionsFilepath):
        try:
            census_df = self.readCensusFromPath(censusFilepath)
            additions_df = self.readAdditionsFromPath(additionsFilepath)

            # Make sure none of these blocks are already present...
            intersection = pd.merge(census_df, additions_df, how='inner', on=['blkid'])
            if not intersection.empty:
                print("Aborting additions because some blocks are already present in census data:")
                print(intersection['blkid'].values)
                return

            # Append all additions to the census df and re-generate the JSON
            census_df = census_df.append(additions_df)
            census_df = census_df.sort_values(by=['fips', 'blkid'])
            self.writeCensusFile(censusFilepath, census_df)

        except BaseException as e:
            print("Error running census generate: " + str(e))

    def writeCensusFile(self, censusFilepath, census_df):
        # Write out the census df to a new file. Take note that we are
        # setting up quoting and precision for various values to match the
        # original master record file.
        getcontext().prec = 15

        updatedFilepath = censusFilepath.replace(".csv", "-updated.csv")

        census_df["population"] = pd.to_numeric(census_df["population"])
        census_df["lat"] = pd.to_numeric(census_df["lat"])
        census_df["lat"] = census_df["lat"].apply(lambda x: Decimal(str(x)).quantize(Decimal('.0000001'), rounding=ROUND_UP))

        census_df["lon"] = pd.to_numeric(census_df["lon"])
        census_df["lon"] = census_df["lon"].apply(lambda x: Decimal(str(x)).quantize(Decimal('.0000001'), rounding=ROUND_UP))

        census_df["elev"] = pd.to_numeric(census_df["elev"])
        census_df["elev"] = census_df["elev"].apply(lambda x: Decimal(str(x)).quantize(Decimal('.01'), rounding=ROUND_UP))

        census_df["hill"] = pd.to_numeric(census_df["hill"])
        census_df["urban_pop"] = pd.to_numeric(census_df["urban_pop"])
        census_df.to_csv(updatedFilepath, header=True, mode="w", index=False, quoting=csv.QUOTE_NONNUMERIC, chunksize=1000)

    def generateJSON(self, censusFilepath):

        census_df = self.readCensusFromPath(censusFilepath)

        # Create state-specific json files and an index. If a directory already exists, clean it out. Otherwise
        # create it.
        working_dir = os.path.dirname(censusFilepath)
        json_dir = os.path.join(working_dir, "census")

        if os.path.isdir(json_dir):
            print("Cleaning out census directory...")
            files = glob.glob(json_dir + '/*')
            for f in files:
                os.remove(f)
        else:
            print("Creating census directory...")
            os.mkdir(json_dir)

        # Iterate through the state code map, and for every record that has a FIPS starting with the 2-digit
        # code, include in a JSON file for that state.
        for key,value in self.stateCodeMap.items():
            print("Generating JSON for " + value)
            state_df = census_df.loc[census_df["fips"].str.startswith(key)]
            state_df = state_df.sort_values(by=['fips', 'blkid'])

            blocks = []
            rec_num = 1

            state_df["population"] = pd.to_numeric(census_df["population"])
            state_df["lat"] = pd.to_numeric(census_df["lat"])
            state_df["lat"] = census_df["lat"].apply(lambda x: Decimal(str(x)).quantize(Decimal('.0000001'), rounding=ROUND_UP))

            state_df["lon"] = pd.to_numeric(census_df["lon"])
            state_df["lon"] = census_df["lon"].apply(lambda x: Decimal(str(x)).quantize(Decimal('.0000001'), rounding=ROUND_UP))

            state_df["elev"] = pd.to_numeric(census_df["elev"])
            state_df["elev"] = census_df["elev"].apply(lambda x: Decimal(str(x)).quantize(Decimal('.01'), rounding=ROUND_UP))

            state_df["hill"] = pd.to_numeric(census_df["hill"])
            state_df["urban_pop"] = pd.to_numeric(census_df["urban_pop"])

            for index,row in state_df.iterrows():

                record = {"REC_NO": rec_num,
                          "FIPS": row['fips'],
                          "IDMARPLOT": row['blkid'],
                          "POPULATION": row['population'],
                          "LAT": row['lat'],
                          "LON": row['lon'],
                          "ELEV": row['elev'],
                          "HILL": row['hill'],
                          "URBAN_POP": row['urban_pop']}
                blocks.append(record)
                rec_num += 1

            filename = os.path.join(json_dir, "Blks_" + value + ".json")
            with open(filename, "w") as json_file:
                json.dump(blocks, json_file, indent=4, use_decimal=True)

        self.updateIndex(json_dir)

    def mutate(self, record, operation, row):
        if operation == 'MOVE':
            print("Moving block " + record["blkid"] + " to [" + str(row['lat']) + "," + str(row['lon']) + "]")
            record['lat'] = float(row['lat'])
            record['lon'] = float(row['lon'])
            record['moved'] = '1'
        elif operation == 'ZERO':
            print("Zeroing population for block " + record["blkid"])
            record['population'] = 0

        return record

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
            maxElev = float(block["ELEV"]) if maxElev is None else max(float(block["ELEV"]), maxElev)

        return {"FIPS" : fips, "MIN_REC" : minRec, "NO" : len(blocks), "FILE_NAME" : filename,
                "LAT_MIN" : float(minLat), "LAT_MAX" : float(maxLat), "LON_MIN" : float(minLon), "LON_MAX" : float(maxLon),
                "ELEV_MAX" : int(round(maxElev)), "YEAR" : "1"}

    def updateIndex(self, json_dir):
        # Update the census key file...build an index in-memory, and then use it
        # to create the file
        print("Updating census key...")

        index = {}
        for key, value in self.stateCodeMap.items():
            pathToFile = json_dir + '\\Blks_' + value + '.json'
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

                    minRec += 1

        indexRecords = []
        pathToIndexFile = json_dir + '\\Census_key.json'
        with open(pathToIndexFile, "w") as index_file:
            for key, value in index.items():
                if "STATE" in key or "MIN_REC" in key:
                    continue

                state = index[key+"STATE"]
                minRec = index[key+"MIN_REC"]
                indexRecords.append(self.index(state, minRec, key, value))

            indexRecords = sorted(indexRecords, key=lambda record: record['FIPS'])
            json.dump(indexRecords, index_file, indent=4)

    def readChangesFromPath(self, filepath):
        colnames = ["facid", "category", "blockid", "lat", "lon", "change"]
        with open(filepath, "rb") as f:
            df = pd.read_excel(f, skiprows=0, names=colnames, dtype=str, na_values=[''], keep_default_na=False)
            return df

    def readAdditionsFromPath(self, filepath):
        colnames = ["fips", "blkid", "population", "lat", "lon", "elev", "hill", "urban_pop"]
        with open(filepath, "rb") as f:
            df = pd.read_excel(f, skiprows=0, names=colnames, dtype=str, na_values=[''], keep_default_na=False)
            return df

    def readCensusFromPath(self, filepath):
        colnames = ["fips", "blkid", "population", "lat", "lon", "elev", "hill", "urban_pop"]
        with open(filepath, "rb") as f:
            df = pd.read_csv(f, skiprows=1, names=colnames, dtype=str, na_values=[''], keep_default_na=False)
            return df

    def getStateForCode(self, code):
        return self.stateCodeMap[code]


generator = CensusGenerator()
generator.generateAdditions(censusFilepath=nationalCensusFilepath, additionsFilepath=additionsFilepath)

