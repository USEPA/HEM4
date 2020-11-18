# -*- coding: utf-8 -*-

import os
import re
from abc import ABC
from abc import abstractmethod
import pandas as pd
from tkinter import messagebox


from com.sca.hem4.log.Logger import Logger

class InputFile(ABC):

    def __init__(self, path, createDataframe=True):
        self.path = path
        self.dataframe = None
        self.log = []
        self.numericColumns = []
        self.strColumns = []
        self.skiprows = 0

        if createDataframe:
            self.createDataframe()

    @abstractmethod
    def createDataframe(self):
        return

    def validate(self, df):
        return df

    def clean(self, df):
        return df

    def duplicates(self, df, columns):
        records = []

        dupes = df.duplicated(subset=columns, keep=False)
        indices = dupes[dupes].index.values
        for i in indices:
            records.append(str(df.iloc[i].values.flatten().tolist()))

        return records

    # Read values in from a source .xls(x) file. Note that we initially read everything in as a string,
    # and then convert columns which have been specified as numeric to a float64. That way, all empty
    # values in the resultant dataframe become NaN values. All values will either be strings or float64s.
    def readFromPath(self, colnames):
        with open(self.path, "rb") as f:
            
            try:
                df = pd.read_excel(f, skiprows=self.skiprows, names=colnames, dtype=str, na_values=[''], keep_default_na=False)
            
            except BaseException as e:

                if isinstance(e, ValueError):

                    msg = e.args[0]
                    if msg.startswith("Length mismatch"):
                        # i.e. 'Length mismatch: Expected axis has 5 elements, new values have 31 elements'
                        p = re.compile("Expected axis has (.*) elements, new values have (.*) elements")
                        result = p.search(msg)
                        custom_msg = "Length Mismatch: Input file has " + result.group(1) + " columns, but should have " +\
                            result.group(2) + " columns. Please make sure you have selected the correct file or file version."
                        messagebox.showinfo("Error uploading input file", custom_msg)
                        
                        dataframe = pd.DataFrame()
                        return dataframe
                    
                    else:
                        messagebox.showinfo("Error uploading input file ", str(e) + " Please make sure you have selected the correct file or file version.")
                        
                        dataframe = pd.DataFrame()
                        return dataframe
                else:
                    messagebox.showinfo("Error uploading input file", str(e) + " Please make sure you have selected the correct file or file version.")
                    
                    dataframe = pd.DataFrame()
                    return dataframe

            else:
                df = df.astype(str).applymap(self.convertEmptyToNaN)

                # Verify no type errors
                numeric_only = df.copy()
                numeric_only[self.numericColumns] = numeric_only[self.numericColumns].applymap(InputFile.is_numeric)
                if not numeric_only.equals(df):
                    messagebox.showinfo("Error uploading input file", "Some non-numeric values were found in numeric columns in this data set: " +
                                      os.path.basename(self.path))
                    dataframe = pd.DataFrame()
                    return dataframe

                types = self.get_column_types()
                df = df.astype(dtype=types)

                cleaned = self.clean(df)
                validated = self.validate(cleaned)
                
                if validated is None:
                    return pd.DataFrame()
                else:
                    return validated

    # Read values in from a source .csv file. Note that we initially read everything in as a string,
    # and then convert columns which have been specified as numeric to a float64. That way, all empty
    # values in the resultant dataframe become NaN values. All values will either be strings or float64s.
    def readFromPathCsv(self, colnames):
        with open(self.path, "rb") as f:
            
            self.skiprows = 1
            
            try:
                
                df = pd.read_csv(f, skiprows=self.skiprows, names=colnames, dtype=str, na_values=[''], keep_default_na=False)
            
            except BaseException as e:
                
                Logger.logMessage(str(e))
                
            else:
                
                df = df.astype(str).applymap(self.convertEmptyToNaN)

                # Verify no type errors
                numeric_only = df.copy()
                numeric_only[self.numericColumns] = numeric_only[self.numericColumns].applymap(InputFile.is_numeric)
                if not numeric_only.equals(df):
                    messagebox.showinfo("Error uploading input file", "Some non-numeric values were found in numeric columns in this data set: " +
                                  os.path.basename(self.path))
                    
                    dataframe = pd.Dataframe()
                    return dataframe

                types = self.get_column_types()
                df = df.astype(dtype=types)

                cleaned = self.clean(df)
                validated = self.validate(cleaned)
                
                if validated is None:
                    return pd.DataFrame()
                
                else:
                    return validated

    # This method is being applied to every cell to guard against values which
    # have only whitespace.
    def convertEmptyToNaN(self, x):
        y = x.strip()
        if len(y) == 0:
            return 'nan'
        else:
            return y

    def read(self, path):
        with open(path, "rb") as f:
            
            try:
                df = pd.read_excel(f)
                
            except BaseException as e:
                
                Logger.logMessage(str(e))
                
            else:
            
                return df

    def get_column_types(self):
        floatTypes = {col: pd.np.float64 for col in self.numericColumns}

        dtypes = {col: str for col in self.strColumns}

        # merge both converter dictionaries and return
        dtypes.update(floatTypes)
        return dtypes

    def to_numeric(self, slice):
        
        try:
            
            df = pd.to_numeric(slice,errors="coerce")
            
        except BaseException as e:
                
                Logger.logMessage(str(e))
                
        else:
            
            return df

    @staticmethod
    def is_numeric(x):
        try:
            float(x)
            return x
        except:
            return "nan"