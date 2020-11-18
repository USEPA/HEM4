import os
import pprint
import shutil
from datetime import datetime
import pandas as pd

class Logger:
    """
    Used to log messages (with optional data) in the HEM4 log file as well as
    the UI component that displays messages to the user.
    
    """
    messageQueue = None

    @staticmethod
    def initializeLog():
        # Create output folder if needed
        outdir = "output"
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        fileDir = os.path.dirname(os.path.realpath('__file__'))
        filename = os.path.join(fileDir, 'output/hem4.log')

        if os.path.isfile(filename):
             os.remove(filename)

        Logger.logfile = open(filename, 'w')

        Logger.logMessage("HEM4 Logging Initialized. See output subfolder for the log of your HEM4 run.\n")

    @staticmethod
    def archiveLog(runDir):
        Logger.close(True)
        fileDir = os.path.dirname(os.path.realpath('__file__'))
        filename = os.path.join(fileDir, 'output/hem4.log')
        if os.path.isfile(filename):
            shutil.move(filename, runDir)

    @staticmethod
    def appendLog(runDir):
        Logger.close(True)

        fileDir = os.path.dirname(os.path.realpath('__file__'))
        current_filename = os.path.join(fileDir, 'output/hem4.log')
        rundir_filename = os.path.join(fileDir, runDir + '/hem4.log')

        logfile = open(current_filename, 'r')
        runlogfile = open(rundir_filename, 'a')
        runlogfile.write(logfile.read())

        logfile.close()
        runlogfile.close()

    @staticmethod
    def log(message, data, logToMessageQueue):
        """
        Log a message, with or without data, to the log file and
        [optionally] to the UI.
        """

        # log the system time once per call
        now = str(datetime.now())
        Logger.logfile.write(now + ":    " + message + "\n")
        if logToMessageQueue and Logger.messageQueue is not None:
            Logger.messageQueue.put(message)

        # any data that is given will be logged to the file only
        if data is not None:

            if isinstance(data, pd.DataFrame):
                pd.set_option('display.width', 1000)
                pd.set_option('display.max_rows', 100000)
                pd.set_option('display.max_columns', 200)
                pd.set_option('precision', 22)

            printer = pprint.PrettyPrinter(indent=4, compact=True, width=1000)
            Logger.logfile.write(printer.pformat(data) + "\n")

        Logger.logfile.flush()

    @staticmethod
    def logMessage(message):
        """
        Convenience method for the common case of logging a string message
        with no data to both the UI and the log file.
        """
        Logger.log(message, None, True)

    def close(flush):
        """
        Flush the output stream if requested and then close the file.
        """
        if flush:
            Logger.logfile.flush()

        Logger.logfile.close()