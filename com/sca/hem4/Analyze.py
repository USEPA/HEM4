
import tkinter as tk 
import tkinter.ttk as ttk
from functools import partial
from PIL import ImageTk
import PIL.Image
from tkinter.filedialog import askopenfilename
from tkinter.simpledialog import Dialog, Toplevel
from PyQt5 import QtGui

import numpy as np
import pandas as pd
from pandastable import Table, filedialog, np

import os
import sys
import glob
import importlib 

import shutil
import webbrowser

from concurrent.futures import ThreadPoolExecutor
from com.sca.hem4.dash.HEM4dash import HEM4dash
from threading import Timer
import traceback
import time
from flask import request
from com.sca.hem4.log.Logger import Logger


TITLE_FONT= ("Daytona", 16, 'bold')
SECTION_FONT= ("Daytona", 12, 'bold')

TAB_FONT =("Daytona", 11, 'bold')
TEXT_FONT = ("Daytona", 14)
#SUB_FONT = ("Verdana", 12)




def hyperlink1(event):
    webbrowser.open_new(r"https://www.epa.gov/fera/risk-assessment-and-"+
                        "modeling-human-exposure-model-hem")

def hyperlink2(event):
    webbrowser.open_new(r"https://www.epa.gov/fera/human-exposure-model-hem-3"+
                        "-users-guides")
    


 
class Page(tk.Frame):
    def __init__(self, *args, **kwargs):
        tk.Frame.__init__(self, *args, **kwargs)
        #set mainframe background color
        self.main_color = "white"
        self.tab_color = "lightcyan3"
        self.highlightcolor = "snow3"
    
    def show(self):
        self.lift()
        
      




class Analyze(Page):
    def __init__(self, nav, *args, **kwargs):
        Page.__init__(self, *args, **kwargs)
        
        self.main_color = "white"
        self.tab_color = "lightcyan3"


        meta_container = tk.Frame(self, bg=self.tab_color, bd=2)
        #        self.buttonframe.pack(side="w", fill="y", expand=False)
        meta_container.pack(side="top", fill="both", expand=True)
        
        self.container = tk.Frame(meta_container, bg=self.tab_color, borderwidth=0)
        self.container.grid_rowconfigure(12, weight=1)
        self.container.grid(row=0, column =0)
        
        
        self.s1 = tk.Frame(self.container, width=600, height=50, bg="lightcyan3")
        self.s2 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg="lightcyan3")
        self.s3 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg="lightcyan3")
        self.s4 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg="lightcyan3")
        self.s5 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg="lightcyan3")
        self.s6 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg="lightcyan3")
        self.s7 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg="lightcyan3")
        self.s8 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg="lightcyan3")
        self.s9 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg="lightcyan3")
        self.s10 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg="lightcyan3")
        self.s11 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg="lightcyan3")


        

        self.s1.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.s2.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=20)
        self.s3.grid(row=3, column=0, columnspan=2, sticky="nsew")
        self.s4.grid(row=4, column=0, columnspan=2, sticky="nsew")
        self.s5.grid(row=5, column=0, columnspan=2, sticky="nsew")
        self.s6.grid(row=6, column=0, columnspan=2, sticky="nsew")
        self.s7.grid(row=7, column=0, columnspan=2, sticky="nsew")
        self.s8.grid(row=8, column=0, columnspan=2, sticky="nsew")
        self.s9.grid(row=9, column=0, columnspan=2, sticky="nsew")
        self.s10.grid(row=10, column=0, columnspan=2, sticky="nsew")
        self.s11.grid(row=11, column=0, columnspan=2, sticky="nsew")

        
        self.tt = PIL.Image.open('images\icons8-graph-48-white.png').resize((30,30))
        self.tticon = self.add_margin(self.tt, 5, 0, 5, 0)
        self.titleicon = ImageTk.PhotoImage(self.tticon)
        self.titleLabel = tk.Label(self.s1, image=self.titleicon, bg=self.tab_color)
        self.titleLabel.image = self.titleicon # keep a reference!
        self.titleLabel.grid(row=1, column=0, padx=10, pady=10)
        

        title2 = tk.Label(self.s1, text="VIEW AND ANALYZE OUTPUTS", font=TITLE_FONT, fg="white", bg=self.tab_color)
        title2.grid(row=1, column=1, padx=10, pady=10)
        
        self.add_instructions(self.s2, self.s2)
        
        
        
        fu = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        ficon = self.add_margin(fu, 5, 0, 5, 0)
        fileicon = ImageTk.PhotoImage(ficon)
        self.fileLabel = tk.Label(self.s4, image=fileicon, bg=self.tab_color)
        self.fileLabel.image = fileicon # keep a reference!
        self.fileLabel.grid(row=1, column=0, padx=10)
        
        
        self.button_file = tk.Label(self.s4, text="Open a facility or summary output table",
                                font=TEXT_FONT, bg=self.tab_color)
        self.button_file.grid(row=1, column=1)
        
                                    
        self.button_file.bind("<Enter>", lambda x: self.color_config( self.button_file, self.fileLabel, self.s4, self.highlightcolor, x))
        self.button_file.bind("<Leave>", lambda x: self.remove_config( self.button_file, self.fileLabel, self.s4, self.tab_color, x))
        self.button_file.bind("<Button-1>", partial(self.browse_button))
        
        self.fileLabel.bind("<Enter>", lambda x: self.color_config(self.fileLabel, self.button_file, self.s4, self.highlightcolor, x))
        self.fileLabel.bind("<Leave>", lambda x: self.remove_config(self.fileLabel, self.button_file, self.s4, self.tab_color, x))
        self.fileLabel.bind("<Button-1>", partial(self.browse_button))
        
#
        
        mi = PIL.Image.open('images\icons8-map-marker-48.png').resize((30,30))
        micon = self.add_margin(mi, 5, 0, 5, 0)
        mapicon = ImageTk.PhotoImage(micon)
        self.mapLabel = tk.Label(self.s6, image=mapicon, bg=self.tab_color)
        self.mapLabel.image = mapicon # keep a reference!
        self.mapLabel.grid(row=1, column=0, padx=10)
        
        self.button_maps = tk.Label(self.s6, text="Open a chronic or acute risk map",
                                font=TEXT_FONT, bg=self.tab_color)
        self.button_maps.grid(row=1, column=1)
        
        self.button_maps.bind("<Enter>", lambda x: self.color_config(self.button_maps, self.mapLabel, self.s6, self.highlightcolor, x))
        self.button_maps.bind("<Leave>", lambda x: self.remove_config(self.button_maps, self.mapLabel, self.s6, self.tab_color, x))
        self.button_maps.bind("<Button-1>", partial(self.maps_button))
        
        self.mapLabel.bind("<Enter>", lambda x: self.color_config(self.mapLabel, self.button_maps, self.s6, self.highlightcolor, x))
        self.mapLabel.bind("<Leave>", lambda x: self.remove_config(self.mapLabel, self.button_maps, self.s6, self.tab_color, x))
        self.mapLabel.bind("<Button-1>", partial(self.maps_button))


        di = PIL.Image.open('images\icons8-view-48.png').resize((30,30))
        dicon = self.add_margin(di, 5, 0, 5, 0)
        dashicon = ImageTk.PhotoImage(dicon)
        self.dashLabel = tk.Label(self.s8, image=dashicon, bg=self.tab_color)
        self.dashLabel.image = dashicon # keep a reference!
        self.dashLabel.grid(row=1, column=0, padx=10)

        self.button_dash = tk.Label(self.s8, text="View summary graphical outputs in web browser",
                                font=TEXT_FONT, bg=self.tab_color)
        self.button_dash.grid(row=1, column=1)
        
        self.button_dash.bind("<Enter>", lambda x: self.color_config(self.button_dash, self.dashLabel, self.s8, self.highlightcolor, x))
        self.button_dash.bind("<Leave>", lambda x: self.remove_config(self.button_dash, self.dashLabel, self.s8, self.tab_color, x))
        self.button_dash.bind("<Button-1>", partial(self.dash_button))
        
        self.dashLabel.bind("<Enter>", lambda x: self.color_config(self.button_dash, self.dashLabel, self.s8, self.highlightcolor, x))
        self.dashLabel.bind("<Leave>", lambda x: self.remove_config(self.button_dash, self.dashLabel, self.s8, self.tab_color, x))
        self.dashLabel.bind("<Button-1>", partial(self.dash_button))        
        
#        command=self.maps_button

#        self.button_maps.grid(row=1, column=1)

         #%% Event handlers for porting instructions
    def add_instructions(self, placeholder1, placeholder2):
        
        #Dynamic instructions place holder
        global instruction_instance
        self.instruction_instance = tk.StringVar(placeholder1)
        self.instruction_instance.set(" ")
        self.dynamic_inst = tk.Label(placeholder2, wraplength=600, font=TEXT_FONT, padx=20, bg=self.tab_color) 
        self.dynamic_inst.config(height=4)
        
        self.dynamic_inst["textvariable"] = self.instruction_instance 
        self.dynamic_inst.grid(row=0, column=0)


    #reset instructions space
    def reset_instructions(self):
        """
        Function clears instructions from display box 
        """
        global instruction_instance
        self.instruction_instance.set(" ")    
        
    #general function for browsing instructions
    def browse_inst(self, location):
        """
        Function looks up text file with instructions for specified input
        browse buttons
        """
        global instruction_instance
        self.read_inst = open(location, 'r')
        self.instruction_instance.set(self.read_inst.read())  
    
    def add_margin(self, pil_img, top, right, bottom, left):
        width, height = pil_img.size
        new_width = width + right + left
        new_height = height + top + bottom
        result = PIL.Image.new(pil_img.mode, (new_width, new_height))
        result.paste(pil_img, (left, top))
        return result    
 
    
    def dash_button(self, event):

        # Start a new thread for dash
        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(self.runDash)
        self.instruction_instance.set(" ")
        


    def runDash(self,  arguments=None):
        try:           
            # Redirect stdout
            orig_stdout = sys.stdout
            fileDir = os.path.dirname(os.path.realpath('__file__'))
            stdout_file = os.path.join(fileDir, 'output/hem4.log')
            sys.stdout = open(stdout_file, 'w')

            # Run the dash app
            dirname = tk.filedialog.askdirectory()
            dashapp = HEM4dash(dirname)
            appobj = dashapp.buildApp()   
            if appobj != None:
                Timer(1, self.open_browser).start()
                appobj.run_server(debug= False, port=8030)               

            # Reset stdout to original state
            sys.stdout = orig_stdout
                 
        except BaseException as ex:
            self.exception = ex
            fullStackInfo=''.join(traceback.format_exception(
                etype=type(ex), value=ex, tb=ex.__traceback__))
            message = "An error occurred while trying to run the Dash app:\n" + fullStackInfo
            Logger.logMessage(message)

        
    def open_browser(self):
        webbrowser.open_new('http://localhost:8030/')
        
        
    def browse_button(self, event):
        datatypes = {
            "% of Total Incidence": np.str, "aconc": np.float64,
            "Acute Conc (ug/m3)": np.float64, "Acute conc (ug/m3)": np.float64,
            "Aegl_1 1hr (mg/m3)": np.float64, "Aegl_1 8hr (mg/m3)": np.float64,
            "AEGL_1_11H": np.float64, "Aegl_2 1hr (mg/m3)": np.float64,
            "Aegl_2 8hr (mg/m3)": np.float64, "AEGL_2_1H": np.float64,
            "Angle": np.float64, "angle": np.float64,
            "Angle (from north)": np.float64, "Block": np.str,
            "block": np.str, "BLOCK": np.str,
            "Block type": np.str, "can_blk": np.str,
            "can_latitude": np.float64, "can_longitude": np.float64,
            "can_rcpt_type": np.str, "can_rsk_interpltd": np.str,
            "Cancer Risk": np.float64, "Chem, Centroid, or Discrete": np.str,
            "Conc (ug/m3)": np.float64, "Conc rounded (ug/m3)": np.float64,
            "Conc sci (ug/m3)": np.float64, "CONC_MG": np.float64,
            "devel_blk": np.str, "devel_hi_interpltd": np.str,
            "devel_rcpt_type": np.str, "Developmental HI": np.float64,
            "developmental_hi": np.float64, "Distance": np.float64,
            "distance": np.float64, "Distance (in meters)": np.float64,
            "Distance (m)": np.float64, "Dry deposition (g/m2/yr)": np.float64,
            "Elevation (in meters)": np.float64, "Elevation (m)": np.float64,
            "Emission type": np.str, "Emissions (tons/year)": np.float64,
            "Emissions (tpy)": np.float64, "endo_blk": np.str,
            "endo_hi_interptd": np.str, "endo_rcpt_type": np.str,
            "Endocrine HI": np.float64, "endocrine_hi": np.float64,
            "ERPG_1": np.float64, "Erpg_1 (mg/m3)": np.float64,
            "ERPG_2": np.float64, "Erpg_2 (mg/m3)": np.float64,
            "fac_center_latitude": np.float64, "fac_center_longitude": np.float64,
            "Facil_id": np.str, 'Facility_id': np.str, 'facility_id': np.str, 'Facid': np.str, \
            "Facility count": np.int, "Facility ID": np.str,
            'FACNAME': np.str, "FIPS": np.str,
            "FIPs": np.str, "fips": np.str,
            "Fips": np.str, "Fips + Block": np.str,
            "Fraction emitted as particulate matter (%)": np.float64, "Hazard Index": np.float64,
            "hema_blk": np.str, "hema_hi_interptd": np.str,
            "hema_rcpt_type": np.str,
            "Hematological HI": np.float64,
            "HI Level": np.str,
            "HI Total": np.float64,
            "HI Type": np.str,
            "Hill": np.float64,
            "Hill Height (in meters)": np.float64,
            "Hill height (m)": np.float64,
            "HQ": np.float64,
            "HQ_AEGL1": np.float64,
            "HQ_AEGL2": np.float64,
            "HQ_ERPG1": np.float64,
            "HQ_ERPG2": np.float64,
            "HQ_IDLH": np.float64,
            "HQ_REL": np.float64,
            "IDLH_10": np.float64,
            "Idlh_10 (mg/m3)": np.float64,
            "immun_blk": np.str,
            "immun_hi_interptd": np.str,
            "immun_rcpt_type": np.str,
            "Immunological HI": np.float64,
            "immunological_hi": np.float64,
            "incidence": np.float64,
            "Incidence": np.float64,
            "Incidence rounded": np.float64,
            "Is max conc at any receptor interpolated? (Y/N)": np.str,
            "Is max populated receptor interpolated? (Y/N)": np.str,
            "Kidney HI": np.float64,
            "kidney_blk": np.str,
            "kidney_hi": np.float64,
            "kidney_hi_interptd": np.str,
            "kidney_rcpt_type": np.str,
            "km_to_metstation": np.float64,
            "Lat": np.float64,
            "lat": np.float64,
            "latitude": np.float64,
            "Latitude": np.float64,
            "Level": np.str,
            "Liver HI": np.float64,
            "liver_blk": np.str,
            "liver_hi": np.float64,
            "liver_hi_interpltd": np.str,
            "liver_rcpt_type": np.str,
            "Lon": np.float64,
            "lon": np.float64,
            "longitude": np.float64,
            "Longitude": np.float64,
            "Max conc at any receptor (ug/m3)": np.float64,
            "Max conc at populated receptor (ug/m3)": np.float64,
            "metname": np.str,
            "MIR": np.float64,
            "Mrl (mg/m3)": np.float64,
            "mx_can_rsk": np.float64,
            "neuro_blk": np.str,
            "neuro_hi_interpltd": np.str,
            "neuro_latitude": np.float64,
            "neuro_longitude": np.float64,
            "neuro_rcpt_type": np.str,
            "Neurological Facilities": np.int,
            "Neurological HI": np.float64,
            "Neurological Pop": np.int,
            "neurological_hi": np.float64,
            "Notes": np.str,
            "Number people exposed to > 1 Developmental HI": np.int,
            "Number people exposed to > 1 Endocrinological HI": np.int,
            "Number people exposed to > 1 Hematological HI": np.int,
            "Number people exposed to > 1 Immunological HI": np.int,
            "Number people exposed to > 1 Kidney HI": np.int,
            "Number people exposed to > 1 Liver HI": np.int,
            "Number people exposed to > 1 Neurological HI": np.int,
            "Number people exposed to > 1 Ocular HI": np.int,
            "Number people exposed to > 1 Reproductive HI": np.int,
            "Number people exposed to > 1 Respiratory HI": np.int,
            "Number people exposed to > 1 Skeletal HI": np.int,
            "Number people exposed to > 1 Spleen HI": np.int,
            "Number people exposed to > 1 Thyroid HI": np.int,
            "Number people exposed to > 1 Whole Body HI": np.int,
            "Number people exposed to >= 1 in 1,000 risk": np.int,
            "Number people exposed to >= 1 in 1,000,000 risk": np.int,
            "Number people exposed to >= 1 in 10,000 risk": np.int,
            "Number people exposed to >= 1 in 10,000,000 risk": np.int,
            "Number people exposed to >= 1 in 100,000 risk": np.int,
            "Octant or MIR": np.str,
            "Ocular HI": np.float64,
            "ocular_blk": np.str,
            "ocular_hi": np.float64,
            "ocular_hi_interptd": np.str,
            "ocular_rcpt_type": np.str,
            "Overlap": np.str,
            "Parameter": np.str,
            "Percentage": np.float64,
            "Pollutant": np.str,
            "pop_overlp": np.float64,
            "Population": np.int,
            "Rec_type": np.str,
            "Receptor type": np.str,
            "REL": np.float64,
            "Rel (mg/m3)": np.float64,
            "repro_blk": np.str,
            "repro_hi_interptd": np.str,
            "repro_rcpt_type": np.str,
            "Reproductive Facilities": np.int,
            "Reproductive HI": np.float64,
            "Reproductive Pop": np.int,
            "reproductive_hi": np.float64,
            "resp_blk": np.str,
            "resp_hi_interpltd": np.str,
            "resp_latitude": np.float64,
            "resp_longitude": np.float64,
            "resp_rcpt_type": np.str,
            "Respiratory Facilities": np.int,
            "Respiratory HI": np.float64,
            "Respiratory Pop": np.int,
            "respiratory_hi": np.float64,
            "RFc (mg/m3)": np.float64,
            "Ring number": np.int,
            "Risk": np.float64,
            "Risk Contribution": np.float64,
            "Risk level": np.str,
            "Risk Type": np.str,
            "Rural/Urban": np.str,
            "rural_urban": np.str,
            "Sector": np.int,
            "Site type": np.str,
            "skel_blk": np.str,
            "skel_hi_interptd": np.str,
            "skel_rcpt_type": np.str,
            "Skeletal HI": np.float64,
            "skeletal_hi": np.float64,
            "Source ID": np.str,
            'source_id': np.str,
            'Source_id': np.str,
            'SOURCE_ID': np.str,
            'SRCID': np.str, \
            "Spleen HI": np.float64,
            "spleen_blk": np.str,
            "spleen_hi": np.float64,
            "spleen_hi_interptd": np.str,
            "spleen_rcpt_type": np.str,
            "Src Cat": np.str,
            "Teel_0 (mg/m3)": np.float64,
            "Teel_1 (mg/m3)": np.float64,
            "Thyroid HI": np.float64,
            "thyroid_blk": np.str,
            "thyroid_hi": np.float64,
            "thyroid_hi_interptd": np.str,
            "thyroid_rcpt_type": np.str,
            "Total Inhalation As Cancer Risk": np.float64,
            "Total Inhalation Cancer Risk": np.float64,
            "Total Inhalation D/F Cancer Risk": np.float64,
            "Total Inhalation PAH Cancer Risk": np.float64,
            "URE 1/(ug/m3)": np.float64,
            "Utm easting": np.int,
            "UTM easting": np.int,
            "Utm northing": np.int,
            "UTM northing": np.int,
            "Utm_east": np.int,
            "Utm_north": np.int,
            "Value": np.float64,
            "Value rounded": np.float64,
            "Value scientific notation": np.float64,
            "Value_rnd": np.float64,
            "Value_sci": np.float64,
            "Warning": np.str,
            "Wet deposition (g/m2/yr)": np.float64,
            "Whole body HI": np.float64,
            "whole_blk": np.str,
            "whole_body_hi": np.float64,
            "whole_hi_interptd": np.str,
            "whole_rcpt_type": np.str,
            "X": np.int,
            "Y": np.int
        }


        try:
            filename = tk.filedialog.askopenfilename(filetypes = [("Excel or csv files","*.xls; *xlsx; *.csv*")])
            if filename.split(".")[-1].lower() in ["xlsx", "xls"]:
                df = pd.read_excel(filename, dtype=datatypes)#Added dtypes here
                ### Removing characters in column names that prevent pandastable calculations
                df.columns = df.columns.str.replace(' ', '_').str.replace('(', '') \
                    .str.replace(')', '').str.replace('/','_').str.replace('%','pct') \
                    .str.replace('>', 'gt').str.replace('>=', 'gte')
            else:
                df = pd.read_csv(filename, dtype=datatypes)
                ### Removing characters in column names that prevent pandastable calculations
                df.columns = df.columns.str.replace(' ', '_').str.replace('(', '') \
                    .str.replace(')', '').str.replace('/','_').str.replace('%','pct') \
                    .str.replace('>', 'gt').str.replace('>=', 'gte')
    
            curr_windo=tk.Toplevel()
            curr_windo.title(filename)
            curr_windo.geometry('900x600+40+20')
            pt = Table(curr_windo, dataframe = df, showtoolbar=True, showstatusbar=True)
            pt.autoResizeColumns()
            pt.colheadercolor='#448BB9'
            pt.floatprecision = 6
            pt.show()
        
        except Exception as e:
             print(e)

    def maps_button(self, event):
        filename = tk.filedialog.askopenfilename(filetypes = [("html or kml files","*.html; *.kml; *.kmz")])
        webbrowser.open(filename)

    def browse(self):

        self.fullpath = tk.filedialog.askdirectory()
        #print(fullpath)
        self.mod_group_list.set(self.fullpath)


    def color_config(self, widget1, widget2, container, color, event):
        
         widget1.configure(bg=color)
         widget2.configure(bg=color)
         container.configure(bg=color)   
         
         #serve instructions
         if self.fileLabel in [widget1, widget2]:
             if self.instruction_instance.get() == " ":
                 
                 self.browse_inst("instructions/source_cat_browse.txt")
                 
             else:
                 self.instruction_instance.set(" ")
                 
         elif self.mapLabel in [widget1, widget2]:
             if self.instruction_instance.get() == " ":
                 self.browse_inst("instructions/kmz_browse.txt")
                 
             else:
                 self.instruction_instance.set(" ")
                 
         elif self.dashLabel in [widget1, widget2]:
             if self.instruction_instance.get() == " ":
                 self.browse_inst("instructions/sum_browse.txt")
                 
             else:
                 self.instruction_instance.set(" ")
                 
                 
    def remove_config(self, widget1, widget2, container, color, event):
    
         widget1.configure(bg=color)
         widget2.configure(bg=color)
         container.configure(bg=color)
         self.instruction_instance.set(" ")
         
         
    def container_config(self, container, widget1, widget2, color, event):
         widget1.configure(bg=color)
         widget2.configure(bg=color)
         container.configure(bg=color)   
         
         #serve instructions
         if self.dashLabel in [widget1, widget2]:
             if self.instruction_instance.get() == " ":
                 
                 self.browse_inst("instructions/source_cat_browse.txt")
                 
             else:
                 self.instruction_instance.set(" ")
                 
         elif self.fileLabel in [widget1, widget2]:
             if self.instruction_instance.get() == " ":
                 self.browse_inst("instructions/kmz_browse.txt")
                 
             else:
                 self.instruction_instance.set(" ")
                 
         elif self.mapLabel in [widget1, widget2]:
             if self.instruction_instance.get() == " ":
                 self.browse_inst("instructions/sum_browse.txt")
                 
             else:
                 self.instruction_instance.set(" ")
         
    def container_remove(self, container, widget1, widget2, color, event):
        
         widget1.configure(bg=color)
         widget2.configure(bg=color)
         container.configure(bg=color)
         self.instruction_instance.set(" ")
        
        