import tkinter as tk 
import tkinter.ttk as ttk
from functools import partial
from tkinter import messagebox
from PIL import ImageTk
#from PIL import Image
import PIL.Image
from datetime import datetime
import pprint


import shutil
import webbrowser
from threading import Event
from concurrent.futures import ThreadPoolExecutor
import threading
from com.sca.hem4.log.Logger import Logger


import os
import glob
import importlib 

from com.sca.hem4.log.Logger import Logger

from com.sca.hem4.writer.excel.FacilityMaxRiskandHI import FacilityMaxRiskandHI
from com.sca.hem4.runner.FacilityRunner import FacilityRunner
from com.sca.hem4.writer.excel.FacilityCancerRiskExp import FacilityCancerRiskExp
from com.sca.hem4.writer.excel.FacilityTOSHIExp import FacilityTOSHIExp
from com.sca.hem4.writer.kml.KMLWriter import KMLWriter
from com.sca.hem4.inputsfolder.InputsPackager import InputsPackager

maxRiskReportModule = importlib.import_module("com.sca.hem4.writer.excel.summary.MaxRisk")
cancerDriversReportModule = importlib.import_module("com.sca.hem4.writer.excel.summary.CancerDrivers")
hazardIndexDriversReportModule = importlib.import_module("com.sca.hem4.writer.excel.summary.HazardIndexDrivers")
histogramModule = importlib.import_module("com.sca.hem4.writer.excel.summary.Histogram")
hiHistogramModule = importlib.import_module("com.sca.hem4.writer.excel.summary.HI_Histogram")
incidenceDriversReportModule = importlib.import_module("com.sca.hem4.writer.excel.summary.IncidenceDrivers")
acuteImpactsReportModule = importlib.import_module("com.sca.hem4.writer.excel.summary.AcuteImpacts")
sourceTypeRiskHistogramModule = importlib.import_module("com.sca.hem4.writer.excel.summary.SourceTypeRiskHistogram")
multiPathwayModule = importlib.import_module("com.sca.hem4.writer.excel.summary.MultiPathway")

from com.sca.hem4.summary.SummaryManager import SummaryManager


TITLE_FONT= ("Daytona", 16, 'bold')

SECTION_FONT= ("Daytona", 12, 'bold')
TAB_FONT =("Daytona", 11, 'bold')
TEXT_FONT = ("Daytona", 12)
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
        self.highlight_color = "snow3"
        self.running_tab = "palegreen3"
  
    
    def show(self):
        self.lift()
        
    def fix_config(self, widget1, widget2, previous):
        
         try: 
            widget1.configure(bg=self.tab_color)
            widget2.configure(bg=self.tab_color)
            
            if len(previous) > 0:
                
                for i in previous:
                    i.configure(bg=self.main_color)
                    
         except:
                pass
        
        

         

class Summary(Page):
    
    def __init__(self, nav, *args, **kwargs):
        Page.__init__(self, *args, **kwargs)
        self.nav = nav
        
        self.checked = []
        self.checked_icons = []
        
        meta_container = tk.Frame(self, bg=self.tab_color, bd=2)
        meta_container.pack(side="top", fill="both", expand=True)
        
        self.meta_two = tk.Frame(self, bg=self.tab_color)
        self.meta_two.pack(side="bottom", fill="both")
        self.meta_two.columnconfigure(2, weight=1)
        
        self.container = tk.Frame(meta_container, bg=self.tab_color, borderwidth=0)
        self.container.grid(row=0, column =0)
        self.container.grid_rowconfigure(11, weight=1)

#        self.buttonframe.pack(side="right", fill="y", expand=False)
        
#        self.s=ttk.Style()
#        print(self.s.theme_names())
#        self.s.theme_use('clam')
#        
#        
#        self.tabControl.add(self.container, text='Summaries')      # Add the tab

         #create grid
        self.s1 = tk.Frame(self.container, width=600, height=50, bg=self.tab_color)
        self.s2 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
#        self.s3 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s4 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.l4 = tk.Frame(self.s4, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        self.r4 = tk.Frame(self.s4, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        
        self.s5 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.l5 = tk.Frame(self.s5, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        self.r5 = tk.Frame(self.s5, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        
        self.s6 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.l6 = tk.Frame(self.s6, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        self.r6 = tk.Frame(self.s6, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        
        self.s7 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.l7 = tk.Frame(self.s7, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        self.r7 = tk.Frame(self.s7, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        
        self.s8 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.l8 = tk.Frame(self.s8, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        self.r8 = tk.Frame(self.s8, width=300, height=50, pady=5, padx=5, bg=self.tab_color)

        
        self.s9 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.r9 = tk.Frame(self.s9, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        self.l9 = tk.Frame(self.s9, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        
        self.s10 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.l10 = tk.Frame(self.s10, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        self.r10 = tk.Frame(self.s10, width=300, height=50, pady=5, padx=5, bg=self.tab_color)
        
        self.s11 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
      
        self.container.grid_rowconfigure(12, weight=1)
        self.container.grid_columnconfigure(2, weight=1)
        self.container.grid(sticky = "nsew")
#          
        self.s1.grid(row=1, column=0, columnspan=4, sticky="nsew")
        self.s2.grid(row=2, column=0, columnspan=4, sticky="nsew")
#        self.s3.grid(row=3, column=0, columnspan=2, sticky="nsew")
        self.s4.grid(row=3, column=0, columnspan=2, sticky="nsew")
        self.l4.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.r4.grid(row=1, column=2, columnspan=2, sticky="nsew")
        
        self.s5.grid(row=4, column=0, columnspan=2, sticky="nsew")
        self.l5.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.r5.grid(row=1, column=2, columnspan=2, sticky="nsew")

        self.s6.grid(row=5, column=0, columnspan=2, sticky="nsew")
        self.l6.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.r6.grid(row=1, column=2, columnspan=2, sticky="nsew")

        self.s7.grid(row=6, column=0, columnspan=2, sticky="nsew")
        self.l7.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.r7.grid(row=1, column=2, columnspan=2, sticky="nsew")
        
        self.s8.grid(row=7, column=0, columnspan=2, sticky="nsew")
        self.l8.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.r8.grid(row=1, column=2, columnspan=2, sticky="nsew")
        
        self.s9.grid(row=8, column=0, columnspan=2, sticky="nsew")
        self.l9.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.r9.grid(row=1, column=2, columnspan=2, sticky="nsew")
        
        self.s10.grid(row=9, column=0, columnspan=2, sticky="nsew")
        self.l10.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.r10.grid(row=1, column=2, columnspan=2, sticky="nsew")

        self.s11.grid(row=10, column=0, columnspan=2, sticky="nsew")
        
        
        
        self.tt = PIL.Image.open('images\icons8-edit-graph-report-48-white.png').resize((30,30))
        self.tticon = self.add_margin(self.tt, 5, 0, 5, 0)
        self.titleicon = ImageTk.PhotoImage(self.tticon)
        self.titleLabel = tk.Label(self.s1, image=self.titleicon, bg=self.tab_color)
        self.titleLabel.image = self.titleicon # keep a reference!
        self.titleLabel.grid(row=1, column=0, padx=10, pady=10)
        
        
        #title
        title = tk.Label(self.s1, text="CREATE RISK SUMMARY REPORTS", font=TITLE_FONT, fg="white", bg=self.tab_color, anchor="w")
        title.grid(row=1, column=1, pady=10, padx=10)
        
        
        
        fu = PIL.Image.open('images\icons8-folder-48.png').resize((30,30))
        ficon = self.add_margin(fu, 5, 0, 5, 0)
        fileicon = ImageTk.PhotoImage(ficon)
        self.fileLabel = tk.Label(self.s2, image=fileicon, bg=self.tab_color)
        self.fileLabel.image = fileicon # keep a reference!
        self.fileLabel.grid(row=1, column=0, padx=10)
        
        self.folder_select = tk.Label(self.s2, text="Select output folder", font=TITLE_FONT, bg=self.tab_color, anchor="w")
        self.folder_select.grid(pady=10, padx=10, row=1, column=1)


        self.fileLabel.bind("<Enter>", partial(self.color_config, self.fileLabel, self.folder_select, self.s2, 'light grey'))
        self.fileLabel.bind("<Leave>", partial(self.color_config, self.fileLabel, self.folder_select, self.s2, self.tab_color))
        self.fileLabel.bind("<Button-1>", partial(self.browse, self.folder_select))
        
        self.folder_select.bind("<Enter>", partial(self.color_config, self.folder_select, self.fileLabel, self.s2, 'light grey'))
        self.folder_select.bind("<Leave>", partial(self.color_config, self.folder_select, self.fileLabel, self.s2, self.tab_color))
        self.folder_select.bind("<Button-1>", partial(self.browse, self.folder_select))
        
        
        
#%%        
        ##unchecked box icon
        ui = PIL.Image.open('images\icons8-unchecked-checkbox-48.png').resize((30,30))
        unchecked = self.add_margin(ui, 5, 0, 5, 0)
        self.uncheckedIcon = ImageTk.PhotoImage(unchecked)
        
        #checked box icon
        ci = PIL.Image.open('images\icons8-checked-checkbox-48.png').resize((30,30))
        checked = self.add_margin(ci, 5, 0, 5, 0)
        self.checkedIcon = ImageTk.PhotoImage(checked)       
        
        
       
        #max risk
        group_label = tk.Label(self.l4, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, 
                             text="Max Risk Report")
        group_label.grid(row=1, column=1, padx=5, sticky='W')

        self.iconLabel = tk.Label(self.l4, image=self.uncheckedIcon, bg=self.tab_color)
        self.iconLabel.image = self.uncheckedIcon # keep a reference!
        
        self.iconLabel.grid(row=1, column=0, padx=10, sticky='W')
        
        group_label.bind("<Enter>", partial(self.color_config, group_label, self.iconLabel, self.l4, 'light grey'))
        group_label.bind("<Leave>", partial(self.color_config, group_label, self.iconLabel, self.l4, self.tab_color))
        group_label.bind("<Button-1>", partial(self.check_box, self.iconLabel, "Max Risk"))
        
        self.iconLabel.bind("<Enter>", partial(self.color_config, group_label, self.iconLabel, self.l4, 'light grey'))
        self.iconLabel.bind("<Leave>", partial(self.color_config, group_label, self.iconLabel, self.l4, self.tab_color))
        self.iconLabel.bind("<Button-1>", partial(self.check_box, self.iconLabel, "Max Risk"))
        
        self.l4.bind("<Enter>", partial(self.color_config, group_label, self.iconLabel, self.l4, 'light grey'))
        self.l4.bind("<Leave>", partial(self.color_config, group_label, self.iconLabel, self.l4, self.tab_color))
        self.l4.bind("<Button-1>", partial(self.check_box, self.iconLabel, "Max Risk"))
        
        
        #%%
           
        c_label = tk.Label(self.l5, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, 
                             text="Cancer Drivers")
        c_label.grid(row=1, column=1, padx=5, sticky='W')
        
        #unchecked box      
        self.crLabel = tk.Label(self.l5, image=self.uncheckedIcon, bg=self.tab_color)
        self.crLabel.image = self.uncheckedIcon # keep a reference!
        self.crLabel.grid(row=1, column=0, padx=10, sticky='W')
        
        c_label.bind("<Enter>", partial(self.color_config, c_label, self.crLabel, self.l5, 'light grey'))
        c_label.bind("<Leave>", partial(self.color_config, c_label, self.crLabel, self.l5, self.tab_color))
        c_label.bind("<Button-1>", partial(self.check_box, self.crLabel, "Cancer Drivers"))        
        
        self.crLabel.bind("<Enter>", partial(self.color_config, c_label, self.crLabel, self.l5, 'light grey'))
        self.crLabel.bind("<Leave>", partial(self.color_config, c_label, self.crLabel, self.l5, self.tab_color))
        self.crLabel.bind("<Button-1>", partial(self.check_box, self.crLabel, "Cancer Drivers"))

        self.l5.bind("<Enter>", partial(self.color_config, c_label, self.crLabel, self.l5, 'light grey'))
        self.l5.bind("<Leave>", partial(self.color_config, c_label, self.crLabel, self.l5, self.tab_color))
        self.l5.bind("<Button-1>", partial(self.check_box, self.crLabel, "Cancer Drivers"))        
        
# %%        
        h_label = tk.Label(self.l6, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, 
                             text="Hazard Index Drivers")
        h_label.grid(row=1, column=1, padx=5, sticky='W')
        
        #unchecked box      
        self.hiLabel = tk.Label(self.l6, image=self.uncheckedIcon, bg=self.tab_color)
        self.hiLabel.image = self.uncheckedIcon # keep a reference!
        
        self.hiLabel.grid(row=1, column=0, padx=10, sticky='W')
        
        h_label.bind("<Enter>", partial(self.color_config, h_label, self.hiLabel, self.l6, 'light grey'))
        h_label.bind("<Leave>", partial(self.color_config, h_label, self.hiLabel, self.l6, self.tab_color))
        h_label.bind("<Button-1>", partial(self.check_box, self.hiLabel, "Hazard Index Drivers")) 

        self.hiLabel.bind("<Enter>", partial(self.color_config, h_label, self.hiLabel, self.l6, 'light grey'))
        self.hiLabel.bind("<Leave>", partial(self.color_config, h_label, self.hiLabel, self.l6, self.tab_color))
        self.hiLabel.bind("<Button-1>", partial(self.check_box, self.hiLabel, "Hazard Index Drivers"))
        
        self.l6.bind("<Enter>", partial(self.color_config, h_label, self.hiLabel, self.l6, 'light grey'))
        self.l6.bind("<Leave>", partial(self.color_config, h_label, self.hiLabel, self.l6, self.tab_color))
        self.l6.bind("<Button-1>", partial(self.check_box, self.hiLabel, "Hazard Index Drivers"))
#%%        
        his_label = tk.Label(self.l7, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, 
                             text="Risk Histogram")
        his_label.grid(row=1, column=1, padx=5, sticky='W')
        
        #unchecked box      
        self.gramLabel = tk.Label(self.l7, image=self.uncheckedIcon, bg=self.tab_color)
        self.gramLabel.image = self.uncheckedIcon # keep a reference!
        
        self.gramLabel.grid(row=1, column=0, padx=10, sticky='W')
        
        his_label.bind("<Enter>", partial(self.color_config, his_label, self.gramLabel, self.l7, 'light grey'))
        his_label.bind("<Leave>", partial(self.color_config, his_label, self.gramLabel, self.l7, self.tab_color))
        his_label.bind("<Button-1>", partial(self.check_box, self.gramLabel, "Risk Histogram")) 
        
        self.gramLabel.bind("<Enter>", partial(self.color_config, his_label, self.gramLabel, self.l7, 'light grey'))
        self.gramLabel.bind("<Leave>", partial(self.color_config, his_label, self.gramLabel, self.l7, self.tab_color))
        self.gramLabel.bind("<Button-1>", partial(self.check_box, self.gramLabel, "Risk Histogram"))
        
        self.l7.bind("<Enter>", partial(self.color_config, his_label, self.gramLabel, self.l7, 'light grey'))
        self.l7.bind("<Leave>", partial(self.color_config, his_label, self.gramLabel, self.l7, self.tab_color))
        self.l7.bind("<Button-1>", partial(self.check_box, self.gramLabel, "Risk Histogram"))
#%%        
        hz_label = tk.Label(self.l8, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, 
                             text="Hazard Index Histogram")
        hz_label.grid(row=1, column=1, padx=5, sticky='W')
        
        #unchecked box      
        self.zLabel = tk.Label(self.l8, image=self.uncheckedIcon, bg=self.tab_color)
        self.zLabel.image = self.uncheckedIcon # keep a reference!
        
        self.zLabel.grid(row=1, column=0, padx=10, sticky='W')
        
        hz_label.bind("<Enter>", partial(self.color_config, hz_label, self.zLabel, self.l8, 'light grey'))
        hz_label.bind("<Leave>", partial(self.color_config, hz_label, self.zLabel, self.l8, self.tab_color))
        hz_label.bind("<Button-1>", partial(self.check_box, self.zLabel, "Hazard Index Histogram"))
        
        self.zLabel.bind("<Enter>", partial(self.color_config, hz_label, self.zLabel, self.l8, 'light grey'))
        self.zLabel.bind("<Leave>", partial(self.color_config, hz_label, self.zLabel, self.l8, self.tab_color))
        self.zLabel.bind("<Button-1>", partial(self.check_box, self.zLabel, "Hazard Index Histogram"))
        
        self.l8.bind("<Enter>", partial(self.color_config, hz_label, self.zLabel, self.l8, 'light grey'))
        self.l8.bind("<Leave>", partial(self.color_config, hz_label, self.zLabel, self.l8, self.tab_color))
        self.l8.bind("<Button-1>", partial(self.check_box, self.zLabel, "Hazard Index Histogram"))
        
        
#%%        
        i_label = tk.Label(self.l9, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, 
                             text="Incidence Drivers")
        i_label.grid(row=1, column=1, padx=5, sticky='W')
        
        #unchecked box      
        self.idLabel = tk.Label(self.l9, image=self.uncheckedIcon, bg=self.tab_color)
        self.idLabel.image = self.uncheckedIcon # keep a reference!
        
        self.idLabel.grid(row=1, column=0, padx=10, sticky='W') 
        
        i_label.bind("<Enter>", partial(self.color_config, i_label, self.idLabel, self.l9, 'light grey'))
        i_label.bind("<Leave>", partial(self.color_config, i_label, self.idLabel, self.l9, self.tab_color))
        i_label.bind("<Button-1>", partial(self.check_box, self.idLabel, "Incidence Drivers")) 
        
        self.idLabel.bind("<Enter>", partial(self.color_config, i_label, self.idLabel, self.l9, 'light grey'))
        self.idLabel.bind("<Leave>", partial(self.color_config, i_label, self.idLabel, self.l9, self.tab_color))
        self.idLabel.bind("<Button-1>", partial(self.check_box, self.idLabel, "Incidence Drivers")) 
        
        self.l9.bind("<Enter>", partial(self.color_config, i_label, self.idLabel, self.l9, 'light grey'))
        self.l9.bind("<Leave>", partial(self.color_config, i_label, self.idLabel, self.l9, self.tab_color))
        self.l9.bind("<Button-1>", partial(self.check_box, self.idLabel, "Incidence Drivers")) 
#%%        
    
        
        ai_label = tk.Label(self.l10, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, text="Acute Impacts")
        ai_label.grid(row=1, column=4, padx=5, sticky="W")

        #unchecked box      
        self.aLabel = tk.Label(self.l10, image=self.uncheckedIcon, bg=self.tab_color)
        self.aLabel.image = self.uncheckedIcon # keep a reference!
        
        self.aLabel.grid(row=1, column=3, padx=10, sticky='W') 
        
        ai_label.bind("<Enter>", partial(self.color_config, ai_label, self.aLabel, self.l10, 'light grey'))
        ai_label.bind("<Leave>", partial(self.color_config, ai_label, self.aLabel, self.l10, self.tab_color))
        ai_label.bind("<Button-1>", partial(self.check_box, self.aLabel, "Acute Impacts")) 
        
        self.aLabel.bind("<Enter>", partial(self.color_config, ai_label, self.aLabel, self.l10, 'light grey'))
        self.aLabel.bind("<Leave>", partial(self.color_config, ai_label, self.aLabel, self.l10, self.tab_color))
        self.aLabel.bind("<Button-1>", partial(self.check_box, self.aLabel, "Acute Impacts")) 
        
        self.l10.bind("<Enter>", partial(self.color_config, ai_label, self.aLabel, self.l10, 'light grey'))
        self.l10.bind("<Leave>", partial(self.color_config, ai_label, self.aLabel, self.l10, self.tab_color))
        self.l10.bind("<Button-1>", partial(self.check_box, self.aLabel, "Acute Impacts")) 
        
#%%
       
        mp_label = tk.Label(self.r4, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, text="Multipathway")
        mp_label.grid(row=1, column=4, padx=5, sticky="W")
        
        #unchecked box      
        self.mLabel = tk.Label(self.r4, image=self.uncheckedIcon, bg=self.tab_color)
        self.mLabel.image = self.uncheckedIcon # keep a reference!
        
        self.mLabel.grid(row=1, column=3, padx=10, sticky='W') 
        
        mp_label.bind("<Enter>", partial(self.color_config, mp_label, self.mLabel, self.r4, 'light grey'))
        mp_label.bind("<Leave>", partial(self.color_config, mp_label, self.mLabel, self.r4, self.tab_color))
        mp_label.bind("<Button-1>", partial(self.check_box, self.mLabel, "Multipathway")) 
        
        self.mLabel.bind("<Enter>", partial(self.color_config, mp_label, self.mLabel, self.r4, 'light grey'))
        self.mLabel.bind("<Leave>", partial(self.color_config, mp_label, self.mLabel, self.r4, self.tab_color))
        self.mLabel.bind("<Button-1>", partial(self.check_box, self.mLabel, "Multipathway"))
        
        self.r4.bind("<Enter>", partial(self.color_config, mp_label, self.mLabel, self.r4, 'light grey'))
        self.r4.bind("<Leave>", partial(self.color_config, mp_label, self.mLabel, self.r4, self.tab_color))
        self.r4.bind("<Button-1>", partial(self.check_box, self.mLabel, "Multipathway"))

#%%        
        st_label = tk.Label(self.r5, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, text="Source Type Risk Histogram")
        st_label.grid(row=1, column=4, padx=5, sticky="W")
        
        #unchecked box      
        self.sLabel = tk.Label(self.r5, image=self.uncheckedIcon, bg=self.tab_color)
        self.sLabel.image = self.uncheckedIcon # keep a reference!
        
        self.sLabel.grid(row=1, column=3, padx=10, sticky='W') 
        
        self.sLabel.bind("<Enter>", partial(self.color_config, st_label, self.sLabel, self.r5, 'light grey'))
        self.sLabel.bind("<Leave>", partial(self.color_config, st_label, self.sLabel, self.r5, self.tab_color))
        self.sLabel.bind("<Button-1>", partial(self.set_sourcetype, self.sLabel, "Source Type Risk Histogram"))
        
        st_label.bind("<Enter>", partial(self.color_config, self.sLabel, st_label, self.r5, 'light grey'))
        st_label.bind("<Leave>", partial(self.color_config, self.sLabel, st_label, self.r5, self.tab_color))
        st_label.bind("<Button-1>", partial(self.set_sourcetype, self.sLabel, "Source Type Risk Histogram"))
        
        self.r5.bind("<Enter>", partial(self.color_config, st_label, self.sLabel, self.r5, 'light grey'))
        self.r5.bind("<Leave>", partial(self.color_config, st_label, self.sLabel, self.r5, self.tab_color))
        self.r5.bind("<Button-1>", partial(self.set_sourcetype, self.sLabel, "Source Type Risk Histogram"))
        
        

#%%        
        z_label = tk.Label(self.r6, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, text="")
        z_label.grid(row=1, column=4, padx=5, sticky="W")
        
        #unchecked box      
        self.vLabel = tk.Label(self.r6, text="", width=5, bg=self.tab_color)
#        self.vLabel.image = self.uncheckedIcon # keep a reference!
        
        self.vLabel.grid(row=1, column=3, padx=10, sticky='W') 
        
        z_label.bind("<Enter>", partial(self.fake_config, z_label, self.vLabel, self.r6, 'light grey'))
        z_label.bind("<Leave>", partial(self.fake_config, z_label, self.vLabel, self.r6, self.tab_color))
        
        
        
#%%        
        w_label = tk.Label(self.r7, font=TEXT_FONT, width=32, anchor='w', bg=self.tab_color, text="")
        w_label.grid(row=1, column=4, padx=5, sticky="W")
        
        #unchecked box      
        self.uLabel = tk.Label(self.r7, text="", width=5, bg=self.tab_color)
#        self.uLabel.image = self.uncheckedIcon # keep a reference!
        
        self.uLabel.grid(row=1, column=3, padx=10, sticky='W') 
        
        w_label.bind("<Enter>", partial(self.fake_config, w_label, self.uLabel, self.r7, 'light grey'))
        w_label.bind("<Leave>", partial(self.fake_config, w_label, self.uLabel, self.r7, self.tab_color))
        






#%%        
        t_label = tk.Label(self.r8, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, text="")
        t_label.grid(row=1, column=4, padx=5, sticky="W")
        
        #unchecked box      
        self.zLabel = tk.Label(self.r8, text="", width=5, bg=self.tab_color)
#        self.zLabel.image = self.uncheckedIcon # keep a reference!
        
        self.zLabel.grid(row=1, column=3, padx=10, sticky='W') 
        
        t_label.bind("<Enter>", partial(self.fake_config, t_label, self.zLabel, self.r8, 'light grey'))
        t_label.bind("<Leave>", partial(self.fake_config, t_label, self.zLabel, self.r8, self.tab_color))
        
                
        #%%        
        h_label = tk.Label(self.r9, font=TEXT_FONT, width=52, anchor='w', bg=self.tab_color, text="")
        h_label.grid(row=1, column=4, padx=5, sticky="W")
        
        #unchecked box      
        self.rLabel = tk.Label(self.r9, text="", width=5, bg=self.tab_color)
#        self.rLabel.image = self.uncheckedIcon # keep a reference!
        
        self.rLabel.grid(row=1, column=3, padx=10, sticky='W') 
        
        h_label.bind("<Enter>", partial(self.fake_config, h_label, self.rLabel, self.r9, 'light grey'))
        h_label.bind("<Leave>", partial(self.fake_config, h_label, self.rLabel, self.r9, self.tab_color))
        
        
            
#%%
        ru = PIL.Image.open('images\icons8-create-48.png').resize((30,30))
        ricon = self.add_margin(ru, 5, 0, 5, 0)
        rileicon = ImageTk.PhotoImage(ricon)
        rileLabel = tk.Label(self.meta_two, image=rileicon, bg=self.tab_color)
        rileLabel.image = rileicon # keep a reference!
        rileLabel.grid(row=0, column=1, padx=5, pady=20, sticky='E')
        
        
        run_button = tk.Label(self.meta_two, text="Run Reports", font=TEXT_FONT, bg=self.tab_color)
        run_button.grid(row=0, column=2, padx=5, pady=20, sticky='W')
        
        
        
        
        run_button.bind("<Enter>", partial(self.color_config, run_button, rileLabel, self.meta_two, 'light grey'))
        run_button.bind("<Leave>", partial(self.color_config, run_button, rileLabel, self.meta_two, self.tab_color))
        run_button.bind("<Button-1>", self.run_reports)
        
        rileLabel.bind("<Enter>", partial(self.color_config, rileLabel, run_button, self.meta_two, 'light grey'))
        rileLabel.bind("<Leave>", partial(self.color_config, rileLabel, run_button, self.meta_two, self.tab_color))
        rileLabel.bind("<Button-1>", self.run_reports)
##%%
#        
#        
        
        
        
        
        
        
        
    def check_box(self, icon, text, event):
        print(self.checked)

        if text not in self.checked:
            icon.configure(image=self.checkedIcon)
            self.checked.append(text)
            self.checked_icons.append(icon)
            
        elif text in self.checked:
            icon.configure(image=self.uncheckedIcon)
            self.checked.remove(text)
            self.checked_icons.remove(icon)
            

        
            
 #maybe move checked and unchecked logic out to be if else bind partial as checked
        
    
    def add_margin(self, pil_img, top, right, bottom, left):
        width, height = pil_img.size
        new_width = width + right + left
        new_height = height + top + bottom
        result = PIL.Image.new(pil_img.mode, (new_width, new_height))
        result.paste(pil_img, (left, top))
        return result    

        
    def browse(self, icon, event):
        
        
        self.fullpath = tk.filedialog.askdirectory()
        print(self.fullpath)
        icon["text"] = self.fullpath.split("/")[-1]

        
    def set_sourcetype(self, icon, text, event):
            
        if text not in self.checked:
            icon.configure(image=self.checkedIcon)
            self.checked.append(text)
            self.checked_icons.append(icon)
            
       
  
            self.pos = tk.Label(self.r6, font=TEXT_FONT, bg=self.tab_color, text="Enter the position in the source ID where the\n source ID type begins.The default is 1.")
            self.pos.grid(row=1, column=4, padx=5, sticky="W")
            
            self.pos_num = ttk.Entry(self.r6)
            self.pos_num["width"] = 5
            self.pos_num.grid(row=1, column=3, padx=5, sticky="W")
        
            self.chars = tk.Label(self.r7, font=TEXT_FONT, bg=self.tab_color, text="Enter the number of characters \nof the sourcetype ID")
            self.chars.grid(row=1, column=4, padx=5, sticky="W")
            
            self.chars_num = ttk.Entry(self.r7)
            self.chars_num["width"] = 5
            self.chars_num.grid(row=1, column=3, padx=5, sticky="W")
        
        elif text in self.checked:
            
            icon.configure(image=self.uncheckedIcon)
            self.checked.remove(text)
            self.checked_icons.remove(icon)
            
            
            self.pos.destroy()
            self.pos_num.destroy()
            self.chars.destroy()
            self.chars_num.destroy()
            
            z_label = tk.Label(self.r6, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, text="")
            z_label.grid(row=1, column=4, padx=5, sticky="W")
            
            #unchecked box      
            self.vLabel = tk.Label(self.r6, text="", width=5, bg=self.tab_color)
    #        self.vLabel.image = self.uncheckedIcon # keep a reference!
            
            self.vLabel.grid(row=1, column=3, padx=10, sticky='W') 
            
            z_label.bind("<Enter>", partial(self.fake_config, z_label, self.vLabel, self.r6, 'light grey'))
            z_label.bind("<Leave>", partial(self.fake_config, z_label, self.vLabel, self.r6, self.tab_color))
            
            
            
            
            w_label = tk.Label(self.r7, font=TEXT_FONT, width=32, anchor='w', bg=self.tab_color, text="")
            w_label.grid(row=1, column=4, padx=5, sticky="W")
            
            #unchecked box      
            self.uLabel = tk.Label(self.r7, text="", width=5, bg=self.tab_color)
    #        self.uLabel.image = self.uncheckedIcon # keep a reference!
            
            self.uLabel.grid(row=1, column=3, padx=10, sticky='W') 
            
            w_label.bind("<Enter>", partial(self.fake_config, w_label, self.uLabel, self.r7, 'light grey'))
            w_label.bind("<Leave>", partial(self.fake_config, w_label, self.uLabel, self.r7, self.tab_color))
                
                
                
    def run_reports(self, event):
        
         self.nav.summaryLabel.configure(image=self.nav.greenIcon)
        
         executor = ThreadPoolExecutor(max_workers=1)
         future = executor.submit(self.createReports)
         #future.add_done_callback(self.reset_reports) 
         
         self.lift_page(self.nav.liLabel, self.nav.logLabel, self.nav.log, self.nav.current_button)

        
    def createReports(self,  arguments=None):

        ready= False
        print('ready')
        #check to see if there is a directory location
        print(self.fullpath)
        
        #set log file to append to in folder 
        logpath = self.fullpath +"/hem4.log"
        
        #open log 
        self.logfile = open(logpath, 'a')
        now = str(datetime.now())

        
        

        
        try:
                        
            # Figure out which facilities will be included in the report
            skeleton = os.path.join(self.fullpath, '*facility_max_risk_and_hi.xl*')
            print(skeleton)
            fname = glob.glob(skeleton)
            print(fname)
            
            if fname:
                head, tail = os.path.split(fname[0])
                groupname = tail[:tail.find('facility_max_risk_and_hi')-1]
                facmaxrisk = FacilityMaxRiskandHI(targetDir=self.fullpath, filenameOverride=tail)
                facmaxrisk_df = facmaxrisk.createDataframe()
                faclist = facmaxrisk_df['Facil_id'].tolist()
            else:
                
                Logger.logMessage("Cannot generate summaries because there is no Facility_Max_Risk_and_HI Excel file \
                                  in the folder you selected.")
                
                messagebox.showinfo("Error", "Cannot generate summaries because there is no Facility_Max_Risk_and_HI Excel file \
                                  in the folder you selected.")
                ready = False 
          
            
        except Exception as e:
             print(e)
             print("No facilities selected.",
                "Please select a run folder.")
             messagebox.showinfo("No facilities selected",
                "Please select a run folder.")
             
             ready = False
        # Figure out which facilities will be included in the report.
        # Facilities listed in the facility_max_risk_and_hi HEM4 output will be used
        # and the modeling group name is taken from the first part of the filename.
        
        
                
        #get reports and set arguments
        reportNames = []
        reportNameArgs = {}
        
        try:
            for report in self.checked:
                print(self.checked)
                
                
                if report == 'Max Risk':
                    reportNames.append('MaxRisk')
                    reportNameArgs['MaxRisk'] = None
                if report == 'Cancer Drivers':
                    reportNames.append('CancerDrivers')
                    reportNameArgs['CancerDrivers'] = None
                if report == 'Hazard Index Drivers':
                    reportNames.append('HazardIndexDrivers')
                    reportNameArgs['HazardIndexDrivers'] = None
                if report == 'Risk Histogram':
                    reportNames.append('Histogram')
                    reportNameArgs['Histogram'] = None
                if report == 'Hazard Index Histogram':
                    reportNames.append('HI_Histogram')
                    reportNameArgs['HI_Histogram'] = None
                if report == 'Incidence Drivers':
                    reportNames.append('IncidenceDrivers')
                    reportNameArgs['IncidenceDrivers'] = None
                if report == "Acute Impacts":
                    reportNames.append('AcuteImpacts')
                    reportNameArgs['AcuteImpacts'] = None                
                if report == "Source Type Risk Histogram":
                    reportNames.append('SourceTypeRiskHistogram')
                    # Pass starting position and number of characters
                    # Translate user supplied starting position to array index value (0-based indexing)
                    if self.pos_num.get() == '' or self.pos_num.get() == '0':
                        startpos = 0
                        print(startpos)
                    else:
                        startpos = int(self.pos_num.get()) - 1
                        print(startpos)
                    
                    # Convert non-numeric to 0 (handles blank case)
                    if self.chars_num.get().isnumeric():
                        numchars = int(self.chars_num.get())
                    else:
                        numchars = 0
                    print(numchars)
                        
                    reportNameArgs['SourceTypeRiskHistogram'] = [startpos, numchars] 
                    
                if report == "Multipathway":
                    reportNames.append('MultiPathway')
                    reportNameArgs['MultiPathway'] = None
                    
        except Exception as e:
             print(e)
               

        #add run checks
        if len(self.checked) == 0:
            
            messagebox.showinfo("No report selected",
                "Please select one or more report types to run.")
            
            ready = False
        else:
            
            #check if source type has been selected
            if "Source Type Risk Histogram" in self.checked:
                if startpos < 0:
                    messagebox.showinfo('Invalid starting position',
                                        'Starting position of the sourcetype ID must be > 0.')
                    ready = False
                else:
                    if numchars <= 0:
                        messagebox.showinfo('Invalid number of sourcetype ID characters',
                                            'Please enter a valid number of characters of the sourcetype ID.')
                    
                        ready = False
      
                    else:
                        
                        ready = True
                    
            else:
                ready = True
                    
                    
        #if checks have been passed 
        if ready == True:
        

         
            running_message = "\nRunning report(s) on facilities: " + ', '.join(faclist)
            
            
            
            #write to log
            self.logfile.write(str(datetime.now()) + ":    " + running_message + "\n")
            
            self.nav.log.scr.configure(state='normal')
            self.nav.log.scr.insert(tk.INSERT, running_message)
            self.nav.log.scr.insert(tk.INSERT, "\n")
            self.nav.log.scr.configure(state='disabled')
    
            summaryMgr = SummaryManager(self.fullpath, groupname, faclist)
                    
            #loop through for each report selected
            for reportName in reportNames:
                report_message = "Creating " + reportName + " report."
                
                self.nav.log.scr.configure(state='normal')
                self.nav.log.scr.insert(tk.INSERT, report_message)
                self.nav.log.scr.insert(tk.INSERT, "\n")
                self.nav.log.scr.configure(state='disabled')
                
                self.logfile.write(str(datetime.now()) + ":    " + report_message + "\n")
                
                args = reportNameArgs[reportName]
                summaryMgr.createReport(self.fullpath, reportName, args)
                
                if summaryMgr.status == True:
                
                    report_complete = reportName +  " complete."
                    self.nav.log.scr.configure(state='normal')
                    self.nav.log.scr.insert(tk.INSERT, report_complete)
                    self.nav.log.scr.insert(tk.INSERT, "\n")
                    self.nav.log.scr.configure(state='disabled')
                    
                    self.logfile.write(str(datetime.now()) + ":    " + report_complete + "\n")

                    
                else:
                    
                    break
                
            self.nav.log.scr.configure(state='normal')
            self.nav.log.scr.insert(tk.INSERT, "Risk Summary Reports Finished.")
            self.nav.log.scr.insert(tk.INSERT, "\n")
            self.nav.log.scr.configure(state='disabled')
            self.logfile.write(str(datetime.now()) + ":    " + "Risk Summary Reports Finished." + "\n")

            
            messagebox.showinfo("Summary Reports Finished", "Risk summary reports for  " + ', '.join(faclist) + " run.")
            
            if "Source Type Risk Histogram" in self.checked:
                self.pos.destroy()
                self.pos_num.destroy()
                self.chars.destroy()
                self.chars_num.destroy()
                
                z_label = tk.Label(self.r6, font=TEXT_FONT, width=22, anchor='w', bg=self.tab_color, text="")
                z_label.grid(row=1, column=3, padx=5, sticky="W")
                
                #unchecked box      
                self.vLabel = tk.Label(self.r6, text="", width=5, bg=self.tab_color)
        #        self.vLabel.image = self.uncheckedIcon # keep a reference!
                
                self.vLabel.grid(row=1, column=4, padx=10, sticky='W') 
                
                z_label.bind("<Enter>", partial(self.fake_config, z_label, self.vLabel, self.r6, 'light grey'))
                z_label.bind("<Leave>", partial(self.fake_config, z_label, self.vLabel, self.r6, self.tab_color))
                
                                
                w_label = tk.Label(self.r7, font=TEXT_FONT, width=32, anchor='w', bg=self.tab_color, text="")
                w_label.grid(row=1, column=3, padx=5, sticky="W")
                
                #unchecked box      
                self.uLabel = tk.Label(self.r7, text="", width=5, bg=self.tab_color)
        #        self.uLabel.image = self.uncheckedIcon # keep a reference!
                
                self.uLabel.grid(row=1, column=4, padx=10, sticky='W') 
                
                w_label.bind("<Enter>", partial(self.fake_config, w_label, self.uLabel, self.r7, 'light grey'))
                w_label.bind("<Leave>", partial(self.fake_config, w_label, self.uLabel, self.r7, self.tab_color))
                

            
            for icon in self.checked_icons:
                icon.configure(image=self.uncheckedIcon)
                
                
            self.folder_select['text'] = "Select output folder"
            self.nav.summaryLabel.configure(image=self.nav.summaryIcon)
            
            self.logfile.close()
            
            
            
            


    def fake_config(self, widget1, widget2, container, color, event):
        widget1

                   

#
    def lift_page(self, widget1, widget2, page, previous):
        """
        Function lifts page and changes button color to active, 
        changes previous button color
        """
        try: 
            widget1.configure(bg=self.tab_color)
            widget2.configure(bg=self.tab_color)
            
            if len(self.nav.current_button) > 0:
                
                for i in self.nav.current_button:
                    i.configure(bg=self.main_color)
            
            print('Current Button before:', self.nav.current_button)         
            print('page:', page)
            page.lift()
            self.nav.current_button = [widget1, widget2]
            print('Current Button after:', self.nav.current_button)  
        except Exception as e:
            
            print(e)

    
    def color_config(self, widget1, widget2, container, color, event):
        
         widget1.configure(bg=color)
         widget2.configure(bg=color)
         container.configure(bg=color)   
