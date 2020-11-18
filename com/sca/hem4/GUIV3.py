
# Importing tkinter module 
import tkinter as tk 
import tkinter.ttk as ttk
from tkinter import messagebox
from tkinter import scrolledtext
from tkinter.filedialog import askopenfilename
from tkinter.simpledialog import Dialog, Toplevel


from PIL import ImageTk
import PIL.Image
from PyQt5 import QtGui

from com.sca.hem4.Analyze import Analyze
from com.sca.hem4.RiskReport import Summary
from com.sca.hem4.HEM4 import *

from com.sca.hem4.runner.FacilityRunner import FacilityRunner
from com.sca.hem4.Processor import Processor
from com.sca.hem4.log.Logger import Logger
from com.sca.hem4.model.Model import Model
from com.sca.hem4.upload.FileUploader import FileUploader
from com.sca.hem4.checker.InputChecker import InputChecker
from com.sca.hem4.DepositionDepletion import check_dep, check_phase
from com.sca.hem4.SaveState import SaveState

from com.sca.hem4.writer.excel.FacilityMaxRiskandHI import FacilityMaxRiskandHI
from com.sca.hem4.writer.excel.FacilityCancerRiskExp import FacilityCancerRiskExp
from com.sca.hem4.writer.excel.FacilityTOSHIExp import FacilityTOSHIExp
from com.sca.hem4.writer.kml.KMLWriter import KMLWriter
from com.sca.hem4.inputsfolder.InputsPackager import InputsPackager


from com.sca.hem4.tools.CensusUpdater import CensusUpdater


import shutil
import webbrowser
import sys
from functools import partial
import os
from collections import defaultdict
import uuid
import glob
import importlib
from datetime import datetime
import pickle

 
from concurrent.futures import ThreadPoolExecutor
import threading
from threading import Event
import traceback
import queue



import numpy as np
from pandastable import Table, filedialog, np



#%%

#NAV_FONT = tkFont.Font(family='Verdana', )

TITLE_FONT= ("Daytona", 16, 'bold')
TAB_FONT =("Daytona", 11, 'bold')
TEXT_FONT = ("Daytona", 14)
#SUB_FONT = ("Verdana", 12)



 
class Page(tk.Frame):
    def __init__(self, *args, **kwargs):
        tk.Frame.__init__(self, *args, **kwargs)
        #set mainframe background color
        self.main_color = "white"
        self.tab_color = "lightcyan3"
        self.highlightcolor = "snow3"
   
                
    
    def add_margin(self, pil_img, top, right, bottom, left):
        width, height = pil_img.size
        new_width = width + right + left
        new_height = height + top + bottom
        result = PIL.Image.new(pil_img.mode, (new_width, new_height))
        result.paste(pil_img, (left, top))
        return result  
    
    def color_config(self, widget1, widget2, container, color, event):
        
         widget1.configure(bg=color)
         widget2.configure(bg=color)
         container.configure(bg=color)  
         
    def fix_config(self, widget1, widget2, previous):
        
         try: 
            widget1.configure(bg=self.tab_color)
            widget2.configure(bg=self.tab_color)
            
            if len(previous) > 0:
                
                for i in previous:
                    i.configure(bg=self.main_color)
                    
         except:
                pass
            
    
       # Instructions for HEM4    
  
     #%% Event handlers for porting instructions
    def add_instructions(self, placeholder1, placeholder2):
        
        #Dynamic instructions place holder
        global instruction_instance
        self.instruction_instance = tk.StringVar(placeholder1)
        self.instruction_instance.set(" ")
        self.optional.instruction_instance.set(" ")
        self.depdeplt.instruction_instance.set(" ")
        
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
        self.optional.instruction_instance.set(" ")
        self.depdeplt.instruction_instance.set(" ")
        
    #general function for browsing instructions
    def browse(self, location):
        """
        Function looks up text file with instructions for specified input
        browse buttons
        """
        global instruction_instance
        self.read_inst = open(location, 'r')
        self.instruction_instance.set(self.read_inst.read())
        self.optional.instruction_instance.set(self.read_inst.read())
        self.depdeplt.instruction_instance.set(self.read_inst.read())
        
        
        
    def show(self):
        self.lift()
        
    def disabled_message(self, event):
        """ Pop up for user when trying to run census updatae and hem4 modelling 
            concurrently
            
        """
        
        messagebox.showinfo("Application Running","This feature is disabled while the application is running.")
        
        
        
    #%% File upload helpers
    
    def is_valid_extension(self, filepath):
        """
        Function checks to make sure excel/csv files are selected for inputs
          
        """
        extensions = [".xls", ".xlsx", ".XLS", ".csv", ".CSV"]
        return any(ext in filepath for ext in extensions)

    def openFile(self, filename):
        """
        This function opens file dialogs for uploading inputs
        
        """

        if filename is None:
            # upload was canceled
            print("Canceled!")
            return None
        elif not self.is_valid_extension(filename):
            messagebox.showinfo("Invalid file format", 
                                "Not a valid file format, please upload an excel/csv file as per the instructions.")
            return None
        else:
            return os.path.abspath(filename)
    
            


class MainView(tk.Frame):
    def __init__(self, master, *args, **kwargs):
        tk.Frame.__init__(self, master=master, *args, **kwargs)
        
        #set mainframe background color
        self.main_color = "white" 
        self.tab_color = "lightcyan3"
        self.highlightcolor = "snow3"
        self.current_button = []
        
        
        self.home = master
        self.container = tk.Frame(self, width=750, height=600, bg=self.main_color)
        self.container.pack(fill="both", expand=True)
        
        #instantiate start page
        self.nav = Start(self)
        self.nav.place(in_=self.container, relx=0.3, relwidth=0.7, relheight=1)

        #instantiate log tab
        self.log = Log(self)
        self.log.place(in_=self.container, relx=0.3, relwidth=0.7, relheight=1)
        self.log.lower()

        #instantiate hem4 start page
        self.hem = Hem(self)
        self.hem.place(in_=self.container, relx=0.3, relwidth=0.7, relheight=1)
        self.hem.lower()
        
        #instantiate summary tab
        self.summary = Summary(self)
        self.summary.place(in_=self.container, relx=0.3, relwidth=0.7, relheight=1)
        self.summary.lower()
        
        #instantiate analyze outputs
        self.analyze = Analyze(self)
        self.analyze.place(in_=self.container, relx=0.3, relwidth=0.7, relheight=1)
        self.analyze.lower()
        
        
        self.options = Options(self)
        self.options.place(in_=self.container, relx=0.3, relwidth=0.7, relheight=1)
        self.options.lower()
        
#%%
        #HEM4 nav button
        self.newrunLabel= tk.Label(self, text="RUN HEM4", font=TAB_FONT, bg=self.main_color, height=2, pady=2, anchor="w")
        self.newrunLabel.place(in_=self.container, relwidth=0.2, relx=0.1, rely=0.09)
        
        #add run icon with margin for highlight
        #ri = PIL.Image.open('images\loading-png-gif-transparent.png').resize((30,30))
        self.ri = PIL.Image.open('images\icons8-virtual-machine-52.png').resize((30,30))
        self.gi = PIL.Image.open('images\icons8-green-circle-48.png').resize((30,30))
        self.cani = PIL.Image.open('images\icons8-cancel-48.png').resize((30,30))

        
        run_new = self.add_margin(self.ri, 5, 0, 5, 0)
        run_change = self.add_margin(self.gi, 5, 0, 5, 0)
        cancel_change = self.add_margin(self.cani, 5, 0, 5, 0)
        
        self.runIcon = ImageTk.PhotoImage(run_new)
        self.greenIcon = ImageTk.PhotoImage(run_change)
        self.cancelIcon = ImageTk.PhotoImage(cancel_change)

        
        self.iconLabel = tk.Label(self, image=self.runIcon, bg=self.main_color)
        self.iconLabel.image = self.runIcon # keep a reference!
        self.iconLabel.place(in_=self.container, relwidth=0.1, rely=0.09)
        
        #bind icon and label events
        self.newrunLabel.bind("<Enter>", partial(self.color_config, self.newrunLabel, self.iconLabel, self.highlightcolor))
        self.newrunLabel.bind("<Leave>", partial(self.color_config, self.newrunLabel, self.iconLabel, self.main_color))
        self.newrunLabel.bind("<Button-1>", partial(self.lift_page, self.newrunLabel, self.iconLabel, self.hem, self.current_button))
        
        self.iconLabel.bind("<Enter>", partial(self.color_config, self.iconLabel, self.newrunLabel, self.highlightcolor))
        self.iconLabel.bind("<Leave>", partial(self.color_config, self.iconLabel, self.newrunLabel,self.main_color))
        self.iconLabel.bind("<Button-1>", partial(self.lift_page, self.iconLabel, self.newrunLabel, self.hem, self.current_button))
        
        
        
        
        #%%
        
        #Options nav button
        self.optionsLabel= tk.Label(self, text="REVISE CENSUS DATA", font=TAB_FONT, bg=self.main_color, height=2, anchor="w")
        self.optionsLabel.place(in_=self.container, relwidth=0.2, rely=0.18, relx=0.1)

#        #add run icon with margin for highlight
        oi = PIL.Image.open('images\icons8-settings-48.png').resize((30,30))
        oinew = self.add_margin(oi, 4, 0, 4, 0)
        
        optionIcon = ImageTk.PhotoImage(oinew)
        self.gearLabel = tk.Label(self, image=optionIcon, bg=self.main_color)
        self.gearLabel.image = optionIcon # keep a reference!
        self.gearLabel.place(in_=self.container, relwidth=0.1, rely=0.18)
        
         #bind icon and label events
        self.optionsLabel.bind("<Enter>", partial(self.color_config, self.optionsLabel, self.gearLabel, self.highlightcolor))
        self.optionsLabel.bind("<Leave>", partial(self.color_config, self.optionsLabel, self.gearLabel,self.main_color))
        self.optionsLabel.bind("<Button-1>", partial(self.lift_page, self.optionsLabel, self.gearLabel, self.options, self.current_button))
        
        self.gearLabel.bind("<Enter>", partial(self.color_config, self.gearLabel, self.optionsLabel, self.highlightcolor))
        self.gearLabel.bind("<Leave>", partial(self.color_config, self.gearLabel, self.optionsLabel,self.main_color))
        self.gearLabel.bind("<Button-1>", partial(self.lift_page, self.gearLabel, self.optionsLabel, self.options, self.current_button))
        
        
        #%%       

#        #Risk Summary nav button
        self.riskLabel= tk.Label(self, text="SUMMARIZE RISKS", font=TAB_FONT, bg=self.main_color, height=2, pady=2, anchor="w")
        self.riskLabel.place(in_=self.container, relwidth=0.2, rely=0.27, relx=0.1)
        
        #add run icon with margin for highlight
        self.si = PIL.Image.open('images\icons8-edit-graph-report-48.png').resize((30,30))

        summary_new = self.add_margin(self.si, 5, 0, 5, 0)
        
        self.summaryIcon = ImageTk.PhotoImage(summary_new)
        
        
        self.summaryLabel = tk.Label(self, image=self.summaryIcon, bg=self.main_color)
        self.summaryLabel.image = self.summaryIcon # keep a reference!
        self.summaryLabel.place(in_=self.container, relwidth=0.1, rely=0.27)
        
         #bind icon and label events
        self.riskLabel.bind("<Enter>", partial(self.color_config, self.riskLabel, self.summaryLabel, self.highlightcolor))
        self.riskLabel.bind("<Leave>", partial(self.color_config, self.riskLabel, self.summaryLabel, self.main_color))
        self.riskLabel.bind("<Button-1>", partial(self.lift_page, self.riskLabel, self.summaryLabel, self.summary, self.current_button))
        
        self.summaryLabel.bind("<Enter>", partial(self.color_config, self.summaryLabel, self.riskLabel, self.highlightcolor))
        self.summaryLabel.bind("<Leave>", partial(self.color_config, self.summaryLabel, self.riskLabel, self.main_color))
        self.summaryLabel.bind("<Button-1>", partial(self.lift_page, self.summaryLabel, self.riskLabel, self.summary, self.current_button))

        #%%       
        
        #Analyze Outputs nav button
        self.analyzeLabel= tk.Label(self, text="ANALYZE OUTPUTS", font=TAB_FONT, bg=self.main_color, height=2, pady=2, anchor="w")
        self.analyzeLabel.place(in_=self.container, relwidth=0.2, rely=0.36, relx=0.1)

        #add run icon with margin for highlight
        ai = PIL.Image.open('images\icons8-graph-48.png').resize((30,30))
        analyzenew = self.add_margin(ai, 5, 0, 5, 0)
        
        analyzeIcon = ImageTk.PhotoImage(analyzenew)
        self.outputLabel = tk.Label(self, image=analyzeIcon, bg=self.main_color)
        self.outputLabel.image = analyzeIcon # keep a reference!
        self.outputLabel.place(in_=self.container, relwidth=0.1, rely=0.36)
        
         #bind icon and label events
        self.analyzeLabel.bind("<Enter>", partial(self.color_config, self.analyzeLabel, self.outputLabel, self.highlightcolor))
        self.analyzeLabel.bind("<Leave>", partial(self.color_config, self.analyzeLabel, self.outputLabel,self.main_color))
        self.analyzeLabel.bind("<Button-1>", partial(self.lift_page, self.analyzeLabel, self.outputLabel, self.analyze, self.current_button))
        
        self.outputLabel.bind("<Enter>", partial(self.color_config, self.outputLabel, self.analyzeLabel, self.highlightcolor))
        self.outputLabel.bind("<Leave>", partial(self.color_config, self.outputLabel, self.analyzeLabel,self.main_color))
        self.outputLabel.bind("<Button-1>", partial(self.lift_page, self.outputLabel, self.analyzeLabel, self.analyze, self.current_button))
        

        #%%     
        #Log nav button
        self.logLabel= tk.Label(self, text="LOG", font=TAB_FONT, bg=self.main_color, height=2, anchor="w")
        self.logLabel.place(in_=self.container, relwidth=0.2, rely=0.45, relx=0.1)

#        #add run icon with margin for highlight
        self.li = PIL.Image.open('images\icons8-console-48.png').resize((30,30))
        linew = self.add_margin(self.li, 4, 0, 4, 0)
        
        logIcon = ImageTk.PhotoImage(linew)
        self.liLabel = tk.Label(self, image=logIcon, bg=self.main_color)
        self.liLabel.image = logIcon # keep a reference!
        self.liLabel.place(in_=self.container, relwidth=0.1, rely=0.45)
        
         #bind icon and label events
        self.logLabel.bind("<Enter>", partial(self.color_config, self.logLabel, self.liLabel, self.highlightcolor))
        self.logLabel.bind("<Leave>", partial(self.color_config, self.logLabel, self.liLabel, self.main_color))
        self.logLabel.bind("<Button-1>", partial(self.lift_page, self.logLabel, self.liLabel, self.log, self.current_button))
        
        self.liLabel.bind("<Enter>", partial(self.color_config, self.liLabel, self.logLabel, self.highlightcolor))
        self.liLabel.bind("<Leave>", partial(self.color_config, self.liLabel, self.logLabel,self.main_color))
        self.liLabel.bind("<Button-1>", partial(self.lift_page, self.liLabel, self.logLabel, self.log, self.current_button))
#
        
        #%%
        
        #abort icon
        #add run icon with margin for highlight
        
#        abr = PIL.Image.open('images\icons8-close-window-48.png').resize((30,30))
#        abrnew = self.add_margin(abr, 4, 0, 4, 0)
#        
#        abrIcon = ImageTk.PhotoImage(abrnew)
#        abortLabel2 = tk.Label(self.nav, bg=self.main_color)
#        abortLabel2.image = abrIcon # keep a reference!
        
        
         
        #%% USER GUIDE
    
        #user nav button
        ugLabel= tk.Label(self, text="HEM4 USER GUIDE", font=TAB_FONT, bg=self.main_color, height=2, anchor="w")
        ugLabel.place(in_=self.container, relwidth=0.2, rely=0.72, relx=0.1)

#        #add run icon with margin for highlight
        ug = PIL.Image.open('images\icons8-user-manual-48.png').resize((30,30))
        ugnew = self.add_margin(ug, 4, 0, 4, 0)
        
        ugIcon = ImageTk.PhotoImage(ugnew)
        bookLabel = tk.Label(self, image=ugIcon, bg=self.main_color)
        bookLabel.image = ugIcon # keep a reference!
        bookLabel.place(in_=self.container, relwidth=0.1, rely=0.72)
        
         #bind icon and label events
        ugLabel.bind("<Enter>", partial(self.color_config, ugLabel, bookLabel, self.highlightcolor))
        ugLabel.bind("<Leave>", partial(self.color_config, ugLabel, bookLabel, self.main_color))
        ugLabel.bind("<Button-1>", self.hyperlink1) 
        bookLabel.bind("<Enter>", partial(self.color_config, bookLabel, ugLabel, self.highlightcolor))
        bookLabel.bind("<Leave>", partial(self.color_config, bookLabel, ugLabel,self.main_color))
        bookLabel.bind("<Button-1>", self.hyperlink1)
#        

        #%%
        
        #aermod user nav button
        agLabel= tk.Label(self, text="AERMOD USER GUIDE", font=TAB_FONT, bg=self.main_color, height=2, anchor="w")
        agLabel.place(in_=self.container, relwidth=0.2, rely=0.81, relx=0.1)

#        #add run icon with margin for highlight
        ag = PIL.Image.open('images\icons8-user-manual-48.png').resize((30,30))
        agnew = self.add_margin(ag, 4, 0, 4, 0)
        
        agIcon = ImageTk.PhotoImage(agnew)
        bookLabel2 = tk.Label(self, image=agIcon, bg=self.main_color)
        bookLabel2.image = agIcon # keep a reference!
        bookLabel2.place(in_=self.container, relwidth=0.1, rely=0.81)
        
         #bind icon and label events
        agLabel.bind("<Enter>", partial(self.color_config, agLabel, bookLabel2, self.highlightcolor))
        agLabel.bind("<Leave>", partial(self.color_config, agLabel, bookLabel2, self.main_color))
        agLabel.bind("<Button-1>", self.hyperlink2) 
        bookLabel2.bind("<Enter>", partial(self.color_config, bookLabel2, agLabel, self.highlightcolor))
        bookLabel2.bind("<Leave>", partial(self.color_config, bookLabel2, agLabel,self.main_color))
        bookLabel2.bind("<Button-1>", self.hyperlink2)
        
        #%%        
        #exit button
        #aermod user nav button
        closeLabel= tk.Label(self, text="EXIT", font=TAB_FONT, bg=self.main_color, height=2, anchor="w")
        closeLabel.place(in_=self.container, relwidth=0.2, rely=0.90, relx=0.1)

#        #add run icon with margin for highlight
        clo = PIL.Image.open('images\icons8-close-window-48.png').resize((30,30))
        clonew = self.add_margin(clo, 4, 0, 4, 0)
        
        closeIcon = ImageTk.PhotoImage(clonew)
        closeLabel2 = tk.Label(self, image=closeIcon, bg=self.main_color)
        closeLabel2.image = closeIcon # keep a reference!
        closeLabel2.place(in_=self.container, relwidth=0.1, rely=0.90)
        
         #bind icon and label events
        closeLabel.bind("<Enter>", partial(self.color_config, closeLabel, closeLabel2, self.highlightcolor))
        closeLabel.bind("<Leave>", partial(self.color_config, closeLabel, closeLabel2, self.main_color))
        closeLabel.bind("<Button-1>", partial(self.on_closing, self.hem)) 
        closeLabel2.bind("<Enter>", partial(self.color_config, closeLabel2, closeLabel, self.highlightcolor))
        closeLabel2.bind("<Leave>", partial(self.color_config, closeLabel2, closeLabel,self.main_color))
        closeLabel2.bind("<Button-1>", partial(self.on_closing, self.hem))

        
# setting geometry of tk window 

    def lift_page(self, widget1, widget2, page, previous, event):
        """
        Function lifts page and changes button color to active, 
        changes previous button color
        """
        try: 
            widget1.configure(bg=self.tab_color)
            widget2.configure(bg=self.tab_color)
            
            if len(self.current_button) > 0:
                
                for i in self.current_button:
                    i.configure(bg=self.main_color)
            
            print('Current Button before:', self.current_button)         
            print('page:', page)
            page.lift()
            self.current_button = [widget1, widget2]
            print('Current Button after:', self.current_button)  
        except Exception as e:
            
            print(e)
            
        
    def color_config(self, widget1, widget2, color, event):
        
        if widget1 not in self.current_button and widget2 not in self.current_button:
             widget1.configure(bg=color)
             widget2.configure(bg=color)
         
    def add_margin(self, pil_img, top, right, bottom, left):
        width, height = pil_img.size
        new_width = width + right + left
        new_height = height + top + bottom
        result = PIL.Image.new(pil_img.mode, (new_width, new_height))
        result.paste(pil_img, (left, top))
        return result    

    def hyperlink1(self, event):
        webbrowser.open_new(r"https://www.epa.gov/fera/risk-assessment-and-"+
                        "modeling-human-exposure-model-hem")
    
    def hyperlink2(self, event):
        webbrowser.open_new(r"https://www3.epa.gov/ttn/scram/models/aermod/aermod_userguide.pdf")
        
    def on_closing(self, hem, event):
    
        if hem.running == True:
        
                hem.quit_app()
                if hem.aborted == True:
                    self.home.destroy()
                    
        else:
            self.home.destroy()


#%%
class Start(Page):
    def __init__(self, *args, **kwargs):
        Page.__init__(self, *args, **kwargs)

        container = tk.Frame(self, bg=self.tab_color, bd=2)
        #        self.buttonframe.pack(side="w", fill="y", expand=False)
        container.pack(side="top", fill="both", expand=True) 

        self.s1 = tk.Frame(container, width=600, height=50, bg=self.tab_color)
        self.s2 = tk.Frame(container, width=600, height=50, bg=self.tab_color)
        self.s3 = tk.Frame(container, width=600, height=50, bg=self.tab_color)
        self.s4 = tk.Frame(container, width=600, height=50, bg=self.tab_color)
        self.s5 = tk.Frame(container, width=600, height=50, bg=self.tab_color)
#
        #grid layout for main inputs 
        self.s1.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.s1.columnconfigure(2, weight=1)
        
        self.s2.grid(row=2, column=0, columnspan=2, sticky="nsew")
        self.s2.columnconfigure(2, weight=1)
        
        self.s3.grid(row=3, column=0, columnspan=2, sticky="nsew")
        self.s3.columnconfigure(2, weight=1)
        
        self.s4.grid(row=4, column=0, columnspan=2, sticky="nsew")
        self.s4.columnconfigure(2, weight=1)
        
        self.s5.grid(row=5, column=0, columnspan=2, sticky="nsew")
        self.s5.columnconfigure(2, weight=1)
        
        #title in first grid space
        title1 = tk.Label(self.s1, text="HEM4", font=TITLE_FONT, bg=self.tab_color)
        title1.grid(row=0, column=2, pady=20)

        title2 = tk.Label(self.s1, text="HEM4 Version 1.0 ", font=TEXT_FONT, bg=self.tab_color)
        title2.grid(row=1, column=2, pady=20)

        
        
        
        #some information
        prepared_for = tk.Label(self.s4, text="Prepared for: \nAir Toxics" +
                            " Assessment Group \nU.S. EPA \nResearch Triangle Park, NC 27711",
                             font=TEXT_FONT, bg=self.tab_color)
        prepared_for.grid(row=1, column=0, padx=60)


        image1 = ImageTk.PhotoImage(PIL.Image.open('images\smokestack.jpg').resize((220,200)))
        ione = tk.Label(self.s3, image=image1)
        ione.image = image1 # keep a reference!
        ione.grid(row=1, column=1, padx=70, sticky='W', pady=20)

        prepared_by = tk.Label(self.s4, text="Prepared by: \nSC&A Incorporated\n" +
                            "1414 Raleigh Rd, Suite 450\nChapel Hill, NC 27517",
                             font=TEXT_FONT, bg=self.tab_color)
        prepared_by.grid(row=1, column=2, padx=10, sticky='E')


        img = PIL.Image.open('images\\usersguides.jpg')
        img = img.resize((250,200), PIL.Image.ANTIALIAS)
        image2 = ImageTk.PhotoImage(img)
        itwo = tk.Label(self.s3, image=image2)
        itwo.image = image2 # keep a reference!
        itwo.grid(row=1, column=2, padx=10, sticky='E', pady=20)

#        ## hyperlink
        
#        link_to_site = tk.Label(self.s5, text="For more information on HEM4 visit the HEM4 EPA Website",
#                               font=TEXT_FONT, bg=self.tab_color, anchor='center')
#        link_to_site.grid(row=0, padx=60, pady=50, sticky='E')
#        link_to_site.bind('<Button-1>', hyperlink1)

    

#%%
        
class Hem(Page):
    def __init__(self, nav, *args, **kwargs):
        Page.__init__(self, *args, **kwargs)
        
        if getattr(sys, 'frozen', False):
            os.environ['PROJ_LIB'] = os.path.join(os.path.split(__file__)[0], 'pyproj')
        else :
            pass                
            
        
        self.nav = nav

        # Create the model
        self.model = Model()

        # Create threading helpers
        self.messageQueue = queue.Queue()
        self.callbackQueue = queue.Queue()
        self.processor = None
        self.lastException = None

        self.after(25, self.after_callback)
        self.after(500, self.check_processing)

        Logger.messageQueue = self.messageQueue

         # Create a file uploader
        self.uploader = FileUploader(self.model)
        
         # Upload the Dose response, Target Organ Endponts, and MetLib libraries
        success = self.uploader.uploadLibrary("haplib")
        if not success:
            messagebox.showinfo('Error', "Invalid Dose Response file. Check log for details.")

        success = self.uploader.uploadLibrary("organs")
        if not success:
            messagebox.showinfo('Error', "Invalid Target Organs file. Check log for details.")

        success = self.uploader.uploadLibrary("metlib")
        if not success:
            messagebox.showinfo('Error', "Invalid Met Libary file. Check log for details.")
        

        
 
        
        # Create running helpers
        self.running = False
        self.aborted = False
        self.ready = False

        #Create containers for gui
        meta_container = tk.Frame(self, bg=self.tab_color, bd=2)
        meta_container.pack(side="top", fill="both", expand=True)
        
        self.meta_two = tk.Frame(self, bg=self.tab_color)
        self.meta_two.pack(side="bottom", fill="both")
        self.meta_two.columnconfigure(2, weight=1)
        
        self.container = tk.Frame(meta_container, bg=self.tab_color, borderwidth=0)
        self.container.grid(row=0, column =0)
        self.container.grid_rowconfigure(15, weight=1)
        
        
        #HEM4 GUI tabs       
        self.optional = Optional(self)
        self.optional.place(in_=meta_container, relheight=1, relwidth=1)
        self.optional.lower()
        
        self.depdeplt = DepDplt(self)
        self.depdeplt.place(in_=meta_container, relheight=1, relwidth=1)
        self.depdeplt.lower()
        
        self.running = False
        self.aborted = False
        self.ready = False

        self.fac_up = None
        self.hap_up = None
        self.emisloc_up = None
        self.urep = None
        self.urepaltButton = None
        
        #stringvar defaults for each input label
        
        #faclist
        self.faclbl = tk.StringVar()
        self.faclbl.set("1. Please select a Facilities List Options file:")
        
        #HAPemissions
        self.haplbl = tk.StringVar()
        self.haplbl.set("2. Please select a HAP Emissions file:")
        
        #emissions locations
        self.emislbl = tk.StringVar()
        self.emislbl.set("3. Please select an Emissions Location file:")
        
        #alt receptors
        self.altlbl = tk.StringVar()
        self.altlbl.set("Please select an alternate receptor CSV file:")
        
        
                
        
        #tab placehodler for nav
        self.current_tab = self.nav
        
        #highlight place holder for HEM4 upload buttons
        self.current_highlight = None
        
        ##Frames for main inputs
       # self.required_inputs = tk.Frame(self, width=600, bg=self.tab_color)
        #self.required_inputs.pack(fill="both", expand=True, side="top")
        
        self.s1 = tk.Frame(self.container, width=600, height=50, bg=self.tab_color)
        self.s2 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s3 = tk.Frame(self.container, width=600, height=75, pady=5, padx=5, bg=self.tab_color)
        self.s4 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s5 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s6 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s7 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s8 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s9 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s10 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
#        self.s11 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
#        self.s12 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
#        self.s13 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
#        self.s14 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
#        self.s15 = tk.Frame(self.container, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        




        self.alturep = tk.Frame(self.container, width=250, height=250,  bg=self.tab_color, pady=5, padx=5)

        #grid layout for main inputs 
        self.s1.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.s2.grid(row=2, column=0, columnspan=2, sticky="nsew")
        self.alturep.grid(row=4, column=0, columnspan=2, sticky="nsew")
        self.s3.grid(row=3, column=0, columnspan=2, sticky="nsew", pady=10)
        self.s4.grid(row=5, column=0, columnspan=2, sticky="nsew")
        self.s5.grid(row=6, column=0, columnspan=2, sticky="nsew")
        self.s6.grid(row=7, column=0, columnspan=2, sticky="nsew")
        self.s7.grid(row=8, column=0, columnspan=2, sticky="nsew")
        self.s8.grid(row=9, column=0, columnspan=2, sticky="nsew")
        self.s9.grid(row=10, column=0, columnspan=2, sticky="nsew")
        self.s10.grid(row=11, column=0, columnspan=2, sticky="nsew")
#        self.s11.grid(row=12, column=0, columnspan=2, sticky="nsew")
#        self.s12.grid(row=13, column=0, columnspan=2, sticky="nsew")
#        self.s13.grid(row=14, column=0, columnspan=2, sticky="nsew")
#        self.s14.grid(row=15, column=0, columnspan=2, sticky="nsew")
#        self.s15.grid(row=16, column=0, columnspan=2, sticky="nsew")


        
        
        self.s2.grid_propagate(0)
        
        self.tt = PIL.Image.open('images\icons8-virtual-machine-52-white.png').resize((30,30))
        self.tticon = self.add_margin(self.tt, 5, 0, 5, 0)
        self.titleicon = ImageTk.PhotoImage(self.tticon)
        self.titleLabel = tk.Label(self.s1, image=self.titleicon, bg=self.tab_color)
        self.titleLabel.image = self.titleicon # keep a reference!
        self.titleLabel.grid(row=1, column=0, padx=10, pady=10)
        
        
        self.title = tk.Label(self.s1, font=TITLE_FONT, fg="white", bg=self.tab_color, 
                             text="HEM4")
        self.title.grid(row=1, column=1, sticky="W", pady=10, padx=10)
        
#%% Setting up  directions text space
        
        self.add_instructions(self.s3, self.s3)

# %% Setting up each file upload space (includes browse button)
        
        group_label = tk.Label(self.s4, font=TEXT_FONT, bg=self.tab_color, 
                             text="Name Run Group (optional):")
        group_label.grid(row=1, column=0, sticky="W")
        #group text entry
        self.group_list = tk.StringVar(self.s4)
        self.group_list_man = ttk.Entry(self.s4)
        self.group_list_man["width"] = 25
        self.group_list_man["textvariable"]= self.group_list
        self.group_list_man.grid(row=1, column=1, sticky='W', pady=20)
        
        
        self.check_altrec = tk.IntVar()
        self.check_altrec.set(1)
        
        self.defaultrec_sel = tk.Radiobutton(self.alturep, text="Use U.S. Census receptors",
                                           variable = self.check_altrec, bg=self.tab_color,
                                           command = self.set_altrec, font=TEXT_FONT, value=1)
        self.defaultrec_sel.grid(row=0, column=1, sticky='W')
        
        self.altrec_sel = tk.Radiobutton(self.alturep, text="Use alternate receptors",
                                           variable = self.check_altrec, bg=self.tab_color,
                                           command = self.set_altrec, font=TEXT_FONT, value=2)
        self.altrec_sel.grid(row=0, column=2, sticky='W')
        
        

        #%%facilities label
    
        self.fu = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.ficon = self.add_margin(self.fu, 5, 0, 5, 0)
        self.fileicon = ImageTk.PhotoImage(self.ficon)
        self.fileLabel = tk.Label(self.s5, image=self.fileicon, bg=self.tab_color)
        self.fileLabel.image = self.fileicon # keep a reference!
        self.fileLabel.grid(row=3, column=0, padx=10)
        
        
        self.button_file = tk.Label(self.s5, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.faclbl)
        self.button_file.grid(row=3, column=1, sticky='W')
        
                                    
        self.button_file.bind("<Enter>", lambda x: self.color_config( self.button_file, self.fileLabel, self.s5, self.highlightcolor, x))
        self.button_file.bind("<Leave>", lambda x: self.remove_config( self.button_file, self.fileLabel, self.s5, self.tab_color, x))
        self.button_file.bind("<Button-1>", lambda x: self.uploadFacilitiesList(self.s5, self.button_file, x))
        
        self.fileLabel.bind("<Enter>", lambda x: self.color_config(self.fileLabel, self.button_file, self.s5, self.highlightcolor, x))
        self.fileLabel.bind("<Leave>", lambda x: self.remove_config(self.fileLabel, self.button_file, self.s5, self.tab_color, x))
        self.fileLabel.bind("<Button-1>",  lambda x: self.uploadFacilitiesList(self.s5, self.button_file, x))
#        
#        self.s5.bind("<Enter>", lambda x: self.color_config( self.button_file, self.fileLabel, self.s5, self.highlightcolor, x))
#        self.s5.bind("<Leave>", lambda x: self.remove_config( self.button_file, self.fileLabel, self.s5, self.tab_color, x))
##        
       
                
        #%%Hap emissions label
        self.hu = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.hicon = self.add_margin(self.hu, 5, 0, 5, 0)
        self.hileicon = ImageTk.PhotoImage(self.hicon)
        self.hapLabel = tk.Label(self.s6, image=self.hileicon, bg=self.tab_color)
        self.hapLabel.image = self.fileicon # keep a reference!
        self.hapLabel.grid(row=3, column=0, padx=10)
        
        
        self.hap_file = tk.Label(self.s6, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.haplbl)
        self.hap_file.grid(row=3, column=1, sticky='W')
        
                                    
        self.hap_file.bind("<Enter>", lambda x: self.color_config( self.hap_file, self.hapLabel, self.s6, self.highlightcolor, x))
        self.hap_file.bind("<Leave>", lambda x: self.remove_config( self.hap_file, self.hapLabel, self.s6, self.tab_color, x))
        self.hap_file.bind("<Button-1>", lambda x: self.uploadHAPEmissions(self.s6, self.hap_file, x))
        
        self.hapLabel.bind("<Enter>", lambda x: self.color_config(self.hapLabel, self.hap_file, self.s6, self.highlightcolor, x))
        self.hapLabel.bind("<Leave>", lambda x: self.remove_config(self.hapLabel, self.hap_file, self.s6, self.tab_color, x))
        self.hapLabel.bind("<Button-1>",  lambda x: self.uploadHAPEmissions(self.s6, self.hap_file, x))
        
#        self.s6.bind("<Enter>", lambda x: self.color_config(self.hap_file, self.hapLabel, self.s6, self.highlightcolor, x))
#        self.s6.bind("<Leave>", lambda x: self.remove_config(self.hap_file, self.hapLabel, self.s6, self.highlightcolor, x))
#        

        
        #%%Emissions location label
      
        self.eu = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.eicon = self.add_margin(self.eu, 5, 0, 5, 0)
        self.eileicon = ImageTk.PhotoImage(self.eicon)
        self.emisLabel = tk.Label(self.s7, image=self.eileicon, bg=self.tab_color)
        self.emisLabel.image = self.fileicon # keep a reference!
        self.emisLabel.grid(row=2, column=0, padx=10)
                
        
        self.emis_file = tk.Label(self.s7, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.emislbl)
        self.emis_file.grid(row=2, column=1, sticky='W')
        
                                    
        self.emis_file.bind("<Enter>", lambda x: self.color_config( self.emis_file, self.emisLabel, self.s7, self.highlightcolor, x))
        self.emis_file.bind("<Leave>", lambda x: self.remove_config( self.emis_file, self.emisLabel, self.s7, self.tab_color, x))
        self.emis_file.bind("<Button-1>", lambda x: self.uploadEmissionLocations(self.s7, self.emis_file, x))
        
        self.emisLabel.bind("<Enter>", lambda x: self.color_config(self.emisLabel, self.emis_file, self.s7, self.highlightcolor, x))
        self.emisLabel.bind("<Leave>", lambda x: self.remove_config(self.emisLabel, self.emis_file, self.s7, self.tab_color, x))
        self.emisLabel.bind("<Button-1>",  lambda x: self.uploadEmissionLocations(self.s7, self.emis_file, x))

#        self.s7.bind("<Enter>", lambda x: self.color_config(self.emis_file, self.emisLabel, self.s7, self.highlightcolor, x))
#        self.s7.bind("<Leave>", lambda x: self.remove_config(self.emis_file, self.emisLabel, self.s7, self.highlightcolor, x))
##        
    
        #next button
        self.next = tk.Button(self.meta_two, text="Next", bg='lightgrey', relief='solid', borderwidth=2,
                                    command=self.lift_tab, font=TEXT_FONT)
        self.next.grid(row=0, column=2, sticky='E', padx=20, pady=20)




#%% Nav

    def lift_tab(self):
        print("current tab:", self.current_tab)
        #if on main pae
        
        if self.current_tab == self.nav: 
            
        
            if ('buoyant' in self.model.dependencies or 'poly' in self.model.dependencies or 
                       'bldg_dw' in self.model.dependencies or 'user_rcpt' in self.model.dependencies 
                       or 'emis_var' in self.model.dependencies):
                
                #add back button
                self.back = tk.Button(self.meta_two, text="Back", bg='lightgrey', relief='solid', borderwidth=2,
                                    command=self.back_tab, font=TEXT_FONT)
                self.back.grid(row=0, column=1, sticky='W', padx=20, pady=20)
                
                self.optional.lift()
                self.current_tab = self.optional
            
            else:
            
                if self.current_tab == self.nav and 'particle size' in self.model.dependencies or 'land use' in self.model.dependencies or 'seasons' in self.model.dependencies:
                    #add back button
                    self.back = tk.Button(self.meta_two, text="Back", bg='lightgrey', relief='solid', borderwidth=2,
                                    command=self.back_tab, font=TEXT_FONT, padx=20, pady=20)
                    self.back.grid( sticky='W')
                    
                    self.depdeplt.lift()
                    self.current_tab = self.depdeplt
                
                else:
                    self.run()     
                
                
         #if on optional page one      
        elif self.current_tab == self.optional:
            
            if 'particle size' in self.model.dependencies or 'land use' in self.model.dependencies or 'seasons' in self.model.dependencies:
                 self.depdeplt.lift()
                 self.current_tab = self.depdeplt
                 
            else:
                
                self.run()
          
        #if on optional page two
        elif self.current_tab == self.depdeplt:
            self.run()
                
            
    def back_tab(self):
        
        if self.current_tab == self.optional:    
            self.lift()
            self.current_tab = self.nav
            self.back.destroy()
            
        if self.current_tab == self.depdeplt:
            
             if 'buoyant' in self.model.dependencies or 'poly' in self.model.dependencies or 'bldg_dw' or 'emis_var' in self.model.dependencies:
                 self.current_tab = self.optional
                 self.optional.lift()
                 
             else:
                 self.current_tab = self.nav
                 self.lift()
                 self.back.destroy()
                 
                
            
            
            
            
            
    #%% functions for uploading inputs
    
    def uploadFacilitiesList(self, container, label, event):
        """
        Function for uploading Facilities List option file. Also creates
        user receptor input space if indicated
        """
                
        fullpath = self.openFile(askopenfilename())
        if fullpath is not None:
            
            self.uploader.upload("faclist", fullpath)
            
            if self.model.faclist.dataframe.empty == False:
                
                #reset all other inputs
                self.reset_inputs('faclist')
                
         
                self.model.facids = self.model.faclist.dataframe['fac_id']
    
                # Update the UI
                [self.nav.log.scr.insert(tk.INSERT, msg) for msg in self.model.faclist.log]
    #            container.configure(bg='light green')
                self.faclbl.set('')
                self.faclbl.set(fullpath.split("\\")[-1])
                
                #trigger additional inputs fo user recptors, assuming we are not in "user receptors only" mode
                if 'Y' in self.model.faclist.dataframe['user_rcpt'].tolist():
                    #create user and reset text variable
                    
                    self.add_ur()
                    self.model.dependencies.append('user_rcpt')
                    
                        
                #trigger additional inputs for emisvar
                if 'Y' in self.model.faclist.dataframe['emis_var'].tolist():
                    #create create emis var
                    self.add_variation()
                    self.model.dependencies.append('emis_var')
    
                        
                #trigger additional inputs for building downwash
                if 'Y' in self.model.faclist.dataframe['bldg_dw'].tolist():
                    
                    
                    #create building downwash input
                    self.add_bldgdw()
                    self.model.dependencies.append('bldg_dw')
                
        

    def uploadHAPEmissions(self, container, label, event):
        """
        Function for uploading Hap Emissions file
        """
        try: 
            
            self.model.faclist.dataframe
            
        except:
            
            messagebox.showinfo('Error', "Please upload a Facilities List Options file first")
            
            
        else:
            fullpath = self.openFile(askopenfilename())
            if fullpath is not None:
                self.uploader.upload("hapemis", fullpath)
                
                if self.model.hapemis.dataframe.empty == False:
        
                    # Update the UI
                    [self.nav.log.scr.insert(tk.INSERT, msg) for msg in self.model.hapemis.log]
    #                container.configure(bg='light green')
                    
                    self.haplbl.set('')
                    self.haplbl.set(fullpath.split("\\")[-1])
     
                    
            
    
    def uploadEmissionLocations(self, container, label, event):
        """
        Function for uploading Emissions Locations file. Also creates optional 
        input spaces if indicated in file or removes optional spaces if upload
        is triggered again and there are no optional inputs indicated
        """
        
        try: 
            
            self.model.faclist.dataframe
            
        except:
            
            messagebox.showinfo('Error', "Please upload a Facilities List Options file first")
            
            
        else:
            
            try:
            
                self.model.hapemis.dataframe
                    
            except:
            
                messagebox.showinfo('Error', "Please upload Hap Emissions file before " +
                                  "uploading the Emissions Location file")
                
            else:
  
                fullpath = self.openFile(askopenfilename())
                if fullpath is not None:
                    self.uploader.upload("emisloc", fullpath)
                    
                    if self.model.emisloc.dataframe.empty == False:
                        
                         #reset dependent inputs for emis loc
                        self.reset_inputs('emisloc')
        
                        # Update the UI
                        [self.nav.log.scr.insert(tk.INSERT, msg) for msg in self.model.emisloc.log]
    #                    container.configure(bg='light green')
                        
                        self.emislbl.set('')
                        self.emislbl.set(fullpath.split("\\")[-1])
     
        
                        #trigger additional inputs for buoyant line and polyvertex
                        if 'I' in self.model.emisloc.dataframe['source_type'].tolist():
                            
                            #enable optional input tab
                            self.optionaltab = False
                            
                            #create polyvertex upload 
                            self.add_poly()
                            self.model.dependencies.append('poly')
                            
                      
                                
                                
                        if 'B' in self.model.emisloc.dataframe['source_type'].tolist():
                            
                            #enable optional input tab
                            self.optionaltab = False
                            
                            #create buoyant line upload
                            self.add_buoyant()
                            self.model.dependencies.append('buoyant')
                            
                       
            
                        # Deposition and depletion check
            
                        # set phase column in faclist dataframe to None
                        self.model.faclist.dataframe['phase'] = None
            
                        for i, r in self.model.faclist.dataframe.iterrows():
            
                            phase = check_phase(r)
                            #                phaseList.append([r['fac_id'], phase])
                            self.model.faclist.dataframe.at[i, 'phase'] = phase
            
                        deposition_depletion = check_dep(self.model.faclist.dataframe, self.model.emisloc.dataframe)
            
                        #pull out facilities using depdeplt
                        self.model.depdeplt = [x[0] for x in deposition_depletion]
                        print('DEPDEP:', self.model.depdeplt)
    
                        #pull out facilities needing landuse and seasons files (gas dry dep/depl)
                        self.model.gasdryfacs = [x[0] for x in deposition_depletion if 'land use' in x]

                        #pull out facilities needing particle size data (method one particle dry/wet dep/depl)
                        self.model.particlefacs = [x[0] for x in deposition_depletion if 'particle size' in x]
            
                        #pull out conditional inputs
                        conditional = set([y for x in deposition_depletion for y in x[1:]])
                        #print('conditional', conditional)
            
                        if conditional is not None:
                            #enable deposition and depletion input tab
                            self.deptab = True
            
            
                            #if deposition or depletion present load gas params library
                            self.uploader.uploadLibrary("gas params")
                            for required in conditional:
                                print("required", required)
                                if required == 'particle size':
                                    self.add_particle()
                                    self.model.dependencies.append('particle size')
            
                                elif required == 'land use':
                                    self.add_land()
                                    self.model.dependencies.append('land use')
            
                                elif required == 'seasons':
                                    self.add_seasons()
                                    self.model.dependencies.append('seasons')
                       
    
    def uploadUserReceptors(self, container, label, event):
        """
        Function for uploading user receptors
        """
        

        if self.model.faclist is None:
            messagebox.showinfo("Facilities List Option File Missing",
                "Please upload a Facilities List Options file before selecting"+
                " a User Receptors file.")
            return

        fullpath = self.openFile(askopenfilename())
        if fullpath is not None:

            self.uploader.uploadDependent("user receptors", fullpath, 
                                          self.model.faclist.dataframe)
            
            if self.model.ureceptr.dataframe.empty == False:
            
                self.model.model_optns['ureceptr'] = True
                # Update the UI
                [self.nav.log.scr.insert(tk.INSERT, msg) for msg in self.model.ureceptr.log]
    #            container.configure(bg='light green')
                
                self.optional.urlbl.set('')
                self.optional.urlbl.set(fullpath.split("\\")[-1])
            
            
    def uploadAltReceptors(self, container, label, event):
        """
        Function for uploading Alternate Receptors
        """
        
        fullpath = self.openFile(askopenfilename())
        if fullpath is not None:


            self.uploader.upload("alt receptors", fullpath)
            
            if self.model.altreceptr.dataframe.empty == False:
                self.model.altRec_optns["path"] = fullpath
                self.model.altRec_optns["altrec"] = True
    
                # Update the UI
                [self.scr.insert(tk.INSERT, msg) for msg in self.model.altreceptr.log]
                
                self.altlbl.set('')
                self.altlbl.set(fullpath.split("\\")[-1])
                
            
    def uploadVariation(self, container, label, event):

        """
        Function for uploading emissions variation inputs
        """
        
        
        
        fullpath = self.openFile(askopenfilename())
        if fullpath is not None:
        
            self.uploader.uploadDependent("emissions variation", fullpath, self.model)
            
       
            
            if self.model.emisvar.dataframe.empty == False:
    
                # Update the UI
                [self.scr.insert(tk.INSERT, msg) for msg in self.model.emisvar.log]
                
                self.optional.varlbl.set('')
                self.optional.varlbl.set(fullpath.split("\\")[-1])
               
    
    def set_altrec(self):
        
        if self.check_altrec.get() == 2:
            
            if 'altrec' not in self.model.dependencies:
                self.model.dependencies.append('altrec')
                self.add_urepalt()
                
            self.check_altrec.set(2)
            
        elif self.check_altrec.get() == 1:
        
           # self.check_altrec.set(1)
            if 'altrec' in self.model.dependencies:
                self.model.dependencies.remove('altrec')
            self.urepLabel.destroy()
            self.urep_file.destroy()
        
    
            
    
            


    def add_ur(self):
        """
        Function for creating row and upload widgets for user receptors
        """

        # set the appropriate instructions text
        browse = "instructions/urep_browse.txt"
        man = "instructions/urep_man.txt"

        
        #user recptors label
        self.ur = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.uricon = self.add_margin(self.ur, 5, 0, 5, 0)
        self.urileicon = ImageTk.PhotoImage(self.uricon)
        self.urLabel = tk.Label(self.optional.s8, image=self.urileicon, bg=self.tab_color)
        self.urLabel.image = self.urileicon # keep a reference!
        self.urLabel.grid(row=1, column=0, padx=10)
        
        
        self.ur_file = tk.Label(self.optional.s8, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.optional.urlbl)
        self.ur_file.grid(row=1, column=1, sticky='W')
        
                                    
        self.ur_file.bind("<Enter>", lambda x: self.color_config( self.ur_file, self.urLabel, self.optional.s8, self.highlightcolor, x))
        self.ur_file.bind("<Leave>", lambda x: self.remove_config( self.ur_file, self.urLabel, self.optional.s8, self.tab_color, x))
        self.ur_file.bind("<Button-1>", lambda x: self.optional.uploadUserReceptors(self.optional.s8, self.ur_file, x))
        
        self.urLabel.bind("<Enter>", lambda x: self.color_config(self.urLabel, self.ur_file, self.optional.s8, self.highlightcolor, x))
        self.urLabel.bind("<Leave>", lambda x: self.remove_config(self.urLabel, self.ur_file, self.optional.s8, self.tab_color, x))
        self.urLabel.bind("<Button-1>",  lambda x: self.optional.uploadUserReceptors(self.optional.s8, self.ur_file, x))

#        self.optional.s8.bind("<Enter>", lambda x: self.color_config(self.urLabel, self.ur_file, self.optional.s8, self.highlightcolor, x))
#        self.optional.s8.bind("<Leave>", lambda x: self.remove_config(self.urLabel, self.ur_file, self.optional.s8, self.tab_color, x))
#       
        
        
        
    def add_urepalt(self):
        """
        Function for creating row and upload widgets for alternate user receptors
        """

        # set the appropriate instructions text
        browse = "instructions/urepalt_browse.txt"
        man = "instructions/urepalt_man.txt"

            
        #user recptors labe
        self.urep = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.urepicon = self.add_margin(self.urep, 5, 0, 5, 0)
        self.urepileicon = ImageTk.PhotoImage(self.urep)
        self.urepLabel = tk.Label(self.s10, image=self.urepileicon, bg=self.tab_color)
        self.urepLabel.image = self.urepileicon # keep a reference!
        self.urepLabel.grid(row=0, column=0, padx=10, sticky='W')
        
        
        self.urep_file = tk.Label(self.s10, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.altlbl)
        self.urep_file.grid(row=0, column=1, sticky='W')
        
                                    
        self.urep_file.bind("<Enter>", lambda x: self.color_config( self.urep_file, self.urepLabel, self.s10, self.highlightcolor, x))
        self.urep_file.bind("<Leave>", lambda x: self.remove_config( self.urep_file, self.urepLabel, self.s10, self.tab_color, x))
        self.urep_file.bind("<Button-1>", lambda x: self.uploadAltReceptors(self.alturep, self.urep_file, x))
        
        self.urepLabel.bind("<Enter>", lambda x: self.color_config(self.urepLabel, self.urep_file, self.s10, self.highlightcolor, x))
        self.urepLabel.bind("<Leave>", lambda x: self.remove_config(self.urepLabel, self.urep_file, self.s10, self.tab_color, x))
        self.urepLabel.bind("<Button-1>",  lambda x: self.uploadAltReceptors(self.alturep, self.urep_file, x))
        
#        self.s10.bind("<Enter>", lambda x: self.color_config( self.urep_file, self.urepLabel, self.s10, self.highlightcolor, x))
#        self.s10.bind("<Leave>", lambda x: self.remove_config( self.urep_file, self.urepLabel, self.s10, self.tab_color, x))
#       
        

    def add_variation(self):
        """
        Function for creating emission variation input space
        """   
        #emissions variation label

                       
        self.emisvar = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.emisvaricon = self.add_margin(self.emisvar, 5, 0, 5, 0)
        self.emisvarileicon = ImageTk.PhotoImage(self.emisvaricon)
        self.emisvarLabel = tk.Label(self.optional.s9, image=self.emisvarileicon, bg=self.tab_color)
        self.emisvarLabel.image = self.emisvarileicon # keep a reference!
        self.emisvarLabel.grid(row=1, column=0, padx=10)
        
        
        self.emisvar_file = tk.Label(self.optional.s9, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.optional.varlbl)
        self.emisvar_file.grid(row=1, column=1, sticky='W')
        
                                    
        self.emisvar_file.bind("<Enter>", lambda x: self.color_config( self.emisvar_file, self.emisvarLabel, self.optional.s9, self.highlightcolor, x))
        self.emisvar_file.bind("<Leave>", lambda x: self.remove_config( self.emisvar_file, self.emisvarLabel, self.optional.s9, self.tab_color, x))
        self.emisvar_file.bind("<Button-1>", lambda x: self.uploadVariation(self.optional.s9, self.emisvar_file, x))
        
        self.emisvarLabel.bind("<Enter>", lambda x: self.color_config(self.emisvarLabel, self.emisvar_file, self.optional.s9, self.highlightcolor, x))
        self.emisvarLabel.bind("<Leave>", lambda x: self.remove_config(self.emisvarLabel, self.emisvar_file, self.optional.s9, self.tab_color, x))
        self.emisvarLabel.bind("<Button-1>",  lambda x: self.uploadVariation(self.optional.s9, self.emisvar_file, x))
        
#        self.optional.s9.bind("<Enter>", lambda x: self.color_config(self.emisvarLabel, self.emisvar_file, self.optional.s9, self.highlightcolor, x))
#        self.optional.s9.bind("<Leave>", lambda x: self.remove_config(self.emisvarLabel, self.emisvar_file, self.optional.s9, self.tab_color, x))
#       


    
    def add_buoyant(self):
        """
        Function for creating row and buoyant line parameter upload widgets
        """
    
        self.buoy = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.buoyicon = self.add_margin(self.buoy, 5, 0, 5, 0)
        self.buoyileicon = ImageTk.PhotoImage(self.buoy)
        self.buoyLabel = tk.Label(self.optional.s4, image=self.buoyileicon, bg=self.tab_color)
        self.buoyLabel.image = self.buoyileicon # keep a reference!
        self.buoyLabel.grid(row=1, column=0, padx=10)
        
        
        self.buoy_file = tk.Label(self.optional.s4, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.optional.buoylbl)
        self.buoy_file.grid(row=1, column=1, sticky='W')
        
                                    
        self.buoy_file.bind("<Enter>", lambda x: self.color_config( self.buoy_file, self.buoyLabel, self.optional.s4, self.highlightcolor, x))
        self.buoy_file.bind("<Leave>", lambda x: self.remove_config( self.buoy_file, self.buoyLabel, self.optional.s4, self.tab_color, x))
        self.buoy_file.bind("<Button-1>", lambda x: self.optional.uploadbuoyant(self.optional.s4, self.buoy_file, x))
        
        self.buoyLabel.bind("<Enter>", lambda x: self.color_config(self.buoyLabel, self.buoy_file, self.optional.s4, self.highlightcolor, x))
        self.buoyLabel.bind("<Leave>", lambda x: self.remove_config(self.buoyLabel, self.buoy_file, self.optional.s4, self.tab_color, x))
        self.buoyLabel.bind("<Button-1>",  lambda x: self.optional.uploadbuoyant(self.optional.s4, self.buoy_file, x))
        
#        self.optional.s4.bind("<Enter>", lambda x: self.color_config(self.buoyLabel, self.buoy_file, self.optional.s4, self.highlightcolor, x))
#        self.optional.s4.bind("<Leave>", lambda x: self.remove_config(self.buoyLabel, self.buoy_file, self.optional.s4, self.tab_color, x))
#        
#        

    def add_poly(self):
        """
        Function for creating row and polyvertex file upload widgets
        """
       
        self.poly = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.polyicon = self.add_margin(self.poly, 5, 0, 5, 0)
        self.polyileicon = ImageTk.PhotoImage(self.poly)
        self.polyLabel = tk.Label(self.optional.s5, image=self.polyileicon, bg=self.tab_color)
        self.polyLabel.image = self.polyileicon # keep a reference!
        self.polyLabel.grid(row=1, column=0, padx=10)
        
        
        self.poly_file = tk.Label(self.optional.s5, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.optional.polylbl)
        self.poly_file.grid(row=1, column=1, sticky='W')
        
                                    
        self.poly_file.bind("<Enter>", lambda x: self.color_config( self.poly_file, self.polyLabel, self.optional.s5, self.highlightcolor, x))
        self.poly_file.bind("<Leave>", lambda x: self.remove_config( self.poly_file, self.polyLabel, self.optional.s5, self.tab_color, x))
        self.poly_file.bind("<Button-1>", lambda x: self.optional.uploadPolyvertex(self.optional.s5, self.poly_file, x))
        
        self.polyLabel.bind("<Enter>", lambda x: self.color_config(self.polyLabel, self.poly_file, self.optional.s5, self.highlightcolor, x))
        self.polyLabel.bind("<Leave>", lambda x: self.remove_config(self.polyLabel, self.poly_file, self.optional.s5, self.tab_color, x))
        self.polyLabel.bind("<Button-1>",  lambda x: self.optional.uploadPolyvertex(self.optional.s5, self.poly_file, x))

#        self.optional.s5.bind("<Enter>", lambda x: self.color_config(self.polyLabel, self.poly_file, self.optional.s5, self.highlightcolor, x))
#        self.optional.s5.bind("<Leave>", lambda x: self.remove_config(self.polyLabel, self.poly_file, self.optional.s5, self.tab_color, x))
#        
        
    def add_bldgdw(self):
        """ 
        Function for creating row and building downwash file upload widgets
        """
        
        self.bldgdw = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.bldgdwicon = self.add_margin(self.bldgdw, 5, 0, 5, 0)
        self.bldgdwileicon = ImageTk.PhotoImage(self.bldgdw)
        self.bldgdwLabel = tk.Label(self.optional.s6, image=self.bldgdwileicon, bg=self.tab_color)
        self.bldgdwLabel.image = self.bldgdwileicon # keep a reference!
        self.bldgdwLabel.grid(row=1, column=0, padx=10)
        
        
        self.bldgdw_file = tk.Label(self.optional.s6, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.optional.bldgdwlbl)
        self.bldgdw_file.grid(row=1, column=1, sticky='W')
        
                                    
        self.bldgdw_file.bind("<Enter>", lambda x: self.color_config( self.bldgdw_file, self.bldgdwLabel, self.optional.s6, self.highlightcolor, x))
        self.bldgdw_file.bind("<Leave>", lambda x: self.remove_config( self.bldgdw_file, self.bldgdwLabel, self.optional.s6, self.tab_color, x))
        self.bldgdw_file.bind("<Button-1>", lambda x: self.optional.uploadBuildingDownwash(self.optional.s6, self.bldgdw_file, x))
        
        self.bldgdwLabel.bind("<Enter>", lambda x: self.color_config(self.bldgdwLabel, self.bldgdw_file, self.optional.s6, self.highlightcolor, x))
        self.bldgdwLabel.bind("<Leave>", lambda x: self.remove_config(self.bldgdwLabel, self.bldgdw_file, self.optional.s6, self.tab_color, x))
        self.bldgdwLabel.bind("<Button-1>",  lambda x: self.optional.uploadBuildingDownwash(self.optional.s6, self.bldgdw_file, x))

#        self.optional.s6.bind("<Enter>", lambda x: self.color_config( self.bldgdw_file, self.bldgdwLabel, self.optional.s6, self.highlightcolor, x))
#        self.optional.s6.bind("<Leave>", lambda x: self.remove_config( self.bldgdw_file, self.bldgdwLabel, self.optional.s6, self.tab_color, x))
#        

    def add_particle(self):
        """
        Function for creating column for particle size file upload widgets
        """
        
               
        self.particle = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.particleicon = self.add_margin(self.particle, 5, 0, 5, 0)
        self.particleileicon = ImageTk.PhotoImage(self.particle)
        self.particleLabel = tk.Label(self.depdeplt.s4, image=self.particleileicon, bg=self.tab_color)
        self.particleLabel.image = self.particleileicon # keep a reference!
        self.particleLabel.grid(row=1, column=0, padx=10)
        
        
        self.particle_file = tk.Label(self.depdeplt.s4, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.depdeplt.partlbl)
        self.particle_file.grid(row=1, column=1, sticky='W')
        
                                    
        self.particle_file.bind("<Enter>", lambda x: self.color_config( self.particle_file, self.particleLabel, self.depdeplt.s4, self.highlightcolor, x))
        self.particle_file.bind("<Leave>", lambda x: self.remove_config( self.particle_file, self.particleLabel, self.depdeplt.s4, self.tab_color, x))
        self.particle_file.bind("<Button-1>", lambda x: self.depdeplt.uploadParticle(self.model.particlefacs, self.depdeplt.s4, self.particle_file, x))
        
        self.particleLabel.bind("<Enter>", lambda x: self.color_config(self.particleLabel, self.particle_file, self.depdeplt.s4, self.highlightcolor, x))
        self.particleLabel.bind("<Leave>", lambda x: self.remove_config(self.particleLabel, self.particle_file, self.depdeplt.s4, self.tab_color, x))
        self.particleLabel.bind("<Button-1>",  lambda x: self.depdeplt.uploadParticle(self.model.particlefacs, self.depdeplt.s4, self.particle_file, x))

#        self.depdeplt.s4.bind("<Enter>", lambda x: self.color_config( self.particle_file, self.particleLabel, self.depdeplt.s4, self.highlightcolor, x))
#        self.depdeplt.s4.bind("<Leave>", lambda x: self.remove_config( self.particle_file, self.particleLabel, self.depdeplt.s4, self.tab_color, x))
#        

    def add_land(self):
        
        """
        Function for creating column for land use upload widgets
        """
             
        self.land = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.landicon = self.add_margin(self.land, 5, 0, 5, 0)
        self.landileicon = ImageTk.PhotoImage(self.land)
        self.landLabel = tk.Label(self.depdeplt.s5, image=self.landileicon, bg=self.tab_color)
        self.landLabel.image = self.landileicon # keep a reference!
        self.landLabel.grid(row=1, column=0, padx=10)
        
        
        self.land_file = tk.Label(self.depdeplt.s5, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.depdeplt.landlbl)
        self.land_file.grid(row=1, column=1, sticky='W')
        
                                    
        self.land_file.bind("<Enter>", lambda x: self.color_config( self.land_file, self.landLabel, self.depdeplt.s5, self.highlightcolor, x))
        self.land_file.bind("<Leave>", lambda x: self.remove_config( self.land_file, self.landLabel, self.depdeplt.s5, self.tab_color, x))
        self.land_file.bind("<Button-1>", lambda x: self.depdeplt.uploadLandUse(self.depdeplt.s5, self.land_file, x))
        
        self.landLabel.bind("<Enter>", lambda x: self.color_config(self.landLabel, self.land_file, self.depdeplt.s5, self.highlightcolor, x))
        self.landLabel.bind("<Leave>", lambda x: self.remove_config(self.landLabel, self.land_file, self.depdeplt.s5, self.tab_color, x))
        self.landLabel.bind("<Button-1>",  lambda x: self.depdeplt.uploadLandUse(self.depdeplt.s5, self.land_file, x))

#        self.depdeplt.s5.bind("<Enter>", lambda x: self.color_config( self.land_file, self.landLabel, self.depdeplt.s5, self.highlightcolor, x))
#        self.depdeplt.s5.bind("<Leave>", lambda x: self.remove_config( self.land_file, self.landLabel, self.depdeplt.s5, self.tab_color, x))
#        
        
    def add_seasons(self):
        """
        Function for creating column for seasonal vegetation upload widgets
        """        
        self.seasons = PIL.Image.open('images\icons8-import-48.png').resize((30,30))
        self.seasonsicon = self.add_margin(self.seasons, 5, 0, 5, 0)
        self.seasonsileicon = ImageTk.PhotoImage(self.seasons)
        self.seasonsLabel = tk.Label(self.depdeplt.s6, image=self.seasonsileicon, bg=self.tab_color)
        self.seasonsLabel.image = self.seasonsileicon # keep a reference!
        self.seasonsLabel.grid(row=1, column=0, padx=10)
        
        
        self.seasons_file = tk.Label(self.depdeplt.s6, font=TEXT_FONT, bg=self.tab_color, 
                             textvariable=self.depdeplt.seasonlbl)
        self.seasons_file.grid(row=1, column=1, sticky='W')
        
                                    
        self.seasons_file.bind("<Enter>", lambda x: self.color_config( self.seasons_file, self.seasonsLabel, self.depdeplt.s6, self.highlightcolor, x))
        self.seasons_file.bind("<Leave>", lambda x: self.remove_config( self.seasons_file, self.seasonsLabel, self.depdeplt.s6, self.tab_color, x))
        self.seasons_file.bind("<Button-1>", lambda x: self.depdeplt.uploadSeasons(self.depdeplt.s6, self.seasons_file, x))
        
        self.seasonsLabel.bind("<Enter>", lambda x: self.color_config(self.seasonsLabel, self.seasons_file, self.depdeplt.s6, self.highlightcolor, x))
        self.seasonsLabel.bind("<Leave>", lambda x: self.remove_config(self.seasonsLabel, self.seasons_file, self.depdeplt.s6, self.tab_color, x))
        self.seasonsLabel.bind("<Button-1>",  lambda x: self.depdeplt.uploadSeasons(self.depdeplt.s6, self.seasons_file, x))
        
#        self.depdeplt.s6.bind("<Enter>", lambda x: self.color_config( self.seasons_file, self.seasonsLabel, self.depdeplt.s6, self.highlightcolor, x))
#        self.depdeplt.s6.bind("<Leave>", lambda x: self.remove_config( self.seasons_file, self.seasonsLabel, self.depdeplt.s6, self.tab_color, x))
#        


    def run(self):
        """ 
        Function passes model class to InputChecker class, then uses returned 
        dictionary to either run HEM4 or display user input error.
        
        To run each facility, function loops through facility ids and logs 
        returned errors and messages to the log tab via the queue method
        
        When all facilites are done the GUI is reset and all optional inputs
        are destroyed
        """

        self.ready = False
        
        #make sure there is census and MetData
        if os.path.isdir('census') == False:
            messagebox.showinfo('No census folder', 'The census folder does not exist. Please create a census folder and populate with census data.')
            return None
        else:
            if len(os.listdir('census') ) == 0:
                messagebox.showinfo('Census data missing', 'Census data is missing. Please check the census folder.')
                return None
        
        if os.path.isdir('aermod/MetData') == False:
            messagebox.showinfo('No MetData folder', 'The MetData folder does not exist. Please create a MetData folder and populate with meteorology data.')
            return None
        else:
            if len(os.listdir('aermod/MetData') ) == 0:
                messagebox.showinfo('MetData missing', 'Meteorology data is missing. Please check the MetData folder.')
                return None
        
        
        #Check inputs
        check_inputs = InputChecker(self.model)
        
        try:
            required = check_inputs.check_required()
            
        except Exception as e:
                
                Logger.logMessage(str(e))
                
        else:
            if 'ready' not in required['result']:
                messagebox.showinfo('Error', required['result'])
                self.ready = False
                
            elif required['dependencies'] is not []:
                try:
                    
                    optional = check_inputs.check_dependent(required['dependencies'])
                    
                except Exception as e:
                    
                    Logger.logMessage(str(e))
                    
                else:
                    
                
                    if 'ready' not in optional['result']:
                        messagebox.showinfo('Error', optional['result'])
                        self.ready = False
                    
                    else:
                        self.ready = True
                        
                        
                        #get deposition exclusions
                        print('Checking depletion.... against', self.model.depdeplt)
                        
                        if self.model.depdeplt != None:
                        
                            #look through hapemis for facilities that are running deposition or depletion
                            hapDep = self.model.hapemis.dataframe[self.model.hapemis.dataframe['fac_id'].isin(self.model.depdeplt)]
                            
                            #now check phase in facilities list option file
                            facDep = self.model.faclist.dataframe[self.model.faclist.dataframe['fac_id'].isin(self.model.depdeplt)]
                                                        
                            # Initialize lists that will contain sources that should not be modeled as P or V
                            particleExcludeList = []
                            vaporExcludeList = []
                            
                            for i, r in facDep.iterrows():
                                if r['phase'] in ['P', 'V', 'B']:
                                    
                                    #look at pollutants
                                    pols = hapDep[hapDep['fac_id'] == r['fac_id']]
                                    
                                    #get sourcelist
                                    sourcesList = set(pols['source_id'].tolist())
                                    
                                    for source in sourcesList:
                                        
                                        if r['phase'] == 'P' or r['phase'] == 'B':
                                            #get the sum of part frac
                                            polSum = sum(pols[pols['source_id'] == source]['part_frac'].tolist())
                                            
                                            #if they are zero then its not particulate at all 
                                            if polSum == 0:
                                                
                                                #add it to the list of source exclusions
                                                particleExcludeList.append(source)
                                            
                                        elif r['phase'] == 'V':
                                            
                                            #get
                                            so = pols[pols['source_id'] == source]['part_frac'].tolist()
                                            polSum = sum(so)
                                            allPart = len(so) * 100
                                            
                                            #if they are all particle (100%)
                                            if polSum == allPart:
                                                
                                                #add it to the list of source exclusions
                                                vaporExcludeList.append(source)
                                                
                                    self.model.sourceExclusion['P'+r['fac_id']] = particleExcludeList
                                    self.model.sourceExclusion['V'+r['fac_id']] = vaporExcludeList
                                
                                else:
                                    self.ready = True
            

       #%%if the object is ready
        if self.ready == True:

            #tell user to check the Progress/Log section
            override = messagebox.askokcancel("Confirm HEM4 Run", "Clicking 'OK'"+
                                   " will start HEM4. Check the log tab for" +
                                   " updates on your modeling run.")

            if override:
#                global instruction_instance
#                self.instruction_instance.set("HEM4 Running, check the log tab for updates")
                self.nav.hem.lift()
                self.fix_config(self.nav.liLabel, self.nav.logLabel, self.nav.current_button)

                self.lift_page(self.nav.liLabel, self.nav.logLabel, self.nav.log, self.nav.current_button)
                
                self.nav.iconLabel.configure(image=self.nav.greenIcon)
                
                
                Logger.logMessage("\nHEM4 is starting...")
                
                
    
                
                 #set run name
                if len(self.group_list.get()) > 0:
                    self.model.group_name = self.group_list.get()
                    
                else:
                    
                    runid = str(uuid.uuid4())[:7]
                    self.model.group_name = "rungroup_" + str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S'))
                
                #set output folder
                self.model.rootoutput = "output/" + self.model.group_name + "/"
                if os.path.exists(self.model.rootoutput):
                    shutil.rmtree(self.model.rootoutput)                
                os.makedirs(self.model.rootoutput)
                
#                #set save folder
#                save_state = SaveState(self.model.group_name, self.model)
#                self.model.save = save_state
#                
#                #save model
#                model_loc = save_state.save_folder + "/model.pkl"
#                modelHandler = open(model_loc, 'wb') 
#     
                
                

                try:
                    print('the processor function')
                    self.process()

                    
                except BaseException as ex:
                
                    Logger.logMessage(str(ex))

    def process(self):
        """
        Function creates thread for running HEM4 concurrently with tkinter GUI
        """
        executor = ThreadPoolExecutor(max_workers=1)
        print('created executor')

        self.running = True
        self.disable_buttons()
        
        #disable options tab
        self.nav.optionsLabel.bind("<Button-1>", partial(self.disabled_message))
        self.nav.gearLabel.bind("<Button-1>", partial(self.disabled_message))
        
        
        
         #exit button
        #aermod user nav button
        self.abortLabel= tk.Label(self.nav, text="  ABORT HEM RUN", font=TAB_FONT, fg='red', bg=self.main_color, height=2, anchor="w")
        self.abortLabel.place(in_=self.nav.container, relwidth=0.2, rely=0.54, relx=0.1)


#        
         #bind icon and label events
        self.abortLabel.bind("<Enter>", partial(self.otr_config, self.abortLabel,  self.highlightcolor))
        self.abortLabel.bind("<Leave>", partial(self.otr_config, self.abortLabel, self.main_color))
        self.abortLabel.bind("<Button-1>", lambda x:self.quit_app())
#        self.nav.abortLabel2.bind("<Enter>", partial(self.color_config, self.nav.abortLabel2, abortLabel, self.highlightcolor))
#        self.nav.abortLabel2.bind("<Leave>", partial(self.color_config, self.nav.abortLabel2, abortLabel,self.main_color))
#        self.nav.abortLabel2.bind("<Button-1>", partial(self.quit_app))

       
        
        
        
#        self.stop = tk.Button(self.meta_two, text="ABORT HEM RUN", bg='lightgrey', relief='solid', borderwidth=2,
#                    command=self.quit_app, font=TEXT_FONT, padx=20, pady=20)
#        self.stop.grid(row=0, column=2, sticky='E')
#        

        

        if hasattr(self, 'back'):
            self.back.destroy()

        
        self.processor = Processor(self, self.model, Event())
        print('about to send to future')
        future = executor.submit(self.processor.process)
        future.add_done_callback(self.processing_finish)
        
        

    def processing_finish(self, future):
        """
        Callback that gets run in the same thread as the processor, after the target method
        has finished. It's purpose is to update the shared callback queue so that the main
        thread can update the GUI (which cannot be done in this thread!)
        :param future:
        self.callbackQueue.put(self.finish_run)
        :return: None
        """
        
        self.callbackQueue.put(self.finish_run)
        

    def finish_run(self):
        """
        Return Hem4 running state to False, and either reset or quit the GUI, depending on
        whether or not the processing finished naturally or was aborted.
        :return: None
        """
        self.running = False
        if self.aborted:
            self.aborted = False
            self.reset_gui()
            self.after(500, self.check_processing)

    def check_processing(self):
        """
        Check the callback queue to see if the processing thread has indicated that
        it's finished running. If an entry is found, it's the method to run that
        instructs this class how to reset/kill the GUI. If not, schedule another
        check soon.
        :return: None
        """
        try:
            callback = self.callbackQueue.get(block=False)
        except queue.Empty: #raised when queue is empty
            self.after(500, self.check_processing)
            return

        print("About to call callback...")
        callback()

    def after_callback(self):
        """
        Function listens on thread Running HEM4 for error and completion messages
        logged via queue method
        """
        try:
            message = self.messageQueue.get(block=False)
        except queue.Empty:
            # let's try again later
            self.after(25, self.after_callback)
            return

        print('after_callback got', message)
        if message is not None:
            self.nav.log.scr.configure(state='normal')
            self.nav.log.scr.insert(tk.INSERT, message)
            self.nav.log.scr.insert(tk.INSERT, "\n")
            self.nav.log.scr.configure(state='disabled')
            self.nav.log.after(25, self.after_callback)
                 

#%% Stop Function    
    def quit_app(self):
        """
        
        Function handles quiting HEM4 by closing the window containing
        the GUI and exiting all background processes & threads
        """
        if self.running:
            override = messagebox.askokcancel("Confirm HEM4 Quit", "Are you "+
                                              "sure? HEM4 is currently running. Clicking 'OK' will stop HEM4.")

            if override:
                # Abort the thread and wait for it to stop...once it has
                # completed, it will signal this class to kill the GUI
                self.nav.iconLabel.configure(image=self.nav.cancelIcon)
                Logger.logMessage("Stopping HEM4...")
                self.processor.abortProcessing()
                self.abortLabel.unbind('<Button-1>')
                self.abortLabel['text'] = "ABORTING..."
                self.aborted = True
#                self.display_app_quit()

        else:
            # If we're not running, the only thing to do is reset the GUI...
           
            Logger.logMessage("HEM4 stopped")             

#    def display_app_quit(self):
#        self.enable_widgets(self.main, False)
#
#        Logger.logMessage = "HEM4 is stopping. Please wait."
#        tk.Label(self, text=message, font=TEXT_FONT, bg=self.tab_color).pack()
#        
    def disable_buttons(self):
            
        self.next.destroy()
            
            
        self.button_file.unbind('<Button1>')
        self.fileLabel.unbind('<Button1>')
        
        self.hap_file.unbind('<Button1>')
        self.hapLabel.unbind('<Button1>')
        
        self.emis_file.unbind('<Button1>')
        self.emisLabel.unbind('<Button1>')
        
        if 'user_rcpt' in self.model.dependencies:
            self.ur_file.unbind('<Button1>')
            self.urLabel.unbind('<Button1>')
                
                
        #emis var
        if 'emis_var' in self.model.dependencies:
            self.emisvar_file.unbind('<Button1>')
            self.emisvarLabel.unbind('<Button1>')
            
        if 'buoyant' in self.model.dependencies:
            self.buoy_file.unbind('<Button1>')
            self.buoyLabel.unbind('<Button1>')
                
        if 'poly' in self.model.dependencies:
            self.poly_file.unbind('<Button1>') 
            self.polyLabel.unbind('<Button1>')
            

        if 'bldg_dw' in self.model.dependencies:
            self.bldgdw_file.unbind('<Button1>')
            self.bldgdwLabel.unbind('<Button1>')
            
        if 'particle size' in self.model.dependencies:
            self.particle_file.unbind('<Button1>')
            self.particleLabel.unbind('<Button1>')
                
        if 'land use' in self.model.dependencies:
            self.land_file.unbind('<Button1>')
            self.landLabel.unbind('<Button1>')
            
        if 'seasons' in self.model.dependencies:
            self.seasons_file.unbind('<Button1>')
            self.seasonsLabel.unbind('<Button1>')


            
    def reset_gui(self):
        #reset all inputs if everything finished. actually destroy and recreate all inputs

        
        
        self.faclbl.set('')
        self.faclbl.set("1. Please select a Facilities List Options file:")
        self.button_file.unbind('<Button1>')
        self.fileLabel.unbind('<Button1>')
        
        
        self.haplbl.set('')
        self.haplbl.set("2. Please select a HAP Emissions file:")
        self.hap_file.unbind('<Button1>')
        self.hapLabel.unbind('<Button1>')
        
        
        self.emislbl.set('')
        self.emislbl.set("3. Please select an Emissions Location file:")
        self.emis_file.unbind('<Button1>')
        self.emisLabel.unbind('<Button1>')
        
        self.group_list.set('')
        
        self.altlbl.set('')
        self.altlbl.set("Please select an alternate receptor CSV file:")
        #reset alt reeceptors
        if 'altrec' in self.model.dependencies:

            self.model.dependencies.remove('altrec')
            self.urepLabel.destroy()
            self.urep_file.destroy()
            self.check_altrec.set(1)#alt receptors
            
            
        
         #find the last next button and disable that one

        if 'user_rcpt' in self.model.dependencies:
            for child in self.optional.s8.winfo_children():
                child.destroy()
                
                
        #emis var
        if 'emis_var' in self.model.dependencies:
            for child in self.optional.s9.winfo_children():
                child.destroy()
            
        if 'buoyant' in self.model.dependencies:
            for child in self.optional.s4.winfo_children():
                child.destroy()
                
        if 'poly' in self.model.dependencies:
            for child in self.optional.s5.winfo_children():
                child.destroy()

        if 'bldg_dw' in self.model.dependencies:
            for child in self.optional.s6.winfo_children():
                child.destroy()
            
        if 'particle size' in self.model.dependencies:
            for child in self.depdeplt.s4.winfo_children():
                child.destroy()
                
        if 'land use' in self.model.dependencies:
            for child in self.depdeplt.s5.winfo_children():
                child.destroy()
            
        if 'seasons' in self.model.dependencies:
            for child in self.depdeplt.s6.winfo_children():
                child.destroy()
 
        #destroy stop
        if hasattr(self, 'stop'):
            self.stop.destroy()
        
        self.current_tab = self.nav
        
        #next button
        self.next = tk.Button(self.meta_two, text="Next", bg='lightgrey', relief='solid', borderwidth=2,
                                    command=self.lift_tab, font=TEXT_FONT)
        self.next.grid(row=0, column=2, sticky='E', padx=20, pady=20)
        
        global instruction_instance
        self.instruction_instance.set(" ")
        self.optional.instruction_instance.set(" ")
        self.depdeplt.instruction_instance.set(" ")
        
        #reenable options tab
        self.nav.optionsLabel.bind("<Button-1>", partial(self.nav.lift_page, self.nav.optionsLabel, self.nav.gearLabel, self.nav.options, self.nav.current_button))
        self.nav.gearLabel.bind("<Button-1>", partial(self.nav.lift_page, self.nav.gearLabel, self.nav.optionsLabel, self.nav.options, self.nav.current_button))
        
        self.model.reset()
        self.nav.iconLabel.configure(image=self.nav.runIcon)
        self.abortLabel.destroy()
        
        
        self.running = False
  
#%%
        
    def reset_inputs(self, inputtype):
        """ 
        Resets itenerant HEM4 dependent inputs when a facilities list option file is reuploaded or
        emissions location is reuploaded. For facilities list option file that is all inputs after it,
        for emissions location file that is all inputs after that input.
        """
        
        if inputtype == 'faclist':
            #reset everything as you would the gui
            
            self.haplbl.set('')
            self.haplbl.set("2. Please select a HAP Emissions file:")
            self.hap_file.unbind('<Button1>')
            self.hapLabel.unbind('<Button1>')
            
            
            self.emislbl.set('')
            self.emislbl.set("3. Please select an Emissions Location file:")
            self.emis_file.unbind('<Button1>')
            self.emisLabel.unbind('<Button1>')
            
            
            #user receptor
            self.optional.urlbl = tk.StringVar()
            self.optional.urlbl.set('')
            self.optional.urlbl.set("Please select a User Receptors file:")
            
            #variation
            self.optional.varlbl = tk.StringVar()
            self.optional.varlbl.set('')
            self.optional.varlbl.set("Please select an Emissions Variation file:")
            
            #buoyant line
            self.optional.buoylbl = tk.StringVar()
            self.optional.buoylbl.set('')
            self.optional.buoylbl.set("Please select associated Buoyant Line"+
                                     " Parameters file:")
            
            #poly vertex
            self.optional.polylbl = tk.StringVar()
            self.optional.polylbl.set('')
            self.optional.polylbl.set("Please select associated Polygon Vertex file:")
            
            #building downwash
            self.optional.bldgdwlbl = tk.StringVar()
            self.optional.bldgdwlbl.set('')
            self.optional.bldgdwlbl.set("Please select associated Building Dimensions file:")
                
            
            #particle size input
            self.depdeplt.partlbl = tk.StringVar()
            self.depdeplt.partlbl.set('')
            self.depdeplt.partlbl.set("Please select Particle Size file:")
            
            #land file input
            self.depdeplt.landlbl = tk.StringVar()
            
            self.depdeplt.landlbl.set('')
            self.depdeplt.landlbl.set("Please select Land Use file:")
            
            #seasons file input
            self.depdeplt.seasonlbl = tk.StringVar()
            self.depdeplt.seasonlbl.set('')
            self.depdeplt.seasonlbl.set("Please select Month-to-Season Vegetation file:")
            
           
           #reset model values 
            self.model.emisloc = None
            self.model.hapemis = None
            self.model.multipoly = None
            self.model.multibuoy = None
            self.model.ureceptr = None
            self.model.bldgdw = None
            self.model.partdep = None
            self.model.landuse = None
            self.model.seasons = None
            self.model.emisvar = None
            self.model.depdeplt = None
            self.model.gasdryfacs = None
            self.model.particlefacs = None
            

            
            if 'user_rcpt' in self.model.dependencies:
                for child in self.optional.s8.winfo_children():
                    child.destroy()
                                   
            #emis var
            if 'emis_var' in self.model.dependencies:
                for child in self.optional.s9.winfo_children():
                    child.destroy()
                
            if 'buoyant' in self.model.dependencies:
                for child in self.optional.s4.winfo_children():
                    child.destroy()
                    
            if 'poly' in self.model.dependencies:
                for child in self.optional.s5.winfo_children():
                    child.destroy()
    
            if 'bldg_dw' in self.model.dependencies:
                for child in self.optional.s6.winfo_children():
                    child.destroy()
                
            if 'particle size' in self.model.dependencies:
                for child in self.depdeplt.s4.winfo_children():
                    child.destroy()
                    
            if 'land use' in self.model.dependencies:
                for child in self.depdeplt.s5.winfo_children():
                    child.destroy()
                
            if 'seasons' in self.model.dependencies:
                for child in self.depdeplt.s6.winfo_children():
                    child.destroy()
                    
            self.model.dependencies = []
 
        elif inputtype == 'emisloc':
            
            #reset model values
            self.model.multipoly = None
            self.model.multibuoy = None
            self.model.depdeplt = None
            self.model.gasdryfacs = None
            self.model.particlefacs = None
            
            #buoyant line
            self.optional.buoylbl = tk.StringVar()
            self.optional.buoylbl.set('')
            self.optional.buoylbl.set("Please select associated Buoyant Line"+
                                     " Parameters file:")
            
            #poly vertex
            self.optional.polylbl = tk.StringVar()
            self.optional.polylbl.set('')
            self.optional.polylbl.set("Please select associated Polygon Vertex file:")
            
            if 'buoyant' in self.model.dependencies:
                for child in self.optional.s4.winfo_children():
                    child.destroy()
                    
                self.model.dependencies.remove('buoyant')
                    
            if 'poly' in self.model.dependencies:
                for child in self.optional.s5.winfo_children():
                    child.destroy()
            
                self.model.dependencies.remove('poly')
            
    
        
    def otr_config(self, widget1, color, event):
        
         widget1.configure(bg=color)
       
        
    def color_config(self, widget1, widget2, container, color, event):
        
         widget1.configure(bg=color)
         widget2.configure(bg=color)
         container.configure(bg=color)
         
  
     
         #serve instructions
         if self.hapLabel in [widget1, widget2]:
             if self.instruction_instance.get() == " ":
                 
                 self.browse("instructions/hap_browse.txt")
                 
             else:
                 self.instruction_instance.set(" ")
                
         elif self.emisLabel in [widget1, widget2]:
              
             if self.instruction_instance.get() == " ":
                 
                 self.browse("instructions/emis_browse.txt")
                 
             else:
                 self.instruction_instance.set(" ")
                 
             
         elif self.fileLabel in [widget1, widget2]:
             
             if self.instruction_instance.get() == " ":
                 
                 self.browse("instructions/fac_browse.txt")
                 
             else:
                 self.instruction_instance.set(" ")
                 

         elif "Please select an alternate receptor CSV file:" in [widget1['text'], widget2['text']]:
             
             if self.instruction_instance.get() == " ":
                 
                 self.browse("instructions/urepalt_browse.txt")
                 
             else:
                 self.instruction_instance.set(" ")
                 
                 
         elif "Please select associated Buoyant Line Parameters file:" in [widget1['text'], widget2['text']]:
             
             if self.optional.instruction_instance.get() == " ":
                 
                 self.optional.browse("instructions/buoyant_browse.txt")
                 
             else:
                 self.optional.instruction_instance.set(" ")
                 
                                  
         elif "Please select associated Polygon Vertex file:" in [widget1['text'], widget2['text']]:
             
             if self.optional.instruction_instance.get() == " ":
                 
                 self.optional.browse("instructions/poly_browse.txt")
                 
             else:
                 self.optional.instruction_instance.set(" ")
                 
         elif 'Please select a User Receptors file:' in [widget1['text'], widget2['text']]:
             
             if self.optional.instruction_instance.get() == " ":
                 
                 self.optional.browse("instructions/urep_browse.txt")
                 
             else:
                 self.optional.instruction_instance.set(" ")
                 
         elif 'Please select an Emissions Variation file:' in [widget1['text'], widget2['text']]:
             
             if self.optional.instruction_instance.get() == " ":
                 
                 self.optional.browse("instructions/emvar_browse.txt")
                 
             else:
                 self.optional.instruction_instance.set(" ")
                 
        
         elif "Please select associated Building Dimensions file:" in [widget1['text'], widget2['text']]:
             
             if self.optional.instruction_instance.get() == " ":
                 
                 self.optional.browse("instructions/bd_browse.txt")
                 
             else:
                 self.optional.instruction_instance.set(" ")
             
        
         elif "Please select Particle Size file:" in [widget1['text'], widget2['text']]:
             
             if self.depdeplt.instruction_instance.get() == " ":
                 
                 self.depdeplt.browse("instructions/dep_part_browse.txt")
                 
             else:
                 self.depdeplt.instruction_instance.set(" ")
                 
                 
         elif "Please select Land Use file:" in [widget1['text'], widget2['text']]:
             
             if self.depdeplt.instruction_instance.get() == " ":
                 
                 self.depdeplt.browse("instructions/dep_land_browse.txt")
                 
             else:
                 self.depdeplt.instruction_instance.set(" ")
                 
         elif "Please select Month-to-Season Vegetation file:" in [widget1['text'], widget2['text']]:
             
             if self.depdeplt.instruction_instance.get() == " ":
                 
                 self.depdeplt.browse("instructions/dep_veg_browse.txt")
                 
             else:
                 self.depdeplt.instruction_instance.set(" ")
                 
         
            
           
#         print(self.current_highlight)
         
    

    def remove_config(self, widget1, widget2, container, color, event):
        
         widget1.configure(bg=color)
         widget2.configure(bg=color)
         container.configure(bg=color)
         self.instruction_instance.set(" ")
         self.optional.instruction_instance.set(" ")
         self.depdeplt.instruction_instance.set(" ")
         
         
   

         
         
         
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
    
        
class Options(Page):
    def __init__(self, nav, *args, **kwargs):
        Page.__init__(self, *args, **kwargs)
        
        self.home = nav 
        
        container = tk.Frame(self, bg=self.tab_color, bd=2)
        #        self.buttonframe.pack(side="w", fill="y", expand=False)
        container.pack(side="top", fill="both", expand=True) 

        self.s1 = tk.Frame(container, width=600, height=50, bg=self.tab_color)
        self.s2 = tk.Frame(container, width=600, height=50, bg=self.tab_color)
        self.s3 = tk.Frame(container, width=600, height=50, bg=self.tab_color)
        self.s4 = tk.Frame(container, width=600, height=50, bg=self.tab_color)
        self.s5 = tk.Frame(container, width=600, height=50, bg=self.tab_color)
#
        self.s1.grid(row=0, column=0, columnspan=2, sticky="nsew")
        self.s2.grid(row=1, column=0, columnspan=2, sticky="nsew")
        self.s3.grid(row=2, column=0, columnspan=2, sticky="nsew")
        self.s4.grid(row=3, column=0, columnspan=2, sticky="nsew")
        self.s5.grid(row=4, column=0, columnspan=2, sticky="nsew")

        self.tt = PIL.Image.open('images\icons8-settings-48-white.png').resize((30,30))
        self.tticon = self.add_margin(self.tt, 5, 0, 5, 0)
        self.titleicon = ImageTk.PhotoImage(self.tticon)
        self.titleLabel = tk.Label(self.s1, image=self.titleicon, bg=self.tab_color)
        self.titleLabel.image = self.titleicon # keep a reference!
        self.titleLabel.grid(row=1, column=0, padx=10, pady=10)

        title2 = tk.Label(self.s1, text="REVISE CENSUS", font=TITLE_FONT, fg="white", bg=self.tab_color)
        title2.grid(row=1, column=1, padx=10, sticky='W', pady=10)

        
        fu = PIL.Image.open('images\icons8-folder-48.png').resize((30,30))
        ficon = self.add_margin(fu, 5, 0, 5, 0)
        fileicon = ImageTk.PhotoImage(ficon)
        self.fileLabel = tk.Label(self.s3, image=fileicon, bg=self.tab_color)
        self.fileLabel.image = fileicon # keep a reference!
        self.fileLabel.grid(row=0, column=0, padx=10)
        
        self.folder_select = tk.Label(self.s3, text="Please select a census update file:", font=TITLE_FONT, bg=self.tab_color, anchor="w")
        self.folder_select.grid(pady=10, padx=10, row=0, column=1)


        self.fileLabel.bind("<Enter>", partial(self.color_config, self.fileLabel, self.folder_select, self.s3, 'light grey'))
        self.fileLabel.bind("<Leave>", partial(self.color_config, self.fileLabel, self.folder_select, self.s3, self.tab_color))
        self.fileLabel.bind("<Button-1>", partial(self.uploadCensusUpdates))
        
        self.folder_select.bind("<Enter>", partial(self.color_config, self.folder_select, self.fileLabel, self.s3, 'light grey'))
        self.folder_select.bind("<Leave>", partial(self.color_config, self.folder_select, self.fileLabel, self.s3, self.tab_color))
        self.folder_select.bind("<Button-1>", partial(self.uploadCensusUpdates))
        
        
        
        
        ru = PIL.Image.open('images\icons8-update-48.png').resize((30,30))
        ricon = self.add_margin(ru, 5, 0, 5, 0)
        rileicon = ImageTk.PhotoImage(ricon)
        rileLabel = tk.Label(self.s4, image=rileicon, bg=self.tab_color)
        rileLabel.image = rileicon # keep a reference!
        rileLabel.grid(row=0, column=1, padx=10, sticky='E')
        
        
        run_button = tk.Label(self.s4, text="Revise", font=TEXT_FONT, bg=self.tab_color)
        run_button.grid(row=0, column=2, padx=5, pady=10, sticky='E')
        
        run_button.bind("<Enter>", partial(self.color_config, run_button, rileLabel, self.s4, 'light grey'))
        run_button.bind("<Leave>", partial(self.color_config, run_button, rileLabel, self.s4, self.tab_color))
        run_button.bind("<Button-1>", self.update_census)
        
        rileLabel.bind("<Enter>", partial(self.color_config, rileLabel, run_button, self.s4, 'light grey'))
        rileLabel.bind("<Leave>", partial(self.color_config, rileLabel, run_button, self.s4, self.tab_color))
        rileLabel.bind("<Button-1>", self.update_census)
##%%
        
        
        
        
        
    
    
    def update_census(self, event):
        """
        Function creates thread for running HEM4 concurrently with tkinter GUI
        """
        
        #disable hem4 tab
        self.home.newrunLabel.bind("<Button-1>", partial(self.disabled_message))
        self.home.iconLabel.bind("<Button-1>", partial(self.disabled_message))
        
        
        executor = ThreadPoolExecutor(max_workers=1)

        future = executor.submit(self.censusupdater.update, self.censusUpdatePath)
        future.add_done_callback(self.update_census_finish)
        
        

    def update_census_finish(self, future):
        self.callbackQueue.put(self.finish_census_update)

    def finish_census_update(self):
        self.folder_select['text'] = "Please select a census update file:"
        
        #reenable hem4 tab
        self.home.newrunLabel.bind("<Button-1>", partial(self.home.lift_page, self.home.newrunLabel, self.home.iconLabel, self.home.hem, self.home.current_button))
        self.home.iconLabel.bind("<Button-1>", partial(self.home.lift_page, self.home.iconLabel, self.home.newrunLabel, self.home.hem, self.home.current_button))

    def uploadCensusUpdates(self, event):
        self.censusupdater = CensusUpdater()
        fullpath = self.openFile(askopenfilename())
        if fullpath is not None:
            self.censusUpdatePath = fullpath
            self.folder_select['text'] = fullpath.split("\\")[-1]
    #%% Run function with checks if somethign is missing raise the error here and 
#   create an additional dialogue before trying to run the file


#%%
class Log(Page):
    
    def __init__(self, nav, *args, **kwargs):
        Page.__init__(self, *args, **kwargs)
        
        self.nav = nav 
        
        container = tk.Frame(self, bg=self.tab_color, bd=2)
        
        #        self.buttonframe.pack(side="w", fill="y", expand=False)
        container.pack(side="top", fill="both", expand=True) 
        
        # Adding a Textbox Entry widget
        #        scrolW  = 65; scrolH  =  25
        self.scr = scrolledtext.ScrolledText(container, wrap=tk.WORD, width=1000, height=1000, font=TEXT_FONT)
        self.scr.configure(cursor='none')
        self.scr.pack(expand=1, fill="both")
        self.scr.bind("<Button-1>", self.interfere)
        
    def interfere(self, event):
        print("Block interruption")
        

         # Pack to make visible

#%%
         
         
#protocol for handlign closing tkinter when HEM4 is running

def on_closing(hem):
    
    if hem.running == True:
    
            hem.quit_app()
            if hem.aborted == True:
                root.destroy()
                
    else:
        root.destroy()


# infinite loop which is required to 
# run tkinter program infinitely 
# until an interrupt occurs
if __name__ == "__main__":
    root = tk.Tk()
    w, h = root.winfo_screenwidth(), root.winfo_screenheight()
    root.tk.call('wm', 'iconphoto', root._w, tk.PhotoImage(file='images/HEM4.png'))
    root.title("")

    #root.geometry("%dx%d" % (w/2, h/2))
#   root.iconbitmap('images/HEM4.ico')
#    root.configure(bg="lightcyan3")
#    root.wm_attributes('-transparentcolor', root['bg'])
    main = MainView(root)
    main.pack(side="top", fill="both", expand=True)
    root.protocol("WM_DELETE_WINDOW", lambda: on_closing(main.hem))
    root.wm_minsize(1000,800)
#    root.state('zoomed')
    root.mainloop() #if we want to open full screen
