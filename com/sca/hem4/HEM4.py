# -*- coding: utf-8 -*-
"""
Created on Thu May  7 12:19:07 2020

@author: David Lindsey
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Apr  6 22:56:18 2020

@author: David Lindsey
"""

import os
import queue
import sys
import tkinter as tk
import tkinter.ttk as ttk
from PIL import ImageTk
import PIL.Image
import pickle
import shutil


from concurrent.futures import ThreadPoolExecutor
from threading import Event
from tkinter import messagebox
from tkinter import scrolledtext
import numpy as np

from datetime import datetime
from com.sca.hem4.Processor import Processor
from com.sca.hem4.log.Logger import Logger
from com.sca.hem4.tools.CensusUpdater import CensusUpdater
from com.sca.hem4.model.Model import Model
from com.sca.hem4.upload.FileUploader import FileUploader
from tkinter.filedialog import askopenfilename
from com.sca.hem4.checker.InputChecker import InputChecker
from com.sca.hem4.DepositionDepletion import check_dep, check_phase
from com.sca.hem4.SaveState import SaveState
from tkinter.simpledialog import Dialog, Toplevel
from ttkthemes import ThemedStyle

from collections import defaultdict
import uuid


TITLE_FONT= ("Daytona", 16, 'bold')
TEXT_FONT = ("Verdana", 14)
LOG_FONT = ("Verdana", 12)

class Page(tk.Frame):
    def __init__(self, *args, **kwargs):
        tk.Frame.__init__(self, *args, **kwargs)
        self.main_color = "white"
        self.tab_color = "lightcyan3"
        self.highlightcolor = "snow3"
    
    def color_config(self, widget1, widget2, container, color, event):
        
         widget1.configure(bg=color)
         widget2.configure(bg=color)
         container.configure(bg=color)   
    
    def add_margin(self, pil_img, top, right, bottom, left):
        width, height = pil_img.size
        new_width = width + right + left
        new_height = height + top + bottom
        result = PIL.Image.new(pil_img.mode, (new_width, new_height))
        result.paste(pil_img, (left, top))
        return result  
        
       # Instructions for HEM4    
    def add_instructions(self, placeholder1, placeholder2):
        
        #Dynamic instructions place holder
        global instruction_instance
        self.instruction_instance = tk.StringVar(placeholder1)
        self.instruction_instance.set(" ")
        self.dynamic_inst = tk.Label(placeholder2, wraplength=600, font=TEXT_FONT, padx=20, bg=self.tab_color) 
        self.dynamic_inst.config(height=6)
        
        self.dynamic_inst["textvariable"] = self.instruction_instance 
        self.dynamic_inst.grid(row=0, column=0)


        
     #%% Event handlers for porting instructions

    #reset instructions space
    def reset_instructions(self):
        """
        Function clears instructions from display box 
        """
        global instruction_instance
        self.instruction_instance.set(" ")    
        
    #general function for browsing instructions
    def browse(self, location):
        """
        Function looks up text file with instructions for specified input
        browse buttons
        """
        global instruction_instance
        self.read_inst = open(location, 'r')
        self.instruction_instance.set(self.read_inst.read())  
        
        
    def show(self):
        self.lift()
        
        
        
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
    
            

#%% OPtional Tab

class Optional(Page):
    def __init__(self, nav, *args, **kwargs):
        Page.__init__(self, *args, **kwargs) 
        
        self.poly_up = None
        self.buoyant_up = None
        self.bldgdw_up = None
        self.ur = None
        
        #set text variables for input labels
        
        #user receptor
        self.urlbl = tk.StringVar()
        self.urlbl.set("Please select a User Receptors file:")
        
        #variation
        self.varlbl = tk.StringVar()
        self.varlbl.set("Please select an Emissions Variation file:")
        
        #buoyant line
        self.buoylbl = tk.StringVar()
        self.buoylbl.set("Please select associated Buoyant Line"+
                                 " Parameters file:")
        
        #poly vertex
        self.polylbl = tk.StringVar()
        self.polylbl.set("Please select associated Polygon Vertex file:")
        
        #building downwash
        self.bldgdwlbl = tk.StringVar()
        self.bldgdwlbl.set("Please select associated Building Dimensions file:")
        
        
    
        self.model = nav.model
        self.uploader = nav.uploader

        self.nav = nav 
                
         ##Frames for main inputs
        self.required_inputs = tk.Frame(self, width=600, bg=self.tab_color)
        self.required_inputs.pack(fill="both", expand=True, side="top")
        
        
        self.s1 = tk.Frame(self.required_inputs, width=600, height=50, bg=self.tab_color)
        self.s2 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s3 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s4 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s5 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s6 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s7 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s8 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s9 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)


        #grid layout for main inputs 
        self.s1.grid(row=0, column=0, columnspan=2, sticky="nsew")
        self.s2.grid(row=1, column=0)
        self.s3.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=10)
        self.s8.grid(row=3, column=0, columnspan=2, sticky="nsew")
        self.s9.grid(row=4, column=0, columnspan=2, sticky="nsew")
        self.s4.grid(row=5, column=0, columnspan=2, sticky="nsew")
        self.s5.grid(row=6, column=0, columnspan=2, sticky="nsew")
        self.s6.grid(row=7, column=0, columnspan=2, sticky="nsew")
        self.s7.grid(row=10, column=0, columnspan=2, sticky="nsew")
        
        
        

        self.required_inputs.grid_rowconfigure(10, weight=1)
        self.required_inputs.grid_columnconfigure(2, weight=1)
        self.s2.grid_propagate(0)
        
        self.tt = PIL.Image.open('images\icons8-add-column-48-white.png').resize((30,30))
        self.tticon = self.add_margin(self.tt, 5, 0, 5, 0)
        self.titleicon = ImageTk.PhotoImage(self.tticon)
        self.titleLabel = tk.Label(self.s1, image=self.titleicon, bg=self.tab_color)
        self.titleLabel.image = self.titleicon # keep a reference!
        self.titleLabel.grid(row=1, column=0, padx=10, pady=10)
        
        
        self.title = tk.Label(self.s1, font=TITLE_FONT, fg="white", bg=self.tab_color, 
                             text="ADDITIONAL INPUTS")
        self.title.grid(row=1, column=1, sticky="W", pady=10, padx=10)

#%% Setting up  directions text space
        
        self.add_instructions(self.s3, self.s3)

# %% Setting up each file upload space (includes browse button, and manual text entry for file path)
       
    def lift_tab(self):

        if 'particle size' in self.model.dependencies or 'land use' in self.model.dependencies or 'seasons' in self.model.dependencies:
            self.nav.depdeplt.lift()

        else:

            self.nav.run() 
            
    def back_tab(self):
        
        self.nav.lift()
        


    def uploadPolyvertex(self, container, label, event):
        """
        Function for uploading polyvertex source file
        """
        

        
        if self.model.emisloc.dataframe is None:
            messagebox.showinfo("Emissions Locations File Missing",
                "Please upload an Emissions Locations file before adding" +
                " a Polyvertex file.")
        fullpath = self.openFile(askopenfilename())
        if fullpath is not None:
            self.uploader.uploadDependent("polyvertex", fullpath, 
                                                   self.model.emisloc.dataframe)

            
            if self.model.multipoly.dataframe.empty == False:



                # Update the UI
                [self.nav.nav.log.scr.insert(tk.INSERT, msg) for msg in self.model.multipoly.log]
    #            container.configure(bg='light green')
                
                self.polylbl.set('')
                self.polylbl.set(fullpath.split("\\")[-1])
 

    def uploadbuoyant(self, container, label, event):
        """
        Function for uploading buoyant line parameter file
        """

    
        if self.model.emisloc.dataframe is None:
            messagebox.showinfo("Emissions Locations File Missing",
                "Please upload an Emissions Locations file before adding"+ 
                " a buoyant line file.")
        
        fullpath = self.openFile(askopenfilename())
        if fullpath is not None:
            self.uploader.uploadDependent("buoyant line", fullpath, 
                                                   self.model.emisloc.dataframe)
            
            if self.model.multibuoy.dataframe.empty == False:


                # Update the UI
                [self.nav.nav.log.scr.insert(tk.INSERT, msg) for msg in self.model.multibuoy.log]
    #            container.configure(bg='light green')
                
                self.buoylbl.set('')
                self.buoylbl.set(fullpath.split("\\")[-1])
 
    
    def uploadBuildingDownwash(self, container, label, event):
        """ 
        Function for uploading building downwash
        """

        if self.model.faclist.dataframe is None:
            messagebox.showinfo("Facilities List Option File Missing",
                "Please upload a Facilities List Options file before selecting"+
                " a building downwash file.")

        fullpath = self.openFile(askopenfilename())
        if fullpath is not None: 
            self.uploader.uploadDependent("building downwash", fullpath, 
                                          self.model)

            
            if self.model.bldgdw.dataframe.empty == False:
                
                # Update the UI
                [self.nav.nav.log.scr.insert(tk.INSERT, msg) for msg in self.model.bldgdw.log]
    #            container.configure(bg='light green')
    
                self.bldgdwlbl.set('')
                self.bldgdwlbl.set(fullpath.split("\\")[-1])



    def uploadUserReceptors(self, container, label, event):
        """
        Function for uploading user receptors
        """
        
        if self.model.faclist.dataframe is None:
            messagebox.showinfo("Facilities List Option File Missing",
                "Please upload a Facilities List Options file before selecting"+
                " a User Receptors file.")
            return

        fullpath = self.openFile(askopenfilename())
        if fullpath is not None:
            self.uploader.uploadDependent("user receptors", fullpath, self.model.faclist.dataframe)
            
            if self.model.ureceptr.dataframe.empty == False:


                    self.model.model_optns['ureceptr'] = True
                    # Update the UI
                    [self.nav.nav.log.scr.insert(tk.INSERT, msg) for msg in self.model.ureceptr.log]
                    
                    self.urlbl.set('')
                    self.urlbl.set(fullpath.split("\\")[-1])
     
                
            
#%% Deposition
            
            
class DepDplt(Page):
    def __init__(self, nav, *args, **kwargs):
        Page.__init__(self, *args, **kwargs) 
        
        self.model = nav.model
        self.uploader = nav.uploader
        
        self.nav = nav
        
        self.dep_part_up = None
        self.dep_land_up = None
        self.dep_seasons_up = None
        
        #set text variables for labels
        
        #particle size input
        self.partlbl = tk.StringVar()
        self.partlbl.set("Please select Particle Size file:")
        
        #land file input
        self.landlbl = tk.StringVar()
        self.landlbl.set("Please select Land Use file:")
        
        #seasons file input
        self.seasonlbl = tk.StringVar()
        self.seasonlbl.set("Please select Month-to-Season Vegetation file:")
        
        
        
         ##Frames for main inputs
        self.required_inputs = tk.Frame(self, width=600, bg=self.tab_color)
        self.required_inputs.pack(fill="both", expand=True, side="top")
        
        self.s1 = tk.Frame(self.required_inputs, width=600, height=50, bg=self.tab_color)
        self.s2 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s3 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s4 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s5 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        self.s6 = tk.Frame(self.required_inputs, width=600, height=50, pady=5, padx=5, bg=self.tab_color)
        



        #grid layout for main inputs 
        self.s1.grid(row=0, column=0, columnspan=2, sticky="nsew")
        self.s2.grid(row=1, column=0)
        self.s3.grid(row=2, column=0, columnspan=2, sticky="nsew", pady=10)
        self.s4.grid(row=3, column=0, columnspan=2, sticky="nsew")
        self.s5.grid(row=4, column=0, columnspan=2, sticky="nsew")
        self.s6.grid(row=5, column=0, columnspan=2, sticky="nsew")
        
        
        self.required_inputs.grid_rowconfigure(8, weight=4)
        self.required_inputs.grid_columnconfigure(2, weight=1)
        
        self.s2.grid_propagate(0)
        
        self.tt = PIL.Image.open('images\icons8-add-column-48-white.png').resize((30,30))
        self.tticon = self.add_margin(self.tt, 5, 0, 5, 0)
        self.titleicon = ImageTk.PhotoImage(self.tticon)
        self.titleLabel = tk.Label(self.s1, image=self.titleicon, bg=self.tab_color)
        self.titleLabel.image = self.titleicon # keep a reference!
        self.titleLabel.grid(row=1, column=0, padx=10, pady=10)
        
        self.title = tk.Label(self.s1, font=TITLE_FONT, fg="white", bg=self.tab_color, 
                             text="DEPOSITION & DEPLETION INPUTS")
        self.title.grid(row=1, column=1, sticky="W", pady=10, padx=10)


#%% Setting up  directions text space
        
        self.add_instructions(self.s3, self.s3)
    

    def lift_tab(self):

        self.nav.run()   
        
    def back_tab(self):
        
        if 'buoyant' in self.nav.model.dependencies or 'poly' in self.nav.model.dependencies or 'bldg_dw' in self.nav.model.dependencies or 'emisvar' in self.nav.model.dependencies:
            self.optional.lift()
            
        else:
            self.nav.lift()

    def uploadParticle(self, facilities, container, label, event):
        """ 
        Function for uploading particle size
        """
        if self.model.faclist.dataframe is None:
            messagebox.showinfo("Facilities List Option File Missing",
                "Please upload a Facilities List Options file before selecting"+
                " a particle file.")

        fullpath = self.openFile(askopenfilename())
        if fullpath is not None: 
            self.uploader.uploadDependent("particle depletion", fullpath, 
                                           self.model.hapemis.dataframe, facilities)
            
            if self.model.partdep.dataframe.empty == False:

                # Update the UI
                [self.nav.nav.log.scr.insert(tk.INSERT, msg) for msg in self.model.partdep.log]
    #            container.configure(bg='light green')
    
                self.partlbl.set('')
                self.partlbl.set(fullpath.split("\\")[-1])

    def uploadLandUse(self, container, label, event):
        """
        Function for uploading land use information
        """
        if self.model.faclist.dataframe is None:
            messagebox.showinfo("Facilities List Option File Missing",
                "Please upload a Facilities List Options file before selecting"+
                " a particle file.")

        fullpath = self.openFile(askopenfilename())
        if fullpath is not None: 
            self.uploader.uploadDependent("land use", fullpath, 
                                          self.model.gasdryfacs)
            
            if self.model.landuse.dataframe.empty == False:


                # Update the UI
                [self.nav.nav.log.scr.insert(tk.INSERT, msg) for msg in self.model.landuse.log]
    #            container.configure(bg='light green')
 
                self.landlbl.set('')
                self.landlbl.set(fullpath.split("\\")[-1])

    def uploadSeasons(self, container, label, event):
        """ 
        Function for uploading seasonal vegetation information
        """
        if self.model.faclist.dataframe is None:
            messagebox.showinfo("Facilities List Option File Missing",
                "Please upload a Facilities List Options file before selecting"+
                " a particle file.")

        fullpath = self.openFile(askopenfilename())
        if fullpath is not None: 
            self.uploader.uploadDependent("seasons", fullpath, 
                                          self.model.gasdryfacs)
            
            if self.model.seasons.dataframe.empty == False:


                # Update the UI
                [self.nav.nav.log.scr.insert(tk.INSERT, msg) for msg in self.model.seasons.log]
    #            container.configure(bg='light green')
                
                self.seasonlbl.set('')
                self.seasonlbl.set(fullpath.split("\\")[-1])
