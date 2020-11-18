# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 10:30:46 2020

@author: Steve Fudge
"""

import os, shutil


class InputsPackager():
    
    def __init__(self, targetDir, model):
        """
        The Inputs Packager ensures that all input files used for the HEM4
        run are stored in the Inputs folder which is a subfolder in the
        group output folder.
        
        The following actions are performed:
            1) Check for existence of the Inputs folder.
            2) If Inputs does not exist, then create it. If it does exist
                then clear it out.
            3) Copy all HEM4 inputs into the Inputs folder using generic
                filenames (e.g. emisloc, hapemis, faclist) without the 
                group name.
        """
        
        self.categoryFolder = targetDir
        self.model = model
        self.inputsFolder = os.path.join(targetDir, "Inputs")
        
    
    def createInputs(self):
        
        # Make sure Inputs folder exists and is empty
        if os.path.exists(self.inputsFolder) == False:
            os.makedirs(self.inputsFolder)
        else:
            for filename in os.listdir(self.inputsFolder):
                file_path = os.path.join(self.inputsFolder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except BaseException as e:
                    emessage = "Failed to delete " + file_path + ". Reason: " + e
                    raise Exception(emessage)
        
        # Copy HEM4 inputs to Inputs folder
        haplib_file = self.model.haplib.path
        inputs_haplib_file = os.path.join(self.inputsFolder, "haplib.xlsx")
        shutil.copyfile(haplib_file, inputs_haplib_file)

        organs_file = self.model.organs.path
        inputs_organs_file = os.path.join(self.inputsFolder, "target_organs.xlsx")
        shutil.copyfile(organs_file, inputs_organs_file)
        
        faclist_file = self.model.faclist.path
        inputs_faclist_file = os.path.join(self.inputsFolder, "faclist.xlsx")
        shutil.copyfile(faclist_file, inputs_faclist_file)
        
        emisloc_file = self.model.emisloc.path
        inputs_emisloc_file = os.path.join(self.inputsFolder, "emisloc.xlsx")
        shutil.copyfile(emisloc_file, inputs_emisloc_file)
               
        hapemis_file = self.model.hapemis.path
        inputs_hapemis_file = os.path.join(self.inputsFolder, "hapemis.xlsx")
        shutil.copyfile(hapemis_file, inputs_hapemis_file)

        if self.model.altRec_optns.get("altrec", None):
            alt_rcpt_file = self.model.altRec_optns.get("path", None)
            inputs_altrec_file = os.path.join(self.inputsFolder, "alt_receptors.csv")
            shutil.copyfile(alt_rcpt_file, inputs_altrec_file)

        if self.model.ureceptr is not None:
            user_rcpt_file = self.model.ureceptr.path
            inputs_urec_file = os.path.join(self.inputsFolder, "user_receptors.xlsx")
            shutil.copyfile(user_rcpt_file, inputs_urec_file)

        if self.model.partdep is not None:
            part_size_file = self.model.partdep.path
            inputs_partsize_file = os.path.join(self.inputsFolder, "particle_data.xlsx")
            shutil.copyfile(part_size_file, inputs_partsize_file)

        if self.model.bldgdw is not None:
            bldg_file = self.model.bldgdw.path
            inputs_bldg_file = os.path.join(self.inputsFolder, "building_dimensions.xlsx")
            shutil.copyfile(bldg_file, inputs_bldg_file)
        
        if self.model.landuse is not None:
            landuse_file = self.model.landuse.path
            inputs_landuse_file = os.path.join(self.inputsFolder, "landuse.xlsx")
            shutil.copyfile(landuse_file, inputs_landuse_file)

        if self.model.seasons is not None:
            season_file = self.model.seasons.path
            inputs_season_file = os.path.join(self.inputsFolder, "month-to-seasons.xlsx")
            shutil.copyfile(season_file, inputs_season_file)

        if self.model.emisvar is not None:
            emisvar_file = self.model.emisvar.path
            inputs_emisvar_file = os.path.join(self.inputsFolder, "emisvar.xlsx")
            shutil.copyfile(emisvar_file, inputs_emisvar_file)

        if self.model.multipoly is not None:
            vertex_file = self.model.multipoly.path
            inputs_vertex_file = os.path.join(self.inputsFolder, "polygon_vertex.xlsx")
            shutil.copyfile(vertex_file, inputs_vertex_file)

        if self.model.multibuoy is not None:
            blp_file = self.model.multibuoy.path
            inputs_blp_file = os.path.join(self.inputsFolder, "buoyant_line_parameters.xlsx")
            shutil.copyfile(blp_file, inputs_blp_file)
        