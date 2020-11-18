# -*- coding: utf-8 -*-
"""
Created on Thu Mar  7 08:04:27 2019

@author: MMORRIS
"""
import os
import warnings

import pandas as pd
import geopandas as gp
from fiona import _shim, schema
from shapely.geometry import Point
from bokeh.io import curdoc
from bokeh.tile_providers import STAMEN_TONER_LABELS
from bokeh.models import WMTSTileSource, LabelSet, ColumnDataSource, HoverTool, \
    WheelZoomTool, ZoomInTool, ZoomOutTool, PanTool, ResetTool, SaveTool
from bokeh.models.widgets import Panel, Tabs
from bokeh.plotting import figure, save

from com.sca.hem4.writer.csv.AllPolarReceptors import AllPolarReceptors
from com.sca.hem4.writer.excel.summary.AcuteImpacts import *

pd.set_option('display.max_columns', 500)

#------------------------------------------
# Creates html files with maps displaying acute impact data. CSV files are also created
# should someone want to create their own visualization with the raw data.
#------------------------------------------
class AcuteImpactsVisualizer():

    def __init__(self, sourceDir):
        self.sourceDir = sourceDir
        self.basepath = os.path.basename(os.path.normpath(sourceDir))
        files = os.listdir(sourceDir)
        rootpath = sourceDir+'/'
        self.facilityIds = [ item for item in files if os.path.isdir(os.path.join(rootpath, item))
                                       and 'inputs' not in item.lower() and 'acute maps' not in item.lower() ]
    def visualize(self):

        # Suppress various bokeh warnings
        warnings.filterwarnings("ignore")

        flag_list = []
        acuteImpacts = AcuteImpacts(targetDir=self.sourceDir, facilityIds=self.facilityIds, parameters=[self.basepath])

        flag_df = acuteImpacts.createDataframe()

        for index, row in flag_df.iterrows():
            if row[hq_rel] >= 1.5:
                flag_list.append((row[fac_id],row.pollutant, "REL"))
            if row[hq_aegl1] >= 1.5:
                flag_list.append((row[fac_id],row.pollutant, "AEGL-1 1-hr"))
            if row[hq_erpg1] >= 1.5:
                flag_list.append((row[fac_id],row.pollutant, "ERPG-1"))
            if row[hq_aegl2] >= 1.5:
                flag_list.append((row[fac_id],row.pollutant, "AEGL-2 1-hr"))
            if row[hq_erpg2] >= 1.5:
                flag_list.append((row[fac_id],row.pollutant, "ERPG-2"))

        flag_list.sort()

        # If the flag file has no cases of interest, don't do anything.
        # Otherwise, create a directory for the created acute files.
        if len(flag_list)==0:
            Logger.logMessage("Acute impacts visualization - " +
                              "No acute impact was greater than or equal to 1.5. No HTML files were generated.")
            return
        else:
            if os.path.isdir(self.sourceDir + '/Acute Maps') == 0:
                os.mkdir(self.sourceDir + '/Acute Maps')

        # Find the HEM dose-response library and create df of it
        # Under the HEM4 dir names, "Reference" would be "Resources"
        RefFile = 'resources/Dose_Response_Library.xlsx'
        RefDF = pd.read_excel(RefFile)
        RefDF['Pollutant'] = RefDF['Pollutant'].str.lower()
        RefDF.set_index('Pollutant',inplace=True)
        RefDict = {'REL': 'REL\n(mg/m3)',\
                   'AEGL-1 1-hr':'AEGL-1  (1-hr)\n(mg/m3)',\
                   'ERPG-1':'ERPG-1\n(mg/m3)',\
                   'AEGL-2 1-hr':'AEGL-2  (1-hr)\n(mg/m3)',\
                   'ERPG-2':'ERPG-2\n(mg/m3)',\
                   'AEGL-1 8-hr':'AEGL-1  (8-hr)\n(mg/m3)',\
                   'AEGL-2 8-hr':'AEGL-2  (8-hr)\n(mg/m3)'}

        tablist=[]
        for acuteset in (flag_list):

            Fac = acuteset[0]
            HAP = acuteset[1]
            refType = acuteset[2]
            
            path = self.sourceDir + '/' + Fac + '/'

            HAP = HAP.lower()

            # Find the all polar file for a given facility, create df of it
            allpolar = AllPolarReceptors(targetDir=path, facilityId=Fac, acuteyn='Y')
            allpolar_df = allpolar.createDataframe()
            allpolar_df[pollutant] = allpolar_df[pollutant].str.lower()

            # Find the reference value
            ref_typ_col = RefDict[refType]
            refVal = RefDF.loc[HAP, ref_typ_col]
            
            # Aggregate the all polar file to get HQ for each receptor
            allpolar_df.set_index('pollutant',inplace=True)
            HAP_df = allpolar_df.loc[HAP, : ]
            f = {distance: 'first', angle: 'first', aconc: 'sum'}
            df = HAP_df.groupby([lat, lon], as_index=False).agg(f)
            df['HQ'] = df[aconc]/refVal/1000
            ac_File = '%s%s%s%s%s' %(self.sourceDir, '/Acute Maps/', Fac+'_', HAP+'_', refType+'.csv')
            df.to_csv(path_or_buf = ac_File, mode = 'w+')
              
              #Convert df to geo df
            df['Coordinates'] = list(zip(df.lon, df.lat))
            df['Coordinates'] = df['Coordinates'].apply(Point)
            gdf = gp.GeoDataFrame(df, geometry='Coordinates', crs = {'init' :'epsg:4326'})
            gdf = gdf.to_crs(epsg=3857)
            
            ESRI_tile = WMTSTileSource(url='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{Z}/{Y}/{X}.jpg')
            
            gdf['x'] = gdf.centroid.map(lambda p: p.x)
            gdf['y'] = gdf.centroid.map(lambda p: p.y)
            avg_x = gdf['x'].mean()
            avg_y = gdf['y'].mean()
            gdf = gdf.drop('Coordinates', axis=1)
            gdf['HQ'] = gdf['HQ'].map(lambda x: '%.1g' % x)
            gdf['lat'] = gdf['lat'].map(lambda x: '%.6f' % x)
            gdf['lon'] = gdf['lon'].map(lambda x: '%.6f' % x)
            gdf['angle'] = gdf['angle'].map(lambda x: '%.1f' % x)
                
            source = ColumnDataSource(gdf)
            
            tooltips = [
                        ("Latitude", "@lat"),
                        ("Longitude", "@lon"),
                        ("Acute HQ", "@HQ"),
                        ("Distance (m)", "@distance"),
                        ("Angle (deg)", "@angle")
                    ]
            
            title = '%s %s Acute HQ (%s)' %(Fac, HAP.title(), refType)
            tools = [ZoomInTool(), ZoomOutTool(), PanTool(),\
                       WheelZoomTool(), ResetTool(), HoverTool(tooltips=tooltips)]
            
            p = figure(plot_width=800, plot_height=600, tools = tools,\
                       x_range=(avg_x-3000, avg_x+3000), y_range=(avg_y-3000, avg_y+3000),\
                       title=title)

            p.toolbar.active_scroll = p.select_one(WheelZoomTool)
            p.add_tile(ESRI_tile)
            p.add_tile(STAMEN_TONER_LABELS)

            p.circle('x', 'y', color = 'yellow', size = 7, source=source)
            p.xaxis.visible = False
            p.yaxis.visible = False
            p.xgrid.visible = False
            p.ygrid.visible = False
            p.background_fill_color = None
            p.border_fill_color = None

#            labels = LabelSet(x='x', y='y', text='HQ', source = source,\
#                              level='glyph', x_offset=0, y_offset=0, text_font_size='8pt',\
#                              text_color='black', background_fill_color='yellow',\
#                              text_font_style='bold', text_align='center', text_baseline='middle')
            
            labels = LabelSet(x='x', y='y', text='HQ', source = source,\
                              x_offset=0, y_offset=0, text_font_size='8pt',\
                              text_color='black', background_fill_color='yellow',\
                              text_font_style='bold', text_align='center', text_baseline='middle')
            p.add_layout(labels)
            curdoc().add_root(p)
            
            mapName = '%s%s%s%s%s' %(self.sourceDir, '/Acute Maps/', Fac+'_', HAP+'_', refType+'.html')
            save(p, filename = mapName)
            tab = Panel(child=p, title=HAP.title() + " (" + refType +")")
            tablist.append(tab)

        tabs = Tabs(tabs=tablist)
        curdoc().add_root(tabs)

        mapName2 = '%s%s%s' %(self.sourceDir, '/Acute Maps/', "All Acute Maps.html")
        save(tabs, filename = mapName2, title="All Acute HQ Maps")

        Logger.logMessage("Acute impacts visualization - HTML files successfully created.")
