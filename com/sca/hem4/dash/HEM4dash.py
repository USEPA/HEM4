# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 07:07:56 2020

@author: MMORRIS
"""
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import subprocess, webbrowser
from threading import Timer
import pandas as pd
import numpy as np
import plotly.express as px
import plotly
import os
from tkinter import messagebox

from flask import request
from concurrent.futures import ThreadPoolExecutor
from dash.dependencies import Input, Output
import time



class HEM4dash():
    
    def __init__(self, dirtouse):
        self.dir = dirtouse
        self.SCname = self.dir.split('/')[-1]

    def buildApp(self):
        
        if self.dir == "" or self.dir == None:
            messagebox.showinfo("Invalid directory", " Please select a directory containing the results of a model run and summary reports for cancer risk drivers, "+
                                "max risks, max TOSHI drivers, pollutant incidence drivers, source type incidence drivers, and cancer histograms.")
            return None
            
        else:
            
            
            
            ## Rather than using the external css, could use local "assets" dir that has the css file
            external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
            
            app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
            app.title = 'HEM 4 Results: ' + self.SCname
                    
            # Create dataframe of max risks
            fname = self.SCname + "_facility_max_risk_and_hi.xlsx"
            max_rsk_hi = os.path.join(self.dir, fname)
            cols2use = ('A,B,D,E,F,G,H,M,N,Q,R,W,X,AA,AB,AE,AF,AI,AJ,AM,AN,AQ,AR,AU,AV,AY,AZ,BC,BD,BG,BH,BK,BL,BO,BQ,BR,BS,BT,BU,BV')
            #cols2use = ('Facil_id', 'mx_can_rsk', 'can_rcpt_type',
            #       'can_latitude', 'can_longitude', 'can_blk', 'respiratory_hi',
            #       'resp_blk', 'liver_hi', 'liver_blk', 'neurological_hi', 'neuro_blk',
            #       'developmental_hi', 'devel_blk', 'reproductive_hi', 'repro_blk',
            #       'kidney_hi', 'kidney_blk', 'ocular_hi', 'ocular_blk', 'endocrine_hi',
            #       'endo_blk', 'hematological_hi', 'hema_blk', 'immunological_hi',
            #       'immun_blk', 'skeletal_hi', 'skel_blk', 'spleen_hi', 'spleen_blk',
            #       'thyroid_hi', 'thyroid_blk', 'whole_body_hi', 'whole_blk', 'incidence',
            #       'metname', 'km_to_metstation', 'fac_center_latitude', 'fac_center_longitude', 'rural_urban')
            dataTypes1 = {'Facil_id':str, 'mx_can_rsk':float, 'can_rcpt_type':str,
                   'can_latitude':float, 'can_longitude':float, 'can_blk':str, 'respiratory_hi':float,
                   'resp_blk':str, 'liver_hi':float, 'liver_blk':str, 'neurological_hi':float, 'neuro_blk':str,
                   'developmental_hi':float, 'devel_blk':str, 'reproductive_hi':float, 'repro_blk':str,
                   'kidney_hi':float, 'kidney_blk':str, 'ocular_hi':float, 'ocular_blk':str, 'endocrine_hi':float,
                   'endo_blk':str, 'hematological_hi':float, 'hema_blk':str, 'immunological_hi':float,
                   'immun_blk':str, 'skeletal_hi':float, 'skel_blk':str, 'spleen_hi':float, 'spleen_blk':str,
                   'thyroid_hi':float, 'thyroid_blk':str, 'whole_body_hi':float, 'whole_blk':str, 'incidence':float,
                   'metname':str, 'km_to_metstation':int, 'fac_center_latitude':float, 'fac_center_longitude':float,
                   'rural_urban':str}
            df_max_can = pd.read_excel(max_rsk_hi, dtype=dataTypes1, usecols = cols2use)
            df_max_can['mx_can_rsk'] = df_max_can['mx_can_rsk'].apply(lambda x: x*1000000)
            df_max_can.columns = ['Facility', 'MIR (in a million)', 'MIR Receptor Type',
                   'MIR Lat', 'MIR Lon', 'MIR Block', 'Respiratory HI',
                   'Resp Block', 'Liver HI', 'Liver Block', 'Neurological HI', 'Neuro Block',
                   'Developmental HI', 'Devel Block', 'Reproductive HI', 'Repro Block',
                   'Kidney HI', 'Kidney Block', 'Ocular HI', 'Ocular Block', 'Endocrine HI',
                   'Endo Block', 'Hematological HI', 'Hema Block', 'Immunological HI',
                   'Immun Block', 'Skeletal HI', 'Skel Block', 'Spleen HI', 'Spleen Block',
                   'Thyroid HI', 'Thyroid Block', 'Whole body HI', 'Whole Body Block', 'Cancer Incidence',
                   'Met Station', 'Distance to Met Station (km)', 'Facility Center Lat', 'Facility Center Lon',
                   'Rural or Urban']
            avglat = df_max_can.loc[:,'Facility Center Lat'].mean()
            avglon = df_max_can.loc[:,'Facility Center Lon'].mean()
            numFacs = df_max_can.loc[:,'Facility'].count()
            MaxRisk = df_max_can.loc[:,'MIR (in a million)'].max()
            
            try:
            
                
                # Create dataframe of cancer risk drivers
                fname = self.SCname + "_cancer_drivers.xlsx"
                canc_driv_file = os.path.join(self.dir, fname)
                dataTypes2 = {'Facility ID':str, 'Source ID': str}
                df_canc_driv_temp = pd.read_excel(canc_driv_file, dtype=dataTypes2,
                                             usecols = ('A,B,C,D,F'))
                df_canc_driv = df_canc_driv_temp.loc[(df_canc_driv_temp['MIR']>=5E-7) & (df_canc_driv_temp['Cancer Risk'] >= .1 * df_canc_driv_temp['MIR'])]
                df_canc_driv['Source/Pollutant Risk_MILL'] = df_canc_driv['Cancer Risk']*1000000
                df_canc_driv.columns = ['Facility', 'Facility MIR', 'Pollutant', 'S/P Risk', 'Source ID', 'Source/Pollutant Risk']
                df_canc_driv['Pollutant'] = df_canc_driv['Pollutant'].str.title()
                df_canc_driv['Facility']= df_canc_driv['Facility'] = 'F' + df_canc_driv['Facility'].astype(str)
                df_canc_driv.sort_values(by = ['Facility MIR'],ascending = False, inplace = True)
                        
                # Create dataframe of max TOSHI drivers
                fname = self.SCname + "_hazard_index_drivers.xlsx"
                hi_driv_file = os.path.join(self.dir, fname)
                df_max_HI = pd.read_excel(hi_driv_file, dtype=dataTypes2)
                HI_types_formax = list(set(df_max_HI['HI Type']))
                df_max_HI = df_max_HI.loc[(df_max_HI['HI Total'] >= 0.2) & (df_max_HI['Hazard Index'] >= .1 * df_max_HI['HI Total'])]
                df_max_HI['Pollutant'] = df_max_HI['Pollutant'].str.title()
                df_max_HI['Facility ID']= df_max_HI['Facility ID'] = 'F' + df_max_HI['Facility ID'].astype(str)
                df_max_HI.sort_values(by = ['HI Total'], ascending = False, inplace = True)
                
                
                HI_types = list(set(df_max_HI['HI Type']))
                numTOs = len(HI_types)
                
                MaxHI = df_max_can[HI_types_formax].max(axis=1, skipna =True)
                MaxHIid = df_max_can[HI_types_formax].idxmax(axis=1)
                df_max_can.insert(6, "Max TOSHI", MaxHI)
                df_max_can.insert(7, "Max TOSHI Organ", MaxHIid)
                df_max_can.loc[df_max_can["Max TOSHI"] == 0, "Max TOSHI Organ"] = ''
                Overall_MaxHI = df_max_can.loc[:,"Max TOSHI"].max()
                
                #Creating a df just for the dashtable
                df_dashtable = df_max_can.copy()
                
                # Create dataframe of pollutant incidence drivers
                fname = self.SCname + "_incidence_drivers.xlsx"
                can_inc_drv = os.path.join(self.dir, fname)
                df_inc_drv_temp = pd.read_excel(can_inc_drv, dtype={'Pollutant': str, 'Incidence': float},
                                           usecols = ('A,B'))
                df_inc_drv = df_inc_drv_temp.dropna()
                TotalInc = df_inc_drv_temp[df_inc_drv_temp.Pollutant=='Total incidence'].Incidence.item()
                df_inc_drv.drop(df_inc_drv.index[df_inc_drv['Pollutant'] == 'Total incidence'], inplace = True)
                df_inc_drv.drop(df_inc_drv.index[df_inc_drv['Incidence'] < TotalInc*0.01], inplace = True)
                df_inc_drv['Pollutant'] = df_inc_drv['Pollutant'].str.title()
                df_inc_drv.columns = ['Pollutant', 'Cancer Incidence']
                
                # Create dataframe of source type incidence drivers
                fname = self.SCname + "_source_type_risk.xlsx"
                can_inc_src_drvFile = os.path.join(self.dir, fname)
                df_inc_src_drv = pd.read_excel(can_inc_src_drvFile, skiprows=1)
                df_inc_src_drv.drop(columns = 'Maximum Overall', inplace=True)
                Inc_row = df_inc_src_drv.loc[df_inc_src_drv['Unnamed: 0']=='Incidence']
                Inc_row.drop(columns = 'Unnamed: 0', inplace = True)
                Inc_row_melt = pd.melt(Inc_row, var_name = 'Source Type', value_name = 'Incidence',
                                        value_vars = Inc_row.columns)
                Inc_row_melt.drop(Inc_row_melt.index[Inc_row_melt['Incidence'] == 0], inplace = True)
                Inc_row_melt.columns = ['Source Type', 'Cancer Incidence']
                 
                # Create dataframe of cancer histogram
                fname = self.SCname + "_histogram_risk.xlsx"
                can_histo_file = os.path.join(self.dir, fname)
                df_can_histo = pd.read_excel(can_histo_file, dtype={'Risk level': str, 'Population': float},
                                           usecols = ('A,B'))
                df_can_histo.columns = ['Risk Level', 'Population']
                df_can_histo = df_can_histo.fillna(0)
                
                # Create dataframe of cancer histogram
                fname = self.SCname + "_hi_histogram.xlsx"
                HI_hist_file = os.path.join(self.dir, fname)
                df_hi_histo = pd.read_excel(HI_hist_file)
                cols = [c for c in df_hi_histo if 'facilities' in c.lower()]
                df_hi_histo.drop(columns=cols, inplace=True)
                df_hi_histo.columns = df_hi_histo.columns.str.replace(' Pop','')
                df_hi_histo_melt= pd.melt(df_hi_histo, id_vars=['HI Level'], var_name = 'Target Organ', value_name = 'Population')
                
                
                #Determine whether to use log or linear scales in graphics
                if (MaxRisk >= 10 * df_max_can.loc[:,'MIR (in a million)'].median()):
                    riskScale = 'log'
                else:
                    riskScale = 'linear'
                    
                if (Overall_MaxHI >= 10 * df_max_can.loc[:,'Max TOSHI'].median()):
                    HIScale = 'log'
                else:
                    HIScale = 'linear'
                    
                        
                #Determine whether to display cancer risk or maximum TOSHI in the map
                if ((Overall_MaxHI <= 1.49) or ((MaxRisk >= 50) and (Overall_MaxHI <= 2))):
                    mapMetric = df_max_can['MIR (in a million)']
                    mapMetr_name = 'MIR (in a million)'
                else:
                    mapMetric = df_max_can['Max TOSHI']
                    mapMetr_name = 'Max TOSHI'
                
                #Format the max risk file so the map popups don't have so many digits
                cols2format_E=['MIR (in a million)', 'Respiratory HI', 'Liver HI', 'Neurological HI',
                      'Developmental HI','Reproductive HI','Kidney HI', 'Ocular HI',
                      'Endocrine HI', 'Hematological HI', 'Immunological HI', 'Skeletal HI',
                      'Spleen HI', 'Thyroid HI',  'Whole body HI', 'Max TOSHI', 'Cancer Incidence']
        
                cols2format_f=['MIR Lat', 'MIR Lon','Facility Center Lat', 'Facility Center Lon']
        
                for column in cols2format_E:
                    df_max_can[column] = df_max_can[column].map(lambda x: '{:.2E}'.format(x))
        
                for column in cols2format_f:
                    df_max_can[column] = df_max_can[column].map(lambda x: '{:.6f}'.format(x)) 
                        
                # Create a map from df_max_can
                popup = ["Facility: {} <br>MIR (in a million): {} <br>MIR Block: {} <br>Max TOSHI: {} <br>Max TOSHI Organ: {}".format(i,j,k,l,m)\
                         for i,j,k,l,m in zip(df_max_can['Facility'], df_max_can['MIR (in a million)'], df_max_can['MIR Block'],\
                                              df_max_can['Max TOSHI'], df_max_can['Max TOSHI Organ'])]
                
                riskmapTitle = 'Facility Map' + ' for ' + self.SCname
                riskMap = go.Figure({"data": [{
                                "type": "scattermapbox",
                                "lat": df_max_can['Facility Center Lat'],
                                "lon": df_max_can['Facility Center Lon'],
                                "hoverinfo": "text",
                                "text": popup,
                                "mode": "markers",
                                "marker": {
                                    "size": 8,
                                    "opacity": 1,
                                    "color": mapMetric,
                                    "colorbar.title" : mapMetr_name,
                                    "cmin": mapMetric.min(),
                                    "cmax": mapMetric.max(),
                                    "showscale": True
                                    }
                                }],
                                
                            "layout": {'autosize':True,
                            'height':500,
                            'hovermode':"closest",
                            'mapbox':
                                {'style':"carto-positron",
                                 'center':{
                                    'lon': avglon,
                                    'lat': avglat
                                    },
                                'zoom':3.5,
                                'accesstoken':${{ secrets.ACCESS_TOKEN }}
                                },
                                
                            }
                            })
                
                Map_updatemenus=list([
                    
                    dict(
                        buttons=list([
                            
                            dict(
                                args=[{'mapbox.style': 'carto-positron'}],
                                label='Light',
                                method='relayout'
                            ),
                            
                            dict(
                                args=[{'mapbox.style': 'open-street-map'}],
                                label='Street Map',
                                method='relayout'
                            ),                    
                            
                            dict(
                                args=[{'mapbox.style': 'carto-darkmatter'}],
                                label='Dark',
                                method='relayout'
                            ),
                            
                            dict(
                                args=[{'mapbox.style': 'satellite-streets'}],
                                label='Satellite',
                                method='relayout'
                            )
                        ]),
                        # direction where I want the menu to expand when I click on it
                        direction = 'up',
                        active = 0,
                        showactive = True,
                      
                        # here I specify where I want to place this drop-down on the map
                        x = 0.05,
                        xanchor = 'left',
                        y = .05,
                        yanchor = 'bottom',
                 
                    ),
                      
                    ])
                
                riskMap.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
                riskMap.update_layout(title = riskmapTitle, updatemenus = Map_updatemenus,
                                      coloraxis_colorbar=dict(title=mapMetr_name))
                
                # Create a bar chart of risk drivers
                
                if df_canc_driv.empty:
                    pass
                else:
                    riskDriv = px.bar(df_canc_driv, x = 'Facility', y = 'Source/Pollutant Risk',
                                      color = 'Pollutant', barmode = 'relative', hover_data=('Source ID', 'Pollutant'),
                                      text = 'Source ID', range_x = [0,15], height = 600)
                    riskDriv.layout.yaxis = {'title': 'Source/Pollutant Risk (in a million)', 'type': riskScale}
                    riskDriv.update_layout(title = 'Source and Pollutant Risk Drivers of Max Risk' + ' for ' + self.SCname +
                                           '<br>(facility risk ≥ 0.5 in a million)',
                                           yaxis={'type': riskScale},
                                           xaxis={'type':'category', 'categoryorder': 'array',
                                                  'categoryarray': df_canc_driv['Facility']}
                                           )
                        
                # Create a bar chart of HI drivers
                
                if df_max_HI.empty:
                    pass
                else:
                            
                    HIDriv = px.bar(df_max_HI, x="Facility ID", y="Hazard Index", color="Pollutant", barmode="relative",
                                    facet_row ="HI Type", text = 'Source ID', height=300*numTOs,
                                    range_x = [0, 15], opacity=1)
                    HIDriv.update_layout(title = 'Source and Pollutant Drivers of Max HI' + ' for ' + self.SCname +
                                         '<br>(facility HI ≥ 0.2)',
                                         yaxis={'type': HIScale},
                                         xaxis={'type':'category', 'categoryorder': 'array', 'categoryarray': df_max_HI['Facility ID']})
                    # HIDriv.update_xaxes(matches=None, showticklabels = True)
                    HIDriv.update_yaxes(matches=None)
                
                
                try:
                    # Create dataframe of acute HQs
                    fname = self.SCname + "_acute_impacts.xlsx"
                    acute_file = os.path.join(self.dir, fname)
                    df_acute = pd.read_excel(acute_file, dtype=dataTypes2,
                                             usecols = ('A,B,J,K,L,N,O'))
                    df_acute.columns = ['Facility', 'Pollutant', 'REL', 'AEGL-1', 'ERPG-1', 'AEGL-2', 'ERPG-2']
                    df_acute['Pollutant'] = df_acute['Pollutant'].str.title()
                    df_acute_melt = pd.melt(df_acute, id_vars = ['Facility', 'Pollutant'],var_name = 'Reference Value', value_name = 'HQ',
                                            value_vars = ['REL', 'AEGL-1', 'ERPG-1', 'AEGL-2', 'ERPG-2'])
                    indexNames = df_acute_melt[ df_acute_melt['HQ'] == 0 ].index
                    df_acute_melt.drop(indexNames , inplace=True)
                    df_acute_melt['Facility']= df_acute_melt['Facility'] = 'F' + df_acute_melt['Facility'].astype(str)
                    df_acute_melt= df_acute_melt.loc[df_acute_melt['HQ']>=0.5]
                    df_acute_melt.sort_values(by = ['HQ'],ascending = False, inplace = True)
                    
                    Acute_types = set(df_acute_melt['Reference Value'])
                    Acute_pols = set(df_acute_melt['Pollutant'])
                    numAcutRefs = len(Acute_types)
                    numAcutPols = len(Acute_pols)
                    
                    if (df_acute_melt.loc[:,'HQ'].max() >= 10 * df_acute_melt.loc[:,'HQ'].median()):
                        acuteScale = 'log'
                    else:
                        acuteScale = 'linear' 
                    
                    # Create bar charts of acute HQs
                    acuteBar = px.bar(df_acute_melt, x="Pollutant", y="HQ", color='Reference Value', barmode= 'overlay',
                                    opacity = .7, range_x = [0, 5], height = 700, hover_name = "Facility",
                                    text = 'Facility')
                    acuteBar.update_layout(title = 'Acute Screening Hazard Quotients' + ' for ' + self.SCname +
                                           '<br>(for pollutants with HQ ≥ 0.5)',
                                           yaxis={'type': acuteScale},
                                           xaxis={'type':'category', 'categoryorder': 'array', 'categoryarray': df_acute_melt['Pollutant']},
                                           )
                except:
                    pass
                
                #Create a pie chart of cancer incidence by pollutant
                
                if df_inc_drv.empty:
                    pass
                else:
                    inc_Pie = px.pie(df_inc_drv, names = 'Pollutant', values = 'Cancer Incidence',
                                     title = 'Cancer Incidence by Pollutant' + ' for ' + self.SCname +
                                     '<br>(for pollutants that contribute at least 1%)'
                                     )
                    inc_Pie.update_layout(title={'x' : 0.75, 'xref' : 'container', 'xanchor': 'right'})
                
                #Create a pie chart of cancer incidence by source type
                
                if Inc_row_melt.empty:
                    pass
                else:
                    src_inc_Pie = px.pie(Inc_row_melt, names = 'Source Type', values = 'Cancer Incidence',
                                     labels = {'Total Incidence': TotalInc},
                                     title = 'Cancer Incidence by Source Type' + ' for ' + self.SCname
                                     )
                    src_inc_Pie.update_layout(title={'x' : 0.75, 'xref' : 'container', 'xanchor': 'right'})
                
                # Create a cancer histogram
                
                if df_can_histo.empty:
                    pass
                else:
                    can_histo = px.bar(df_can_histo, x = 'Risk Level', y = 'Population',
                                       
                                       log_y = 'True', text= '{:}'.format('Population'),
                                       title = 'Cancer Population Risks' + ' for ' + self.SCname)
                
                # Create a noncancer histogram
                
                if df_hi_histo_melt.empty:
                    pass
                else:
                    hi_histo = px.bar(df_hi_histo_melt, x="HI Level", y="Population", color="Target Organ", barmode="group",
                                    log_y=True, text= "{}".format("Population"))
                    hi_histo.update_layout(title = 'NonCancer Population Risks' + ' for ' + self.SCname)
                    hi_histo.update_xaxes(autorange="reversed")
                
                
                # Create layout of the app
                # The config code modifies the dcc graph objects
                
                map_config = {'modeBarButtonsToRemove': ['toggleSpikelines','hoverCompareCartesian', 'lassoSelect'],
                                'doubleClickDelay': 1000,
                                'toImageButtonOptions': {
                                    'format': 'png', # one of png, svg, jpeg, webp
                                    'filename': 'HEM4 Results ' + self.SCname + ' Map',
                                    'height': 700,
                                    'width': 1500,
                                    'scale': 1 # Multiply title/legend/axis/canvas sizes by this factor
                                    }
                                }
                
                
                chart_config = {'modeBarButtonsToRemove': ['toggleSpikelines','hoverCompareCartesian', 'lassoSelect', 'boxSelect'],
                                'doubleClickDelay': 1000,
                                'toImageButtonOptions': {
                                    'format': 'png', # one of png, svg, jpeg, webp
                                    'filename': 'HEM4 Results ' + self.SCname,
                                    'height': 700,
                                    'width': 1100,
                                    'scale': 1 # Multiply title/legend/axis/canvas sizes by this factor
                                    }
                                }
                
                HIDriv_config = {'modeBarButtonsToRemove': ['toggleSpikelines','hoverCompareCartesian', 'lassoSelect'],
                                'doubleClickDelay': 1000,
                                'toImageButtonOptions': {
                                    'format': 'png', # one of png, svg, jpeg, webp
                                    'filename': 'HEM4 Results ' + self.SCname,
                                    'height': 1200,
                                    'width': 1100,
                                    'scale': 1 # Multiply title/legend/axis/canvas sizes by this factor
                                    }
                                }
                
                #Here, if the dataframe for the graph is empty, don't make a graph
                if df_max_can.empty:
                    map_dcc = ''
                else:
                    map_dcc = dcc.Graph(figure = riskMap, config = map_config) 
                    
                if df_canc_driv.empty:
                    riskdriv_dcc = ''
                else:
                    riskdriv_dcc = dcc.Graph(figure = riskDriv, config = chart_config)
                    
                if df_max_HI.empty:
                    HIDriv_dcc = ''
                else:
                    HIDriv_dcc = dcc.Graph(figure = HIDriv, config = HIDriv_config)
                    
                try:
                    acute_dcc = dcc.Graph(figure = acuteBar, config = chart_config)
                except:
                    acute_dcc = ''    
                    
                if df_inc_drv.empty:
                    pollinc_dcc = ''
                else:
                    pollinc_dcc = dcc.Graph(figure = inc_Pie, config = chart_config)    
                    
                if df_inc_src_drv.empty:
                    srcinc_dcc = ''
                else:
                    srcinc_dcc = dcc.Graph(figure = src_inc_Pie, config = chart_config)    
                    
                if df_can_histo.empty:
                    canhisto_dcc = ''
                else:
                    canhisto_dcc = dcc.Graph(figure = can_histo, config = chart_config)    
                    
                if df_hi_histo.empty:
                    hihisto_dcc = ''
                else:
                    hihisto_dcc = dcc.Graph(figure = hi_histo, config = chart_config) 
                
                
                #Reformatting columns because dashtable does not sort scientific notation
                # for column in cols2format_E  +  cols2format_f:
                #     df_dashtable[column] = df_dashtable[column].map(lambda x: '{:.6f}'.format(x))
                
                
                app.layout = html.Div([

#                    dcc.Interval(id='interval1', interval=5 * 1000, n_intervals=0),
#                    html.H1(id='label1', children=''),

                    dcc.Input(id="input1", type="hidden", value="shutdown"),
                    dcc.Input(id="input2", type="hidden"),
                    
                    html.Div([
                                html.H1("HEM4 Results for " + self.SCname + " Model Run", style={'text-align':'center', 'font-weight': 'bold'}),
                                html.Hr(),
                                html.H4("Facility Map ({} Facilities)".format(numFacs), style={'font-weight': 'bold'}),
                                html.Hr()
                            ]),
                    
                    html.Div([
                        html.Div([
                            map_dcc,            
                        ]),   
                        
                        html.Div([
                            html.Hr(),
                            html.H4("Cancer Incidence by Pollutant and Source Type", style={'font-weight': 'bold'}),
                            html.H5("(Total Incidence is {0:.2E})".format(TotalInc)),
                            html.Hr()
                        ])
                    ]),
                    
                    
                    html.Div([
                        html.Div([
                            pollinc_dcc,            
                        ], style={'width': '48%', 'display': 'inline-block'}),
                        
                        html.Div([
                            srcinc_dcc,                       
                        ],style={'width': '48%', 'display': 'inline-block'}),
                        
                        html.Div([
                            html.Hr(),
                            html.H4("Population Risks", style={'font-weight': 'bold'}),
                            html.Hr()
                        ])
                    ]),
                        
                        
                    html.Div([
                        canhisto_dcc,
                        html.Hr()
                    ]),
                    
                    html.Div([
                        hihisto_dcc,
                        html.Hr(),
                        html.H4("Pollutant and Source Risk and HI Drivers (of Max Risk and HI)", style={'font-weight': 'bold'}),
                        html.Hr()
                    ]),
                    html.Div([
                        riskdriv_dcc
                        
                    ], style={'width': '100%'}),
                   
                        
                    html.Div([
                        html.Hr(),
                        HIDriv_dcc,
                        html.Hr(),
                        html.H4("Acute Screening Estimates", style={'font-weight': 'bold'}),
                        html.Hr()
                    ]),
                        
                     html.Div([
                        acute_dcc,
                        html.Hr(),
                        html.H4("Maximum Risk and HI Data", style={'font-weight': 'bold'}),
                        html.Hr()
                    ]),
                    
                    html.Div([
                            dash_table.DataTable(
                                id='table',
                                columns=[{"name": i, "id": i} for i in df_dashtable.columns],
                                data=df_dashtable.to_dict('records'),
                                style_as_list_view=True,
                                style_table={'width': '100%', 'min-width': '100%', 'height': 500,}, 
                                             # 'overflowX': 'scroll', 'overflowY': 'scroll'},
                                style_data_conditional=[
                                    {'if': {'row_index': 'odd'},
                                     'backgroundColor': 'Moccasin'
                                     }],
                                style_header={'color':'#191A1A', 'size':'14', 
                                                  'backgroundColor':'LightCyan',
                                                  'fontWeight':'bold'
                                                  },
                                style_cell={'text-align':'center', 'font-family':'arial', 'font=-size':'x-small',
                                            'minWidth': '150px'},
                                fixed_columns = { 'headers': True, 'data': 1 },
                                fixed_rows={ 'headers': True, 'data': 0 },
                                sort_action = 'native',
                                filter_action = 'native',
                                page_action= 'native',
                                sort_mode = 'multi',
                                export_columns = 'all',
                                export_format = 'xlsx',
                                export_headers = 'display',
                                include_headers_on_copy_paste = True
                                )
                    ])    
                    
                ])


                @app.callback(
                    Output(component_id='input2', component_property='children'),
                    [Input(component_id='input1', component_property='value')]
                )
                def check_status(value):
                    self.shutdown()
                    return 'Shutting down server'

                    
                return app
            
            except Exception as e:
                messagebox.showinfo("Input Error", e)
 


    def shutdown(self):
        func = request.environ.get('werkzeug.server.shutdown')
        if func is None:
            raise RuntimeError('Not running with the Werkzeug Server')
        time.sleep(20)
        func()
 
 
