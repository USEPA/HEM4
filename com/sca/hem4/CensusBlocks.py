import math

from com.sca.hem4.support.UTM import *
from com.sca.hem4.model.Model import *
from com.sca.hem4.log.Logger import Logger

rec_id = 'rec_id';
fips = 'fips';
idmarplot = 'idmarplot';
population = 'population';
moved = 'moved';
urban_pop = 'urban_pop';
rec_type = 'rec_type';
distance = 'distance';
angle = 'angle';



#%% compute a bearing from the center of a facility to a census receptor
def bearing(utme, utmn, cenx, ceny):
    if utmn > ceny:
        if utme > cenx:
            angle = math.degrees(math.atan((utme-cenx)/(utmn-ceny)))
        else:
            angle = 360 + math.degrees(math.atan((utme-cenx)/(utmn-ceny)))
    elif utmn < ceny:
        angle = 180 + math.degrees(math.atan((utme-cenx)/(utmn-ceny)))
    else:
        if utme >= cenx:
            angle = 90
        else:
            angle = 270
         
    return angle        


#%%

def polygonbox(vertex1, vertex2, blkcoor, modeldist):
    nearpoly = False
    
    v1_to_blk = ((vertex1[0]-blkcoor[0])**2) + ((vertex1[1]-blkcoor[1])**2)
    v2_to_blk = ((vertex2[0]-blkcoor[0])**2) + ((vertex2[1]-blkcoor[1])**2)
    v1_to_v2 = ((vertex1[0]-vertex2[0])**2) + ((vertex1[1]-vertex2[1])**2)
    if v2_to_blk <= modeldist**2:
        nearpoly = True
    elif v1_to_blk + v1_to_v2 > v2_to_blk and v2_to_blk + v1_to_v2 > v1_to_blk:
        d = np.linalg.norm(np.cross(vertex2-vertex1,vertex1-blkcoor))/np.linalg.norm(vertex2-vertex1)
        if d <= modeldist:
            nearpoly = True
    return nearpoly


#%%

def rotatedbox(xt, yt, box_x, box_y, len_x, len_y, angle, fringe, overlap_dist):

    # Determines whether a rececptor point (xt,yt) is within a fringe around a box
    #	with southwest corner (box_x,box_y), dimensions (Len_x,Len_y), and oriented at a given
    #   "Angle", measured clockwise from North.
    # Also determine if this box overlaps the point.
    
    inbox = False
    overlap = "N"
        
    A_rad = math.radians(angle)
    D_e = (yt-box_y)*math.cos(A_rad) + (xt-box_x)*math.sin(A_rad) - len_y
    D_w = (box_y-yt)*math.cos(A_rad) + (box_x-xt)*math.sin(A_rad)
    D_n = (xt-box_x)*math.cos(A_rad) + (yt-box_y)*math.sin(A_rad) - len_x
    D_s = (box_x-xt)*math.cos(A_rad) + (box_y-yt)*math.sin(A_rad)
    D_sw = math.sqrt((xt-box_x)**2 + (yt-box_y)**2)
    D_se = (math.sqrt((box_x+len_x*math.cos(A_rad) - xt)**2 
                    + (box_y-len_x*math.sin(A_rad) - yt)**2))
    D_ne = (math.sqrt((box_x+len_x*math.cos(A_rad)+len_y*math.sin(A_rad) - xt)**2
				                    + (box_y+len_y*math.cos(A_rad)-len_x*math.sin(A_rad) - yt)**2))
    D_nw = (math.sqrt((box_x+len_y*math.sin(A_rad) - xt)**2
				                    + (box_y+len_y*math.cos(A_rad) - yt)**2))
    if D_e <= 0 and D_w <= 0:
        D_test = max(D_e, D_w, D_n, D_s)
    elif D_n <= 0 and D_s <= 0:
        D_test = max(D_e, D_w, D_n, D_s)
    else:
        D_test = min(D_ne, D_nw, D_se, D_sw)

           
    # First see if the point is in the rectangle
    if  (xt < box_x + math.tan(A_rad)*(yt-box_y) + (len_x+fringe)/math.cos(A_rad)
	                        and xt > box_x + math.tan(A_rad)*(yt-box_y) - fringe/math.cos(A_rad)
	                        and yt < box_y - math.tan(A_rad)*(xt-box_x) + (len_y+fringe)/math.cos(A_rad)
	                        and yt > box_y - math.tan(A_rad)*(xt-box_x) - fringe/math.cos(A_rad)):
         
         # Now check the corners
         if ((xt < box_x + math.tan(A_rad)*(yt-box_y)
			                   and yt < box_y - math.tan(A_rad)*(xt-box_x)
			                   and fringe < math.sqrt((box_x - xt)**2 + (box_y - yt)**2))
		                   or (xt > box_x + math.tan(A_rad)*(yt-box_y) + len_x/math.cos(A_rad)
			                   and yt < box_y - math.tan(A_rad)*(xt-box_x)
			                   and fringe < math.sqrt((box_x+len_x*math.cos(A_rad) - xt)**2
				                + (box_y-len_x*math.sin(A_rad) - yt)**2))
		                   or (xt > box_x + math.tan(A_rad)*(yt-box_y) + len_x/math.cos(A_rad)
			                   and yt > box_y - math.tan(A_rad)*(xt-box_x) + len_y/math.cos(A_rad)
			                   and fringe < math.sqrt((box_x+len_x*math.cos(A_rad)+len_y*math.sin(A_rad) - xt)**2
				                + (box_y+len_y*math.cos(A_rad)-len_x*math.sin(A_rad) - yt)**2))
		                   or (xt < box_x + math.tan(A_rad)*(yt-box_y)
			                   and yt > box_y - math.tan(A_rad)*(xt-box_x) + len_y/math.cos(A_rad)
			                   and fringe < math.sqrt((box_x+len_y*math.sin(A_rad) - xt)**2
				                + (box_y+len_y*math.cos(A_rad) - yt)**2))):
                   inbox = False
         else:
               inbox = True


    # Check for overlap
    if  (xt < box_x + math.tan(A_rad)*(yt-box_y) + (len_x+overlap_dist)/math.cos(A_rad)
	                        and xt > box_x + math.tan(A_rad)*(yt-box_y) - overlap_dist/math.cos(A_rad)
	                        and yt < box_y - math.tan(A_rad)*(xt-box_x) + (len_y+overlap_dist)/math.cos(A_rad)
	                        and yt > box_y - math.tan(A_rad)*(xt-box_x) - overlap_dist/math.cos(A_rad)):         
         if ((xt < box_x + math.tan(A_rad)*(yt-box_y)
			                   and yt < box_y - math.tan(A_rad)*(xt-box_x)
			                   and overlap_dist < math.sqrt((box_x - xt)**2 + (box_y - yt)**2))
		                   or (xt > box_x + math.tan(A_rad)*(yt-box_y) + len_x/math.cos(A_rad)
			                   and yt < box_y - math.tan(A_rad)*(xt-box_x)
			                   and overlap_dist < math.sqrt((box_x+len_x*math.cos(A_rad) - xt)*2
				                + (box_y-len_x*math.sin(A_rad) - yt)**2))
		                   or (xt > box_x + math.tan(A_rad)*(yt-box_y) + len_x/math.cos(A_rad)
			                   and yt > box_y - math.tan(A_rad)*(xt-box_x) + len_y/math.cos(A_rad)
			                   and overlap_dist < math.sqrt((box_x+len_x*math.cos(A_rad)+len_y*math.sin(A_rad) - xt)**2
				                + (box_y+len_y*math.cos(A_rad)-len_x*math.sin(A_rad) - yt)**2))
		                   or (xt < box_x + math.tan(A_rad)*(yt-box_y)
			                   and yt > box_y - math.tan(A_rad)*(xt-box_x) + len_y/math.cos(A_rad)
			                   and overlap_dist < math.sqrt((box_x+len_y*math.sin(A_rad) - xt)**2
				                + (box_y+len_y*math.cos(A_rad) - yt)**2))):
                   overlap = "N"
         else:
               overlap = "Y"
    
    return inbox, overlap

#%%    

def in_box(modelblks, sourcelocs, modeldist, maxdist, overlap_dist, model):

    ## This function determines if a block within modelblks is within a fringe of any source ##
    
    outerblks = modelblks.copy()
    # Initialize overlap
    outerblks['overlap'] = 'N'

    # Create empty inner blocks data frame
    colnames = list(modelblks.columns)
    innerblks = pd.DataFrame([], columns=colnames)
    
    #...... Find blocks within modeldist of point sources.........
        
    ptsources = sourcelocs.query("source_type in ('P','C','H','V','N','B')")
    for index, row in ptsources.iterrows():
        src_x = row[utme]
        src_y = row[utmn]
        indist = outerblks.query('sqrt((@src_x - utme)**2 + (@src_y - utmn)**2) <= @modeldist')

        # Determine overlap
        indist['overlap'] = np.where(np.sqrt(np.double((indist[utme]-src_x)**2 +
                                       (indist[utmn]-src_y)**2)) <= overlap_dist, "Y", "N")
      
        if len(indist) > 0:
            # Append to innerblks and shrink outerblks
            innerblks = innerblks.append(indist).reset_index(drop=True)
            innerblks = innerblks[~innerblks[rec_id].duplicated()]
            outerblks = outerblks[~outerblks[rec_id].isin(innerblks[rec_id])].copy()

#            #Do any of these inner or outer blocks overlap this source?
#            innerblks.loc[innerblks['overlap'] != 'Y', 'overlap'] = np.where(np.sqrt(np.double((innerblks[utme]-src_x)**2 +
#                                           (innerblks[utmn]-src_y)**2)) <= overlap_dist, "Y", "N")
#            if not outerblks.empty:
#                outerblks.loc[outerblks['overlap'] != 'Y', 'overlap'] = np.where(np.sqrt(np.double((outerblks[utme]-src_x)**2 +
#                                               (outerblks[utmn]-src_y)**2)) <= overlap_dist, "Y", "N")


    #....... Find blocks within modeldist of area sources ..........

    if not outerblks.empty:
                
        areasources = sourcelocs.query("source_type in ('A')")
        for index, row in areasources.iterrows():
            box_x = row[utme]
            box_y = row[utmn]
            len_x = row["lengthx"]
            len_y = row["lengthy"]
            angle_val = row["angle"]
            fringe = modeldist
            outerblks["inbox"], outerblks['overlap'] = zip(*outerblks.apply(lambda row1: rotatedbox(row1[utme],
                     row1[utmn], box_x, box_y, len_x, len_y, angle_val, fringe, overlap_dist), axis=1))
            indist = outerblks.query('inbox == True')
            if len(indist) > 0:
                # Append to innerblks and shrink outerblks
                innerblks = innerblks.append(indist).reset_index(drop=True)
                innerblks = innerblks[~innerblks[rec_id].duplicated()]
                outerblks = outerblks[~outerblks[rec_id].isin(innerblks[rec_id])]
            
            if outerblks.empty:
                # Break for loop if no more outer blocks
                break
                  

    #....... If there are polygon sources, find blocks within modeldist of any polygon side ..........
    
    if not outerblks.empty:
        polyvertices = sourcelocs.query("source_type in ('I')")
        if len(polyvertices) > 1:
                
            # If this polygon is a census tract (e.g. NATA application), then any outer receptor within tract will be
            # considered an inner receptor. Do not perform this check for the user receptor only application.
            if not model.altRec_optns["altrec"]:
                outerblks.loc[:, "tract"] = outerblks[idmarplot].str[1:11]
                polyvertices.loc[:, "tract"] = polyvertices[fac_id].str[0:10]
                intract = pd.merge(outerblks, polyvertices, how='inner', on='tract')
                if len(intract) > 0:
                    innerblks = innerblks.append(intract).reset_index(drop=True)
                    innerblks = innerblks[~innerblks[rec_id].duplicated()]
                    outerblks = outerblks[~outerblks[rec_id].isin(innerblks[rec_id])]
            
            # Are any blocks within the modeldist of any polygon side?
            # Process each source_id
            if not outerblks.empty:
                for grp,df in polyvertices.groupby(source_id):
                    # loop over each row of the source_id specific dataframe (df)
                    for i in range(0, df.shape[0]-1):
                        v1 = np.array([df.iloc[i][utme], df.iloc[i][utmn]])
                        v2 = np.array([df.iloc[i+1][utme], df.iloc[i+1][utmn]])
                        outerblks["nearpoly"] = (outerblks.apply(lambda row: polygonbox(v1, v2, 
                             np.array([row[utme],row[utmn]]), modeldist), axis=1))
                        polyblks = outerblks.query('nearpoly == True')
                        if len(polyblks) > 0:
                            innerblks = innerblks.append(polyblks).reset_index(drop=True)
                            innerblks = innerblks[~innerblks[rec_id].duplicated()]
                            outerblks = outerblks[~outerblks[rec_id].isin(innerblks[rec_id])]
                    if outerblks.empty:
                        # Break for loop if no more outer blocks
                        break

        
    return innerblks, outerblks

#%%
def read_json_file(path_to_file, dtype_dict):
    with open(path_to_file) as p:
        raw = pd.read_json(p, orient="columns", dtype=eval(dtype_dict))
        raw.columns = [x.lower() for x in raw.columns]
        return raw

    
#%%
def cntyinzone(lat_min, lon_min, lat_max, lon_max, cenlat, cenlon, maxdist_deg):
    inzone = False
    if ((cenlat - lat_max <= maxdist_deg and cenlat >= lat_min) or (lat_min - cenlat <= maxdist_deg and cenlat <= lat_max)) and ((cenlon - lon_max <= maxdist_deg/math.cos(math.radians(cenlat)) and cenlon >= lon_min) or (lon_min - cenlon <= maxdist_deg/math.cos(math.radians(cenlat)) and cenlon <= lon_max)):
            inzone = True
    return inzone

#%%
def getblocks(cenx, ceny, cenlon, cenlat, utmzone, hemi, maxdist, modeldist, sourcelocs, overlap_dist, model):
        
    # convert max outer ring distance from meters to degrees latitude
    maxdist_deg = maxdist*39.36/36/2000/60

    ##%% census key look-up

    #load census key into data frame
    dtype_dict = '{"ELEV_MAX":float,"FILE_NAME":object,"FIPS":object,"LAT_MAX":float,"LAT_MIN":float,"LON_MAX":float,"LON_MIN":float,"MIN_REC":int,"NO":int,"YEAR":int}'
    census_key = read_json_file("census/census_key.json", dtype_dict)
    census_key.columns = [x.lower() for x in census_key.columns]


    #create selection for "inzone" and find where true in census_key dataframe

    census_key["inzone"] = census_key.apply(lambda row: cntyinzone(row["lat_min"], row["lon_min"], row["lat_max"], row["lon_max"], cenlat, cenlon, maxdist_deg), axis=1)
    cntyinzone_df = census_key.query('inzone == True')
    
    censusfile2use = {}    
    
    # Find all blocks within the intersecting counties that intersect the modeling zone. Store them in modelblks.
    frames = []

    for index, row in cntyinzone_df.iterrows():
        
        state = "census/" + row['file_name'] + ".json"
        # Query state census file
        if state in censusfile2use:
            censusfile2use[state].append(str(row[fips]))
        else:
            censusfile2use[state] = [str(row[fips])]
       
    for state, FIPS in censusfile2use.items():
        locations = FIPS
        dtype_dict = '{"REC_NO":int, "FIPS":object, "IDMARPLOT":object, "POPULATION":int, "LAT":float, "LON":float, "ELEV":float, "HILL":float, "MOVED":object, "URBAN_POP":int}'
        state_pd = read_json_file(state, dtype_dict)
        state_pd.columns = [x.lower() for x in state_pd.columns]
        state_pd.rename(inplace=True, index=str, columns={'rec_no' : 'rec_id'})
        check = state_pd[state_pd[fips].isin(locations)]
        frames.append(check)

    # If no blocks within max distance, then this facility cannot be modeled; skip it.
    if len(frames) == 0:
        Logger.logMessage("There are no discrete receptors within the max distance of this facility. " +
                          "Aborting processing of this facility.")
        raise ValueError("No discrete receptors selected within max distance")
            

    #combine all frames df's
    censusblks = pd.concat(frames)

    #compute UTMs from lat/lon using the common zone       
    censusblks[[utmn, utme]] = censusblks.apply(lambda row: UTM.ll2utm_alt(row[lat],row[lon],utmzone,hemi), 
                                               result_type="expand", axis=1)

    #set utmz as the common zone
    censusblks[utmz] = utmzone
    
   
    #coerce hill and elevation into floats
    censusblks[hill] = pd.to_numeric(censusblks[hill], errors='coerce').fillna(0)
    censusblks[elev] = pd.to_numeric(censusblks[elev], errors='coerce').fillna(0)

    #compute distance and bearing (angle) from the center of the facility
    censusblks[distance] = np.sqrt((cenx - censusblks.utme)**2 + (ceny - censusblks.utmn)**2)
    censusblks[angle] = censusblks.apply(lambda row: bearing(row[utme],row[utmn],cenx,ceny), axis=1)

    #subset the censusblks dataframe to blocks that are within the modeling distance of the facility 
    modelblks = censusblks.query('distance <= @maxdist').copy()

    # Add overlap column and default to N
    modelblks[overlap] = 'N'
    
    # Split modelblks into inner and outer block receptors
    innerblks, outerblks = in_box(modelblks, sourcelocs, modeldist, maxdist, overlap_dist, model)
        
    # For inner blocks, convert utme, utmn, utmz, and population to appropriate numeric types
    innerblks[utme] = innerblks[utme].astype(np.float64)
    innerblks[utmn] = innerblks[utmn].astype(np.float64)
    innerblks[utmz] = innerblks[utmz].astype(int)
    innerblks[population] = pd.to_numeric(innerblks[population], errors='coerce').astype(int)
    innerblks[rec_type] = 'C'

    if not outerblks.empty:
        # For outer blocks, convert utme, utmn, utmz, and population to appropriate numeric types
        outerblks[utme] = outerblks[utme].astype(np.float64)
        outerblks[utmn] = outerblks[utmn].astype(np.float64)
        outerblks[utmz] = outerblks[utmz].astype(int)
        outerblks[population] = pd.to_numeric(outerblks[population], errors='coerce').astype(int)
        outerblks[rec_type] = 'C'
        
        
    return innerblks, outerblks







