from collections import defaultdict

fac_id = 'fac_id';
source_id = 'source_id';
pollutant = 'pollutant';
group = 'group';
lon = 'lon';
lat = 'lat';
elev = 'elev';
hill = 'hill';
overlap = 'overlap';
conc = 'conc'
result = 'result';
ddp = 'ddp';
wdp = 'wdp';
flag = 'flag';
avg_time = 'avg_time';
num_yrs = 'num_yrs';
net_id = 'net_id';
angle = 'angle';
concdate = 'concdate';
rank = 'rank';

class Model():

    def __init__(self):
        """
        The model contains all information for all facilties used in HEM4 runs
        
        The following attributes house HEM4 inputs for ALL facilities: 
       
        faclist - facilities list options file 
        emisloc - emissions location file
        hapemis - hap emissions file
        multipoly - polyvertex file
        mulitbuoy - buoyant line file
        ureceptr - user provided receptors
        haplib - HAP library
        bldgdw - building downwash file
        partdep - particle size file
        landuse - land use file
        seasons - seasons file
        emisvar - emissions variation file
        facids - ALL facility ids
        depdeplt- deposition and depletion options for ALL facilties
        gasparams - gaseous dry depostion parameters
        group_name - name assigned to all facilities being modeled
        rootoutput - root output folder name
        gasdryfacs - facilities needing landuse and season data
        particlefacs - facilities needing particle size data
        
        extensions include:
            - .dataframe: contains csv converted to dataframe for input file
            - .msg: contains message passed to queue about input upload, or 
            internal input checks
            
        The following attributes are facility-specific:
            
        computedValues - facility specific values computed during the run
        model_optns - default dictionary for storing model options like phase, 
                      elevation, urban, user receptors, acute 
        save- contains the SaveState model for saving facility runs
        organs - 
        riskfacs_df - 
        all_polar_receptors_df - 
        all_inner_receptors_df - 
        all_outer_receptors_df - 
        risk_by_latlon - 
        max_indiv_risk_df - 
        facops - facility specific options from faclist
        """
        self.faclist = None
        self.emisloc = None
        self.hapemis = None
        self.multipoly = None
        self.multibuoy = None
        self.ureceptr = None
        self.haplib = None
        self.organs = None
        self.metlib = None
        self.bldgdw = None
        self.partdep = None
        self.landuse = None
        self.seasons = None
        self.emisvar = None
        self.facids = None
        self.depdeplt = None
        self.gasdryfacs = None
        self.particlefacs = None
        self.polargrid = None
        self.sourcelocs = None
        self.group_name = None
        self.rootoutput = None
        self.temporal = None
        self.tempvar = None
        self.seasonvar = None
        self.dependencies = []
        

        # Facility-specific values that are computed during the run - these are ephemeral
        # and get overwritten when the next facility runs.
        self.computedValues = {}
        self.gasparams = None
        self.model_optns = defaultdict() 
        self.save = None
        self.all_polar_receptors_df = None
        self.all_inner_receptors_df = None
        self.all_outer_receptors_df = None
        self.block_summary_chronic_df = None
        self.innerblks_df = None
        self.outerblks_df = None
        self.risk_by_latlon = None
        self.max_indiv_risk_df = None
        self.facops = None
        self.sourceExclusion = {}
        self.aermod = None
        self.acuteplot_df = None

        # Initialize model options
        self.initializeAltRecOptions()

    @property
    def fac_ids(self):
        """Read-only array of facility ids"""
        return self.facids

    def reset(self):
        self.faclist = None
        self.emisloc = None
        self.hapemis = None
        self.multipoly = None
        self.multibuoy = None
        self.ureceptr = None
        self.bldgdw = None
        self.partdep = None
        self.landuse = None
        self.seasons = None
        self.emisvar = None
        self.facids = None
        self.depdeplt = None
        self.gasdryfacs = None
        self.particlefacs = None
        self.polargrid = None
        self.riskfacs_df = None
        self.all_polar_receptors_df = None
        self.all_inner_receptors_df = None
        self.all_outer_receptors_df = None
        self.block_summary_chronic_df = None
        self.innerblks_df = None
        self.outerblks_df = None
        self.risk_by_latlon = None
        self.max_indiv_risk_df = None
        self.sourcelocs = None
        self.gasparams = None
        self.model_optns = defaultdict()
        self.save = None
        self.group_name = None
        self.rootoutput = None
        self.temporal = None
        self.tempvar = None
        self.seasonvar = None
        self.sourceExclusion = {}
        self.aermod = None
        self.acuteplot_df = None
        self.dependencies = []


        # Initialize model options
        self.initializeAltRecOptions()


    def initializeAltRecOptions(self):
        self.altRec_optns = defaultdict()
        self.altRec_optns['path'] = ''
        self.altRec_optns['altrec'] = False
        self.altRec_optns['altrec_nopop'] = False
        self.altRec_optns['altrec_flat'] = False