"""
Central configuration for the Magdalena Flood Risk Assessment project.
All parameters, constants, and GEE asset paths are defined here.

Adapted from the Antioquia framework for the Department of Magdalena, Colombia.
Key differences:
  - 30 municipalities (vs 125 in Antioquia)
  - 5 subregions (vs 9)
  - Mixed terrain: Sierra Nevada de Santa Marta + coastal/fluvial lowlands
  - Coastal/estuarine influence (Cienaga Grande, Caribbean coast)
  - Both ASCENDING and DESCENDING orbits recommended (Sierra Nevada shadows)
"""

import os
import ee
from dotenv import load_dotenv

load_dotenv()

# --- GEE Initialization ---
try:
    ee.Initialize(project=os.getenv('GEE_PROJECT_ID', 'ee-maestria-tesis'))
except Exception:
    ee.Authenticate()
    ee.Initialize(project=os.getenv('GEE_PROJECT_ID', 'ee-maestria-tesis'))

# ============================================================================
# STUDY AREA: Department of Magdalena, Colombia
# ============================================================================

# Administrative boundaries from FAO GAUL Level 1
ADMIN_DATASET = 'FAO/GAUL/2015/level1'
DEPARTMENT_NAME = 'Magdalena'
COUNTRY_NAME = 'Colombia'

# Municipal boundaries from FAO GAUL Level 2
MUNICIPAL_DATASET = 'FAO/GAUL/2015/level2'

# HydroSHEDS basin boundaries
HYDROBASINS_L5 = 'WWF/HydroSHEDS/v1/Basins/hybas_sa_lev05_v1c'
HYDROBASINS_L7 = 'WWF/HydroSHEDS/v1/Basins/hybas_sa_lev07_v1c'

# 5 Subregions of Magdalena with their municipalities
# Names in ASCII (no accents) for GEE/GAUL compatibility
SUBREGIONS = {
    'Santa Marta': [
        'Santa Marta',
    ],
    'Norte': [
        'Algarrobo', 'Aracataca', 'Cienaga', 'El Reten',
        'Fundacion', 'Puebloviejo', 'Zona Bananera',
    ],
    'Rio': [
        'Cerro de San Antonio', 'Concordia', 'El Pinon',
        'Pedraza', 'Pivijay', 'Remolino', 'Salamina',
        'Sitionuevo', 'Zapayan',
    ],
    'Centro': [
        'Ariguani', 'Chibolo', 'Nueva Granada',
        'Plato', 'Sabanas de San Angel', 'Tenerife',
    ],
    'Sur': [
        'El Banco', 'Guamal', 'Pijino del Carmen',
        'San Sebastian de Buenavista', 'San Zenon',
        'Santa Ana', 'Santa Barbara de Pinto',
    ],
}

# ============================================================================
# TEMPORAL CONFIGURATION
# ============================================================================

# Full analysis period: 2015-2025 (Sentinel-1 era)
ANALYSIS_START = '2015-01-01'
ANALYSIS_END = '2025-12-31'

# Sentinel-1 availability
S1_START = '2014-10-03'  # Sentinel-1A launch
S1B_FAILURE = '2021-12-23'  # Sentinel-1B failure
S1C_LAUNCH = '2024-04-25'  # Sentinel-1C launch

# Seasonal periods for Magdalena (bimodal precipitation, Caribbean region)
# Second wet season (SON) is typically the most intense and produces worst floods
SEASONS = {
    'DJF': {'months': [12, 1, 2], 'label': 'Dry season 1 (principal)'},
    'MAM': {'months': [3, 4, 5], 'label': 'Wet season 1 (first rains)'},
    'JJA': {'months': [6, 7, 8], 'label': 'Dry season 2 (veranillo)'},
    'SON': {'months': [9, 10, 11], 'label': 'Wet season 2 (peak floods)'},
}

# Annual analysis windows
ANNUAL_WINDOWS = {year: {
    'start': f'{year}-01-01',
    'end': f'{year}-12-31',
} for year in range(2015, 2026)}

# ============================================================================
# SATELLITE DATA SOURCES
# ============================================================================

# Sentinel-1 SAR GRD (Ground Range Detected)
S1_COLLECTION = 'COPERNICUS/S1_GRD'
S1_PARAMS = {
    'instrumentMode': 'IW',  # Interferometric Wide swath
    'resolution': 10,  # meters
    'polarization': ['VV', 'VH'],  # Dual-pol
    'orbitProperties_pass': 'DESCENDING',  # Primary orbit; use both for Sierra Nevada
    'resolution_meters': 10,
}

# Water detection thresholds for SAR (VV polarization)
# Magdalena adjustments: flat terrain favors larger filter windows
SAR_WATER_THRESHOLDS = {
    'otsu_adaptive': True,  # Use Otsu automatic thresholding
    'vv_default': -15.0,  # dB, fallback threshold
    'vv_range': (-20.0, -12.0),  # Valid range for water detection
    'vh_default': -22.0,  # dB, fallback for VH
    'min_water_area_ha': 1.0,  # Minimum mappable water body
    'speckle_filter_radius': 50,  # meters, focal median radius
}

# JRC Global Surface Water v1.4
JRC_GSW = 'JRC/GSW1_4/GlobalSurfaceWater'
JRC_GSW_MONTHLY = 'JRC/GSW1_4/MonthlyHistory'
JRC_GSW_YEARLY = 'JRC/GSW1_4/YearlyHistory'

# JRC GLOFAS Flood Hazard
GLOFAS_FLOOD_HAZARD = 'JRC/CEMS_GLOFAS/FloodHazard/v1'

# SRTM Digital Elevation Model (30m)
SRTM = 'USGS/SRTMGL1_003'

# MERIT Hydro (90m) - pre-computed hydrological layers
MERIT_HYDRO = 'MERIT/Hydro/v1_0_1'

# HAND (Height Above Nearest Drainage) - derived from SRTM
HAND_DATASET = 'users/gaborimbre/HAND_30m_SA'  # South America HAND

# CHIRPS Daily Precipitation (5.5km)
CHIRPS = 'UCSB-CHG/CHIRPS/DAILY'

# ERA5-Land Monthly (11km)
ERA5_LAND = 'ECMWF/ERA5_LAND/MONTHLY_AGGR'

# MODIS LST (1km)
MODIS_LST = 'MODIS/061/MOD11A2'

# WorldPop Population Density (100m)
WORLDPOP = 'WorldPop/GP/100m/pop'

# ESA WorldCover (10m land cover)
WORLDCOVER = 'ESA/WorldCover/v200'

# ============================================================================
# FLOOD RISK MODEL PARAMETERS
# ============================================================================

# HAND thresholds for flood susceptibility
# Magdalena: relaxed for flat lowlands (HAND <10m covers broader floodplains)
HAND_CLASSES = {
    'very_high': {'range': (0, 5), 'label': 'Very High', 'color': '#d73027'},
    'high': {'range': (5, 15), 'label': 'High', 'color': '#fc8d59'},
    'moderate': {'range': (15, 30), 'label': 'Moderate', 'color': '#fee08b'},
    'low': {'range': (30, 60), 'label': 'Low', 'color': '#d9ef8b'},
    'very_low': {'range': (60, 9999), 'label': 'Very Low', 'color': '#1a9850'},
}

# Flood frequency classes (based on JRC occurrence)
FLOOD_FREQUENCY_CLASSES = {
    'permanent': {'range': (75, 100), 'label': 'Permanent water', 'color': '#08306b'},
    'very_frequent': {'range': (50, 75), 'label': 'Very frequent', 'color': '#2171b5'},
    'frequent': {'range': (25, 50), 'label': 'Frequent flooding', 'color': '#6baed6'},
    'occasional': {'range': (10, 25), 'label': 'Occasional flooding', 'color': '#bdd7e7'},
    'rare': {'range': (1, 10), 'label': 'Rare flooding', 'color': '#eff3ff'},
}

# ML Model parameters
ML_PARAMS = {
    'random_forest': {
        'n_estimators': 500,
        'max_depth': 15,
        'min_samples_split': 10,
        'min_samples_leaf': 5,
        'random_state': 42,
        'n_jobs': -1,
    },
    'xgboost': {
        'n_estimators': 500,
        'max_depth': 8,
        'learning_rate': 0.05,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'random_state': 42,
        'eval_metric': 'auc',
    },
    'lightgbm': {
        'n_estimators': 500,
        'max_depth': 8,
        'learning_rate': 0.05,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'random_state': 42,
        'metric': 'auc',
        'verbose': -1,
    },
}

# Flood susceptibility features (predictors)
SUSCEPTIBILITY_FEATURES = [
    'elevation',            # SRTM 30m
    'slope',                # Derived from SRTM
    'aspect',               # Derived from SRTM
    'curvature',            # Plan curvature
    'hand',                 # Height Above Nearest Drainage
    'twi',                  # Topographic Wetness Index
    'spi',                  # Stream Power Index
    'dist_rivers',          # Distance to nearest river
    'dist_roads',           # Distance to nearest road
    'drainage_density',     # River density per unit area
    'rainfall_annual',      # Mean annual precipitation (CHIRPS)
    'rainfall_max_monthly', # Max monthly precipitation
    'land_cover',           # ESA WorldCover 2021
    'ndvi_mean',            # Mean annual NDVI
    'soil_moisture',        # ERA5-Land soil moisture
    'pop_density',          # WorldPop 100m
    'flood_frequency_jrc',  # JRC water occurrence
    'sar_water_frequency',  # Sentinel-1 derived water frequency
]

# Cross-validation parameters
CV_PARAMS = {
    'n_splits': 5,  # Stratified spatial K-fold
    'test_size': 0.3,
    'random_state': 42,
}

# ============================================================================
# VISUALIZATION PARAMETERS
# ============================================================================

# Color palettes
WATER_PALETTE = ['#ffffff', '#bdd7e7', '#6baed6', '#2171b5', '#08306b']
RISK_PALETTE = ['#1a9850', '#91cf60', '#d9ef8b', '#fee08b',
                '#fc8d59', '#d73027']
SUBREGION_PALETTE = [
    '#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00',
]

# Map center for Magdalena
MAP_CENTER = {'lat': 9.5, 'lon': -74.5}
MAP_ZOOM = 9

# Figure DPI for publication
FIGURE_DPI = 600
FIGURE_FORMAT = ['pdf', 'png']

# ============================================================================
# OUTPUT CONFIGURATION
# ============================================================================

# Export scale (meters)
EXPORT_SCALE = 30

# Project paths
import pathlib
PROJECT_ROOT = pathlib.Path(__file__).parent
DATA_DIR = PROJECT_ROOT / 'data'
OUTPUTS_DIR = PROJECT_ROOT / 'outputs'
FIGURES_DIR = OUTPUTS_DIR / 'figures'
TABLES_DIR = OUTPUTS_DIR / 'tables'
OVERLEAF_DIR = PROJECT_ROOT / 'overleaf'
LOGS_DIR = PROJECT_ROOT / 'logs'
