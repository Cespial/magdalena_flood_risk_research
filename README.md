# Municipality-Scale Flood Risk Mapping in Magdalena, Colombia

**Using Sentinel-1 SAR and Ensemble Machine Learning (2015--2025)**

Cristian Espinal Maya [![ORCID](https://img.shields.io/badge/ORCID-0009--0000--1009--8388-green)](https://orcid.org/0009-0000-1009-8388) · Santiago Jimenez Londono [![ORCID](https://img.shields.io/badge/ORCID-0009--0007--9862--7133-green)](https://orcid.org/0009-0007-9862-7133)

School of Applied Sciences and Engineering, Universidad EAFIT, Medellin, Colombia

[![SSRN](https://img.shields.io/badge/Preprint-SSRN-blue)](https://www.ssrn.com/) · [![License: MIT](https://img.shields.io/badge/Code-MIT-yellow)](LICENSE) · [![License: CC BY 4.0](https://img.shields.io/badge/Manuscript-CC%20BY%204.0-lightgrey)](https://creativecommons.org/licenses/by/4.0/)

---

## Abstract

We present a reproducible, open-access framework that delivers municipality-level flood risk statistics for all 30 municipalities of the Department of Magdalena, Colombia (23,188 km²; ~1.3 million inhabitants). The department encompasses diverse terrain ranging from the Sierra Nevada de Santa Marta (5,775 m a.s.l.) to the coastal lowlands of the Cienaga Grande de Santa Marta and the Rio Magdalena floodplain. We processed Sentinel-1 C-band SAR scenes (2015–2025) within Google Earth Engine using adaptive Otsu thresholding to produce monthly water extent maps at 10 m resolution. Eighteen predictor variables were integrated into a weighted ensemble of Random Forest, XGBoost, and LightGBM, evaluated under spatial five-fold cross-validation. HAND, SAR flood frequency, and elevation were identified as dominant predictors via SHAP analysis. Overlaying the susceptibility surface with 100 m population data quantifies the population residing in high or very high susceptibility zones. La Nina years amplify mean flood extent relative to El Nino years.

## Study Area

The Department of Magdalena is located in northern Colombia along the Caribbean coast. It features:

- **Sierra Nevada de Santa Marta**: The world's highest coastal mountain range (5,775 m)
- **Cienaga Grande de Santa Marta**: Colombia's largest coastal lagoon system (Ramsar wetland)
- **Rio Magdalena floodplain**: Extensive fluvial lowlands and the Depresion Momposina
- **Caribbean coast**: Coastal and estuarine dynamics
- **Bimodal precipitation**: Two wet seasons (MAM, SON) with peak flooding in October-November

### 5 Subregions (30 Municipalities)

| Subregion | Municipalities | Key Features |
|-----------|---------------|--------------|
| Santa Marta | 1 | Capital city, Sierra Nevada foothills |
| Norte | 7 | Zona Bananera, Cienaga Grande |
| Rio | 9 | Rio Magdalena floodplain, wetlands |
| Centro | 6 | Transitional terrain |
| Sur | 7 | Depresion Momposina, El Banco |

## Repository Structure

```
.
├── overleaf/                  # Manuscript (preprint format)
│   ├── main.tex               # Main LaTeX source
│   ├── arxiv.sty              # Preprint style file
│   ├── references.bib         # Bibliography
│   └── figures/               # All figure PDFs
├── scripts/                   # Processing and analysis pipeline
│   ├── 01_sar_water_detection.py
│   ├── 02_jrc_water_analysis.py
│   ├── 03_flood_susceptibility_features.py
│   ├── 04_ml_flood_susceptibility.py
│   ├── 05_population_exposure.py
│   ├── 06_climate_analysis.py
│   ├── 07_visualization.py
│   ├── 08_generate_tables.py
│   ├── 09_quality_control.py
│   └── utils.py
├── bib/                       # Additional bibliography files
│   └── references.bib
├── gee_config.py              # Central GEE configuration
├── utils.py                   # Root-level utility functions
├── requirements.txt           # Python dependencies
└── README.md
```

## Data Sources

All data are open-access and processed via [Google Earth Engine](https://earthengine.google.com/):

- **Sentinel-1 GRD** (ESA/Copernicus) — 10 m SAR flood detection
- **JRC Global Surface Water v1.4** — 38-year water dynamics
- **SRTM DEM v3** — Topographic features (30 m)
- **MERIT Hydro** — HAND computation (90 m)
- **CHIRPS / ERA5-Land** — Precipitation and soil moisture
- **ESA WorldCover / Sentinel-2** — Land cover and NDVI
- **WorldPop** — Population density (100 m)
- **FAO GAUL** — Administrative boundaries

## Reproducing the Analysis

### Requirements

- Python 3.10+
- Google Earth Engine account ([sign up](https://signup.earthengine.google.com/))
- Libraries: `earthengine-api`, `scikit-learn`, `xgboost`, `lightgbm`, `shap`, `matplotlib`, `geopandas`

### Pipeline

```bash
# 1. SAR water detection (runs on GEE)
python scripts/01_sar_water_detection.py

# 2. JRC validation analysis
python scripts/02_jrc_water_analysis.py

# 3. Feature engineering
python scripts/03_flood_susceptibility_features.py

# 4. ML model training and evaluation
python scripts/04_ml_flood_susceptibility.py

# 5. Population exposure analysis
python scripts/05_population_exposure.py

# 6. ENSO and seasonal analysis
python scripts/06_climate_analysis.py

# 7. Generate all figures
python scripts/07_visualization.py

# 8. Generate all tables
python scripts/08_generate_tables.py

# 9. Quality control
python scripts/09_quality_control.py
```

## Magdalena-Specific Considerations

1. **Dual SAR orbits**: Both ASCENDING and DESCENDING passes are recommended due to radar shadow effects from the Sierra Nevada de Santa Marta.
2. **Coastal dynamics**: The Cienaga Grande and Caribbean coast introduce tidal and estuarine water fluctuations that must be distinguished from fluvial flooding.
3. **Flat terrain**: The extensive lowlands (HAND < 10 m) require adjusted thresholds for flood susceptibility mapping compared to mountainous regions.
4. **Seasonal flooding**: The Rio Magdalena and its tributaries produce extensive seasonal inundation, particularly during the October-November wet season.

## Citation

If you use this work, please cite:

```bibtex
@article{EspinalMaya2026Magdalena,
  author  = {Espinal Maya, Cristian and Jim\'enez Londo\~no, Santiago},
  title   = {Municipality-Scale Flood Risk Mapping in {Magdalena}, {Colombia},
             Using {Sentinel-1} {SAR} and Ensemble Machine Learning (2015--2025)},
  year    = {2026},
  note    = {Available at SSRN}
}
```

## License

Source code: [MIT License](LICENSE). Manuscript and figures: CC BY 4.0.
