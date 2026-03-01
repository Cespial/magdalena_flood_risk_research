# Replication Guide: Magdalena Flood Risk Assessment

This guide documents how this project was adapted from the Antioquia Flood Risk Assessment framework for the Department of Magdalena, Colombia, and provides instructions for replicating the analysis for other Colombian departments.

---

## 1. Overview of Adaptations

### 1.1 Geographic Changes

| Parameter | Antioquia (Original) | Magdalena (This Project) |
|-----------|---------------------|--------------------------|
| Department | Antioquia | Magdalena |
| Area (km2) | 63,612 | 23,188 |
| Municipalities | 125 | 30 |
| Subregions | 9 | 5 |
| Terrain | Mountainous (Andes) | Mixed (Sierra Nevada + lowlands) |
| Max elevation | ~4,080 m | ~5,775 m (Sierra Nevada) |
| Coastal | No | Yes (Caribbean coast) |
| SAR orbit | DESCENDING only | BOTH (ASCENDING + DESCENDING) |
| Map center | 6.9°N, 75.6°W | 9.5°N, 74.5°W |

### 1.2 Key Differences

1. **Terrain**: Magdalena has extensive flat lowlands (Rio Magdalena floodplain, Cienaga Grande, Depresion Momposina) unlike Antioquia's mountainous terrain. HAND thresholds were relaxed accordingly.
2. **Coastal influence**: Tidal and estuarine dynamics affect water detection near the coast and must be accounted for in SAR-based flood mapping.
3. **Dual SAR orbits**: The Sierra Nevada de Santa Marta creates radar shadows in DESCENDING orbit, requiring ASCENDING data as supplement.
4. **Fewer administrative units**: 30 municipalities vs 125, and 5 subregions vs 9, simplifying some analyses but requiring adjusted spatial cross-validation strategies.

---

## 2. Files Modified

### 2.1 Central Configuration

- **`gee_config.py`**: Complete rewrite for Magdalena
  - `DEPARTMENT_NAME = 'Magdalena'`
  - 5 subregions with 30 municipalities (ASCII names for GAUL compatibility)
  - `MAP_CENTER = {'lat': 9.5, 'lon': -74.5}`
  - `SUBREGION_PALETTE` reduced from 9 to 5 colors
  - GEE project ID: `ee-flood-risk-magdalena`

### 2.2 Root Utilities

- **`utils.py`**: Updated constants
  - `MAGDALENA_AREA_KM2 = 23_188.0`
  - Boundary file paths updated (30 municipalities, 5 subregions)
  - All function names and docstrings updated

### 2.3 Pipeline Scripts

All 9 pipeline scripts (`scripts/01_*` through `scripts/09_*`) and `scripts/utils.py` were adapted:

- Renamed all GEE Drive folder references: `magdalena_flood_risk`
- Updated all asset IDs: `projects/ee-flood-risk-magdalena/assets/...`
- Updated municipality count expectations: 125 -> 30
- Updated subregion count expectations: 9 -> 5
- Updated area expectations: 63,612 -> 23,188 km2
- Updated all plot titles and labels
- Updated file name patterns (e.g., `magdalena_flood_training_samples.csv`)

### 2.4 Documentation

- **`README.md`**: Rewritten for Magdalena
- **`bib/references.bib`**: New bibliography with Magdalena-specific references
- **`overleaf/references.bib`**: Copied from bib/
- **`.env`**: GEE project ID configuration

---

## 3. How to Replicate for Another Department

### Step 1: Create New Project Directory

```bash
mkdir -p <department>_flood_risk_research/{scripts,data/{boundaries,satellite_exports},outputs/{phase1_sar,phase2_jrc,phase3_risk_model,phase4_exposure,phase5_qc,figures,tables},overleaf/{figures,tables},logs,bib}
```

### Step 2: Adapt `gee_config.py`

Update the following parameters:

```python
DEPARTMENT_NAME = '<Department Name>'  # Must match FAO GAUL ADM1_NAME
SUBREGIONS = { ... }  # Official subregions with municipality lists
MAP_CENTER = {'lat': <lat>, 'lon': <lon>}
MAP_ZOOM = <zoom>  # Adjust for department size
SUBREGION_PALETTE = [...]  # One color per subregion
```

### Step 3: Adapt `utils.py` (Root Level)

```python
<DEPT>_AREA_KM2 = <official_area>  # DANE official area in km2
```

Update boundary file paths:
- `<dept>_department_boundary_GADM41.geojson`
- `<dept>_municipalities_<N>_GADM41.geojson`
- `<dept>_<N>_subregions.geojson`

### Step 4: Bulk Rename in Scripts

Use find-and-replace across all `.py` files:
- `antioquia` -> `<department>`
- `Antioquia` -> `<Department>`
- `ANTIOQUIA` -> `<DEPARTMENT>`
- Update all numeric constants (area, municipality count, subregion count)

### Step 5: Download Boundary Files

Download GADM v4.1 boundaries for the target department:
1. Department boundary (Level 1)
2. Municipality boundaries (Level 2)
3. Create subregion polygons by dissolving municipalities

Place them in `data/boundaries/`.

### Step 6: Configure GEE

1. Create a GEE project: `ee-flood-risk-<department>`
2. Update `.env`: `GEE_PROJECT_ID=ee-flood-risk-<department>`
3. Authenticate: `earthengine authenticate`

### Step 7: Run the Pipeline

Execute scripts in order (01 through 09). Each script is independent and can be re-run.

### Step 8: Department-Specific Considerations

For each new department, evaluate:

1. **SAR orbit direction**: Mountainous terrain may require both orbits
2. **HAND thresholds**: Flat regions need relaxed thresholds; mountainous regions may need tighter ones
3. **Seasonal patterns**: Colombia has regional variations in precipitation timing
4. **Coastal effects**: Coastal departments need tidal corrections
5. **Spatial CV strategy**: Subregion-based folds should ensure balanced representation
6. **Population reference year**: Verify WorldPop data availability

---

## 4. Data Requirements Checklist

- [ ] FAO GAUL Level 1 boundary (department)
- [ ] FAO GAUL Level 2 boundaries (municipalities)
- [ ] Subregion classification (official or custom)
- [ ] GEE project with access to Sentinel-1, JRC GSW, SRTM, CHIRPS, ERA5-Land, WorldPop, ESA WorldCover
- [ ] DANE official area for validation
- [ ] Local flood event records for validation (if available)

---

## 5. Expected Outputs

After running the full pipeline:

```
outputs/
├── phase1_sar/            # SAR water detection maps (monthly, annual)
├── phase2_jrc/            # JRC analysis, flood frequency
├── phase3_risk_model/     # ML model outputs, SHAP analysis
├── phase4_exposure/       # Population exposure statistics
├── phase5_qc/             # QC report
├── figures/               # 12 publication-quality figures (PDF + PNG)
└── tables/                # 7 publication tables (CSV + LaTeX)
```

---

## 6. Troubleshooting

### Common Issues

1. **Municipality names not matching**: GAUL uses ASCII names without accents. Verify against `gee_config.SUBREGIONS`.
2. **GEE quota exceeded**: Reduce `maxPixels` or split exports by subregion.
3. **Sentinel-1B gap (2021-12-23 to 2024-04-25)**: Reduced temporal coverage affects some months. The pipeline handles this automatically.
4. **HAND dataset coverage**: The MERIT Hydro `hnd` band covers South America. For other continents, update the HAND_DATASET path.
