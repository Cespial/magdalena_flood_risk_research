#!/usr/bin/env python3
"""
run_analysis.py
===============
Master pipeline orchestrator for the Magdalena Flood Risk Assessment project.

This script executes the full satellite-based flood risk analysis in 8 sequential
phases, from SAR water detection through quality control. Each phase is isolated
so that failures in one phase do not abort the entire pipeline.

Study area : Department of Magdalena, Colombia
             Boundary sourced from FAO GAUL Level 1 via Google Earth Engine
Analysis period : 2015-2025 (Sentinel-1 era)

Usage
-----
# Run all phases
python run_analysis.py

# Run only specific phases
python run_analysis.py --phases 1,2,3

# Skip GEE-dependent phases (use previously cached exports)
python run_analysis.py --skip-gee

# Only trigger GEE export tasks, do not run local processing
python run_analysis.py --export-only

# Verbose logging
python run_analysis.py --verbose

Author : Magdalena Flood Risk Research Project
Date   : 2026-02-26
"""

import argparse
import importlib.util
import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Project layout
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
SCRIPTS_DIR  = PROJECT_ROOT / "scripts"
OUTPUTS_DIR  = PROJECT_ROOT / "outputs"
LOGS_DIR     = PROJECT_ROOT / "logs"

# Ensure critical directories exist before anything else
for _d in (OUTPUTS_DIR, LOGS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

LOG_FILE = LOGS_DIR / "analysis.log"

def setup_logging(verbose: bool = False) -> logging.Logger:
    """
    Configure root logger to write to both the console and
    logs/analysis.log.  When *verbose* is True the console level is DEBUG;
    otherwise INFO.
    """
    log_level_console = logging.DEBUG if verbose else logging.INFO
    log_format = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    date_fmt    = "%Y-%m-%d %H:%M:%S"

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)          # capture everything at root level

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level_console)
    ch.setFormatter(logging.Formatter(log_format, datefmt=date_fmt))
    root.addHandler(ch)

    # File handler (always DEBUG)
    fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(log_format, datefmt=date_fmt))
    root.addHandler(fh)

    return logging.getLogger("run_analysis")


# ---------------------------------------------------------------------------
# Pipeline phase definitions
# ---------------------------------------------------------------------------

# Each entry:   (phase_number, label, script_filename, gee_dependent)
PHASES = [
    (1, "SAR Water Detection",           "01_sar_water_detection.py",          True),
    (2, "JRC Water Analysis",            "02_jrc_water_analysis.py",           True),
    (3, "Feature Engineering",           "03_flood_susceptibility_features.py", True),
    (4, "ML Flood Susceptibility",       "04_ml_flood_susceptibility.py",      False),
    (5, "Population Exposure",           "05_population_exposure.py",          True),
    (6, "Climate Analysis",              "06_climate_analysis.py",             True),
    (7, "Visualization",                 "07_visualization.py",                False),
    (8, "Quality Control",               "09_quality_control.py",              False),
]

# Map phase number -> tuple entry for quick lookup
PHASE_MAP = {p[0]: p for p in PHASES}

# ---------------------------------------------------------------------------
# Helper: dynamic module loader
# ---------------------------------------------------------------------------

def load_phase_module(script_path: Path, module_name: str):
    """
    Dynamically load a Python script as a module.  The script is expected to
    expose at minimum a ``run(config: dict) -> dict`` function that returns a
    results dictionary.  If the function does not exist the phase is skipped
    with a warning.

    Parameters
    ----------
    script_path : Path
        Absolute path to the .py file.
    module_name : str
        Logical name used to register the module in sys.modules.

    Returns
    -------
    module or None
    """
    if not script_path.exists():
        return None

    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        return None

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# Helper: config builder
# ---------------------------------------------------------------------------

def build_phase_config(
    phase_num: int,
    skip_gee: bool,
    export_only: bool,
    verbose: bool,
) -> dict:
    """
    Build the configuration dictionary that is forwarded to each phase's
    ``run()`` function.  This merges project-level constants from gee_config.py
    with runtime flags so every script receives a single source of truth.

    The GEE study-area boundary is always defined via FAO GAUL Level 1,
    never as a raw bounding box.
    """
    cfg: dict[str, Any] = {
        # Runtime flags
        "phase": phase_num,
        "skip_gee": skip_gee,
        "export_only": export_only,
        "verbose": verbose,

        # Paths (passed as strings for cross-module compatibility)
        "project_root": str(PROJECT_ROOT),
        "data_dir":     str(PROJECT_ROOT / "data"),
        "outputs_dir":  str(OUTPUTS_DIR),
        "logs_dir":     str(LOGS_DIR),
        "figures_dir":  str(OUTPUTS_DIR / "figures"),
        "tables_dir":   str(OUTPUTS_DIR / "tables"),

        # Study area -- always from FAO GAUL, not a bounding box
        "study_area": {
            "source":     "FAO/GAUL/2015/level1",
            "filter_col": "ADM1_NAME",
            "filter_val": "Magdalena",
            "country":    "Colombia",
        },

        # Timestamp for reproducibility
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Merge gee_config constants (non-callable, non-dunder attributes)
    try:
        sys.path.insert(0, str(PROJECT_ROOT))
        import gee_config as gc
        for attr in dir(gc):
            if attr.startswith("_"):
                continue
            val = getattr(gc, attr)
            if callable(val):
                continue
            # Pathlib objects -> str so JSON-serialisable
            if isinstance(val, Path):
                val = str(val)
            cfg[f"gee_config_{attr}"] = val
    except Exception as exc:
        logging.getLogger("run_analysis").warning(
            "Could not import gee_config.py constants: %s", exc
        )

    # Try to import utils
    try:
        import scripts.utils as utils_mod  # noqa: F401 -- expose on sys.path
        cfg["utils_available"] = True
    except Exception:
        cfg["utils_available"] = False

    return cfg


# ---------------------------------------------------------------------------
# Core phase runner
# ---------------------------------------------------------------------------

def run_phase(
    phase_num: int,
    phase_label: str,
    script_name: str,
    gee_dependent: bool,
    skip_gee: bool,
    export_only: bool,
    verbose: bool,
    logger: logging.Logger,
) -> dict:
    """
    Execute a single analysis phase.

    Behaviour
    ---------
    * Loads the script dynamically and calls its ``run(config)`` function.
    * If the script is missing, records a ``skipped`` result.
    * Any exception is caught, logged with its traceback, and the phase
      result is recorded as ``failed`` so the pipeline continues.
    * Saves an intermediate JSON result to outputs/<phase_num>_<label>.json.

    Returns
    -------
    dict
        Phase result record including status, timing, and any metrics.
    """
    separator = "=" * 72
    phase_tag = f"Phase {phase_num}: {phase_label}"

    logger.info(separator)
    logger.info("START  %s", phase_tag)

    result: dict[str, Any] = {
        "phase":      phase_num,
        "label":      phase_label,
        "script":     script_name,
        "status":     "pending",
        "start_utc":  datetime.now(timezone.utc).isoformat(),
        "end_utc":    None,
        "runtime_s":  None,
        "metrics":    {},
        "warnings":   [],
        "error":      None,
    }

    t0 = time.monotonic()

    # -- Skip GEE-dependent phases if requested ----------------------------
    if gee_dependent and skip_gee:
        logger.info(
            "SKIP   %s  (--skip-gee flag active; using cached data)", phase_tag
        )
        result["status"]   = "skipped"
        result["end_utc"]  = datetime.now(timezone.utc).isoformat()
        result["runtime_s"] = 0.0
        result["warnings"].append("Skipped because --skip-gee was specified")
        _save_phase_result(result, phase_num, phase_label, logger)
        return result

    # -- Export-only mode: skip local processing phases --------------------
    if export_only and not gee_dependent:
        logger.info(
            "SKIP   %s  (--export-only flag active; local processing skipped)",
            phase_tag,
        )
        result["status"]    = "skipped"
        result["end_utc"]   = datetime.now(timezone.utc).isoformat()
        result["runtime_s"] = 0.0
        result["warnings"].append("Skipped because --export-only was specified")
        _save_phase_result(result, phase_num, phase_label, logger)
        return result

    # -- Locate the script -------------------------------------------------
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        logger.warning("Script not found: %s", script_path)
        result["status"]    = "skipped"
        result["end_utc"]   = datetime.now(timezone.utc).isoformat()
        result["runtime_s"] = round(time.monotonic() - t0, 3)
        result["warnings"].append(f"Script not found: {script_path}")
        _save_phase_result(result, phase_num, phase_label, logger)
        return result

    # -- Execute the phase as a subprocess ---------------------------------
    # Running each script as a separate process avoids argparse conflicts,
    # module-level side effects, and import ordering issues.
    import subprocess
    logger.info("Executing %s via subprocess ...", phase_tag)

    cmd = [sys.executable, str(script_path)]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT) + os.pathsep + env.get("PYTHONPATH", "")

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            env=env,
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour max per phase
        )

        # Log subprocess output
        if proc.stdout.strip():
            for line in proc.stdout.strip().split("\n"):
                logger.info("  [stdout] %s", line)
        if proc.stderr.strip():
            for line in proc.stderr.strip().split("\n"):
                if "ERROR" in line or "FAIL" in line:
                    logger.error("  [stderr] %s", line)
                else:
                    logger.debug("  [stderr] %s", line)

        if proc.returncode == 0:
            result["status"] = "success"
        else:
            result["status"] = "failed"
            result["error"] = proc.stderr[-2000:] if proc.stderr else f"Exit code {proc.returncode}"

        elapsed = round(time.monotonic() - t0, 3)
        result["end_utc"]   = datetime.now(timezone.utc).isoformat()
        result["runtime_s"] = elapsed

        logger.info(
            "END    %s  [status=%s, %.1f s]",
            phase_tag, result["status"], elapsed,
        )

        # Surface any key metrics to the console
        if result["metrics"]:
            logger.info("Metrics:")
            for k, v in result["metrics"].items():
                logger.info("  %-35s %s", k, v)

    except Exception:
        elapsed = round(time.monotonic() - t0, 3)
        result["status"]    = "failed"
        result["end_utc"]   = datetime.now(timezone.utc).isoformat()
        result["runtime_s"] = elapsed
        result["error"]     = traceback.format_exc()
        logger.error(
            "FAIL   %s  [%.1f s]\n%s", phase_tag, elapsed, result["error"]
        )

    _save_phase_result(result, phase_num, phase_label, logger)
    return result


# ---------------------------------------------------------------------------
# Intermediate result persistence
# ---------------------------------------------------------------------------

def _safe_json_value(v: Any) -> Any:
    """Recursively convert non-JSON-serialisable values to strings."""
    if isinstance(v, dict):
        return {kk: _safe_json_value(vv) for kk, vv in v.items()}
    if isinstance(v, (list, tuple)):
        return [_safe_json_value(i) for i in v]
    if isinstance(v, Path):
        return str(v)
    if isinstance(v, (int, float, str, bool, type(None))):
        return v
    return str(v)


def _save_phase_result(
    result: dict,
    phase_num: int,
    phase_label: str,
    logger: logging.Logger,
) -> None:
    """Persist an individual phase result to outputs/ as JSON."""
    safe_label = phase_label.lower().replace(" ", "_")
    out_path = OUTPUTS_DIR / f"phase{phase_num:02d}_{safe_label}_result.json"
    try:
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(_safe_json_value(result), fh, indent=2, ensure_ascii=False)
        logger.debug("Saved phase result: %s", out_path)
    except Exception as exc:
        logger.warning("Could not save phase result to %s: %s", out_path, exc)


# ---------------------------------------------------------------------------
# Summary report
# ---------------------------------------------------------------------------

def save_analysis_summary(
    pipeline_start: datetime,
    pipeline_end: datetime,
    phase_results: list[dict],
    args: argparse.Namespace,
    logger: logging.Logger,
) -> Path:
    """
    Write a comprehensive analysis_summary.json to the project root.

    The file contains:
    - Pipeline-level metadata (timestamps, flags, git hash if available)
    - Per-phase results (status, runtime, metrics, errors)
    - Aggregate statistics (success count, total runtime)
    """
    total_s = (pipeline_end - pipeline_start).total_seconds()

    n_success = sum(1 for r in phase_results if r["status"] == "success")
    n_failed  = sum(1 for r in phase_results if r["status"] == "failed")
    n_skipped = sum(1 for r in phase_results if r["status"] == "skipped")

    # Attempt to capture git commit hash for reproducibility
    git_hash = "unknown"
    try:
        import subprocess
        git_hash = subprocess.check_output(
            ["git", "-C", str(PROJECT_ROOT), "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        pass

    summary = {
        "project": "Satellite-Based Flood Risk Assessment – Magdalena, Colombia",
        "study_area": {
            "department": "Magdalena",
            "country": "Colombia",
            "boundary_source": "FAO GAUL Level 1 via Google Earth Engine",
            "gee_asset": "FAO/GAUL/2015/level1",
        },
        "analysis_period": {
            "start": "2015-01-01",
            "end":   "2025-12-31",
        },
        "execution": {
            "pipeline_start_utc": pipeline_start.isoformat(),
            "pipeline_end_utc":   pipeline_end.isoformat(),
            "total_runtime_s":    round(total_s, 3),
            "total_runtime_human": _format_duration(total_s),
            "git_commit":         git_hash,
            "python_version":     sys.version,
            "run_timestamp":      pipeline_start.isoformat(),
        },
        "cli_flags": {
            "phases":       getattr(args, "phases", "all"),
            "skip_gee":     getattr(args, "skip_gee", False),
            "export_only":  getattr(args, "export_only", False),
            "verbose":      getattr(args, "verbose", False),
        },
        "aggregate": {
            "phases_total":   len(phase_results),
            "phases_success": n_success,
            "phases_failed":  n_failed,
            "phases_skipped": n_skipped,
            "overall_status": "success" if n_failed == 0 else "partial_failure",
        },
        "phases": [_safe_json_value(r) for r in phase_results],
    }

    out_path = PROJECT_ROOT / "analysis_summary.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=False)

    logger.info("Analysis summary saved: %s", out_path)
    return out_path


def _format_duration(seconds: float) -> str:
    """Convert a duration in seconds to a human-readable string."""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    if h:
        return f"{h}h {m}m {s:.1f}s"
    if m:
        return f"{m}m {s:.1f}s"
    return f"{s:.1f}s"


# ---------------------------------------------------------------------------
# Progress banner helpers
# ---------------------------------------------------------------------------

def print_pipeline_banner(selected_phases: list[int], logger: logging.Logger) -> None:
    """Print the pipeline start banner."""
    logger.info("=" * 72)
    logger.info("  Magdalena Flood Risk Assessment — Master Analysis Pipeline")
    logger.info("  Study area : Department of Magdalena, Colombia")
    logger.info("  Boundary   : FAO GAUL Level 1 via Google Earth Engine")
    logger.info("  Period     : 2015-01-01 to 2025-12-31")
    logger.info("  Started    : %s UTC", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 72)
    logger.info("Selected phases: %s", selected_phases)
    logger.info("")


def print_pipeline_summary(
    phase_results: list[dict],
    total_s: float,
    logger: logging.Logger,
) -> None:
    """Print a per-phase summary table to the logger."""
    logger.info("")
    logger.info("=" * 72)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 72)
    logger.info("  %-4s  %-32s  %-10s  %s", "Ph.", "Label", "Status", "Runtime")
    logger.info("  " + "-" * 68)

    for r in phase_results:
        status_icon = {"success": "OK ", "failed": "ERR", "skipped": "---"}.get(
            r["status"], "???"
        )
        runtime_str = (
            _format_duration(r["runtime_s"]) if r["runtime_s"] is not None else "N/A"
        )
        logger.info(
            "  %-4s  %-32s  %-10s  %s",
            r["phase"],
            r["label"][:32],
            f"[{status_icon}] {r['status']}",
            runtime_str,
        )

    logger.info("  " + "-" * 68)
    logger.info("  Total runtime: %s", _format_duration(total_s))

    n_fail = sum(1 for r in phase_results if r["status"] == "failed")
    if n_fail:
        logger.warning(
            "%d phase(s) failed. Review logs/analysis.log for details.", n_fail
        )
    else:
        logger.info("All executed phases completed successfully.")

    logger.info("=" * 72)


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="run_analysis.py",
        description=(
            "Master pipeline for satellite-based flood risk assessment "
            "in Magdalena, Colombia."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_analysis.py
  python run_analysis.py --phases 1,2,3
  python run_analysis.py --skip-gee --phases 4,7,8
  python run_analysis.py --export-only
  python run_analysis.py --verbose
        """,
    )

    parser.add_argument(
        "--phases",
        type=str,
        default="all",
        help=(
            "Comma-separated list of phase numbers to execute "
            "(e.g. '1,2,3'). Default: all phases."
        ),
    )
    parser.add_argument(
        "--skip-gee",
        action="store_true",
        default=False,
        help=(
            "Skip all Google Earth Engine-dependent phases and use "
            "previously cached satellite exports from data/satellite_exports/."
        ),
    )
    parser.add_argument(
        "--export-only",
        action="store_true",
        default=False,
        help=(
            "Submit GEE export tasks and exit without running local "
            "processing phases. Useful when GEE tasks need to finish "
            "before further analysis."
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Enable DEBUG-level logging to the console.",
    )

    return parser.parse_args()


def resolve_phase_list(phases_arg: str) -> list[int]:
    """
    Convert the --phases argument string to a sorted list of phase numbers.
    Validates that every requested phase exists in PHASE_MAP.
    """
    if phases_arg.strip().lower() == "all":
        return sorted(PHASE_MAP.keys())

    requested = []
    for token in phases_arg.split(","):
        token = token.strip()
        if not token:
            continue
        try:
            num = int(token)
        except ValueError:
            raise ValueError(
                f"Invalid phase number '{token}' in --phases argument. "
                f"Must be integers separated by commas."
            )
        if num not in PHASE_MAP:
            raise ValueError(
                f"Phase {num} is not defined. "
                f"Valid phases: {sorted(PHASE_MAP.keys())}"
            )
        requested.append(num)

    return sorted(set(requested))


# ---------------------------------------------------------------------------
# GEE initialisation (optional, best-effort)
# ---------------------------------------------------------------------------

def init_gee(logger: logging.Logger) -> bool:
    """
    Attempt to initialise the Google Earth Engine API using credentials
    and project ID defined in gee_config.py / environment variables.

    Returns True if initialisation succeeded, False otherwise.
    The pipeline continues even if GEE is unavailable when --skip-gee is set.
    """
    try:
        import ee
        from dotenv import load_dotenv
        load_dotenv()
        project_id = os.getenv("GEE_PROJECT_ID", "ee-maestria-tesis")
        try:
            ee.Initialize(project=project_id)
        except Exception:
            ee.Authenticate()
            ee.Initialize(project=project_id)

        # Verify access by resolving the Magdalena boundary from FAO GAUL
        magdalena = (
            ee.FeatureCollection("FAO/GAUL/2015/level1")
            .filter(ee.Filter.And(
                ee.Filter.eq("ADM0_NAME", "Colombia"),
                ee.Filter.eq("ADM1_NAME", "Magdalena"),
            ))
        )
        count = magdalena.size().getInfo()
        if count == 0:
            raise RuntimeError(
                "FAO GAUL query returned 0 features for Magdalena, Colombia."
            )
        logger.info(
            "GEE initialised (project=%s). "
            "Magdalena boundary verified: %d feature(s) from FAO GAUL L1.",
            project_id, count,
        )
        return True

    except ImportError:
        logger.warning(
            "earthengine-api package not found. "
            "GEE-dependent phases will be skipped or use cached data."
        )
        return False
    except Exception as exc:
        logger.warning(
            "GEE initialisation failed: %s. "
            "GEE-dependent phases may fail unless --skip-gee is set.",
            exc,
        )
        return False


# ---------------------------------------------------------------------------
# Utility imports (scripts/utils.py)
# ---------------------------------------------------------------------------

def load_utils(logger: logging.Logger):
    """
    Attempt to import scripts/utils.py for shared utilities.
    Returns the module or None if unavailable.
    """
    utils_path = SCRIPTS_DIR / "utils.py"
    if not utils_path.exists():
        logger.debug("scripts/utils.py not found; utility helpers unavailable.")
        return None
    try:
        module = load_phase_module(utils_path, "scripts.utils")
        logger.debug("Loaded scripts/utils.py successfully.")
        return module
    except Exception as exc:
        logger.warning("Failed to load scripts/utils.py: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

def preflight_checks(logger: logging.Logger) -> list[str]:
    """
    Verify that expected directories and dependency packages exist.
    Returns a list of warning strings (empty if all checks pass).
    """
    warnings: list[str] = []

    # Required directories
    required_dirs = [
        PROJECT_ROOT / "scripts",
        PROJECT_ROOT / "data",
        PROJECT_ROOT / "outputs",
        PROJECT_ROOT / "logs",
    ]
    for d in required_dirs:
        if not d.exists():
            warnings.append(f"Expected directory not found: {d}")

    # Required output subdirectories (create if missing)
    output_subdirs = [
        OUTPUTS_DIR / "figures",
        OUTPUTS_DIR / "tables",
        OUTPUTS_DIR / "phase1_water_maps",
        OUTPUTS_DIR / "phase2_flood_frequency",
        OUTPUTS_DIR / "phase3_risk_model",
        OUTPUTS_DIR / "phase4_municipal_stats",
        OUTPUTS_DIR / "phase5_qc",
    ]
    for d in output_subdirs:
        d.mkdir(parents=True, exist_ok=True)

    # Key Python packages
    critical_packages = [
        ("numpy",        "numpy"),
        ("pandas",       "pandas"),
        ("geopandas",    "geopandas"),
        ("sklearn",      "scikit-learn"),
        ("rasterio",     "rasterio"),
        ("matplotlib",   "matplotlib"),
    ]
    for import_name, pip_name in critical_packages:
        try:
            importlib.import_module(import_name)
        except ImportError:
            warnings.append(
                f"Missing package '{pip_name}'. Install with: pip install {pip_name}"
            )

    return warnings


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> int:
    """
    Orchestrate the full analysis pipeline.

    Returns
    -------
    int
        0 on full success, 1 if any phase failed.
    """
    args = parse_args()
    logger = setup_logging(verbose=args.verbose)

    # Resolve which phases to run
    try:
        selected_phases = resolve_phase_list(args.phases)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print_pipeline_banner(selected_phases, logger)
    pipeline_start = datetime.now(timezone.utc)

    # Pre-flight checks
    logger.info("Running pre-flight checks ...")
    preflight_warnings = preflight_checks(logger)
    if preflight_warnings:
        for w in preflight_warnings:
            logger.warning("Pre-flight: %s", w)
    else:
        logger.info("Pre-flight checks passed.")

    # Load shared utilities
    _utils = load_utils(logger)

    # Initialise GEE unless we are skipping all GEE phases
    gee_needed = any(
        PHASE_MAP[p][3]  # gee_dependent flag
        for p in selected_phases
        if p in PHASE_MAP
    )
    gee_ok = False
    if gee_needed and not args.skip_gee:
        logger.info("Initialising Google Earth Engine ...")
        gee_ok = init_gee(logger)
        if not gee_ok:
            logger.warning(
                "GEE unavailable. GEE-dependent phases will record as failed "
                "unless --skip-gee is used."
            )
    else:
        logger.info("GEE initialisation skipped (skip_gee=%s, gee_needed=%s).",
                    args.skip_gee, gee_needed)

    # ------------------------------------------------------------------ #
    # Execute phases sequentially
    # ------------------------------------------------------------------ #
    phase_results: list[dict] = []

    for phase_num in selected_phases:
        _, label, script, gee_dep = PHASE_MAP[phase_num]

        result = run_phase(
            phase_num=phase_num,
            phase_label=label,
            script_name=script,
            gee_dependent=gee_dep,
            skip_gee=args.skip_gee,
            export_only=args.export_only,
            verbose=args.verbose,
            logger=logger,
        )
        phase_results.append(result)

        # Brief progress report after each phase
        n_done    = len(phase_results)
        n_total   = len(selected_phases)
        pct_done  = 100.0 * n_done / n_total
        logger.info(
            "Progress: %d/%d phases complete (%.0f%%)",
            n_done, n_total, pct_done,
        )

    # ------------------------------------------------------------------ #
    # Pipeline finished
    # ------------------------------------------------------------------ #
    pipeline_end   = datetime.now(timezone.utc)
    total_runtime  = (pipeline_end - pipeline_start).total_seconds()

    print_pipeline_summary(phase_results, total_runtime, logger)

    summary_path = save_analysis_summary(
        pipeline_start, pipeline_end, phase_results, args, logger
    )
    logger.info("Full results: %s", summary_path)
    logger.info("Log file    : %s", LOG_FILE)

    # Exit code: 1 if any phase failed
    n_failures = sum(1 for r in phase_results if r["status"] == "failed")
    return 1 if n_failures else 0


if __name__ == "__main__":
    sys.exit(main())
