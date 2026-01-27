#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import importlib
import io
import json
import math
import os
import queue
import random
import re
import sys
import textwrap
import threading
import time
import warnings
import webbrowser
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from itertools import combinations, repeat
from pathlib import Path
from typing import Any, Callable, Collection, Optional, Protocol, Sequence, Union

import tkinter as tk
import tkinter.font as tkfont
from tkinter import filedialog, messagebox, simpledialog
from tkinter import ttk


class _DeferredImport:
    def __init__(self, module_name: str, attr_name: Optional[str] = None) -> None:
        self._module_name = module_name
        self._attr_name = attr_name

    def _resolve(self) -> Any:
        spec = _HEAVY_IMPORTS[self._module_name]
        if not spec["event"].is_set():
            _load_heavy_module(self._module_name)
        if spec.get("error") is not None:
            raise spec["error"]
        if self._attr_name is None:
            return spec.get("resolved_alias")
        return spec["resolved_attrs"][self._attr_name]

    def __getattr__(self, item: str) -> Any:
        return getattr(self._resolve(), item)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._resolve()(*args, **kwargs)

    def __iter__(self):
        return iter(self._resolve())

    def __bool__(self) -> bool:
        return bool(self._resolve())


_HEAVY_IMPORTS: dict[str, dict[str, Any]] = {
    "numpy": {"alias": "np"},
    "pandas": {"alias": "pd"},
    "statsmodels.api": {"alias": "sm"},
    "matplotlib.axes": {"attrs": {"Axes": "Axes"}},
    "matplotlib.backend_bases": {"attrs": {"MouseButton": "MouseButton"}},
    "matplotlib.backends.backend_tkagg": {
        "attrs": {"FigureCanvasTkAgg": "FigureCanvasTkAgg"}
    },
    "matplotlib.collections": {"attrs": {"PathCollection": "PathCollection"}},
    "matplotlib.patches": {"attrs": {"Rectangle": "MplRectangle"}},
    "matplotlib.figure": {"attrs": {"Figure": "Figure"}},
    "matplotlib.ticker": {
        "attrs": {"FixedLocator": "FixedLocator", "FormatStrFormatter": "FormatStrFormatter"}
    },
    "matplotlib.transforms": {"attrs": {"Bbox": "Bbox"}},
    "sklearn.metrics": {
        "attrs": {
            "mean_absolute_error": "mean_absolute_error",
            "mean_squared_error": "mean_squared_error",
            "r2_score": "r2_score",
        }
    },
    "sklearn.model_selection": {
        "attrs": {"KFold": "KFold", "train_test_split": "train_test_split"}
    },
    "statsmodels.stats.stattools": {
        "attrs": {
            "jarque_bera": "stattools_jarque_bera",
            "omni_normtest": "stattools_omni_normtest",
        }
    },
}

for _module_name, _spec in _HEAVY_IMPORTS.items():
    _spec["event"] = threading.Event()
    _spec["lock"] = threading.Lock()
    _spec["resolved_alias"] = None
    _spec["resolved_attrs"] = {}
    alias_name = _spec.get("alias")
    if alias_name:
        _spec["alias_placeholder"] = _DeferredImport(_module_name)
        globals()[alias_name] = _spec["alias_placeholder"]
    attr_placeholders: dict[str, _DeferredImport] = {}
    for attr_name, global_name in _spec.get("attrs", {}).items():
        placeholder = _DeferredImport(_module_name, attr_name)
        attr_placeholders[attr_name] = placeholder
        globals()[global_name] = placeholder
    _spec["attr_placeholders"] = attr_placeholders


def _load_heavy_module(module_name: str) -> None:
    spec = _HEAVY_IMPORTS[module_name]
    if spec["event"].is_set():
        if spec.get("error") is not None:
            raise spec["error"]
        return
    with spec["lock"]:
        if spec["event"].is_set():
            if spec.get("error") is not None:
                raise spec["error"]
            return
        try:
            module = importlib.import_module(module_name)
            if spec.get("alias"):
                spec["resolved_alias"] = module
                globals()[spec["alias"]] = module
            for attr_name, global_name in spec.get("attrs", {}).items():
                value = getattr(module, attr_name)
                spec["resolved_attrs"][attr_name] = value
                globals()[global_name] = value
            spec["event"].set()
        except Exception as exc:  # pragma: no cover - defensive
            spec["error"] = exc
            spec["event"].set()
            raise


def _background_imports() -> None:
    for module_name in _HEAVY_IMPORTS:
        try:
            _load_heavy_module(module_name)
        except Exception:
            # Errors are stored in the spec and will be re-raised on demand.
            pass


_background_thread_started = False


def _start_background_imports() -> None:
    """Warm up heavy optional imports when a background thread is desired."""

    global _background_thread_started
    if _background_thread_started:
        return
    threading.Thread(target=_background_imports, daemon=True).start()
    _background_thread_started = True


def _ensure_heavy_imports_loaded() -> None:
    """Synchronously import heavy modules before parallel processing is used."""

    for module_name in _HEAVY_IMPORTS:
        _load_heavy_module(module_name)

warnings.filterwarnings("ignore", category=RuntimeWarning)

IS_WINDOWS = sys.platform.startswith("win")
VERSION = "1.0"
MIN_SEEDS = 1000


def _default_parallel_jobs() -> int:
    return max(1, os.cpu_count() or 1)
MLRX_HOMEPAGE_URL = "https://jacksonalcazar.github.io/MLR-X"
MANUAL_URL = "https://mega.nz/file/aJ9S0awL#i1BiWQaiTTtV0Luo44mdn4BFntyKktDEQ01FyL7TnCE"
BUG_REPORT_URL = "https://github.com/Jacksonalcazar/MLR-X/issues"
PAYPAL_DONATION_URL = "https://www.paypal.com/donate/?hosted_button_id=TTWN9EKMWAHFG"

CITATION_TEXT = (
    "Alcázar, Jackson J., \"MLR-X 1.0: A Scalable Software for Multiple Linear Regression "
    "on Small and Large Datasets\", - the rest will be added when the article is published."
)
CITATION_BIB = """@software{alcazar_mlr_x_1_0,
  author = {Alcázar, Jackson J.},
  title = {{MLR-X 1.0: A Scalable Software for Multiple Linear Regression on Small and Large Datasets}},
  year = {2025},
  version = {1.0},
  note = {Additional details will be provided after publication.}
}
"""


def _coerce_parallel_jobs(value: int) -> int:
    return max(1, int(value))


def _apply_parallel_jobs_policy(value: int, origin: Optional[str] = None) -> tuple[int, Optional[str]]:
    requested = max(1, int(value))
    sanitized = _coerce_parallel_jobs(requested)
    return sanitized, None


def _compute_combination_total(predictor_count: int, max_vars: int) -> int:
    if predictor_count <= 0 or max_vars <= 0:
        return 0
    max_size = min(max_vars, predictor_count)
    total = sum(math.comb(predictor_count, k) for k in range(1, max_size + 1))
    return max(int(total), 0)


def _combination_efficiency_threshold(max_vars: int) -> int:
    if max_vars <= 4:
        return 2_000_000
    return int((8 * max_vars - 30) * 1_000_000)


def _recommend_search_method(predictor_count: int, max_vars: int) -> str:
    total_combinations = _compute_combination_total(predictor_count, max_vars)
    threshold = _combination_efficiency_threshold(max_vars)
    return "all_subsets" if total_combinations <= threshold else "eprs"


COVARIANCE_DISPLAY_TO_KEY: dict[str, str] = {
    "Non robust": "nonrobust",
    "HC0": "HC0",
    "HC1": "HC1",
    "HC2": "HC2",
    "HC3": "HC3",
}

COVARIANCE_KEY_TO_DISPLAY: dict[str, str] = {
    value: key for key, value in COVARIANCE_DISPLAY_TO_KEY.items()
}

COVARIANCE_KEY_NORMALIZED: dict[str, str] = {
    key.lower(): key for key in COVARIANCE_KEY_TO_DISPLAY
}

COVARIANCE_DEFAULT_KEY = "nonrobust"

# Iteration mode options
ITERATION_MODE_AUTO = "auto"
ITERATION_MODE_MANUAL = "manual"
ITERATION_MODE_CONVERGE = "converge"

# Model evaluation labels and thresholds
DEFAULT_EXPORT_LIMIT = 5000
R_SQUARED_SYMBOL = "R²"
Q_SQUARED_SYMBOL = "Q²"
STANDARD_ERROR_SYMBOL = "\U0001D460"
RESULTS_STANDARD_ERROR_LABEL = "s"
DEFAULT_SORT_LABEL = f"{R_SQUARED_SYMBOL}/{Q_SQUARED_SYMBOL}cv/{Q_SQUARED_SYMBOL}F3"

LINEAR_FIT_COLOR_WIDTH = 7

TARGET_METRIC_DISPLAY = {
    "R2": R_SQUARED_SYMBOL,
    "R2_adj": f"adj-{R_SQUARED_SYMBOL}",
    "R2_loo": f"{Q_SQUARED_SYMBOL} (LOO)",
    "RMSE_loo": "RMSE (LOO)",
}

CONFIG_TARGET_METRIC_DISPLAY = {
    "R2": "R2",
    "R2_adj": "adj-R2",
    "R2_loo": "Q2 (LOO)",
    "RMSE_loo": "RMSE (LOO)",
}

RESULTS_SORT_DISPLAY_TO_KEY = {
    DEFAULT_SORT_LABEL: "R2",
    f"adj-{R_SQUARED_SYMBOL}": "adj-R2",
    "RMSE": "RMSE",
    RESULTS_STANDARD_ERROR_LABEL: "s",
    "MAE": "MAE",
    "N pred": "N var",
    "VIFmax": "VIFmax",
    "VIFavg": "VIFavg",
    "Q2F2": "Q2F2",
    "Q2F1": "Q2F1",
}

RESULTS_SORT_LEGACY_DISPLAY_TO_KEY = {
    f"{R_SQUARED_SYMBOL} adj": "adj-R2",
}

RESULTS_SORT_KEY_TO_DISPLAY = {
    value: key for key, value in RESULTS_SORT_DISPLAY_TO_KEY.items()
}

TARGET_METRIC_DISPLAY_TO_KEY = {value: key for key, value in TARGET_METRIC_DISPLAY.items()}
LOO_TARGET_METRICS = {"R2_loo", "RMSE_loo"}


def _metric_score(metric_key: str, metrics: dict[str, float]) -> float:
    """Return a comparable, higher-is-better score for the target metric."""

    value = metrics.get(metric_key)
    if value is None or not np.isfinite(value):
        return float("-inf")

    if metric_key == "RMSE_loo":
        if value <= 0:
            return float("-inf")
        return float(-math.log(value))

    return float(value)


def _format_threshold_display(threshold: object) -> str:
    if threshold is None:
        return "none"
    try:
        numeric = float(threshold)
    except (TypeError, ValueError):
        return "none"
    if not np.isfinite(numeric):
        return "none"
    return f"{numeric:.2f}"


def _metric_threshold_value(config: EPRSConfig) -> float:
    """Normalize the user cutoff for the selected target metric."""

    threshold_raw = getattr(config, "tm_cutoff", None)
    if threshold_raw is None:
        return float("-inf")

    threshold = float(threshold_raw)
    metric_key = getattr(config, "target_metric", "R2")

    if metric_key == "RMSE_loo":
        if threshold <= 0:
            raise ValueError("Target metric cutoff must be greater than zero for RMSE (LOO).")
        return float(-math.log(threshold))

    return threshold

METHOD_DISPLAY_TO_KEY = {
    "All subsets (traditional)": "all_subsets",
    "EPR-S": "eprs",
}

METHOD_KEY_TO_DISPLAY = {value: key for key, value in METHOD_DISPLAY_TO_KEY.items()}


def norm_ppf(probs: np.ndarray | float) -> np.ndarray | float:
    """Approximate the standard normal quantile using Acklam's algorithm."""

    def _ppf(p: np.ndarray) -> np.ndarray:
        # Peter John Acklam's rational approximation of the inverse normal CDF.
        # https://stackedboxes.org/2017/05/01/acklams-normal-quantile-function/
        p = np.asarray(p, dtype=np.float64)
        mask = p < 0.5
        p = np.where(mask, p, 1.0 - p)

        # Coefficients for the approximation
        a1 = -3.969683028665376e+01
        a2 = 2.209460984245205e+02
        a3 = -2.759285104469687e+02
        a4 = 1.383577518672690e+02
        a5 = -3.066479806614716e+01
        a6 = 2.506628277459239e+00

        b1 = -5.447609879822406e+01
        b2 = 1.615858368580409e+02
        b3 = -1.556989798598866e+02
        b4 = 6.680131188771972e+01
        b5 = -1.328068155288572e+01

        c1 = -7.784894002430293e-03
        c2 = -3.223964580411365e-01
        c3 = -2.400758277161838e+00
        c4 = -2.549732539343734e+00
        c5 = 4.374664141464968e+00
        c6 = 2.938163982698783e+00

        d1 = 7.784695709041462e-03
        d2 = 3.224671290700398e-01
        d3 = 2.445134137142996e+00
        d4 = 3.754408661907416e+00

        # Define breakpoints
        p_low = 0.02425
        p_high = 1 - p_low

        result = np.zeros_like(p)

        # Rational approximation for lower region
        mask_low = p < p_low
        if np.any(mask_low):
            q = np.sqrt(-2 * np.log(p[mask_low]))
            result[mask_low] = (((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6) / (
                ((((d1 * q + d2) * q + d3) * q + d4) * q + 1)
            )

        # Rational approximation for central region
        mask_central = (p >= p_low) & (p <= p_high)
        if np.any(mask_central):
            q = p[mask_central] - 0.5
            r = q * q
            result[mask_central] = (
                (((((a1 * r + a2) * r + a3) * r + a4) * r + a5) * r + a6) * q
                / (((((b1 * r + b2) * r + b3) * r + b4) * r + b5) * r + 1)
            )

        # Rational approximation for upper region
        mask_high = p > p_high
        if np.any(mask_high):
            q = np.sqrt(-2 * np.log(1 - p[mask_high]))
            result[mask_high] = -(
                (((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6)
                / ((((d1 * q + d2) * q + d3) * q + d4) * q + 1)
            )

        # Apply sign
        result = np.where(mask, result, -result)
        return result

    probs_array = np.asarray(probs)
    result = _ppf(probs_array)
    if np.isscalar(probs):
        return float(result)
    return result


class AnalysisCancelled(Exception):
    """Raised when the user stops the EPRS-S analysis."""


class EventLike(Protocol):
    """Minimal interface shared by event objects."""

    def is_set(self) -> bool:  # pragma: no cover - protocol definition
        ...

    def set(self) -> None:  # pragma: no cover - protocol definition
        ...

    def clear(self) -> None:  # pragma: no cover - protocol definition
        ...


@dataclass
class EPRSConfig:
    data_path: str = "data.csv"
    delimiter: str = ";"
    dependent_choice: str = "last"
    non_variable_spec: str = "1"
    exclude_constant: bool = False
    constant_threshold: float = 90.0
    excluded_observations: str = ""
    max_vars: int = 1
    n_seeds: int = MIN_SEEDS
    seed_size: int = 1
    random_state: int = 42
    allow_small_seed_count: bool = False
    cov_type: str = COVARIANCE_DEFAULT_KEY
    signif_lvl: float = 0.05
    corr_threshold: float = 0.90
    vif_threshold: float = 4.0
    tm_cutoff: Optional[float] = 0.80
    n_jobs: int = field(default_factory=_default_parallel_jobs)
    iterations_mode: str = ITERATION_MODE_AUTO
    max_iterations_per_seed: Optional[int] = None
    clip_predictions: Optional[tuple[float, float]] = None
    export_limit: int = DEFAULT_EXPORT_LIMIT
    target_metric: str = "R2"
    method: str = "all_subsets"

    def __post_init__(self) -> None:
        self.n_jobs = _coerce_parallel_jobs(self.n_jobs)


@dataclass
class EPRSContext:
    df_full: pd.DataFrame
    train_df: pd.DataFrame
    X: pd.DataFrame
    y: pd.Series
    abs_corr: pd.DataFrame
    abs_corr_y: pd.Series
    X_np: np.ndarray
    y_np: np.ndarray
    cols: list[str]
    col_idx: dict[str, int]
    test_df: Optional[pd.DataFrame]
    X_test_np: Optional[np.ndarray]
    y_test_np: Optional[np.ndarray]
    id_column: str
    observation_column: str
    target_column: str
    full_model_mse: float
    external_df: Optional[pd.DataFrame] = None
    external_X_np: Optional[np.ndarray] = None
    external_y_np: Optional[np.ndarray] = None
    non_variable_columns: tuple[str, ...] = ()
    primary_non_variable_column: Optional[str] = None


@dataclass(frozen=True)
class _ResultsColumnLayout:
    """Definition of a resizable results table column."""

    name: str
    min_width: int
    weight: float = 0.0


class AxisTrace:
    def __init__(self, name: str, verbose: bool = True):
        self.name = name
        self.lines: list[str] = []
        self.verbose = verbose

    def log(self, msg: str) -> None:
        line = f"[{self.name}] {msg}"
        self.lines.append(line)
        if self.verbose:
            print(line)

    def dump(self, filepath: str) -> None:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write("\n".join(self.lines))


def apply_dec(value: float, dec: int) -> float:
    """Round respecting decimal count or integer if dec < 0."""

    return round(value, dec) if dec >= 0 else round(value)


def base_unit(dec: int) -> float:
    """Return 10^(-dec)."""

    return 10.0 ** (-dec)


def compute_dec(vmin: float, vmax: float, trace: AxisTrace) -> int:
    """Compute decimals from range size."""

    rng = abs(vmax - vmin)
    trace.log(f"vmin={vmin}, vmax={vmax}, range={rng}")
    if rng <= 0 or not math.isfinite(rng):
        trace.log("Invalid range ? dec=0")
        return 0
    dec = int(round(-math.log10(rng) + 1))
    trace.log(f"dec = round(-log10(range)+1) = {dec}")
    return dec


def make_candidates(vmin: float, vmax: float, dec: int, trace: AxisTrace):
    b = base_unit(dec)
    trace.log(f"base_unit b = 10^(-dec) = {b}")

    dmin_i = apply_dec(vmin, dec)
    if dmin_i <= vmin:
        dmin_max = dmin_i
    else:
        dmin_max = dmin_i - b
    trace.log(f"dmin_max = {dmin_max}")
    
    dmin_min = (dmin_max // b) * b
    increment = 0.1 * b
    
    dmin_cand = []    
    stop = dmin_min - increment / 2
    values = np.arange(dmin_max, stop, -increment)
    dmin_cand = values.tolist()         
    trace.log(f"dmin_cand ({len(dmin_cand)}): {dmin_cand}")

    dmax_i = apply_dec(vmax, dec)
    if dmax_i < vmax:
        dmax_min = dmax_i + b
    else:
        dmax_min = dmax_i
    trace.log(f"dmax_min = {dmax_min}")

    dmax_max = (dmax_min // b) * b + b
    increment = 0.1 * b
    
    dmax_cand = []    
    stop = dmax_max + increment / 2
    values = np.arange(dmax_min, stop, increment)
    dmax_cand = values.tolist()    
    trace.log(f"dmax_cand ({len(dmax_cand)}): {dmax_cand}")

    dstep_cand = [1.0 * b, 1.5 * b, 2.0 * b, 2.5 * b, 3.0 * b, 4.0 * b, 5.0 * b]
    trace.log(f"dstep_cand: {dstep_cand}")

    def dedup(seq):
        seen, out = set(), []
        for x in seq:
            if x not in seen and math.isfinite(x):
                seen.add(x)
                out.append(x)
        return out
    return dedup(dmin_cand), dedup(dmax_cand), dstep_cand, dmin_max, dmax_min


def ns_relaxed_integer(ns: float, *, tolerance: float = 0.0) -> int:
    """Return the nearest integer when ``ns`` is within ``tolerance``."""

    nearest = round(ns)
    if abs(ns - nearest) <= max(tolerance, 0.0):
        return nearest
    raise ValueError("Not integer-like")


def ns_is_valid(dmin: float, dmax: float, dstep: float, trace: AxisTrace) -> bool:
    if dstep <= 0 or dmax <= dmin:
        return False
    ns = (dmax - dmin) / dstep
    try:
        ns_int = ns_relaxed_integer(ns, tolerance=1e-9)
    except ValueError:
        return False
    ok = 3 <= ns_int <= 7
    trace.log(f"Check ns=({dmax}-{dmin})/{dstep}={ns} ~ {ns_int} ? {ok}")
    return ok


def iterate_cmm_pairs(n_min: int, n_max: int, trace: AxisTrace) -> List[Tuple[int, int]]:
    """
    Generate index pairs (i_max, i_min) following the 'square levels' logic inspired by gen_cmm(n):
      level = 1..L (L = max(n_min, n_max))
      for i in 1..level:
        for j in 1..level:
          combo = (i-1, j-1)  # 0-based indices
    Keep only pairs within bounds and deduplicate preserving first occurrence.

    n_min = len(dmin_cand); n_max = len(dmax_cand)
    RETURNS pairs as (i_max, i_min).
    """
    order: List[Tuple[int, int]] = []
    seen = set()
    L = max(n_min, n_max)

    for level in range(1, L + 1):
        for i in range(1, level + 1):
            for j in range(1, level + 1):
                i_max = i - 1
                i_min = j - 1
                if 0 <= i_max < n_max and 0 <= i_min < n_min:
                    p = (i_max, i_min)
                    if p not in seen:
                        order.append(p)
                        seen.add(p)

    trace.log(f"Pair order (CMM style, {len(order)}): {order[:30]}{' ...' if len(order)>30 else ''}")
    return order

def find_axis(dvals, name: str, verbose: bool):
    trace = AxisTrace(name, verbose)
    vmin, vmax = min(dvals), max(dvals)
    trace.log(f"Data count={len(dvals)}; vmin={vmin}, vmax={vmax}")

    dec = compute_dec(vmin, vmax, trace)
    dmin_cand, dmax_cand, dstep_cand, dmin_max, dmax_min = make_candidates(vmin, vmax, dec, trace)
    pairs = iterate_cmm_pairs(len(dmin_cand), len(dmax_cand), trace)


    for (i_max, i_min) in pairs:
        dmax_c, dmin_c = dmax_cand[i_max], dmin_cand[i_min]
        for dstep_c in dstep_cand:
            if ns_is_valid(dmin_c, dmax_c, dstep_c, trace):
                trace.log(f"FOUND: dmin={dmin_c}, dmax={dmax_c}, dstep={dstep_c}")
                return dmin_c, dmax_c, dstep_c, dec, trace

    b = base_unit(dec)
    dmin_fb, dmax_fb = dmin_max, dmax_min
    dstep_fb = (dmax_fb - dmin_fb) / 10.0 or b
    trace.log(f"FINAL FALLBACK: dmin={dmin_fb}, dmax={dmax_fb}, dstep={dstep_fb}")
    return dmin_fb, dmax_fb, dstep_fb, dec, trace


def build_ticks(dmin: float, dmax: float, step: float) -> list[float]:
    if step <= 0:
        raise ValueError("Step must be positive")

    ns = (dmax - dmin) / step
    nsteps = ns_relaxed_integer(ns, tolerance=1e-9)
    ticks = [dmin + k * step for k in range(nsteps + 1)]
    ticks[0], ticks[-1] = dmin, dmax
    return ticks


@dataclass
class AxisParameters:
    minimum: float
    maximum: float
    step: float
    decimals: int
    ticks: list[float]
    trace: Optional[AxisTrace] = None


def compute_axis_parameters(values: np.ndarray, name: str, verbose: bool = False) -> Optional[AxisParameters]:
    if values is None:
        return None
    arr = np.asarray(values, dtype=float)
    if arr.ndim != 1:
        arr = arr.flatten()
    arr = arr[np.isfinite(arr)]
    if arr.size == 0:
        return None
    dmin, dmax, step, dec, trace = find_axis(arr.tolist(), name, verbose)
    ticks = build_ticks(dmin, dmax, step)
    return AxisParameters(dmin, dmax, step, dec, ticks, trace)


def apply_axis_to_plot(ax: Axes, axis: str, params: AxisParameters) -> None:
    formatter = FormatStrFormatter(f"%.{max(params.decimals, 0)}f")
    locator = FixedLocator(params.ticks)
    if axis == "x":
        ax.set_xlim(params.minimum, params.maximum)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)
    else:
        ax.set_ylim(params.minimum, params.maximum)
        ax.yaxis.set_major_locator(locator)
        ax.yaxis.set_major_formatter(formatter)


METHOD_INFO_TEXT = {
    "EPR-S": "Up to 1000 predictors or more.",
    "All subsets (traditional)": "Feasible only for small combinatorial spaces.",
}

METADATA_PREFIX = "#Metadata:"
METADATA_SUFFIX = ";#"
METADATA_HEADER_LABEL = "#Metadata:"
LEGACY_METADATA_COLUMNS = {"metadata", METADATA_HEADER_LABEL.strip().lower()}


def _serialize_metadata(metadata: dict[str, object]) -> str:
    return METADATA_PREFIX + json.dumps(metadata, ensure_ascii=False)


def _deserialize_metadata(value: object) -> dict:
    if value is None:
        return {}
    if isinstance(value, bytes):
        try:
            text = value.decode("utf-8", errors="ignore")
        except Exception:  # noqa: BLE001
            text = value.decode(errors="ignore")
    else:
        text = str(value)
    payload = text.strip()
    if not payload:
        return {}
    if payload.endswith(METADATA_SUFFIX):
        payload = payload[: -len(METADATA_SUFFIX)].rstrip()
    if payload.startswith(METADATA_PREFIX):
        payload = payload[len(METADATA_PREFIX) :].strip()
    if not payload:
        return {}
    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _split_csv_line(line: str, delimiter: str) -> list[str]:
    reader = csv.reader([line], delimiter=delimiter)
    try:
        return next(reader)
    except StopIteration:
        return []


def _join_csv_line(values: Sequence[object], delimiter: str) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=delimiter, lineterminator="")
    writer.writerow(values)
    return buffer.getvalue()


def _write_results_csv(
    export_df: pd.DataFrame,
    metadata: dict[str, object],
    export_path: Path,
    *,
    sep: str = ";",
    float_format: str = "%.4f",
) -> Path:
    metadata_value = _serialize_metadata(metadata)
    header_values = list(export_df.columns) + [metadata_value, "#"]

    with export_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=sep)
        writer.writerow(header_values)

        if export_df.empty:
            return export_path

        buffer = io.StringIO()
        export_df.to_csv(
            buffer,
            sep=sep,
            index=False,
            header=False,
            float_format=float_format,
            lineterminator="\n",
        )
        buffer.seek(0)
        for raw_line in buffer:
            raw_line = raw_line.rstrip("\n\r")
            handle.write(f"{raw_line}{sep}{sep}\n")

    return export_path
METADATA_VERSION = 1.0

DELIMITER_NAME_TO_VALUE = {
    "comma": ",",
    "semicolon": ";",
    "tab": "\t",
    "pipe": "|",
}

DELIMITER_VALUE_TO_NAME = {
    value: name for name, value in DELIMITER_NAME_TO_VALUE.items()
}

SETTINGS_NUMERIC_KEYS = [
    "max_vars",
    "n_seeds",
    "seed_size",
    "random_state",
    "max_iterations_per_seed",
    "signif_lvl",
    "corr_threshold",
    "vif_threshold",
    "tm_cutoff",
    "export_limit",
    "n_jobs",
]

DEPENDENT_TO_DISPLAY = {
    "last": "Last column",
    "first": "First column",
    "second": "Second column",
    "third": "Third column",
}

CONFIG_LIST_OPTIONS = {
    "delimiter": [
        (";", ";"),
        (",", ","),
        ("\t", "\\t"),
        ("|", "|"),
    ],
    "dependent_choice": [
        ("last", "Last column"),
        ("first", "First column"),
        ("second", "Second column"),
        ("third", "Third column"),
    ],
    "non_variable_spec": [
        ("", "None"),
        ("1", "First column"),
        ("1,2", "First and second column"),
        ("1,2,3", "First, second, and third column"),
    ],
    "method": [
        (value, label) for label, value in METHOD_DISPLAY_TO_KEY.items()
    ],
    "iterations_mode": [
        (ITERATION_MODE_AUTO, "auto"),
        (ITERATION_MODE_MANUAL, "manual"),
        (ITERATION_MODE_CONVERGE, "Until converge"),
    ],
    "cov_type": [
        (key, label) for label, key in COVARIANCE_DISPLAY_TO_KEY.items()
    ],
    "target_metric": [
        (key, label) for key, label in CONFIG_TARGET_METRIC_DISPLAY.items()
    ],
    "split_mode": [
        ("none", "None (use full dataset)"),
        ("random", "Random"),
        ("manual", "Manual"),
    ],
}

NON_VARIABLE_TO_DISPLAY = {
    "": "None",
    "1": "First column",
    "1,2": "First and second column",
    "1,2,3": "First, second, and third column",
}


def _bool_to_text(value: bool) -> str:
    return "true" if value else "false"


def _bool_to_activation_text(value: bool) -> str:
    return "activated" if value else "deactivated"


def _parse_bool(value: object, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return default
    if text in {"true", "1", "yes", "on", "activated"}:
        return True
    if text in {"false", "0", "no", "off", "deactivated"}:
        return False
    return default


def _option_index(options: list[tuple[str, str]], selected_value: str) -> int:
    for idx, (value, _label) in enumerate(options, start=1):
        if value == selected_value:
            return idx
    return 1


CONFIG_OPTION_TITLES = {
    "delimiter": "Delimiter",
    "dependent_choice": "Dependent variable",
    "non_variable_spec": "Non-variable columns",
    "method": "Search method",
    "iterations_mode": "Iterations per seed mode",
    "cov_type": "Covariance type",
    "target_metric": "Target metric",
    "split_mode": "Split mode",
    "external_delimiter": "External delimiter",
}

OPTION_HEADING_TO_KEY = {
    f"{title} options": key for key, title in CONFIG_OPTION_TITLES.items()
}


CONFIG_FIELD_LABELS: dict[str, str] = {
    "data_path": "Dataset path",
    "exclude_constant": "Exclude constant columns",
    "constant_threshold": "Constant threshold",
    "excluded_observations": "Excluded observation IDs",
    "max_vars": "Max predictors per model",
    "n_seeds": "Number of seeds",
    "seed_size": "Seed size",
    "random_state": "Random state",
    "allow_small_seed_count": "Allow fewer than 1000 seeds",
    "iterations_mode": "Iterations per seed mode",
    "max_iterations_per_seed": "Max iterations per seed",
    "signif_lvl": "Significance level",
    "corr_threshold": "Correlation threshold",
    "vif_threshold": "VIF threshold",
    "tm_cutoff": "Target metric cutoff",
    "export_limit": "Top models to export",
    "n_jobs": "CPU cores to use",
    "clip_enabled": "Clip predictions",
    "clip_low": "Clip lower bound",
    "clip_high": "Clip upper bound",
    "random_test_size_percent": "Random split test size (%)",
    "manual_train_ids": "Manual split training IDs",
    "manual_test_ids": "Manual split testing IDs",
    "external_path": "External split path",
    "output_path": "Output path",
}

CONFIG_FIELD_LOOKUP: dict[str, str] = {
    label.lower(): key for key, label in CONFIG_FIELD_LABELS.items()
}
CONFIG_FIELD_LOOKUP.update({key.lower(): key for key in CONFIG_FIELD_LABELS})


def _display_config_field(key: str) -> str:
    return CONFIG_FIELD_LABELS.get(key, key.replace("_", " ").strip().title())


def _format_section_title(title: str) -> list[str]:
    separators = {
        "Dataset": "--------",
        "Data splitting": "--------------",
        "Settings": "---------",
    }
    separator = separators.get(title, "----------")
    return [separator, title, separator]


def _format_field_line(key: str, value: object) -> str:
    label = CONFIG_FIELD_LABELS[key]
    if value is None:
        text = "none"
    else:
        text = str(value)
        if text == "":
            text = "none"
    return f"{label} = {text}"


def _match_option_heading(line: str) -> Optional[str]:
    normalized = line.rstrip(":").strip().lower()
    for heading, key in OPTION_HEADING_TO_KEY.items():
        if heading.lower() == normalized:
            return key
    return None


def _format_option_block(
    *, key: str, options: list[tuple[str, str]], selected_value: str
) -> list[str]:
    title = CONFIG_OPTION_TITLES[key]
    lines = [f"# {title} options:"]
    for idx, (_value, label) in enumerate(options, start=1):
        lines.append(f"{idx}) {label}")
    selected_index = _option_index(options, selected_value)
    lines.append(f"Selected option: {selected_index}")
    lines.append("")
    return lines


def _build_iterations_mode_options(config: EPRSConfig) -> list[tuple[str, str]]:
    manual_value = getattr(config, "max_iterations_per_seed", None)
    if manual_value in (None, ""):
        manual_display = "none"
    else:
        manual_display = str(manual_value)
    return [
        (ITERATION_MODE_AUTO, "auto"),
        (
            ITERATION_MODE_MANUAL,
            f"manual (Max iterations per seed = {manual_display})",
        ),
        (ITERATION_MODE_CONVERGE, "Until converge"),
    ]


def _normalize_ids_for_config(values: Collection[str]) -> str:
    return MLRXApp._ids_to_text(values)


def _build_split_metadata_for_cli(split_settings: Optional[dict]) -> dict:
    meta = {"mode": "none"}
    if not split_settings:
        return meta
    mode = split_settings.get("mode", "none")
    meta["mode"] = mode
    if mode == "random":
        try:
            fraction = float(split_settings.get("test_size", 0.0))
        except (TypeError, ValueError):
            fraction = 0.0
        meta["test_size_percent"] = fraction * 100.0
    elif mode == "manual":
        meta["train_ids"] = MLRXApp._normalize_id_iterable(split_settings.get("train_ids"))
        meta["test_ids"] = MLRXApp._normalize_id_iterable(split_settings.get("test_ids"))
    return meta


def _resolve_iterations_metadata_value(
    method: str,
    iterations_mode: str,
    manual_iterations: Optional[object],
    avg_iterations_per_seed: Optional[object],
    max_iterations_per_seed: Optional[object],
    *,
    formatter: Callable[[object], str],
) -> str:
    if (method or "").lower() != "eprs":
        return "none"

    mode = (iterations_mode or ITERATION_MODE_AUTO).lower()
    if mode == ITERATION_MODE_CONVERGE:
        return "until converge"

    if mode == ITERATION_MODE_MANUAL:
        if manual_iterations in {None, ""}:
            return "none"
        return formatter(manual_iterations)

    def _to_numeric(value: object) -> Optional[float]:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        return numeric if np.isfinite(numeric) else None

    resolved_value = _to_numeric(max_iterations_per_seed)
    if resolved_value is None:
        avg_numeric = _to_numeric(avg_iterations_per_seed)
        if avg_numeric is not None:
            resolved_value = avg_numeric * 2.0

    if resolved_value is None:
        resolved_value = _to_numeric(manual_iterations)

    if resolved_value is None:
        return "none"

    return formatter(resolved_value)


def _build_cli_metadata(
    config: EPRSConfig,
    split_settings: Optional[dict],
    cpu_search_minutes: Optional[float] = None,
    cpu_total_minutes: Optional[float] = None,
    models_found: Optional[int] = None,
    models_explored: Optional[int] = None,
    avg_iterations_per_seed: Optional[object] = None,
    max_iterations_per_seed: Optional[object] = None,
) -> dict:
    delimiter_value = MLRXApp._normalize_delimiter_value(config.delimiter)
    excluded_obs = (getattr(config, "excluded_observations", "") or "").strip() or "none"
    metadata: dict[str, object] = {
        "version": METADATA_VERSION,
        "dataset_path": config.data_path,
        "delimiter": MLRXApp._serialize_delimiter_value(delimiter_value),
        "dependent": config.dependent_choice,
        "non_variable": config.non_variable_spec,
        "exclude_constant": bool(config.exclude_constant),
        "constant_threshold": float(config.constant_threshold),
        "excluded_observations": excluded_obs,
        "target_metric": config.target_metric,
        "top_models": int(config.export_limit),
        "method": config.method,
        "cov_type": getattr(config, "cov_type", COVARIANCE_DEFAULT_KEY),
        "iterations_mode": getattr(config, "iterations_mode", ITERATION_MODE_AUTO),
    }

    settings_values = [
        MLRXApp._format_numeric_value(getattr(config, key, None))
        for key in SETTINGS_NUMERIC_KEYS
    ]
    metadata["settings_values"] = "#".join(settings_values)
    metadata["split"] = _build_split_metadata_for_cli(split_settings)

    metadata["max_iterations_per_seed"] = _resolve_iterations_metadata_value(
        getattr(config, "method", "all_subsets"),
        getattr(config, "iterations_mode", ITERATION_MODE_AUTO),
        getattr(config, "max_iterations_per_seed", None),
        avg_iterations_per_seed,
        max_iterations_per_seed,
        formatter=MLRXApp._format_numeric_value,
    )

    if config.clip_predictions is not None:
        low, high = config.clip_predictions
        metadata["clip"] = {"enabled": True, "low": float(low), "high": float(high)}
    else:
        metadata["clip"] = {"enabled": False}

    metadata["kfold"] = {"enabled": False, "folds": None, "repeats": None}

    def _store_cpu_time(key_prefix: str, value: Optional[float]) -> None:
        if value is None:
            return
        try:
            numeric = float(value)
        except (TypeError, ValueError):  # noqa: BLE001
            return
        if not np.isfinite(numeric):
            return
        metadata[f"{key_prefix}_minutes"] = float(numeric)
        metadata[f"{key_prefix}_seconds"] = float(numeric * 60.0)

    _store_cpu_time("cpu_time_search", cpu_search_minutes)
    _store_cpu_time("cpu_time_total", cpu_total_minutes)

    if models_found is not None:
        try:
            metadata["models_found"] = int(models_found)
        except (TypeError, ValueError):
            pass

    if models_explored is not None:
        try:
            metadata["models_explored"] = int(models_explored)
        except (TypeError, ValueError):
            pass

    return metadata


def _format_iterations_mode_for_cli(config: EPRSConfig) -> str:
    mode = (getattr(config, "iterations_mode", ITERATION_MODE_AUTO) or ITERATION_MODE_AUTO).lower()
    if mode == ITERATION_MODE_MANUAL:
        manual_value = getattr(config, "max_iterations_per_seed", None)
        if manual_value is not None:
            try:
                manual_value = int(manual_value)
            except (TypeError, ValueError):
                manual_value = str(manual_value)
            return f"Manual (max {manual_value})"
        return "Manual"
    if mode == ITERATION_MODE_CONVERGE:
        return "Until convergence"
    return "Auto"


def write_configuration_file(
    path: Path,
    config: EPRSConfig,
    split_settings: dict,
    *,
    output_path: Union[str, Path] = "models.csv",
) -> None:
    lines: list[str] = [
        "# MLR-X configuration file",
        "# Generated by MLR-X version 1.0",
        "",
    ]

    lines.extend(_format_section_title("Dataset"))
    lines.append(_format_field_line("data_path", config.data_path))
    lines.append("")

    delimiter_options = CONFIG_LIST_OPTIONS["delimiter"]
    lines.extend(
        _format_option_block(
            key="delimiter",
            options=delimiter_options,
            selected_value=config.delimiter,
        )
    )

    dependent_options = CONFIG_LIST_OPTIONS["dependent_choice"]
    lines.extend(
        _format_option_block(
            key="dependent_choice",
            options=dependent_options,
            selected_value=config.dependent_choice,
        )
    )

    non_variable_options = CONFIG_LIST_OPTIONS["non_variable_spec"]
    lines.extend(
        _format_option_block(
            key="non_variable_spec",
            options=non_variable_options,
            selected_value=config.non_variable_spec,
        )
    )

    lines.append(_format_field_line("exclude_constant", _bool_to_text(config.exclude_constant)))
    lines.append(_format_field_line("constant_threshold", config.constant_threshold))
    lines.append(
        _format_field_line("excluded_observations", config.excluded_observations)
    )

    lines.append("")
    lines.extend(_format_section_title("Data splitting"))
    split_mode = split_settings.get("mode", "none")
    split_options = CONFIG_LIST_OPTIONS["split_mode"]
    lines.extend(
        _format_option_block(
            key="split_mode",
            options=split_options,
            selected_value=split_mode,
        )
    )

    if split_mode == "random":
        fraction = float(split_settings.get("test_size", 0.0))
        lines.append(
            _format_field_line("random_test_size_percent", fraction * 100.0)
        )
    elif split_mode == "manual":
        train_ids = split_settings.get("train_ids", set())
        test_ids = split_settings.get("test_ids", set())
        lines.append(
            _format_field_line(
                "manual_train_ids", _normalize_ids_for_config(train_ids)
            )
        )
        lines.append(
            _format_field_line(
                "manual_test_ids", _normalize_ids_for_config(test_ids)
            )
        )

    if lines[-1] != "":
        lines.append("")

    lines.extend(_format_section_title("Settings"))
    method_options = CONFIG_LIST_OPTIONS["method"]
    lines.extend(
        _format_option_block(
            key="method",
            options=method_options,
            selected_value=config.method,
        )
    )

    target_options = CONFIG_LIST_OPTIONS["target_metric"]
    lines.extend(
        _format_option_block(
            key="target_metric",
            options=target_options,
            selected_value=config.target_metric,
        )
    )

    cov_options = CONFIG_LIST_OPTIONS["cov_type"]
    lines.extend(
        _format_option_block(
            key="cov_type",
            options=cov_options,
            selected_value=getattr(config, "cov_type", COVARIANCE_DEFAULT_KEY),
        )
    )

    lines.append(_format_field_line("max_vars", config.max_vars))
    lines.append(_format_field_line("signif_lvl", config.signif_lvl))
    lines.append(_format_field_line("corr_threshold", config.corr_threshold))
    lines.append(_format_field_line("vif_threshold", config.vif_threshold))
    lines.append(_format_field_line("tm_cutoff", config.tm_cutoff))
    lines.append(_format_field_line("export_limit", config.export_limit))
    lines.append(_format_field_line("n_jobs", config.n_jobs))
    lines.append("")
    lines.append("# Seed settings for EPR-S")
    lines.append(_format_field_line("n_seeds", config.n_seeds))
    lines.append(_format_field_line("seed_size", config.seed_size))
    lines.append(_format_field_line("random_state", config.random_state))
    lines.append(
        _format_field_line(
            "allow_small_seed_count", _bool_to_activation_text(config.allow_small_seed_count)
        )
    )
    lines.append("")
    iterations_mode_options = _build_iterations_mode_options(config)
    lines.extend(
        _format_option_block(
            key="iterations_mode",
            options=iterations_mode_options,
            selected_value=config.iterations_mode,
        )
    )
    if config.iterations_mode == ITERATION_MODE_MANUAL:
        lines.append(
            _format_field_line(
                "max_iterations_per_seed", getattr(config, "max_iterations_per_seed", "")
            )
        )
        lines.append("")

    lines.append("# Prediction clipping")
    clip_enabled = config.clip_predictions is not None
    lines.append(_format_field_line("clip_enabled", _bool_to_text(clip_enabled)))
    if clip_enabled:
        low, high = config.clip_predictions  # type: ignore[misc]
        lines.append(_format_field_line("clip_low", low))
        lines.append(_format_field_line("clip_high", high))
    else:
        lines.append(_format_field_line("clip_low", ""))
        lines.append(_format_field_line("clip_high", ""))

    lines.append("")
    lines.append("-------")
    lines.append("Output")
    lines.append("-------")
    lines.append(_format_field_line("output_path", Path(output_path)))

    contents = "\n".join(lines) + "\n"
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(contents)


def _normalize_delimiter_from_config(value: str) -> str:
    value = value.strip()
    if value == "\\t":
        return "\t"
    if value.lower() in DELIMITER_NAME_TO_VALUE:
        return DELIMITER_NAME_TO_VALUE[value.lower()]
    return value


def parse_configuration_file(path: Union[str, Path]) -> tuple[EPRSConfig, dict, Path]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    values: dict[str, str] = {}
    option_indices: dict[str, int] = {}
    current_option_key: Optional[str] = None

    with open(config_path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                current_option_key = None
                continue

            if line.startswith("#"):
                heading_text = line[1:].strip()
                matched_key = _match_option_heading(heading_text)
                current_option_key = matched_key
                continue

            if current_option_key:
                selection_match = re.match(
                    r"selected option\s*:\s*(.*)", line, flags=re.IGNORECASE
                )
                if selection_match:
                    raw_value = selection_match.group(1)
                    try:
                        index = int(float(raw_value.strip()))
                    except ValueError as exc:  # noqa: BLE001
                        raise ValueError(
                            f"Invalid selection index for {current_option_key}: {raw_value.strip()}"
                        ) from exc
                    option_indices[current_option_key] = index
                    current_option_key = None
                # Ignore option listing lines like "1) Choice"
                continue

            if line.startswith("#"):
                continue

            kv_match = re.match(r"([^:=#]+?)\s*[:=]\s*(.*)", line)
            if not kv_match:
                continue

            key = kv_match.group(1)
            raw_value = kv_match.group(2)
            normalized_key = key.strip().rstrip(":").lower()
            internal_key = CONFIG_FIELD_LOOKUP.get(normalized_key)
            if not internal_key:
                continue
            values[internal_key] = raw_value.strip()

    for option_key, index in option_indices.items():
        if option_key == "external_delimiter":
            options = CONFIG_LIST_OPTIONS["delimiter"]
        else:
            options = CONFIG_LIST_OPTIONS.get(option_key)
        if not options:
            raise ValueError(f"Unknown configuration option block: {option_key}")
        if index < 1 or index > len(options):
            raise ValueError(f"Selected option out of range for {option_key}: {index}")
        selected_value = options[index - 1][0]
        if option_key in {"delimiter", "external_delimiter"} and selected_value == "\t":
            values[option_key] = "\\t"
        else:
            values[option_key] = selected_value

    def _require(key: str) -> str:
        if key not in values or not values[key]:
            raise ValueError(
                f"Missing required configuration value: {_display_config_field(key)}"
            )
        return values[key]

    data_path = _require("data_path")
    delimiter_raw = values.get("delimiter", ";")
    delimiter = _normalize_delimiter_from_config(delimiter_raw)
    dependent_choice = values.get("dependent_choice", "last")
    non_variable_spec = values.get("non_variable_spec", "")
    exclude_constant = _parse_bool(values.get("exclude_constant"), default=False)
    constant_threshold = float(values.get("constant_threshold", 90.0))
    excluded_observations_raw = values.get("excluded_observations", "")
    if str(excluded_observations_raw).strip().lower() == "none":
        excluded_observations = ""
    else:
        excluded_observations = excluded_observations_raw

    method = values.get("method", "all_subsets")
    if method not in {value for value, _label in CONFIG_LIST_OPTIONS["method"]}:
        raise ValueError(f"Unknown search method: {method}")

    cov_type = values.get("cov_type", COVARIANCE_DEFAULT_KEY)
    if cov_type not in {value for value, _label in CONFIG_LIST_OPTIONS["cov_type"]}:
        raise ValueError(f"Unknown covariance type: {cov_type}")

    target_metric = values.get("target_metric", "R2")
    if target_metric not in {value for value, _label in CONFIG_LIST_OPTIONS["target_metric"]}:
        raise ValueError(f"Unknown target metric: {target_metric}")

    iterations_mode = values.get("iterations_mode", ITERATION_MODE_AUTO).lower()
    if iterations_mode not in {
        ITERATION_MODE_AUTO,
        ITERATION_MODE_MANUAL,
        ITERATION_MODE_CONVERGE,
    }:
        iterations_mode = ITERATION_MODE_AUTO
    manual_iterations: Optional[int] = None
    if iterations_mode == ITERATION_MODE_MANUAL:
        manual_text = values.get("max_iterations_per_seed", "")
        if not manual_text:
            raise ValueError(
                "Missing required value: Max iterations per seed when manual mode is selected",
            )
        manual_iterations = int(float(manual_text))
        if manual_iterations <= 0:
            raise ValueError("Max iterations per seed must be greater than zero")

    def _parse_positive_int(
        key: str,
        default: Optional[int] = None,
        *,
        min_value: Optional[int] = None,
        min_warning: Optional[str] = None,
    ) -> int:
        field_name = _display_config_field(key)
        text = values.get(key)
        if text in (None, ""):
            if default is None:
                raise ValueError(f"Missing required value: {field_name}")
            return default
        value_int = int(float(text))
        if value_int <= 0:
            raise ValueError(f"{field_name} must be greater than zero")
        if min_value is not None and value_int < min_value:
            warning_text = min_warning or f"{field_name} must be at least {min_value}."
            print(f"Warning: {warning_text} Resetting to {min_value}.")
            return min_value
        return value_int

    max_vars = _parse_positive_int("max_vars")
    allow_small_seed_count = _parse_bool(values.get("allow_small_seed_count", False))
    seed_minimum = None if allow_small_seed_count else MIN_SEEDS
    n_seeds = _parse_positive_int(
        "n_seeds",
        min_value=seed_minimum,
        min_warning="Only values greater than 1000 are allowed.",
    )
    seed_size = _parse_positive_int("seed_size")
    random_state = _parse_positive_int("random_state", default=42)
    signif_lvl = float(values.get("signif_lvl", 0.05))
    corr_threshold = float(values.get("corr_threshold", 0.90))
    vif_threshold = float(values.get("vif_threshold", 4.0))
    report_r2_raw = values.get("tm_cutoff", 0.80)
    if str(report_r2_raw).strip().lower() == "none":
        tm_cutoff: Optional[float] = None
    else:
        tm_cutoff = float(report_r2_raw)
        if target_metric == "RMSE_loo" and tm_cutoff <= 0:
            raise ValueError("Target metric cutoff must be greater than zero for RMSE (LOO).")
    export_limit = _parse_positive_int("export_limit", DEFAULT_EXPORT_LIMIT)
    default_jobs = _default_parallel_jobs()
    n_jobs_requested = _parse_positive_int("n_jobs", default_jobs)
    n_jobs, n_jobs_message = _apply_parallel_jobs_policy(
        n_jobs_requested, origin="the configuration file"
    )
    if n_jobs_message:
        print(n_jobs_message)

    clip_enabled = _parse_bool(values.get("clip_enabled"), default=False)
    clip_predictions: Optional[tuple[float, float]] = None
    if clip_enabled:
        clip_low_text = _require("clip_low")
        clip_high_text = _require("clip_high")
        clip_low = float(clip_low_text)
        clip_high = float(clip_high_text)
        if clip_low >= clip_high:
            raise ValueError(
                f"{_display_config_field('clip_low')} must be smaller than {_display_config_field('clip_high')}"
            )
        clip_predictions = (clip_low, clip_high)

    split_mode = values.get("split_mode", "none")
    if split_mode not in {value for value, _label in CONFIG_LIST_OPTIONS["split_mode"]}:
        raise ValueError(f"Unknown split mode: {split_mode}")

    split_settings: dict[str, object] = {"mode": split_mode}
    if split_mode == "random":
        percent_text = values.get("random_test_size_percent", "20.0")
        percent = float(percent_text)
        if not 0.0 < percent < 100.0:
            raise ValueError(
                f"{_display_config_field('random_test_size_percent')} must be between 0 and 100"
            )
        split_settings["test_size"] = percent / 100.0
    elif split_mode == "manual":
        train_ids_text = values.get("manual_train_ids", "")
        test_ids_text = values.get("manual_test_ids", "")
        train_ids = _parse_id_entries(train_ids_text)
        test_ids = _parse_id_entries(test_ids_text)
        split_settings["train_ids"] = train_ids
        split_settings["test_ids"] = test_ids
    elif split_mode == "external":
        raise ValueError(
            "External validation datasets are configured in the Validation tab; "
            "choose none, random, or manual for data splitting."
        )

    output_path_text = values.get("output_path", "models.csv")
    output_path = Path(output_path_text)

    config = EPRSConfig(
        data_path=data_path,
        delimiter=delimiter,
        dependent_choice=dependent_choice,
        non_variable_spec=non_variable_spec,
        exclude_constant=exclude_constant,
        constant_threshold=constant_threshold,
        excluded_observations=excluded_observations,
        max_vars=max_vars,
        n_seeds=n_seeds,
        seed_size=seed_size,
        random_state=random_state,
        allow_small_seed_count=allow_small_seed_count,
        cov_type=cov_type,
        signif_lvl=signif_lvl,
        corr_threshold=corr_threshold,
        vif_threshold=vif_threshold,
        tm_cutoff=tm_cutoff,
        n_jobs=n_jobs,
        clip_predictions=clip_predictions,
        export_limit=export_limit,
        target_metric=target_metric,
        method=method,
        iterations_mode=iterations_mode,
        max_iterations_per_seed=manual_iterations,
    )

    return config, split_settings, output_path


def export_results_to_csv_cli(
    results_df: pd.DataFrame,
    config: EPRSConfig,
    split_settings: Optional[dict],
    output_path: Union[str, Path],
    *,
    cpu_search_minutes: Optional[float] = None,
    cpu_total_minutes: Optional[float] = None,
    models_found: Optional[int] = None,
    models_explored: Optional[int] = None,
    avg_iterations_per_seed: Optional[object] = None,
    max_iterations_per_seed: Optional[object] = None,
) -> Path:
    export_path = Path(output_path)
    export_path.parent.mkdir(parents=True, exist_ok=True)

    export_limit = int(config.export_limit)
    export_df = results_df.head(export_limit).copy()

    if "Variables" in export_df.columns:
        var_lists = export_df["Variables"].apply(MLRXApp._normalize_variables)
    else:
        var_lists = pd.Series([[] for _ in range(len(export_df))], index=export_df.index)

    export_df["N_pred"] = var_lists.apply(len)
    export_df["Predictors"] = var_lists.apply(
        lambda items: ", ".join(items) if items else "-"
    )

    columns = [
        "Model",
        "Predictors",
        "N_pred",
        "R2",
        "RMSE",
        "s",
        "MAE",
        "R2_adj",
        "VIF_max",
        "VIF_avg",
        "R2_loo",
        "RMSE_loo",
        "s_loo",
        "MAE_loo",
        "R2_kfold",
        "RMSE_kfold",
        "s_kfold",
        "MAE_kfold",
        "Q2F1_ext",
        "Q2F2_ext",
        "Q2F3_ext",
        "RMSE_ext",
        "s_ext",
        "MAE_ext",
    ]

    for column in columns:
        if column not in export_df.columns:
            export_df[column] = pd.NA

    export_df = export_df.loc[:, columns]

    metadata = _build_cli_metadata(
        config,
        split_settings,
        cpu_search_minutes,
        cpu_total_minutes,
        models_found,
        models_explored,
        avg_iterations_per_seed,
        max_iterations_per_seed,
    )
    _write_results_csv(export_df, metadata, export_path, sep=";", float_format="%.4f")

    return export_path


def run_cli(config_path: Union[str, Path]) -> None:
    _ensure_heavy_imports_loaded()

    print(f"Loaded configuration from {config_path}")
    config, split_settings, output_path = parse_configuration_file(config_path)

    context = load_dataset(
        config.data_path,
        delimiter=config.delimiter,
        split=split_settings,
        dependent_choice=config.dependent_choice,
        non_variable_spec=config.non_variable_spec,
        exclude_constant=config.exclude_constant,
        constant_threshold=config.constant_threshold,
        excluded_observations=config.excluded_observations,
    )

    total_combos = _compute_combination_total(len(context.cols), config.max_vars)
    threshold = _combination_efficiency_threshold(config.max_vars)
    warnings: list[str] = []
    if config.method == "eprs" and total_combos <= threshold:
        warnings.append(
            "Warning: The current configuration is outside the recommended EPR-S "
            "thresholds. The analysis will proceed, but it is recommended to use the "
            "'All subsets' method for improved efficiency."
        )
    if config.method == "all_subsets" and total_combos > threshold:
        warnings.append(
            "Warning: This configuration may require substantial computation time. "
            "It is recommended to switch to the EPR-S method for better efficiency."
        )
    if config.allow_small_seed_count and config.n_seeds < MIN_SEEDS:
        warnings.append(
            "Warning: The current run is using fewer than 1000 seeds; "
            "method performance may be compromised."
        )
    if warnings:
        print()
        for index, warning in enumerate(warnings):
            print(warning)
            if index < len(warnings) - 1:
                print()
        print()

    print("*" * 30)
    print(f"Dataset: {Path(config.data_path).name}")
    print(f"Predictors available: {len(context.cols)}")
    method_display = METHOD_KEY_TO_DISPLAY.get(config.method, config.method)
    print(f"Search method: {method_display}")
    if (config.method or "").lower() == "eprs":
        print(f"Max predictors per model: {config.max_vars}")
        print(f"Number of seeds: {config.n_seeds}")
        print(f"Seed size: {config.seed_size}")
        print(f"Random state: {config.random_state}")
        iteration_mode_desc = _format_iterations_mode_for_cli(config)
        print(f"Iterations per seed mode: {iteration_mode_desc}")
    elif (config.method or "").lower() == "all_subsets":
        print(f"Max predictors per model: {config.max_vars}")
    print("*" * 30)

    print("Analysis initiated with the user-provided configuration.")
    print("Press Ctrl+C to cancel analysis.")
    print()

    last_stage = {"value": None}
    progress_state = {"active": False, "last_len": 0, "last_text": ""}

    def progress_callback(message: str) -> None:
        if "calibration" in message.lower():
            return
        if progress_state["active"]:
            sys.stdout.write("\r" + " " * progress_state["last_len"] + "\r")
            sys.stdout.flush()
            progress_state["active"] = False
        print(message)

    def _format_stage_label(stage: int) -> str:
        if stage >= 2:
            return "Running (VIF calculations)"
        if config.method == "all_subsets":
            return "Running (model evaluation)"
        return "Running (model searching)"

    def progress_hook(done: int, total: int, stage: int = 1) -> None:
        total = max(total, 1)
        percent = (done / total) * 100.0
        stage = max(1, min(stage, 2))
        if last_stage["value"] is None:
            last_stage["value"] = stage
        if stage != last_stage["value"]:
            if progress_state["active"] and progress_state["last_len"]:
                sys.stdout.write("\n")
                sys.stdout.flush()
            last_stage["value"] = stage
            progress_state["last_text"] = ""
        status_text = _format_stage_label(stage)
        progress_text = f"({stage}/2) Progress: {percent:5.1f}% - {status_text}"
        if progress_state["last_text"] == progress_text:
            return
        padded_text = progress_text.ljust(progress_state["last_len"])
        progress_state["last_len"] = len(progress_text)
        progress_state["last_text"] = progress_text
        sys.stdout.write(f"\r{padded_text}")
        sys.stdout.flush()
        progress_state["active"] = done < total
        if done >= total:
            sys.stdout.write("\n")
            sys.stdout.flush()

    runner = run_all_subsets if config.method == "all_subsets" else run_eprs
    result = runner(
        context,
        config,
        progress_callback=progress_callback,
        progress_hook=progress_hook,
        stop_event=None,
    )

    total_cpu_minutes = float(result.get("cpu_time_total", 0.0))
    search_cpu_minutes = float(result.get("cpu_time_search", total_cpu_minutes))

    print()
    results_df = result.get("results_df")
    models_found = int(result.get("models_found", 0))
    models_explored = int(result.get("models_explored", 0))
    print(f"Total models explored: {models_explored}")
    metric_label = TARGET_METRIC_DISPLAY.get(config.target_metric, config.target_metric)
    comparator = ">=" if config.target_metric != "RMSE_loo" else "<="
    threshold_display = _format_threshold_display(getattr(config, "tm_cutoff", None))
    if getattr(config, "tm_cutoff", None) is None:
        print(f"Models reported without cutoff: {models_found}")
    else:
        print(
            f"Models with {metric_label} {comparator} {threshold_display}: {models_found}"
        )
    filtered_models = len(results_df.index) if results_df is not None else 0
    print(f"Filtrated and reported models: {filtered_models}")

    export_df = results_df if results_df is not None else pd.DataFrame()

    if export_df.empty:
        print(
            "No models met the reporting threshold. "
            "An empty results file will be written."
        )

    export_path = export_results_to_csv_cli(
        export_df,
        config,
        split_settings,
        output_path,
        cpu_search_minutes=search_cpu_minutes,
        cpu_total_minutes=total_cpu_minutes,
        models_found=models_found,
        models_explored=models_explored,
        avg_iterations_per_seed=result.get("avg_r2_calls"),
        max_iterations_per_seed=result.get("max_r2_calls"),
    )
    print(
        f"Model search CPU time: {search_cpu_minutes * 60.0:.2f} s "
        f"({search_cpu_minutes:.2f} min)"
    )
    print(f"Total CPU time: {total_cpu_minutes * 60.0:.2f} s ({total_cpu_minutes:.2f} min)")
    print()
    print(f"Results exported to {export_path}")
    print("Finished.")


def _parse_id_entries(text: str) -> set[str]:
    ids: set[str] = set()
    text = text.strip()
    if not text:
        return ids

    if text.lower() == "none":
        return ids

    for raw_token in text.split(","):
        token = raw_token.strip()
        if not token:
            continue
        if "-" in token:
            start, end = token.split("-", 1)
            start = start.strip()
            end = end.strip()
            if not start or not end:
                raise ValueError(f"Invalid ID range: '{token}'")
            if start.isdigit() and end.isdigit():
                start_i, end_i = int(start), int(end)
                if end_i < start_i:
                    raise ValueError(f"Invalid ID range: '{token}'")
                for value in range(start_i, end_i + 1):
                    ids.add(str(value))
            else:
                raise ValueError(
                    f"Non-numeric IDs cannot be expressed as ranges ('{token}')."
                )
        else:
            ids.add(token)

    return ids


def _parse_column_spec(text: str, max_columns: int) -> set[int]:
    values: set[int] = set()
    cleaned = text.strip()
    if not cleaned:
        return values

    for part in cleaned.split(","):
        token = part.strip()
        if not token:
            continue
        if "-" in token:
            start_str, end_str = token.split("-", 1)
            if not start_str.strip() or not end_str.strip():
                raise ValueError(f"Invalid column range: '{token}'")
            if not start_str.strip().isdigit() or not end_str.strip().isdigit():
                raise ValueError(f"Invalid column range: '{token}'")
            start = int(start_str)
            end = int(end_str)
            if start < 1 or end < 1 or start > max_columns or end > max_columns:
                raise ValueError(
                    f"Column range '{token}' is outside the available 1-{max_columns} span."
                )
            if end < start:
                raise ValueError(f"Invalid column range: '{token}'")
            for value in range(start, end + 1):
                values.add(value - 1)
        else:
            if not token.isdigit():
                raise ValueError(f"Invalid column reference: '{token}'")
            idx = int(token)
            if idx < 1 or idx > max_columns:
                raise ValueError(
                    f"Column reference '{token}' is outside the available 1-{max_columns} span."
                )
            values.add(idx - 1)

    return values


def _make_unique_column_name(existing_columns: list[object], base_name: str = "Observation") -> str:
    existing = {str(col) for col in existing_columns}
    if base_name not in existing:
        return base_name
    counter = 2
    while True:
        candidate = f"{base_name}_{counter}"
        if candidate not in existing:
            return candidate
        counter += 1


def load_dataset(
    path: str,
    delimiter: str = ";",
    split: Optional[dict] = None,
    dependent_choice: str = "last",
    non_variable_spec: str = "1",
    exclude_constant: bool = False,
    constant_threshold: float = 90.0,
    excluded_observations: str = "",
) -> EPRSContext:
    df_full = pd.read_csv(path, delimiter=delimiter)
    df_full.columns = [col.strip() if isinstance(col, str) else col for col in df_full.columns]
    if df_full.shape[1] < 3:
        raise ValueError("Dataset must contain at least an ID, predictors, and a target column.")

    columns = list(df_full.columns)
    non_variable_columns_list: list[str] = []
    total_columns = len(columns)

    choice_normalized = dependent_choice.strip().lower()
    if choice_normalized not in {"last", "first", "second", "third"}:
        raise ValueError("Unsupported dependent variable selection.")

    if choice_normalized == "last":
        dependent_idx = total_columns - 1
    elif choice_normalized == "first":
        dependent_idx = 0
    elif choice_normalized == "second":
        if total_columns < 2:
            raise ValueError("The dataset does not contain a second column to use as the target.")
        dependent_idx = 1
    else:  # third
        if total_columns < 3:
            raise ValueError("The dataset does not contain a third column to use as the target.")
        dependent_idx = 2

    target_column = columns[dependent_idx]

    non_variable_spec_normalized = (non_variable_spec or "").strip()
    non_variable_none_selected = non_variable_spec_normalized.lower() in {"", "none"}

    try:
        parsed_spec = "" if non_variable_none_selected else non_variable_spec_normalized
        non_variable_indices = _parse_column_spec(parsed_spec, total_columns)
    except ValueError as exc:  # noqa: BLE001
        raise ValueError(f"Non-variable columns: {exc}") from exc

    # Ensure the target column is not treated as non-variable.
    non_variable_indices.discard(dependent_idx)

    if non_variable_none_selected:
        id_idx: Optional[int] = None
        id_column = _make_unique_column_name(columns)
        id_series_full = pd.Series(range(1, len(df_full) + 1), name=id_column)
    else:
        if non_variable_indices:
            id_idx = min(non_variable_indices)
        else:
            id_idx_candidates = [idx for idx in range(total_columns) if idx != dependent_idx]
            if not id_idx_candidates:
                raise ValueError("Unable to determine an ID column from the dataset.")
            id_idx = id_idx_candidates[0]

        if id_idx == dependent_idx:
            id_idx_candidates = [idx for idx in range(total_columns) if idx != dependent_idx]
            if not id_idx_candidates:
                raise ValueError("Unable to determine an ID column from the dataset.")
            id_idx = id_idx_candidates[0]

        id_column = columns[id_idx]
        non_variable_indices.discard(id_idx)
        id_series_full = df_full.iloc[:, id_idx].reset_index(drop=True)

    excluded_ids: set[str] = set()
    if excluded_observations:
        try:
            excluded_ids = _parse_id_entries(excluded_observations)
        except ValueError as exc:  # noqa: BLE001
            raise ValueError(f"Exclude observations: {exc}") from exc
        if excluded_ids:
            id_series_to_check = id_series_full.astype(str)
            missing_excluded = excluded_ids - set(id_series_to_check)
            if missing_excluded:
                missing = ", ".join(sorted(missing_excluded))
                raise ValueError(f"Exclude observations: IDs not found: {missing}.")
            mask = ~id_series_to_check.isin(excluded_ids)
            if not mask.any():
                raise ValueError("Exclude observations: all rows would be removed.")
            df_full = df_full.loc[mask].reset_index(drop=True)
            if non_variable_none_selected:
                id_series_full = pd.Series(range(1, len(df_full) + 1), name=id_column)
            else:
                id_series_full = df_full.iloc[:, id_idx].reset_index(drop=True)

    if non_variable_none_selected:
        id_series_full = pd.Series(range(1, len(df_full) + 1), name=id_column)

    if non_variable_none_selected:
        observation_column = id_column
        observation_series_full = id_series_full.copy()
    else:
        observation_column = _make_unique_column_name(list(df_full.columns))
        observation_series_full = pd.Series(
            range(1, len(df_full) + 1), name=observation_column
        )

    current_columns = list(df_full.columns)
    non_variable_columns_list = [
        current_columns[idx]
        for idx in sorted(non_variable_indices)
        if 0 <= idx < len(current_columns)
    ]

    excluded = set(non_variable_indices)
    excluded.add(dependent_idx)
    if not non_variable_none_selected and id_idx is not None:
        excluded.add(id_idx)
    predictor_indices = [idx for idx in range(total_columns) if idx not in excluded]

    if not predictor_indices:
        raise ValueError("No predictor columns remain after applying the configuration.")

    predictors = df_full.iloc[:, predictor_indices]

    cleaned_predictors: list[pd.Series] = []
    for column_name in predictors.columns:
        series = predictors[column_name]
        numeric_series = pd.to_numeric(series, errors="coerce")
        numeric_series.name = column_name
        if numeric_series.isna().any():
            continue
        if numeric_series.nunique(dropna=False) <= 1:
            continue
        cleaned_predictors.append(numeric_series)

    if cleaned_predictors:
        predictors = pd.concat(cleaned_predictors, axis=1)
    else:
        predictors = pd.DataFrame(index=df_full.index)

    if exclude_constant:
        if not 0 < constant_threshold <= 100:
            raise ValueError("Near-constant filter threshold must be between 0 and 100 percent.")
        total_rows = len(predictors)
        if total_rows == 0:
            raise ValueError("The selected dataset does not contain any rows.")
        constant_cols: list[str] = []
        for col in list(predictors.columns):
            series = predictors[col]
            if series.empty:
                continue
            counts = series.value_counts(dropna=False)
            if counts.empty:
                continue
            most_common = counts.iloc[0]
            share = (float(most_common) / float(total_rows)) * 100.0
            if share >= constant_threshold:
                constant_cols.append(col)
        if constant_cols:
            predictors = predictors.drop(columns=constant_cols, errors="ignore")

    if predictors.empty:
        raise ValueError(
            "All predictor columns were removed after applying the current configuration."
        )

    feature_cols = list(predictors.columns)

    observation_df = observation_series_full.to_frame()
    id_df = id_series_full.to_frame()
    non_variable_df = (
        df_full.loc[:, non_variable_columns_list].copy()
        if non_variable_columns_list
        else None
    )
    target_df = df_full[[target_column]].copy()

    combined_parts = [observation_df]
    if id_column != observation_column:
        combined_parts.append(id_df)
    if non_variable_df is not None:
        combined_parts.append(non_variable_df)
    combined_parts.append(predictors)
    combined_parts.append(target_df)

    df_full = pd.concat(combined_parts, axis=1)

    train_df = df_full.copy()
    test_df: Optional[pd.DataFrame] = None

    split_mode = (split or {}).get("mode", "none")

    if split_mode == "random":
        test_size = float((split or {}).get("test_size", 0.2))
        if not 0.0 < test_size < 1.0:
            raise ValueError("Random split: test size must be between 0 and 1.")
        train_df, test_df = train_test_split(
            df_full,
            test_size=test_size,
            random_state=42,
            shuffle=True,
            stratify=None,
        )
    elif split_mode == "manual":
        id_series = df_full[id_column].astype(str)
        train_ids = (split or {}).get("train_ids", set())
        test_ids = (split or {}).get("test_ids", set())
        if not train_ids and not test_ids:
            raise ValueError("Manual split: specify at least one training or testing ID.")
        if train_ids and test_ids and train_ids & test_ids:
            overlap = ", ".join(sorted(train_ids & test_ids))
            raise ValueError(f"Manual split: IDs overlap between training and testing ({overlap}).")

        available_ids = set(id_series.tolist())
        missing_train = train_ids - available_ids
        missing_test = test_ids - available_ids
        if missing_train:
            missing = ", ".join(sorted(missing_train))
            raise ValueError(f"Manual split: training IDs not found: {missing}.")
        if missing_test:
            missing = ", ".join(sorted(missing_test))
            raise ValueError(f"Manual split: testing IDs not found: {missing}.")

        if train_ids:
            train_mask = id_series.isin(train_ids)
        else:
            train_mask = ~id_series.isin(test_ids)

        if test_ids:
            test_mask = id_series.isin(test_ids)
        else:
            test_mask = ~train_mask

        train_df = df_full.loc[train_mask]
        test_df = df_full.loc[test_mask]

        if train_df.empty or test_df.empty:
            raise ValueError("Manual split: both training and testing sets must contain rows.")

        if train_ids and test_ids:
            assigned = train_ids | test_ids
            leftovers = available_ids - assigned
            if leftovers:
                missing = ", ".join(sorted(leftovers))
                raise ValueError(
                    "Manual split: some IDs were not assigned to either training or testing: "
                    f"{missing}."
                )
    elif split_mode == "external":
        raise ValueError(
            "Use the Validation tab to load an external testing dataset instead of "
            "the data splitting controls."
        )

    X_train = train_df.loc[:, feature_cols]
    y_train = train_df[target_column]

    abs_corr = X_train.corr().abs()
    abs_corr_y = X_train.corrwith(y_train).abs()

    X_np = X_train.to_numpy(dtype=np.float64, copy=False)
    y_np = y_train.to_numpy(dtype=np.float64, copy=False)
    cols = list(feature_cols)
    col_idx = {c: i for i, c in enumerate(cols)}

    X_test_np = None
    y_test_np = None
    if test_df is not None:
        X_test_np = test_df.loc[:, feature_cols].to_numpy(dtype=np.float64, copy=False)
        y_test_np = test_df[target_column].to_numpy(dtype=np.float64, copy=False)

    try:
        full_exog = np.c_[np.ones((X_np.shape[0], 1), dtype=X_np.dtype), X_np]
        full_model = sm.OLS(y_np, full_exog).fit()
        if full_model.df_resid > 0:
            full_model_mse = float(full_model.ssr / full_model.df_resid)
        else:
            full_model_mse = float("nan")
    except Exception:  # noqa: BLE001
        full_model_mse = float("nan")

    primary_non_variable_column: Optional[str] = None
    if id_column != observation_column:
        primary_non_variable_column = id_column

    return EPRSContext(
        df_full,
        train_df,
        X_train,
        y_train,
        abs_corr,
        abs_corr_y,
        X_np,
        y_np,
        cols,
        col_idx,
        test_df,
        X_test_np,
        y_test_np,
        id_column,
        observation_column,
        target_column,
        full_model_mse,
        non_variable_columns=tuple(non_variable_columns_list),
        primary_non_variable_column=primary_non_variable_column,
    )


def take(context: EPRSContext, vars_: list[str]) -> np.ndarray:
    idx = [context.col_idx[v] for v in vars_]
    return context.X_np[:, idx]


def _resolve_holdout_data(
    context: EPRSContext,
    vars_: list[str],
) -> tuple[Optional[pd.DataFrame], Optional[np.ndarray], Optional[np.ndarray]]:
    if not vars_:
        return None, None, None

    if (
        context.test_df is not None
        and context.X_test_np is not None
        and context.y_test_np is not None
        and getattr(context.X_test_np, "size", 0) > 0
    ):
        try:
            idx = [context.col_idx[v] for v in vars_]
        except KeyError:
            pass
        else:
            X_subset = context.X_test_np[:, idx]
            if getattr(X_subset, "size", 0) > 0:
                test_df = context.test_df
                id_column = context.id_column
                observation_column = context.observation_column
                if test_df is not None and id_column in test_df.columns:
                    ids = test_df[id_column].reset_index(drop=True)
                else:
                    ids = pd.Series(range(1, X_subset.shape[0] + 1), name=id_column)
                if test_df is not None and observation_column in test_df.columns:
                    obs = test_df[observation_column].reset_index(drop=True)
                else:
                    obs = pd.Series(
                        range(1, X_subset.shape[0] + 1), name=observation_column
                    )
                holdout_columns = {observation_column: obs}
                if id_column != observation_column:
                    holdout_columns[id_column] = ids
                holdout_df = pd.DataFrame(holdout_columns)
                return holdout_df, X_subset, context.y_test_np

    external_df = context.external_df
    if external_df is not None and not external_df.empty:
        missing = [col for col in vars_ if col not in external_df.columns]
        target_col = context.target_column
        if not missing and target_col in external_df.columns:
            try:
                features = external_df.loc[:, vars_]
                if features.empty:
                    return None, None, None
                features_np = features.to_numpy(dtype=np.float64, copy=False)
                target_np = external_df.loc[:, target_col].to_numpy(
                    dtype=np.float64, copy=False
                )
            except Exception:  # noqa: BLE001
                return None, None, None

            id_column = context.id_column
            observation_column = context.observation_column
            if observation_column in external_df.columns:
                obs = external_df.loc[:, observation_column].reset_index(drop=True)
            else:
                start_value = 1
                train_df = context.train_df
                if train_df is not None and observation_column in train_df.columns:
                    try:
                        existing = pd.to_numeric(
                            train_df[observation_column], errors="coerce"
                        )
                        max_existing = existing.max()
                        if pd.notna(max_existing):
                            start_value = int(float(max_existing)) + 1
                        else:
                            start_value = len(train_df) + 1
                    except Exception:  # noqa: BLE001
                        start_value = len(train_df) + 1
                obs = pd.Series(
                    range(start_value, start_value + len(external_df)),
                    name=observation_column,
                )
            holdout_columns = {observation_column: obs}
            if id_column != observation_column:
                if id_column in external_df.columns:
                    ids = external_df.loc[:, id_column].reset_index(drop=True)
                else:
                    ids = pd.Series(range(1, len(external_df) + 1), name=id_column)
                holdout_columns[id_column] = ids
            holdout_df = pd.DataFrame(holdout_columns)
            return holdout_df, features_np, target_np

    return None, None, None


def _compute_holdout_metrics(
    context: EPRSContext,
    vars_: list[str],
    config: Optional[EPRSConfig] = None,
) -> Optional[dict]:
    _, X_test_np, y_test_np = _resolve_holdout_data(context, vars_)
    if X_test_np is None or y_test_np is None:
        return None

    if not vars_:
        return None

    try:
        idx = [context.col_idx[v] for v in vars_]
    except KeyError:
        return None

    train_matrix = take(context, vars_)
    train_exog = np.c_[np.ones(train_matrix.shape[0], dtype=train_matrix.dtype), train_matrix]

    try:
        res = sm.OLS(context.y_np, train_exog).fit()
    except Exception:  # noqa: BLE001
        return None

    if X_test_np.shape[1] == len(vars_):
        test_matrix = X_test_np
    else:
        try:
            test_matrix = X_test_np[:, idx]
        except Exception:  # noqa: BLE001
            return None
    test_exog = np.c_[np.ones(test_matrix.shape[0], dtype=test_matrix.dtype), test_matrix]
    test_preds = test_exog @ res.params

    if config and config.clip_predictions is not None:
        lo, hi = config.clip_predictions
        test_preds = np.clip(test_preds, lo, hi)

    residuals = y_test_np - test_preds
    param_count = len(vars_) + 1
    s_value = _compute_standard_error(residuals, param_count)

    sum_squared_errors = float(np.dot(residuals, residuals))
    train_mean = float(np.nanmean(context.y_np)) if context.y_np.size else float("nan")
    test_mean = float(np.nanmean(y_test_np)) if y_test_np.size else float("nan")
    n_train = int(context.y_np.size)
    n_ext = int(y_test_np.size)
    if n_train > 0 and np.isfinite(train_mean):
        train_centered = np.asarray(context.y_np, dtype=np.float64) - train_mean
        train_centered_sum = float(np.dot(train_centered, train_centered))
    else:
        train_centered_sum = float("nan")

    def _safe_q2(denominator: float) -> float:
        if not np.isfinite(sum_squared_errors) or not np.isfinite(denominator):
            return float("nan")
        if denominator <= 0:
            return float("nan")
        return float(1.0 - (sum_squared_errors / denominator))

    denom_f1 = float(np.dot(y_test_np - train_mean, y_test_np - train_mean))
    denom_f2 = float(np.dot(y_test_np - test_mean, y_test_np - test_mean))

    q2_ext = _safe_q2(denom_f2)

    if (
        not np.isfinite(sum_squared_errors)
        or n_ext <= 0
        or n_train <= 0
        or not np.isfinite(train_centered_sum)
    ):
        q2f3_ext = float("nan")
    else:
        mse_ext = sum_squared_errors / n_ext if n_ext else float("nan")
        denom_f3 = train_centered_sum / n_train if n_train else float("nan")
        if (
            not np.isfinite(mse_ext)
            or not np.isfinite(denom_f3)
            or denom_f3 <= 0
        ):
            q2f3_ext = float("nan")
        else:
            q2f3_ext = float(1.0 - (mse_ext / denom_f3))

    return {
        "R2_ext": q2_ext,
        "RMSE_ext": float(np.sqrt(mean_squared_error(y_test_np, test_preds))),
        "s_ext": s_value,
        "MAE_ext": float(mean_absolute_error(y_test_np, test_preds)),
        "Q2F1_ext": _safe_q2(denom_f1),
        "Q2F2_ext": q2_ext,
        "Q2F3_ext": q2f3_ext,
    }


def _normalize_observation_id(value: object, index: int, prefix: str) -> object:
    if isinstance(value, str):
        trimmed = value.strip()
        if trimmed:
            return trimmed
        return f"{prefix}{index + 1}"
    if value is None:
        return f"{prefix}{index + 1}"
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (np.floating, float)):
        numeric = float(value)
        if not math.isfinite(numeric):
            return f"{prefix}{index + 1}"
        if numeric.is_integer():
            return int(round(numeric))
        return numeric
    if pd.isna(value):
        return f"{prefix}{index + 1}"
    return value


def compute_observation_diagnostics(
    context: Optional[EPRSContext],
    config: Optional[EPRSConfig],
    vars_: list[str],
) -> tuple[pd.DataFrame, float]:
    if context is None or config is None:
        return pd.DataFrame(), float("nan")

    if not vars_:
        return pd.DataFrame(), float("nan")

    try:
        idx = [context.col_idx[v] for v in vars_]
    except KeyError:
        return pd.DataFrame(), float("nan")

    X_train = context.X_np[:, idx]
    if X_train.size == 0:
        return pd.DataFrame(), float("nan")

    train_exog = np.c_[np.ones((X_train.shape[0], 1), dtype=X_train.dtype), X_train]

    try:
        model = sm.OLS(context.y_np, train_exog).fit()
    except Exception:  # noqa: BLE001
        return pd.DataFrame(), float("nan")

    preds = np.asarray(model.fittedvalues, dtype=float)
    if config.clip_predictions is not None:
        lo, hi = config.clip_predictions
        preds = np.clip(preds, lo, hi)

    influence = model.get_influence()
    hat_diag = np.asarray(influence.hat_matrix_diag, dtype=float)
    residuals = np.asarray(context.y_np - preds, dtype=float)

    with np.errstate(divide="ignore", invalid="ignore"):
        correction = 1.0 - hat_diag
        press_residuals = residuals / correction
    press_residuals = np.asarray(press_residuals, dtype=float)
    press_residuals[~np.isfinite(press_residuals)] = np.nan

    loo_pred = np.asarray(context.y_np - press_residuals, dtype=float)
    std_internal = np.asarray(influence.resid_studentized_internal, dtype=float)
    std_external = np.asarray(influence.resid_studentized_external, dtype=float)

    mse_resid = float(model.mse_resid) if np.isfinite(model.mse_resid) else float("nan")
    sigma = float(np.sqrt(mse_resid)) if np.isfinite(mse_resid) else float("nan")
    param_count = max(len(vars_) + 1, 1)

    with np.errstate(divide="ignore", invalid="ignore"):
        std_press = press_residuals / (sigma * np.sqrt(1.0 + hat_diag))
    std_press = np.asarray(std_press, dtype=float)
    std_press[~np.isfinite(std_press)] = np.nan

    cooks_distance = np.full_like(residuals, np.nan, dtype=float)
    cooks_distance_loo = np.full_like(residuals, np.nan, dtype=float)
    if np.isfinite(mse_resid) and mse_resid > 0:
        with np.errstate(divide="ignore", invalid="ignore"):
            leverage_term = hat_diag / np.square(1.0 - hat_diag)
            base = (residuals.astype(float) ** 2) / (param_count * mse_resid)
            cooks_distance = base * leverage_term
        cooks_distance = np.asarray(cooks_distance, dtype=float)
        cooks_distance[~np.isfinite(cooks_distance)] = np.nan

    with np.errstate(divide="ignore", invalid="ignore"):
        leverage_factor = hat_diag / (1.0 - hat_diag)
        cooks_distance_loo = (np.asarray(std_external, dtype=float) ** 2 / param_count) * leverage_factor
    cooks_distance_loo = np.asarray(cooks_distance_loo, dtype=float)
    cooks_distance_loo[~np.isfinite(cooks_distance_loo)] = np.nan

    normalized_cov = np.asarray(model.normalized_cov_params, dtype=float)
    hat_threshold = float("nan")
    if train_exog.shape[0] > 0:
        hat_threshold = float(3.0 * (len(vars_) + 1) / train_exog.shape[0])

    records: list[dict[str, object]] = []
    train_df = context.train_df.reset_index(drop=True)
    observation_column = context.observation_column
    label_column = getattr(context, "primary_non_variable_column", None)
    label_column_display = label_column
    if label_column_display == "Observation":
        label_column_display = "Observation (ID)"

    if observation_column in train_df.columns:
        train_observations = train_df[observation_column].tolist()
    else:
        train_observations = list(range(1, len(train_df) + 1))

    train_label_values: Optional[list[object]] = None
    if label_column and label_column in train_df.columns:
        train_label_values = train_df[label_column].tolist()

    for i, obs_value in enumerate(train_observations):
        normalized_obs = _normalize_observation_id(obs_value, i, "Train")
        record: dict[str, object] = {
            "Observation": normalized_obs,
        }
        if train_label_values is not None and label_column_display:
            record[label_column_display] = train_label_values[i]
        record.update(
            {
                "Set": "Training",
                "Actual": float(context.y_np[i]),
                "Predicted": float(preds[i]),
                "Predicted_LOO": float(loo_pred[i]) if np.isfinite(loo_pred[i]) else np.nan,
                "Residual": float(residuals[i]) if np.isfinite(residuals[i]) else np.nan,
                "Residual_LOO": float(press_residuals[i]) if np.isfinite(press_residuals[i]) else np.nan,
                "Z_value": float(std_internal[i]) if np.isfinite(std_internal[i]) else np.nan,
                "Leverage": float(hat_diag[i]) if np.isfinite(hat_diag[i]) else np.nan,
                "StdPredResid": float(std_press[i]) if np.isfinite(std_press[i]) else np.nan,
                "StdPredResid_LOO": float(std_external[i]) if np.isfinite(std_external[i]) else np.nan,
                "CooksDistance": float(cooks_distance[i]) if np.isfinite(cooks_distance[i]) else np.nan,
                "CooksDistance_LOO": float(cooks_distance_loo[i]) if np.isfinite(cooks_distance_loo[i]) else np.nan,
            }
        )
        records.append(record)

    combined_df = pd.DataFrame(records)

    test_records: list[dict[str, object]] = []
    holdout_df, holdout_X, holdout_y = _resolve_holdout_data(context, vars_)
    if (
        holdout_df is not None
        and holdout_X is not None
        and holdout_y is not None
        and getattr(holdout_X, "shape", (0,))[0] > 0
    ):
        if holdout_X.shape[1] == len(vars_):
            X_test = holdout_X
        else:
            try:
                X_test = holdout_X[:, idx]
            except Exception:  # noqa: BLE001
                X_test = None
        if X_test is None or getattr(X_test, "size", 0) == 0:
            return combined_df, hat_threshold
        test_exog = np.c_[np.ones((X_test.shape[0], 1), dtype=X_test.dtype), X_test]
        preds_test = test_exog @ model.params
        if config.clip_predictions is not None:
            lo, hi = config.clip_predictions
            preds_test = np.clip(preds_test, lo, hi)
        residuals_test = holdout_y - preds_test
        hat_test = np.sum(test_exog * (test_exog @ normalized_cov), axis=1)
        if np.isfinite(sigma) and sigma != 0:
            with np.errstate(divide="ignore", invalid="ignore"):
                z_test = residuals_test / sigma
        else:
            z_test = np.full_like(residuals_test, np.nan, dtype=float)
        with np.errstate(divide="ignore", invalid="ignore"):
            std_pred_test = residuals_test / (sigma * np.sqrt(1.0 + hat_test))
        cooks_test = np.full_like(residuals_test, np.nan, dtype=float)
        if np.isfinite(mse_resid) and mse_resid > 0:
            with np.errstate(divide="ignore", invalid="ignore"):
                leverage_term_test = hat_test / np.square(1.0 - hat_test)
                base_test = (residuals_test.astype(float) ** 2) / (param_count * mse_resid)
                cooks_test = base_test * leverage_term_test
            cooks_test = np.asarray(cooks_test, dtype=float)
            cooks_test[~np.isfinite(cooks_test)] = np.nan
        holdout_df = holdout_df.reset_index(drop=True)
        if observation_column in holdout_df.columns:
            holdout_observations = holdout_df[observation_column].tolist()
        else:
            holdout_observations = list(range(1, len(holdout_df) + 1))

        holdout_label_values: Optional[list[object]] = None
        if label_column and label_column in holdout_df.columns:
            holdout_label_values = holdout_df[label_column].tolist()

        for i, obs_value in enumerate(holdout_observations):
            normalized_obs = _normalize_observation_id(obs_value, i, "Test")
            actual_value = float(holdout_y[i]) if np.isfinite(holdout_y[i]) else np.nan
            pred_value = float(preds_test[i]) if np.isfinite(preds_test[i]) else np.nan
            resid_value = float(residuals_test[i]) if np.isfinite(residuals_test[i]) else np.nan
            hat_value = float(hat_test[i]) if np.isfinite(hat_test[i]) else np.nan
            std_pred_value = (
                float(std_pred_test[i]) if np.isfinite(std_pred_test[i]) else np.nan
            )
            z_value = float(z_test[i]) if np.isfinite(z_test[i]) else np.nan
            record = {
                "Observation": normalized_obs,
                "Set": "Testing",
                "Actual": actual_value,
                "Predicted": pred_value,
                "Predicted_LOO": np.nan,
                "Residual": resid_value,
                "Residual_LOO": np.nan,
                "Z_value": z_value,
                "Leverage": hat_value,
                "StdPredResid": std_pred_value,
                "StdPredResid_LOO": np.nan,
                "CooksDistance": float(cooks_test[i]) if np.isfinite(cooks_test[i]) else np.nan,
                "CooksDistance_LOO": np.nan,
            }
            if holdout_label_values is not None and label_column_display:
                record[label_column_display] = holdout_label_values[i]
            test_records.append(record)

    if test_records:
        combined_df = pd.concat([combined_df, pd.DataFrame(test_records)], ignore_index=True)

    return combined_df, hat_threshold


def _compute_loo_r2(
    design: np.ndarray,
    y: np.ndarray,
    clip: Optional[tuple[float, float]] = None,
) -> Optional[float]:
    n_samples, n_features = design.shape
    if n_samples <= n_features:
        return None

    xtx = design.T @ design
    try:
        xtx_inv = np.linalg.inv(xtx)
    except np.linalg.LinAlgError:
        return None

    xty = design.T @ y
    coefficients = xtx_inv @ xty
    fitted = design @ coefficients
    residuals = y - fitted
    hat_diag = np.sum(design * (design @ xtx_inv), axis=1)

    with np.errstate(divide="ignore", invalid="ignore"):
        correction = 1.0 - hat_diag
        loo_pred = y - residuals / correction

    if clip is not None:
        lo, hi = clip
        loo_pred = np.clip(loo_pred, lo, hi)

    if not np.all(np.isfinite(loo_pred)):
        return None

    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    if ss_tot == 0.0:
        return None

    ss_res = float(np.sum((y - loo_pred) ** 2))
    if not np.isfinite(ss_res):
        return None

    return 1.0 - ss_res / ss_tot


def _compute_standard_error(residuals: np.ndarray, param_count: int) -> float:
    """Compute the residual standard error with a safe degrees-of-freedom fallback."""

    arr = np.asarray(residuals, dtype=np.float64)
    if arr.size == 0:
        return float("nan")

    sse = float(np.dot(arr, arr))
    if not np.isfinite(sse):
        return float("nan")

    try:
        params = int(param_count)
    except (TypeError, ValueError):
        params = 0
    params = max(params, 0)

    dof = arr.size - max(params, 1)
    if dof <= 0:
        dof = arr.size

    if dof <= 0:
        return float("nan")

    value = math.sqrt(sse / dof)
    return float(value) if np.isfinite(value) else float("nan")


def _format_covariance_type_label(raw_cov_type: object) -> str:
    cov_text = str(raw_cov_type).strip() if raw_cov_type is not None else ""
    if not cov_text:
        return "-"
    lookup = COVARIANCE_KEY_TO_DISPLAY.get(cov_text)
    if lookup:
        return lookup
    normalized = cov_text.lower()
    canonical = COVARIANCE_KEY_NORMALIZED.get(normalized)
    if canonical:
        return COVARIANCE_KEY_TO_DISPLAY.get(canonical, cov_text)
    if normalized == "nonrobust":
        return COVARIANCE_KEY_TO_DISPLAY.get(COVARIANCE_DEFAULT_KEY, "Non robust")
    return cov_text.title()


def _apply_covariance_type(
    result,
    config: Optional[EPRSConfig],
    context: Optional[EPRSContext],
    exog: Optional[np.ndarray],
):
    cov_type_key = getattr(config, "cov_type", COVARIANCE_DEFAULT_KEY)
    if not isinstance(cov_type_key, str):
        cov_type_key = COVARIANCE_DEFAULT_KEY
    cov_type_key = cov_type_key.strip() or COVARIANCE_DEFAULT_KEY
    canonical_key = COVARIANCE_KEY_NORMALIZED.get(cov_type_key.lower(), COVARIANCE_DEFAULT_KEY)
    if canonical_key == COVARIANCE_DEFAULT_KEY:
        return result

    cov_type = canonical_key
    robust_kwargs: dict[str, object] = {}

    try:
        return result.get_robustcov_results(cov_type=cov_type, **robust_kwargs)
    except Exception:  # noqa: BLE001 - fall back to original result
        return result


def _recommend_covariance_type(
    model,
    *,
    alpha: float = 0.05,
    prefer_hc: str = "HC3",
    leverage_rule: str = "2p/n",
):
    """Diagnose a fitted OLS result and suggest an appropriate covariance type."""

    notes: list[str] = []
    reasons: list[str] = []

    try:
        resid = np.asarray(model.resid).ravel()
    except Exception:  # noqa: BLE001
        resid = np.asarray([], dtype=float)
        notes.append("Residuals unavailable for covariance diagnostics.")

    try:
        n = int(getattr(model, "nobs", resid.size))
    except Exception:  # noqa: BLE001
        n = int(resid.size)
    n = max(n, 0)

    try:
        p = int(getattr(model, "df_model", 0)) + 1
    except Exception:  # noqa: BLE001
        try:
            p = int(model.model.exog.shape[1])  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            p = 0
    p = max(p, 0)

    tests: dict[str, float] = {
        "p_bp": float("nan"),
        "p_white": float("nan"),
        "n": float(n),
        "p": float(p),
        "lev_threshold": float("nan"),
        "max_leverage": float("nan"),
        "n_high_leverage": float("nan"),
    }

    try:
        from statsmodels.stats.diagnostic import het_breuschpagan, het_white
        from statsmodels.stats.outliers_influence import OLSInfluence
        from statsmodels.tools.tools import add_constant
    except Exception as exc:  # noqa: BLE001
        notes.append(f"Covariance diagnostics unavailable: {exc}.")
        het_breuschpagan = het_white = OLSInfluence = None  # type: ignore[assignment]
        add_constant = None  # type: ignore[assignment]

    hetero = False
    influence_available = False

    exog = getattr(getattr(model, "model", None), "exog", None)
    if exog is not None and het_breuschpagan is not None and add_constant is not None:
        try:
            has_const = np.isclose(np.asarray(exog)[:, 0].var(), 0.0)
        except Exception:  # noqa: BLE001
            has_const = False
        X = np.asarray(exog)
        if has_const:
            X_design = X
        else:
            try:
                X_design = add_constant(X, has_constant="add")
            except Exception:  # noqa: BLE001
                X_design = X
                notes.append("Unable to add constant term for heteroskedasticity tests.")
        try:
            _stat, p_bp, _f, _fp = het_breuschpagan(resid, X_design)
        except Exception:  # noqa: BLE001
            p_bp = float("nan")
        try:
            _stat, p_white, _lm, _f = het_white(resid, X_design)
        except Exception:  # noqa: BLE001
            p_white = float("nan")
        tests["p_bp"] = float(p_bp)
        tests["p_white"] = float(p_white)
        hetero = (np.isfinite(p_bp) and p_bp < alpha) or (np.isfinite(p_white) and p_white < alpha)
    else:
        if exog is None:
            notes.append("Design matrix unavailable for heteroskedasticity tests.")

    max_leverage = float("nan")
    n_high_leverage = 0
    if OLSInfluence is not None:
        try:
            influence = OLSInfluence(model)
            hat_diag = np.asarray(influence.hat_matrix_diag, dtype=float)
            if hat_diag.size:
                max_leverage = float(np.nanmax(hat_diag))
                thr_mult = 2.0 if leverage_rule == "2p/n" else 3.0
                threshold = thr_mult * (p / max(n, 1) if n else 0.0)
                n_high_leverage = int(np.sum(hat_diag > threshold))
                influence_available = True
            else:
                threshold = float("nan")
        except Exception:  # noqa: BLE001
            threshold = float("nan")
            notes.append("Influence diagnostics unavailable.")
    else:
        threshold = float("nan")

    tests["lev_threshold"] = float(threshold)
    tests["max_leverage"] = float(max_leverage)
    tests["n_high_leverage"] = float(n_high_leverage)

    high_leverage = influence_available and (
        (np.isfinite(max_leverage) and np.isfinite(threshold) and max_leverage > threshold)
        or n_high_leverage > 0
    )

    preferred = prefer_hc if prefer_hc in {"HC0", "HC1", "HC2", "HC3"} else "HC3"

    cov_type = "nonrobust"
    if hetero and high_leverage:
        cov_type = "HC3"
        reasons.append("Heteroskedasticity with notable leverage detected.")
    elif hetero:
        cov_type = preferred
        reasons.append("Heteroskedasticity detected.")
    elif high_leverage:
        cov_type = "HC3"
        reasons.append("High leverage detected; using HC3 as a conservative choice.")
    else:
        cov_type = "nonrobust"
        reasons.append("No heteroskedasticity or leverage issues detected; using nonrobust.")

    return {
        "cov_type": cov_type,
        "cov_kwds": None,
        "tests": tests,
        "reasons": reasons,
        "notes": notes,
    }


def _base_compute_metrics(
    context: EPRSContext,
    config: EPRSConfig,
    vars_: list[str],
    counter: Optional[dict] = None,
    *,
    skip_corr_screen: bool = False,
) -> Optional[dict]:
    if not vars_:
        return None

    # Fast correlation screen
    if not skip_corr_screen:
        for i in range(len(vars_)):
            for j in range(i + 1, len(vars_)):
                if context.abs_corr.loc[vars_[i], vars_[j]] > config.corr_threshold:
                    return None

    idx = [context.col_idx[v] for v in vars_]
    Xm = take(context, vars_)
    exog = np.c_[np.ones(Xm.shape[0], dtype=Xm.dtype), Xm]

    base_result = sm.OLS(context.y_np, exog).fit()
    if counter is not None:
        counter["r2_calls"] += 1

    preds = base_result.fittedvalues
    if config.clip_predictions is not None:
        lo, hi = config.clip_predictions
        preds = np.clip(preds, lo, hi)

    residuals = context.y_np - preds
    param_count = len(vars_) + 1
    R2 = float(base_result.rsquared)
    R2_adj = float(base_result.rsquared_adj)
    RMSE = float(np.sqrt(mean_squared_error(context.y_np, preds)))
    s_value = _compute_standard_error(residuals, param_count)
    MAE = float(mean_absolute_error(context.y_np, preds))

    cov_result = _apply_covariance_type(base_result, config, context, exog)

    if np.any(cov_result.pvalues > config.signif_lvl):
        return None

    metrics = {
        "R2": R2,
        "R2_adj": R2_adj,
        "RMSE": RMSE,
        "s": s_value,
        "MAE": MAE,
        "variables": vars_,
    }

    if config.target_metric in LOO_TARGET_METRICS:
        loo_metrics = _evaluate_model_loo(context, vars_, config.clip_predictions)
        if not np.isfinite(loo_metrics.get("R2_loo", float("nan"))):
            return None
        metrics.update(loo_metrics)

    return metrics


def _evaluate_model_loo(
    context: EPRSContext, variables: list[str], clip_predictions: Optional[tuple[float, float]]
) -> dict[str, float]:
    X = take(context, variables)
    y = context.y_np.astype(float)
    n_samples = y.shape[0]
    if n_samples <= X.shape[1]:
        raise ValueError("LOO: samples must exceed the number of variables.")

    design = np.c_[np.ones(n_samples), X]
    xtx = design.T @ design
    try:
        xtx_inv = np.linalg.inv(xtx)
    except np.linalg.LinAlgError:
        raise ValueError("LOO: design matrix is singular.") from None

    xty = design.T @ y
    coefficients = xtx_inv @ xty
    fitted = design @ coefficients
    residuals = y - fitted
    hat_diag = np.sum(design * (design @ xtx_inv), axis=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        loo_pred = y - residuals / (1 - hat_diag)
    if clip_predictions is not None:
        lo, hi = clip_predictions
        loo_pred = np.clip(loo_pred, lo, hi)
    loo_residuals = y - loo_pred

    mse = float(np.mean(loo_residuals**2))
    rmse = float(np.sqrt(mse))
    mae = float(np.mean(np.abs(loo_residuals)))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    ss_res = float(np.sum(loo_residuals**2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot else float("nan")
    param_count = len(variables) + 1
    return {
        "R2_loo": r2,
        "RMSE_loo": rmse,
        "s_loo": _compute_standard_error(loo_residuals, param_count),
        "MAE_loo": mae,
    }


def make_vif_funcs(context: EPRSContext):
    vif_cache: dict[tuple[str, ...], np.ndarray] = {}

    def compute_vif(exog: np.ndarray) -> np.ndarray:
        if exog.ndim != 2 or exog.shape[1] <= 1:
            return np.empty((0,), dtype=float)

        exog = np.asarray(exog, dtype=np.float64)
        n_features = exog.shape[1]
        vifs: list[float] = []
        for j in range(1, n_features):
            y = exog[:, j]
            if not np.isfinite(y).all():
                vifs.append(np.nan)
                continue

            y_mean = float(np.mean(y))
            ss_tot = float(np.sum((y - y_mean) ** 2))
            if ss_tot <= 1e-12:
                vifs.append(np.inf)
                continue

            X_other = np.delete(exog, j, axis=1)
            try:
                beta, *_ = np.linalg.lstsq(X_other, y, rcond=None)
            except np.linalg.LinAlgError:
                vifs.append(np.inf)
                continue

            resid = y - X_other @ beta
            ss_res = float(np.dot(resid, resid))

            r2 = 1.0 - ss_res / ss_tot
            r2 = min(max(r2, 0.0), 1.0)
            denom = 1.0 - r2
            if denom <= 1e-12:
                vifs.append(np.inf)
            else:
                vifs.append(1.0 / denom)

        return np.array(vifs, dtype=float)

    def calc_vif(vars_: list[str]) -> np.ndarray:
        key = tuple(sorted(vars_))
        cached = vif_cache.get(key)
        if cached is not None:
            return cached
        Xm = take(context, vars_)
        exog = np.c_[np.ones(Xm.shape[0], dtype=Xm.dtype), Xm]
        arr = compute_vif(exog)
        vif_cache[key] = arr
        return arr

    def vif_stats(vars_: list[str]) -> tuple[float, float]:
        vals = calc_vif(vars_)
        return (
            float(np.nanmax(vals)) if vals.size else 0.0,
            float(np.nanmean(vals)) if vals.size else 0.0,
        )

    return calc_vif, vif_stats


def _finalize_results_dataframe(
    all_hits: list[dict],
    config: EPRSConfig,
    vif_stats: Callable[[list[str]], tuple[float, float]],
    *,
    progress_hook: Optional[Callable[[int, int, int], None]] = None,
) -> tuple[Optional[pd.DataFrame], float]:
    if not all_hits:
        return None, 0.0

    df_out = pd.DataFrame(all_hits)
    df_out["VarSet"] = df_out["Variables"].apply(lambda v: tuple(sorted(v)))
    df_out = df_out.drop_duplicates("VarSet").drop(columns="VarSet")

    primary_metric = config.target_metric
    if primary_metric not in df_out.columns:
        df_out[primary_metric] = np.nan

    def _sort_key(series: pd.Series) -> pd.Series:
        if primary_metric == "RMSE_loo":
            values = series.astype(float)
            with np.errstate(divide="ignore", invalid="ignore"):
                transformed = -np.log(values)
            transformed = transformed.where(np.isfinite(transformed), float("-inf"))
            return transformed
        return series

    df_out = df_out.sort_values(
        primary_metric,
        ascending=False,
        na_position="last",
        key=_sort_key,
    )

    def timed_vif_stats(vars_list):
        start = time.process_time()
        vif_max, vif_avg = vif_stats(vars_list)
        elapsed = (time.process_time() - start) / 60.0
        return vif_max, vif_avg, elapsed

    sorted_indices = list(df_out.index)
    selected_indices: list[int] = []
    vif_values: dict[int, tuple[float, float]] = {}

    total_models = len(df_out)
    top_limit = int(config.export_limit)
    limit_by_total = total_models <= top_limit
    target_vif_total = total_models if limit_by_total else top_limit
    target_vif_total = max(int(target_vif_total), 0)
    attempted_vifs = 0
    valid_vifs = 0

    def notify_vif_progress(done: int) -> None:
        if progress_hook:
            progress_hook(done, max(target_vif_total, 1), 2)

    vif_cpu_minutes = 0.0

    if target_vif_total > 0:
        notify_vif_progress(0)

    while sorted_indices and len(selected_indices) < config.export_limit:
        idx = sorted_indices.pop(0)
        vif_max, vif_avg, cpu_elapsed = timed_vif_stats(df_out.at[idx, "Variables"])
        vif_cpu_minutes += float(cpu_elapsed)
        attempted_vifs += 1
        if limit_by_total:
            notify_vif_progress(min(attempted_vifs, target_vif_total))

        if not np.isfinite(vif_max) or vif_max > config.vif_threshold:
            continue

        vif_values[idx] = (float(vif_max), float(vif_avg))
        selected_indices.append(idx)
        valid_vifs += 1
        if not limit_by_total:
            notify_vif_progress(min(valid_vifs, target_vif_total))

    if not selected_indices:
        if target_vif_total > 0:
            notify_vif_progress(target_vif_total)
        return None, vif_cpu_minutes

    if not limit_by_total and valid_vifs < target_vif_total:
        notify_vif_progress(target_vif_total)

    df_out = df_out.loc[selected_indices]
    df_out[["VIF_max", "VIF_avg"]] = pd.DataFrame(
        [vif_values[idx] for idx in df_out.index], index=df_out.index
    )

    df_out = df_out.sort_values(
        [primary_metric, "VIF_max"],
        ascending=[False, True],
        na_position="last",
    )

    r2_values = pd.to_numeric(df_out.get("R2"), errors="coerce")
    r2_order = r2_values.sort_values(ascending=False, na_position="last")
    model_ids = pd.Series(
        np.arange(1, len(df_out) + 1, dtype=int), index=r2_order.index
    )
    df_out.insert(0, "Model", model_ids.loc[df_out.index].to_numpy())

    return df_out.copy(deep=True), vif_cpu_minutes


def iter_combinations(
    all_vars: Sequence[str],
    max_size: int,
    stop_event: Optional[EventLike] = None,
):
    for size in range(1, max_size + 1):
        for combo in combinations(all_vars, size):
            if stop_event and stop_event.is_set():
                raise AnalysisCancelled()
            yield combo


def _all_subsets_worker(
    combo: tuple[str, ...],
    context: EPRSContext,
    config: EPRSConfig,
    include_loo: bool,
    threshold_value: float,
):
    _ensure_heavy_imports_loaded()
    calc_vif, _ = make_vif_funcs(context)
    metric_key = config.target_metric
    vars_list = list(combo)
    local_counter = {"r2_calls": 0}
    cpu_start = time.process_time()
    correlation_blocked = False
    for i in range(len(vars_list)):
        for j in range(i + 1, len(vars_list)):
            if context.abs_corr.loc[vars_list[i], vars_list[j]] > config.corr_threshold:
                correlation_blocked = True
                break
        if correlation_blocked:
            break

    metrics = None
    if not correlation_blocked:
        metrics = _base_compute_metrics(
            context,
            config,
            vars_list,
            counter=local_counter,
            skip_corr_screen=True,
        )
    cpu_minutes = (time.process_time() - cpu_start) / 60.0
    if metrics and _metric_score(metric_key, metrics) >= threshold_value:
        vifs = calc_vif(vars_list)
        if vifs.size == 0 or np.nanmax(vifs) <= config.vif_threshold:
            record = {
                "R2": metrics["R2"],
                "R2_adj": metrics["R2_adj"],
                "Variables": vars_list,
                "RMSE": metrics["RMSE"],
                "s": metrics.get("s"),
                "MAE": metrics["MAE"],
            }
            if include_loo:
                if "R2_loo" in metrics:
                    record["R2_loo"] = metrics["R2_loo"]
                if "RMSE_loo" in metrics:
                    record["RMSE_loo"] = metrics["RMSE_loo"]
            return record, local_counter["r2_calls"], cpu_minutes, os.getpid(), False
    return None, local_counter["r2_calls"], cpu_minutes, os.getpid(), correlation_blocked


# EXPAND-PERTURB-REDUCE-SWAP (EPRS-S)
def eprs(
    context: EPRSContext,
    config: EPRSConfig,
    current,
    remaining,
    dropped,
    vif_veto,
    compute_metrics,
    counter,
    max_calls,
):
    remaining = [v for v in remaining if v not in vif_veto]
    metric_key = config.target_metric

    def metric_value(metrics: dict[str, float]) -> float:
        return _metric_score(metric_key, metrics)

    def limit_reached() -> bool:
        return max_calls is not None and counter["r2_calls"] >= max_calls

    m_init = compute_metrics(current)
    best_score_init = metric_value(m_init) if m_init else float("-inf")
    best, best_score = (m_init, best_score_init) if m_init else (None, float("-inf"))
    last_score = float("-inf")
    repeats = 0
    swap_log = []

    while True:
        if limit_reached():
            break

        # Expand
        if len(current) < config.max_vars:
            cands = []
            for v in list(remaining):
                m = compute_metrics(current + [v])
                if not m:
                    continue
                score = metric_value(m)
                if score == float("-inf"):
                    continue
                cands.append((m, v, score))
            if cands:
                m, v, score = max(cands, key=lambda x: x[2])
                if score > best_score:
                    current.append(v)
                    remaining.remove(v)
                    best, best_score = m, score

        if limit_reached():
            break

        # Perturb
        pert = []
        for rem in current:
            for add in remaining:
                m = compute_metrics([v for v in current if v != rem] + [add])
                if not m:
                    continue
                score = metric_value(m)
                if score == float("-inf"):
                    continue
                pert.append((m, rem, add, score))
        if pert:
            m, rem, add, score = max(pert, key=lambda x: x[3])
            if score > best_score:
                current.remove(rem)
                current.append(add)
                remaining.append(rem)
                remaining.remove(add)
                best, best_score = m, score

        if limit_reached():
            break

        # Reduce (r = 1)
        red = []
        for combo in combinations(current, len(current) - 1):
            m = compute_metrics(list(combo))
            if not m:
                continue
            score = metric_value(m)
            if score == float("-inf"):
                continue
            red.append((m, combo, score))
        if red:
            m, combo, score = max(red, key=lambda x: x[2])
            if score > best_score:
                current, best, best_score = list(combo), m, score

        if limit_reached():
            break

        # Correlation clean-up
        again = True
        while again:
            if limit_reached():
                break
            again = False
            for i in range(len(current)):
                for j in range(i + 1, len(current)):
                    v1, v2 = current[i], current[j]
                    if context.abs_corr.loc[v1, v2] > config.corr_threshold:
                        m1 = compute_metrics([v for v in current if v != v1])
                        m2 = compute_metrics([v for v in current if v != v2])
                        if m1 and not m2:
                            drop = v1
                        elif m2 and not m1:
                            drop = v2
                        elif m1 and m2:
                            drop = v2 if metric_value(m1) >= metric_value(m2) else v1
                        else:
                            drop = (
                                v1
                                if context.abs_corr_y[v1] < context.abs_corr_y[v2]
                                else v2
                            )
                        current.remove(drop)
                        dropped.append(drop)
                        remaining = [
                            v
                            for v in context.cols
                            if v not in current and v not in dropped and v not in vif_veto
                        ]
                        swap_log.append((drop, v2 if drop == v1 else v1))
                        m_eval = compute_metrics(current)
                        if m_eval:
                            score = metric_value(m_eval)
                            if score == float("-inf"):
                                best, best_score = None, float("-inf")
                            else:
                                best, best_score = m_eval, score
                        else:
                            best, best_score = None, float("-inf")
                        again = True
                        break
                if again:
                    break

        if limit_reached():
            break

        # Swap
        improved = True
        while improved:
            if limit_reached():
                break
            improved = False
            for rem, keep in swap_log:
                if keep in current and rem not in current:
                    cand = current.copy()
                    cand.remove(keep)
                    cand.append(rem)
                    m = compute_metrics(cand)
                    if not m:
                        continue
                    score = metric_value(m)
                    if score > best_score:
                        current, best, best_score = cand, m, score
                        improved = True
                        break

        # Convergence with fixed tolerance
        repeats = repeats + 1 if best_score == last_score else 0
        last_score = best_score
        if repeats >= 2:  # fixed inline
            break

    return best, current, remaining, dropped


def _process_combination_worker(
    context: EPRSContext,
    config: EPRSConfig,
    init_vars: list[str],
    max_r2_calls: Optional[int],
):
    _ensure_heavy_imports_loaded()
    calc_vif, _ = make_vif_funcs(context)
    metric_key = config.target_metric
    include_loo = metric_key in LOO_TARGET_METRICS
    threshold_value = _metric_threshold_value(config)

    def metric_value_local(metrics: dict[str, float]) -> float:
        return _metric_score(metric_key, metrics)

    local_hits: list[dict] = []
    seen_sets = set()
    counter = {"r2_calls": 0}
    start_time = time.process_time()

    def limit_reached_local() -> bool:
        return max_r2_calls is not None and counter["r2_calls"] >= max_r2_calls

    def compute_metrics(vars_: list[str]) -> Optional[dict]:
        if limit_reached_local():
            return None
        res = _base_compute_metrics(
            context,
            config,
            vars_,
            counter=counter,
        )
        if res and metric_value_local(res) >= threshold_value:
            vifs = calc_vif(vars_)
            if vifs.size == 0 or np.nanmax(vifs) <= config.vif_threshold:
                vset = tuple(sorted(vars_))
                if vset not in seen_sets:
                    seen_sets.add(vset)
                    record = {
                        "R2": res["R2"],
                        "R2_adj": res["R2_adj"],
                        "Variables": vars_.copy(),
                        "RMSE": res["RMSE"],
                        "s": res.get("s"),
                        "MAE": res["MAE"],
                    }
                    if include_loo:
                        if "R2_loo" in res:
                            record["R2_loo"] = res["R2_loo"]
                        if "RMSE_loo" in res:
                            record["RMSE_loo"] = res["RMSE_loo"]
                    local_hits.append(record)
        return res

    current, dropped, vif_veto = init_vars.copy(), [], []

    while True:
        remaining = [
            v
            for v in context.cols
            if v not in current and v not in dropped and v not in vif_veto
        ]
        best, current, remaining, dropped = eprs(
            context,
            config,
            current,
            remaining,
            dropped,
            vif_veto,
            compute_metrics,
            counter,
            max_r2_calls,
        )
        if not best:
            break

        # VIF pruning
        changed = False
        vif_vals = calc_vif(current)
        while vif_vals.size and np.nanmax(vif_vals) > config.vif_threshold:
            hi = [v for v, v_ in zip(current, vif_vals) if v_ > config.vif_threshold]
            scores: list[tuple[float, str]] = []
            for h in hi:
                reduced = [v for v in current if v != h]
                metrics = compute_metrics(reduced)
                if metrics:
                    score = metric_value_local(metrics)
                    if score == float("-inf"):
                        continue
                    scores.append((score, h))
            if not scores:
                break
            _, drop = max(scores, key=lambda x: x[0])
            current.remove(drop)
            dropped.append(drop)
            vif_veto.append(drop)
            changed = True
            if not compute_metrics(current):
                break
            vif_vals = calc_vif(current)
        if not changed:
            break

    end_time = time.process_time()
    cpu_time = (end_time - start_time) / 60.0  # minutes

    return local_hits, counter["r2_calls"], cpu_time


def run_eprs(
    context: EPRSContext,
    config: EPRSConfig,
    progress_callback: Optional[Callable[[str], None]] = None,
    progress_hook: Optional[Callable[[int, int, int], None]] = None,
    stop_event: Optional[EventLike] = None,
):
    _ensure_heavy_imports_loaded()

    def log(message: str):
        if progress_callback:
            progress_callback(message)

    def notify_progress(done: int, stage: int = 1, total_override: Optional[int] = None):
        if progress_hook:
            total = config.n_seeds if total_override is None else total_override
            progress_hook(done, total, stage)

    if stop_event and stop_event.is_set():
        log("Cancellation requested. Aborting run.")
        raise AnalysisCancelled()

    log("Identifying correlated predictors...")

    random.seed(getattr(config, "random_state", 42))
    all_vars = list(context.cols)
    starts = [random.sample(all_vars, config.seed_size) for _ in range(config.n_seeds)]

    _, vif_stats = make_vif_funcs(context)
    max_workers = max(1, config.n_jobs)
    executor = ProcessPoolExecutor(max_workers=max_workers)
    executor_shutdown = False
    cancellation_reported = False

    def shutdown_executor(cancel: bool = False) -> None:
        nonlocal executor_shutdown
        if executor_shutdown:
            return
        try:
            # Always wait for worker cleanup to avoid queue-manager errors on cancellation.
            executor.shutdown(wait=True, cancel_futures=cancel)
        except TypeError:
            # Python < 3.9 does not support the cancel_futures keyword. In that
            # case we already cancelled the futures manually, so fall back to
            # the older signature.
            executor.shutdown(wait=True)
        executor_shutdown = True

    def handle_cancellation(futures: list) -> None:
        nonlocal cancellation_reported
        if not cancellation_reported:
            log("Cancellation requested. Aborting run.")
            cancellation_reported = True
        for fut in futures:
            fut.cancel()
        shutdown_executor(cancel=True)
        raise AnalysisCancelled()

    def submit_indices(indices: list[int], max_r2_calls: int) -> list:
        futures: list = []
        for idx in indices:
            if stop_event and stop_event.is_set():
                handle_cancellation(futures)
            futures.append(
                executor.submit(
                    _process_combination_worker,
                    context,
                    config,
                    starts[idx],
                    max_r2_calls,
                )
            )
        return futures

    def collect_results(futures: list):
        nonlocal processed_seeds
        for future in as_completed(futures):
            if stop_event and stop_event.is_set():
                handle_cancellation(futures)
            result = future.result()
            processed_seeds = min(processed_seeds + 1, config.n_seeds)
            notify_progress(processed_seeds)
            yield result

    log("")
    log("Starting...")

    processed_seeds = 0
    notify_progress(processed_seeds)
    all_results: list = []
    test_results: list = []

    iteration_mode = getattr(config, "iterations_mode", ITERATION_MODE_AUTO) or ITERATION_MODE_AUTO
    iteration_mode = iteration_mode.lower()
    if iteration_mode not in {
        ITERATION_MODE_AUTO,
        ITERATION_MODE_MANUAL,
        ITERATION_MODE_CONVERGE,
    }:
        iteration_mode = ITERATION_MODE_AUTO
    manual_iterations = getattr(config, "max_iterations_per_seed", None)

    try:
        if iteration_mode == ITERATION_MODE_AUTO:
            # Initial calibration: use fixed 200000 calls
            log("Starting calibration run...")
            calibration_count = min(config.n_seeds, max_workers)
            calibration_indices = list(range(calibration_count))
            calibration_futures = submit_indices(calibration_indices, 200000)
            for result in collect_results(calibration_futures):
                test_results.append(result)
                all_results.append(result)

            log("Calibration completed.")

            if stop_event and stop_event.is_set():
                handle_cancellation([])

            r2_calls_list = [calls for _, calls, _ in test_results]
            avg_r2_calls_per_cpu = float(np.mean(r2_calls_list)) if r2_calls_list else 0.0
            auto_iteration_limit = max(1, int(np.ceil(avg_r2_calls_per_cpu)))
            new_MAX_R2_CALLS: Optional[int] = auto_iteration_limit * 2

            # Full execution
            remaining_idx = list(range(calibration_count, config.n_seeds))
            if remaining_idx:
                remaining_futures = submit_indices(remaining_idx, new_MAX_R2_CALLS)
                for item in collect_results(remaining_futures):
                    all_results.append(item)
        else:
            avg_r2_calls_per_cpu = 0.0
            new_MAX_R2_CALLS = (
                None
                if iteration_mode == ITERATION_MODE_CONVERGE
                else manual_iterations
            )
            remaining_idx = list(range(config.n_seeds))
            if remaining_idx:
                remaining_futures = submit_indices(remaining_idx, new_MAX_R2_CALLS)
                for item in collect_results(remaining_futures):
                    all_results.append(item)

        notify_progress(min(processed_seeds, config.n_seeds))

        if stop_event and stop_event.is_set():
            handle_cancellation([])
    finally:
        shutdown_executor()

    # Flatten results
    all_hits = [row for hits, _, _ in all_results for row in hits]
    r2_calls_per_cpu = [calls for _, calls, _ in all_results]
    cpu_time_per_cpu = [t for _, _, t in all_results]

    if r2_calls_per_cpu:
        avg_r2_calls_per_cpu = float(np.mean(r2_calls_per_cpu))

    # CPU time summary
    cpu_search_minutes = float(np.sum(cpu_time_per_cpu)) if cpu_time_per_cpu else 0.0

    models_explored = int(np.sum(r2_calls_per_cpu)) if r2_calls_per_cpu else 0

    models_found = len(all_hits)

    results_df = None

    results_df, vif_cpu_minutes = _finalize_results_dataframe(
        all_hits,
        config,
        vif_stats,
        progress_hook=progress_hook,
    )

    if results_df is None:
        metric_label = TARGET_METRIC_DISPLAY.get(config.target_metric, R_SQUARED_SYMBOL)
        comparator = ">=" if config.target_metric != "RMSE_loo" else "<="
        threshold_display = _format_threshold_display(getattr(config, "tm_cutoff", None))
        if getattr(config, "tm_cutoff", None) is None:
            log("No models found with the current cutoff settings")
        else:
            log(f"No models found with {metric_label} {comparator} {threshold_display}")

    return {
        "results_path": None,
        "models_found": models_found,
        "models_explored": models_explored,
        "avg_r2_calls": avg_r2_calls_per_cpu,
        "max_r2_calls": new_MAX_R2_CALLS,
        "cpu_time_search": cpu_search_minutes,
        "cpu_time_total": cpu_search_minutes + vif_cpu_minutes,
        "cpu_time_vif": vif_cpu_minutes,
        "results_df": results_df,
    }


def run_all_subsets(
    context: EPRSContext,
    config: EPRSConfig,
    progress_callback: Optional[Callable[[str], None]] = None,
    progress_hook: Optional[Callable[[int, int, int], None]] = None,
    stop_event: Optional[EventLike] = None,
):
    _ensure_heavy_imports_loaded()

    def log(message: str):
        if progress_callback:
            progress_callback(message)

    def notify_progress(done: int, total: int, stage: int = 1):
        if progress_hook:
            progress_hook(done, total, stage)

    def check_cancellation():
        if stop_event and stop_event.is_set():
            raise AnalysisCancelled()

    try:
        check_cancellation()
    except AnalysisCancelled:
        log("Cancellation requested. Aborting run.")
        raise

    log("Identifying correlated predictors...")
    all_vars = list(context.cols)
    max_size = min(config.max_vars, len(all_vars))
    if max_size <= 0 or not all_vars:
        log("No predictors available for all subsets evaluation.")
        return {
            "results_path": None,
            "models_found": 0,
            "models_explored": 0,
            "avg_r2_calls": 0.0,
            "max_r2_calls": 0,
            "cpu_time_search": 0.0,
            "cpu_time_total": 0.0,
            "cpu_time_vif": 0.0,
            "results_df": None,
        }

    total_combos = sum(math.comb(len(all_vars), k) for k in range(1, max_size + 1))
    total_combos = max(int(total_combos), 1)

    log("Preparing all possible combinations...")
    log(f"Total subsets: {total_combos}")

    calc_vif, vif_stats = make_vif_funcs(context)
    include_loo = config.target_metric in LOO_TARGET_METRICS
    threshold_value = _metric_threshold_value(config)

    log("Filtering subsets with correlated predictors...")

    log("")
    log("Starting...")

    notify_progress(0, total_combos)

    max_workers = max(1, config.n_jobs)
    executor = ProcessPoolExecutor(max_workers=max_workers)

    hits: list[dict] = []
    processed = 0
    total_calls = 0.0
    cpu_search_minutes = 0.0
    cpu_minutes_per_pid: dict[int, float] = {}

    try:
        combos_iter = iter_combinations(all_vars, max_size, stop_event)
        for record, calls, cpu_minutes, pid, correlation_blocked in executor.map(
            _all_subsets_worker,
            combos_iter,
            repeat(context),
            repeat(config),
            repeat(include_loo),
            repeat(threshold_value),
            chunksize=1000,
        ):
            processed += 1
            notify_progress(processed, total_combos)
            total_calls += float(calls)
            cpu_search_minutes += cpu_minutes
            cpu_minutes_per_pid[pid] = cpu_minutes_per_pid.get(pid, 0.0) + cpu_minutes
            if not correlation_blocked and record is not None:
                hits.append(record)
            if stop_event and stop_event.is_set():
                raise AnalysisCancelled()
    except AnalysisCancelled:
        log("Cancellation requested. Aborting run.")
        raise
    finally:
        try:
            # Always wait for worker shutdown so queue manager threads exit cleanly
            # even when cancellation is requested.
            executor.shutdown(
                wait=True,
                cancel_futures=bool(stop_event and stop_event.is_set()),
            )
        except TypeError:
            executor.shutdown(wait=True)

    results_df, vif_cpu_minutes = _finalize_results_dataframe(
        hits,
        config,
        vif_stats,
        progress_hook=progress_hook,
    )
    models_found = len(hits)

    if results_df is None:
        metric_label = TARGET_METRIC_DISPLAY.get(config.target_metric, R_SQUARED_SYMBOL)
        comparator = ">=" if config.target_metric != "RMSE_loo" else "<="
        threshold_display = _format_threshold_display(getattr(config, "tm_cutoff", None))
        if getattr(config, "tm_cutoff", None) is None:
            log("No models found with the current cutoff settings")
        else:
            log(f"No models found with {metric_label} {comparator} {threshold_display}")

    avg_calls = total_calls / processed if processed else 0.0
    max_calls = int(total_calls)

    cpu_minutes_workers = list(cpu_minutes_per_pid.values())

    return {
        "results_path": None,
        "models_found": models_found,
        "models_explored": int(total_calls),
        "avg_r2_calls": avg_calls,
        "max_r2_calls": max_calls,
        "cpu_time_search": cpu_search_minutes,
        "cpu_time_total": cpu_search_minutes + vif_cpu_minutes,
        "cpu_time_vif": vif_cpu_minutes,
        "results_df": results_df,
    }


class Tooltip:
    """Display a delayed tooltip for Tk widgets."""

    def __init__(self, widget: tk.Widget, text: str, *, delay: int = 500) -> None:
        self.widget = widget
        self.text = text
        self.delay = delay
        self._after_id: Optional[str] = None
        self._tip_window: Optional[tk.Toplevel] = None

        widget.bind("<Enter>", self._schedule, add="+")
        widget.bind("<Leave>", self._cancel, add="+")
        widget.bind("<ButtonPress>", self._cancel, add="+")

    def _schedule(self, _event: tk.Event) -> None:
        self._cancel(_event)
        self._after_id = self.widget.after(self.delay, self._show)

    def _cancel(self, _event: tk.Event) -> None:
        if self._after_id is not None:
            self.widget.after_cancel(self._after_id)
            self._after_id = None
        self._hide()

    def _show(self) -> None:
        if self._tip_window is not None:
            return
        x = self.widget.winfo_rootx() + 10
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 6
        window = tk.Toplevel(self.widget)
        window.wm_overrideredirect(True)
        window.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            window,
            text=self.text,
            justify="left",
            background="#fff8c6",
            relief="solid",
            borderwidth=1,
            padx=6,
            pady=4,
        )
        label.pack()
        self._tip_window = window

    def _hide(self) -> None:
        if self._tip_window is not None:
            self._tip_window.destroy()
            self._tip_window = None

    def show_immediate(self) -> None:
        if self._after_id is not None:
            self.widget.after_cancel(self._after_id)
            self._after_id = None
        self._show()


def show_yellow_popover(parent: tk.Widget, *, title: str, message: str) -> None:
    toplevel = tk.Toplevel(parent)
    toplevel.title(title)
    toplevel.transient(parent.winfo_toplevel())
    toplevel.resizable(False, False)
    toplevel.configure(bg="#fff3b0")

    frame = ttk.Frame(toplevel, padding=12)
    frame.pack(fill="both", expand=True)

    ttk.Label(frame, text=title, font=("TkDefaultFont", 11, "bold")).pack(anchor="w")
    message_label = ttk.Label(frame, text=message, wraplength=320, justify="left")
    message_label.pack(anchor="w", pady=(6, 12))

    ttk.Button(frame, text="Close", command=toplevel.destroy).pack(anchor="e")

    toplevel.grab_set()
    toplevel.wait_window()


def info_button(
    parent: tk.Widget,
    tooltip_text: str,
    title: str,
    *,
    show_popover: bool = True,
) -> ttk.Button:
    style = ttk.Style()
    style.configure("InfoButton.TButton", padding=(0, 0, 0, 0))

    button = ttk.Button(parent, text="\u2139", width=2, style="InfoButton.TButton")
    button.configure(cursor="hand2")
    tooltip = Tooltip(button, tooltip_text)

    def _nudge_geometry(event: tk.Event) -> None:
        widget = event.widget
        manager = widget.winfo_manager()
        try:
            if manager == "pack":
                widget.pack_configure(pady=(-1, 0))
            elif manager == "grid":
                widget.grid_configure(pady=(-1, 0))
        except tk.TclError:
            pass

    button.bind("<Map>", _nudge_geometry, add="+")

    if show_popover:

        def on_click() -> None:
            show_yellow_popover(parent, title=title, message=tooltip_text)

        button.configure(command=on_click)
    else:

        def on_click() -> None:
            tooltip.show_immediate()

        button.configure(command=on_click)

    button._tooltip = tooltip  # type: ignore[attr-defined]
    return button


class MLRXApp(tk.Tk):
    _CORR_CACHE_PREDICTORS = "predictors"
    _CORR_CACHE_WITH_TARGET = "with_target"

    def __init__(self):
        super().__init__()
        self.title("MLR-X 1.0")
        self.geometry("1000x720+250+50")
        self.minsize(900, 640)

        self.queue: "queue.Queue[tuple[str, object]]" = queue.Queue()
        self.worker_thread: Optional[threading.Thread] = None
        self.last_results_df: Optional[pd.DataFrame] = None
        self.full_results_df: Optional[pd.DataFrame] = None
        self.last_context: Optional[EPRSContext] = None
        self.last_config: Optional[EPRSConfig] = None
        self.last_split_settings: dict = {}
        self.last_internal_results: list[dict] = []
        self.last_external_results: list[dict] = []
        self.full_internal_results: list[dict] = []
        self.full_external_results: list[dict] = []
        self._training_order: list[int] = []
        self.last_results_metadata: Optional[dict] = None
        self.last_cpu_time_search_minutes: Optional[float] = None
        self.last_cpu_time_total_minutes: Optional[float] = None
        self.observation_cache: dict[int, tuple[pd.DataFrame, float]] = {}
        self.correlation_cache: dict[tuple[int, str], pd.DataFrame] = {}
        self.results_load_path = tk.StringVar(value="models.csv")
        self.results_export_path = Path("models.csv")
        default_sort_label = RESULTS_SORT_KEY_TO_DISPLAY.get("R2", DEFAULT_SORT_LABEL)
        self.results_sort_var = tk.StringVar(value=default_sort_label)
        self.data_path_var = tk.StringVar(value="data.csv")
        self._filter_variable_entry = tk.StringVar(value="")
        self._filter_variables: list[tuple[str, tk.BooleanVar]] = []
        self._filter_window: Optional[tk.Toplevel] = None
        self._filter_list_frame: Optional[ttk.Frame] = None
        self.stop_event: EventLike = threading.Event()
        self.total_seeds = 0
        self.processed_seeds = 0
        self.last_progress_percent: Optional[int] = None
        self.progress_total_steps = 2
        self.completed_progress_steps = 0
        self.current_progress_stage = 1
        self.progress_started = False
        self.progress_step_var = tk.StringVar(value=self._format_progress_steps())
        self.progress_var = tk.StringVar(value=self._format_progress_text(0))
        self.progress_bar: Optional[ttk.Progressbar] = None
        self._progress_label_style = "RunProgress.TLabel"
        self._progress_label_disabled_style = "RunProgress.Disabled.TLabel"
        self._progressbar_style = "RunProgress.Horizontal.TProgressbar"
        self._progressbar_disabled_style = "RunProgress.Disabled.Horizontal.TProgressbar"
        self.split_mode = tk.StringVar(value="none")
        self.holdout_ready = False
        self._holdout_refresh_job: Optional[str] = None
        self._results_tree_layouts: dict[
            ttk.Treeview, tuple[_ResultsColumnLayout, ...]
        ] = {}
        self._results_tree_last_widths: dict[ttk.Treeview, int] = {}
        self._results_tab_suppression = 0
        self.kfold_settings: dict[str, object] = {
            "enabled": False,
            "folds": None,
            "repeats": None,
        }
        self._splash_frame: Optional[tk.Frame] = None
        self._splash_continue_button: Optional[ttk.Button] = None
        self._main_ui_initialized = False
        self._current_tab_id: Optional[str] = None
        self._citation_window: Optional[tk.Toplevel] = None
        self._help_tab_initialized = False
        self._last_efficiency_notice: Optional[str] = None
        self._efficiency_notify_job: Optional[str] = None
        self._last_efficiency_max_vars: Optional[int] = None
        self._seed_minimum_job: Optional[str] = None
        self.data_path_var.trace_add(
            "write", lambda *_: self._reset_efficiency_notice_tracking()
        )

        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._show_splash_screen()

    def _initialize_main_ui(self) -> None:
        if self._main_ui_initialized:
            return
        self._build_ui()
        self._results_layout_initialized = False
        self.bind("<Map>", self._handle_first_map)
        self._main_ui_initialized = True

    def _show_splash_screen(self) -> None:
        if self._splash_frame is not None:
            return

        self._splash_frame = ttk.Frame(self)
        self._splash_frame.pack(fill="both", expand=True)

        content = ttk.Frame(self._splash_frame, padding=40)
        content.pack(expand=True)

        title_label = ttk.Label(content, text="Welcome to MLR-X 1.0", font=("TkDefaultFont", 14, "bold"))
        title_label.pack(pady=(0, 12))

        message = "A free software for multiple linear regression on small and large datasets."
        description_label = ttk.Label(
            content,
            text=message,
            wraplength=400,
            justify="center",
        )
        description_label.pack(pady=(0, 20))

        license_message = (
            "Distributed under the public license AGPL-3.0\n"
            "© 2025. All rights reserved."
        )
        license_label = ttk.Label(
            content,
            text=license_message,
            wraplength=360,
            justify="center",
        )
        license_label.pack(pady=(0, 20))

        self._splash_continue_button = ttk.Button(
            content,
            text="Enter",
            command=self._on_splash_continue,
        )
        self._splash_continue_button.pack()

    def _hide_splash_screen(self) -> None:
        if self._splash_frame is None:
            return

        self._splash_frame.destroy()
        self._splash_frame = None
        if self._splash_continue_button is not None:
            self._splash_continue_button = None

    def _on_splash_continue(self) -> None:
        if self._splash_continue_button is not None:
            self._splash_continue_button.config(state="disabled")
        if not self._main_ui_initialized:
            self._initialize_main_ui()
        self._hide_splash_screen()

    def _build_ui(self):
        padding = {"padx": 10, "pady": 5}

        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=5, pady=5)
        self.notebook.bind("<<NotebookTabChanged>>", self._handle_tab_changed)

        self.external_test_path = tk.StringVar()
        self.external_delimiter_var = tk.StringVar(value=";")
        self.external_test_path.trace_add("write", self._schedule_holdout_refresh)
        self.external_delimiter_var.trace_add("write", self._schedule_holdout_refresh)

        self.config_tab = ttk.Frame(self.notebook)
        self.results_tab = ttk.Frame(self.notebook)
        self.summary_tab = SummaryTab(self.notebook, self)
        self.validation_tab = ValidationTab(self.notebook, self)
        self.diagnostics_tab = ObservationDiagnosticsTab(self.notebook, self)
        self.visual_tab = VisualizationTab(self.notebook, self)
        self.variable_tab = VariableExplorerTab(self.notebook, self)
        self.help_tab = ttk.Frame(self.notebook)

        self.notebook.add(self.config_tab, text="Setup & Run")
        self.notebook.add(self.results_tab, text=" Models ")
        self.notebook.add(self.validation_tab, text="Validation")
        self.notebook.add(self.diagnostics_tab, text="Diagnostics")
        self.notebook.add(self.visual_tab, text="Visualization")
        self.notebook.add(self.summary_tab, text="Summary")
        self.notebook.add(self.variable_tab, text="Desktop")
        self.notebook.add(self.help_tab, text="  Help  ")

        self.notebook.tab(self.validation_tab, state="disabled")
        self.notebook.tab(self.summary_tab, state="disabled")
        self.notebook.tab(self.diagnostics_tab, state="disabled")
        self.notebook.tab(self.visual_tab, state="disabled")

        self._initialize_help_tab()
        self._current_tab_id = self.notebook.select()

        data_frame = ttk.LabelFrame(self.config_tab, text="Dataset")
        data_frame.pack(fill="x", padx=10, pady=10)

        self.dependent_var_var = tk.StringVar(value="Last column")
        self.non_variable_var = tk.StringVar(value="First column")
        self.constant_filter_enabled = tk.BooleanVar(value=False)
        self.constant_threshold_var = tk.StringVar(value="90")
        self.exclude_obs_var = tk.StringVar(value="")
        dataset_row_padding = {"padx": 10, "pady": (0, 5)}

        ttk.Label(data_frame, text="CSV file:").grid(
            row=0,
            column=0,
            sticky="nw",
            **dataset_row_padding,
        )
        data_frame.grid_columnconfigure(1, weight=1)
        path_entry = ttk.Entry(data_frame, textvariable=self.data_path_var)
        path_entry.grid(row=0, column=1, sticky="nwe", **dataset_row_padding)

        button_grid = ttk.Frame(data_frame)
        button_grid.grid(
            row=0,
            column=2,
            rowspan=2,
            sticky="ne",
            padx=(10, 5),
            pady=dataset_row_padding["pady"],
        )
        button_grid.columnconfigure(0, weight=1, uniform="action_buttons")
        button_grid.columnconfigure(1, weight=1, uniform="action_buttons")
        button_grid.rowconfigure(0, weight=1)
        button_grid.rowconfigure(1, weight=1)

        browse_btn = ttk.Button(button_grid, text="Browse", command=self._browse_file)
        browse_btn.grid(row=0, column=0, sticky="nsew", padx=(0, 5), pady=(0, 5))

        self.run_button = tk.Button(
            button_grid,
            text="Run analysis",
            command=self._start_analysis,
            bg="#b9d6ff",
            fg="black",
            activebackground="#c8e0ff",
            activeforeground="black",
            relief="solid",
            bd=1,
            highlightthickness=1,
            highlightbackground="black",
        )
        self.run_button.grid(row=0, column=1, sticky="nsew", padx=(5, 0), pady=(0, 5))

        preview_btn = ttk.Button(button_grid, text="Preview data", command=self._preview_data)
        preview_btn.grid(row=1, column=0, sticky="nsew", padx=(0, 5), pady=(0, 5))

        self.cancel_button = ttk.Button(
            button_grid,
            text="Cancel",
            command=self._cancel_analysis,
            state="disabled",
        )
        self.cancel_button.grid(row=1, column=1, sticky="nsew", padx=(5, 0), pady=(0, 5))

        ttk.Label(data_frame, text="CSV delimiter:").grid(
            row=1,
            column=0,
            sticky="nw",
            padx=dataset_row_padding["padx"],
            pady=(0, dataset_row_padding["pady"][1]),
        )
        self.delimiter_var = tk.StringVar(value=";")
        delimiter_row = ttk.Frame(data_frame)
        delimiter_row.grid(
            row=1,
            column=1,
            sticky="nw",
            padx=(10, 0),
            pady=(0, dataset_row_padding["pady"][1]),
        )
        self.delimiter_box = ttk.Combobox(
            delimiter_row,
            textvariable=self.delimiter_var,
            values=[";", ",", "\\t", "|"],
            width=6,
            state="normal",
        )
        self.delimiter_box.pack(side="left")
        dependent_frame = ttk.Frame(delimiter_row)
        dependent_frame.pack(side="left", padx=(16, 0))
        ttk.Label(dependent_frame, text="Dependent variable:").pack(side="left")
        self.dependent_box = ttk.Combobox(
            dependent_frame,
            textvariable=self.dependent_var_var,
            values=["Last column", "First column", "Second column", "Third column"],
            width=13,
            state="readonly",
        )
        self.dependent_box.pack(side="left", padx=(4, 0))

        non_variable_frame = ttk.Frame(delimiter_row)
        non_variable_frame.pack(side="left", padx=(16, 0))
        ttk.Label(non_variable_frame, text="Non-variable columns:").pack(side="left")
        self.non_variable_box = ttk.Combobox(
            non_variable_frame,
            textvariable=self.non_variable_var,
            values=[
                "None",
                "First column",
                "First and second column",
                "First, second, and third column",
            ],
            state="readonly",
            width=26,
        )
        self.non_variable_box.pack(side="left", padx=(4, 0))

        constant_frame = ttk.Frame(data_frame)
        constant_frame.grid(
            row=2,
            column=0,
            columnspan=2,
            sticky="w",
            padx=(10, 0),
            pady=(0, 5),
        )
        self.constant_filter_check = ttk.Checkbutton(
            constant_frame,
            text="Exclude near-constant predictors",
            variable=self.constant_filter_enabled,
            command=self._toggle_constant_filter,
        )
        self.constant_filter_check.pack(side="left")
        ttk.Label(constant_frame, text="Threshold (%):").pack(side="left", padx=(12, 4))
        self.constant_threshold_spin = ttk.Spinbox(
            constant_frame,
            from_=50,
            to=100,
            increment=1,
            width=5,
            textvariable=self.constant_threshold_var,
            state="disabled",
        )
        self.constant_threshold_spin.pack(side="left")
        ttk.Label(constant_frame, text="Exclude observations (IDs):").pack(
            side="left", padx=(16, 4)
        )
        self.exclude_obs_entry = ttk.Entry(
            constant_frame,
            textvariable=self.exclude_obs_var,
            width=12,
        )
        self.exclude_obs_entry.pack(side="left")
        ttk.Label(
            constant_frame,
            text="(e.g., 3,9,4-8)",
            foreground="#666666",
        ).pack(side="left", padx=(4, 0))

        progress_container = ttk.Frame(data_frame)
        progress_container.place(relx=1.0, rely=1.0, anchor="se", x=-5, y=-5)
        progress_container.columnconfigure(1, weight=1)
        self._configure_progress_styles()
        self.progress_stage_label = ttk.Label(
            progress_container,
            textvariable=self.progress_step_var,
            anchor="w",
            width=6,
            style=self._progress_label_style,
        )
        self.progress_stage_label.grid(row=0, column=0, sticky="w")
        self.progress_bar = ttk.Progressbar(
            progress_container,
            mode="determinate",
            maximum=100,
            style=self._progressbar_style,
        )
        self.progress_bar.grid(row=0, column=1, sticky="ew")
        self.progress_bar.configure(value=0)
        self.progress_label = ttk.Label(
            progress_container,
            textvariable=self.progress_var,
            anchor="w",
            width=14,
            style=self._progress_label_style,
        )
        self.progress_label.grid(row=0, column=2, sticky="e", padx=(10, 0))
        self._set_progress_disabled_appearance(False)

        data_frame.columnconfigure(1, weight=1)

        # Split controls
        split_frame = ttk.LabelFrame(self.config_tab, text="Data splitting")
        split_frame.pack(fill="x", padx=10, pady=(0, 10))

        ttk.Radiobutton(
            split_frame,
            text="None (use full dataset)",
            variable=self.split_mode,
            value="none",
            command=self._update_split_controls,
        ).grid(row=0, column=0, sticky="w", **padding)

        self.random_test_size = tk.DoubleVar(value=20.0)
        random_row = ttk.Frame(split_frame)
        random_row.grid(
            row=1,
            column=0,
            columnspan=4,
            sticky="w",
            padx=padding["padx"],
            pady=5,
        )
        ttk.Radiobutton(
            random_row,
            text="Random",
            variable=self.split_mode,
            value="random",
            command=self._update_split_controls,
        ).pack(side="left")
        ttk.Label(random_row, text="Test size (%):").pack(side="left", padx=(8, 0))
        self.random_test_entry = ttk.Entry(
            random_row,
            textvariable=self.random_test_size,
            width=6,
            state="disabled",
        )
        self.random_test_entry.pack(side="left", padx=(4, 0))

        ttk.Radiobutton(
            split_frame,
            text="Manual",
            variable=self.split_mode,
            value="manual",
            command=self._update_split_controls,
        ).grid(row=2, column=0, sticky="w", **padding)

        self.manual_frame = ttk.Frame(split_frame)
        self.manual_frame.grid(row=3, column=0, columnspan=4, sticky="we", padx=10)
        self.manual_train_ids = tk.StringVar()
        self.manual_test_ids = tk.StringVar()
        ttk.Label(self.manual_frame, text="Training IDs (e.g., 1-100,105):").grid(
            row=0, column=0, sticky="w", pady=2
        )
        self.manual_train_entry = ttk.Entry(
            self.manual_frame,
            textvariable=self.manual_train_ids,
            width=20,
            state="disabled",
        )
        self.manual_train_entry.grid(row=0, column=1, sticky="w", pady=2, padx=(5, 8))
        ttk.Label(
            self.manual_frame, text="Testing IDs (leave blank to use the rest):"
        ).grid(row=0, column=2, sticky="w", pady=2)
        self.manual_test_entry = ttk.Entry(
            self.manual_frame,
            textvariable=self.manual_test_ids,
            width=20,
            state="disabled",
        )
        self.manual_test_entry.grid(row=0, column=3, sticky="w", pady=2, padx=(5, 0))
        self.manual_frame.columnconfigure(1, weight=0)
        self.manual_frame.columnconfigure(3, weight=0)

        split_frame.columnconfigure(0, weight=1)

        self._update_split_controls()

        params_frame = ttk.LabelFrame(self.config_tab, text="Settings")
        params_frame.pack(fill="x", padx=10, pady=(0, 10))

        settings_row_padding = {"padx": padding["padx"], "pady": (5, 5)}

        defaults = EPRSConfig()
        self.constant_threshold_var.set(str(defaults.constant_threshold))
        self.params_vars = {
            "max_vars": tk.StringVar(value=""),
            "n_seeds": tk.StringVar(value=str(defaults.n_seeds)),
            "seed_size": tk.StringVar(value=""),
            "random_state": tk.StringVar(value=str(defaults.random_state)),
            "signif_lvl": tk.DoubleVar(value=defaults.signif_lvl),
            "corr_threshold": tk.DoubleVar(value=defaults.corr_threshold),
            "vif_threshold": tk.DoubleVar(value=defaults.vif_threshold),
            "tm_cutoff": tk.StringVar(
                value=str(defaults.tm_cutoff)
            ),
            "export_limit": tk.IntVar(value=defaults.export_limit),
            "n_jobs": tk.IntVar(value=defaults.n_jobs),
        }
        default_cov_label = COVARIANCE_KEY_TO_DISPLAY.get(
            defaults.cov_type, next(iter(COVARIANCE_DISPLAY_TO_KEY))
        )
        self.cov_type_var = tk.StringVar(value=default_cov_label)
        self.target_metric_choice = tk.StringVar(
            value=TARGET_METRIC_DISPLAY[defaults.target_metric]
        )
        self.method_choice = tk.StringVar(value=METHOD_KEY_TO_DISPLAY[defaults.method])
        self.method_choice.trace_add("write", lambda *_: self._on_method_change())
        self.params_vars["max_vars"].trace_add(
            "write", lambda *_: self._maybe_notify_method_efficiency()
        )
        self.params_vars["max_vars"].trace_add(
            "write", lambda *_: self._update_seed_settings_summary()
        )
        self.params_vars["n_seeds"].trace_add(
            "write", lambda *_: self._schedule_seed_minimum_enforcement()
        )

        self.seed_entry: Optional[ttk.Entry] = None
        self.seed_size_entry: Optional[ttk.Entry] = None
        self.seed_recommend_btn: Optional[ttk.Button] = None
        self.seed_settings_button: Optional[ttk.Button] = None
        self.seed_settings_dialog: Optional[tk.Toplevel] = None
        self.random_state_entry: Optional[ttk.Entry] = None
        self.seed_size_mode = tk.StringVar(value="default")
        self.seed_size_manual_var = tk.StringVar(value="")
        self.random_state_mode = tk.StringVar(value="default")
        self.random_state_manual_var = tk.StringVar(value="")
        self.allow_small_seed_count = tk.BooleanVar(value=False)
        # Internal override: set to False to allow seed settings with All subsets.
        self._restrict_seed_settings_to_eprs = True
        self.seed_size_applied_var = tk.StringVar(value="")
        self.random_state_applied_var = tk.StringVar(value="")
        self.guardrail_applied_var = tk.StringVar(value="")
        self.seed_settings_summary_var = tk.StringVar(value="")
        self.iterations_mode_var = tk.StringVar(value=ITERATION_MODE_AUTO)
        self.manual_iterations_var = tk.StringVar(value="")
        self.iterations_dialog: Optional[tk.Toplevel] = None
        self.iterations_button: Optional[ttk.Button] = None
        self.iteration_mode_value_var = tk.StringVar(value=ITERATION_MODE_AUTO)
        self.iteration_mode_label: Optional[ttk.Label] = None
        self.iteration_mode_value_label: Optional[ttk.Label] = None
        self._iteration_mode_normal_fg: Optional[str] = None
        self._iteration_mode_disabled_fg: Optional[str] = None
        self.iterations_mode_var.trace_add(
            "write", lambda *_: self._refresh_iteration_mode_display()
        )

        method_row = 0
        method_row_frame = ttk.Frame(params_frame)
        method_row_frame.grid(
            row=method_row,
            column=0,
            columnspan=4,
            sticky="ew",
            padx=settings_row_padding["padx"],
            pady=(padding["pady"], settings_row_padding["pady"][1]),
        )
        method_row_frame.columnconfigure(1, weight=1)
        ttk.Label(method_row_frame, text="Search methods:").grid(row=0, column=0, sticky="w")
        method_buttons = ttk.Frame(method_row_frame)
        method_buttons.grid(row=0, column=1, sticky="w", padx=(6, 0))
        for idx, display_label in enumerate(METHOD_DISPLAY_TO_KEY.keys()):
            option_frame = ttk.Frame(method_buttons)
            option_frame.grid(row=0, column=idx, padx=(0 if idx == 0 else 10, 0))
            ttk.Radiobutton(
                option_frame,
                text=display_label,
                value=display_label,
                variable=self.method_choice,
                command=self._on_method_change,
            ).pack(side="left")
            info_text = METHOD_INFO_TEXT.get(display_label)
            if info_text:
                info_button(
                    option_frame,
                    tooltip_text=info_text,
                    title=display_label,
                    show_popover=False,
                ).pack(side="left", padx=(4, 0))

        style = ttk.Style()
        self._iteration_mode_normal_fg = (
            self._iteration_mode_normal_fg
            or style.lookup("TLabel", "foreground")
            or ""
        )
        self._iteration_mode_disabled_fg = (
            self._iteration_mode_disabled_fg
            or style.lookup("TLabel", "foreground", ("disabled",))
            or style.lookup("TButton", "foreground", ("disabled",))
            or style.lookup("TEntry", "foreground", ("disabled",))
            or "gray50"
        )

        mode_display = ttk.Frame(method_row_frame)
        mode_display.grid(row=0, column=2, sticky="w")
        self.iteration_mode_label = ttk.Label(mode_display, text="Mode:")
        self.iteration_mode_label.pack(side="left")
        self.iteration_mode_value_label = ttk.Label(
            mode_display,
            textvariable=self.iteration_mode_value_var,
            width=9,
            anchor="center",
        )
        self.iteration_mode_value_label.pack(side="left")

        self.iterations_button = ttk.Button(
            method_row_frame,
            text="Set iterations/seed",
            command=self._open_iterations_dialog,
        )
        self.iterations_button.grid(row=0, column=3, sticky="e")

        self._refresh_iteration_mode_display()

        labels = [
            ("Max predictors per model", "max_vars"),
            ("Number of seeds", "n_seeds"),
            ("Significance level", "signif_lvl"),
            ("Correlation threshold", "corr_threshold"),
            ("VIF threshold", "vif_threshold"),
            ("", "tm_cutoff"),
            ("Top models to report", "export_limit"),
            ("CPU cores to use", "n_jobs"),
        ]

        hint_tooltips = {
            "n_seeds": "Values below 1000 are blocked unless you disable the guardrail in Seed settings.",
            "vif_threshold": "No greater than 5.",
            "corr_threshold": "Max allowed predictor correlation (0-1).",
            "signif_lvl": "0.05 is the recommended value. Use a smaller a for stricter criteria.",
        }

        for offset, (label, key) in enumerate(labels, start=1):
            idx = offset
            label_container = ttk.Frame(params_frame)
            label_container.grid(row=idx, column=0, sticky="w", **settings_row_padding)
            if key == "tm_cutoff":
                self.report_threshold_label_var = tk.StringVar()
                self._update_report_threshold_label()
                ttk.Label(
                    label_container, textvariable=self.report_threshold_label_var
                ).pack(side="left")
            else:
                ttk.Label(label_container, text=label + ":").pack(side="left")

            entry_container = ttk.Frame(params_frame)
            entry_container.grid(row=idx, column=1, sticky="w", **settings_row_padding)
            entry = ttk.Entry(entry_container, textvariable=self.params_vars[key], width=10)
            entry.pack(side="left")

            if key == "tm_cutoff":
                info_button(
                    entry_container,
                    tooltip_text=(
                        "Cutoff skips candidate models with insufficient predictive performance.\n"
                        "When RMSE (LOO) is selected, models with errors above the defined cutoff are discarded.\n"
                        "Enter \"none\" to run without a cutoff."
                    ),
                    title="Target metric cutoff",
                    show_popover=False,
                ).pack(side="left", padx=(4, 0))

            if key in hint_tooltips:
                info_button(
                    entry_container,
                    tooltip_text=hint_tooltips[key],
                    title=label,
                    show_popover=False,
                ).pack(side="left", padx=(4, 0))

            if key == "n_seeds":
                self.seed_entry = entry
                self.seed_settings_button = ttk.Button(
                    entry_container,
                    text="Seed settings",
                    command=self._open_seed_settings_dialog,
                )
                self.seed_settings_button.pack(side="left", padx=(6, 0))
                ttk.Label(
                    entry_container,
                    textvariable=self.seed_settings_summary_var,
                    foreground="#666666",
                ).pack(side="left", padx=(6, 0))
                covariance_container = ttk.Frame(params_frame)
                covariance_container.grid(
                    row=idx,
                    column=3,
                    sticky="e",
                    **settings_row_padding,
                )
                cov_row = ttk.Frame(covariance_container)
                cov_row.pack(side="top", anchor="e")
                ttk.Label(cov_row, text="Covariance type:").pack(side="left")
                covariance_combo = ttk.Combobox(
                    cov_row,
                    textvariable=self.cov_type_var,
                    state="readonly",
                    values=tuple(COVARIANCE_DISPLAY_TO_KEY.keys()),
                    width=8,
                )
                covariance_combo.pack(side="left", padx=(5, 0))
            elif key == "max_vars":
                target_container = ttk.Frame(params_frame)
                target_container.grid(
                    row=idx,
                    column=3,
                    sticky="e",
                    **settings_row_padding,
                )
                ttk.Label(target_container, text="Target metric:").pack(side="left")
                target_combo = ttk.Combobox(
                    target_container,
                    textvariable=self.target_metric_choice,
                    state="readonly",
                    values=tuple(TARGET_METRIC_DISPLAY.values()),
                    width=8,
                )
                target_combo.pack(side="left", padx=(5, 0))
            elif key == "n_jobs":
                load_button = ttk.Button(
                    params_frame,
                    text="Load setup (.conf)",
                    width=18,
                    command=self._load_configuration_file,
                )
                load_button.grid(
                    row=idx,
                    column=3,
                    sticky="e",
                    padx=(0, 10),
                    pady=settings_row_padding["pady"],
                )

        params_frame.columnconfigure(3, weight=1)

        self.target_metric_choice.trace_add(
            "write", lambda *_: self._update_report_threshold_label()
        )

        self._on_method_change()

        clip_frame = ttk.Frame(params_frame)
        clip_frame.grid(
            row=len(labels) + 1,
            column=0,
            columnspan=4,
            sticky="ew",
            padx=settings_row_padding["padx"],
            pady=(padding["pady"], settings_row_padding["pady"][1]),
        )

        self.clip_enabled = tk.BooleanVar(value=False)
        self.clip_low = tk.DoubleVar(value=0.0)
        self.clip_high = tk.DoubleVar(value=1.0)

        clip_check = ttk.Checkbutton(
            clip_frame,
            text="Clip predictions",
            variable=self.clip_enabled,
            command=self._toggle_clip_entries,
        )
        clip_check.grid(row=0, column=0, sticky="w")

        self.clip_low_entry = ttk.Entry(clip_frame, textvariable=self.clip_low, width=8, state="disabled")
        self.clip_low_entry.grid(row=0, column=1, padx=(10, 2))
        ttk.Label(clip_frame, text="to").grid(row=0, column=2)
        self.clip_high_entry = ttk.Entry(clip_frame, textvariable=self.clip_high, width=8, state="disabled")
        self.clip_high_entry.grid(row=0, column=3, padx=(2, 0))

        clip_frame.grid_columnconfigure(4, weight=1)
        self.save_conf_button = ttk.Button(
            clip_frame,
            text="Save setup (.conf)",
            width=18,
            command=self._save_configuration_file,
        )
        self.save_conf_button.grid(row=0, column=5, sticky="e", padx=(16, 0))

        actions_frame = ttk.Frame(self.config_tab)
        actions_frame.pack(fill="x", padx=10, pady=(0, 5))

        self.status_var = tk.StringVar(value="Idle")
        self.status_label = ttk.Label(
            actions_frame,
            textvariable=self.status_var,
            width=28,
            anchor="e",
        )
        self.status_label.pack(side="right", padx=(0, 4))

        load_frame = ttk.LabelFrame(self.results_tab, text="Load saved results")
        load_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 5))
        load_frame.columnconfigure(1, weight=1)

        ttk.Label(load_frame, text="Results file:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        load_entry = ttk.Entry(load_frame, textvariable=self.results_load_path)
        load_entry.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        ttk.Button(load_frame, text="Browse", command=self._browse_results_file).grid(row=0, column=2, padx=5, pady=5)
        ttk.Button(load_frame, text="Load", command=self._load_results_file).grid(row=0, column=3, padx=(0, 5), pady=5)
        ttk.Button(load_frame, text="Clear", command=self._clear_results_view).grid(row=0, column=4, padx=(0, 5), pady=5)
        ttk.Button(load_frame, text="Filter", command=self._open_filter_dialog).grid(row=0, column=5, padx=(0, 5), pady=5)
        ttk.Label(load_frame, text="Sorted by:").grid(row=0, column=6, sticky="w", padx=(10, 5), pady=5)
        sort_options = tuple(RESULTS_SORT_DISPLAY_TO_KEY.keys())
        if self.results_sort_var.get() not in sort_options:
            self.results_sort_var.set(RESULTS_SORT_KEY_TO_DISPLAY.get("R2", DEFAULT_SORT_LABEL))
        sort_combo = ttk.Combobox(
            load_frame,
            textvariable=self.results_sort_var,
            state="readonly",
            values=sort_options,
            width=10,
        )
        sort_combo.grid(row=0, column=7, sticky="w", padx=(0, 5), pady=5)
        sort_combo.bind("<<ComboboxSelected>>", self._handle_sort_change)

        self.results_tab.grid_columnconfigure(0, weight=1)
        self.results_tab.grid_rowconfigure(0, weight=0)
        for row in range(1, 4):
            self.results_tab.grid_rowconfigure(row, weight=1, uniform="results_rows")

        training_frame = ttk.LabelFrame(self.results_tab, text="Top models training")
        training_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=(5))
        training_frame.grid_rowconfigure(0, weight=1)
        training_frame.grid_columnconfigure(0, weight=1)
        self.training_frame = training_frame

        self.training_columns = (
            "Model",
            "Variables",
            "N_var",
            "R2",
            "RMSE",
            "MAE",
            "s",
            "R2_adj",
            "VIF_max",
            "VIF_avg",
        )
        training_heading_map = {
            "Variables": "Predictors",
            "R2": f"{R_SQUARED_SYMBOL} (train)",
            "R2_adj": f"adj-{R_SQUARED_SYMBOL}",
            "RMSE": "RMSE (train)",
            "s": f"{RESULTS_STANDARD_ERROR_LABEL} (train)",
            "MAE": "MAE (train)",
            "VIF_max": "VIFmax",
            "VIF_avg": "VIFavg",
            "N_var": "N pred",
        }
        self.training_tree = ttk.Treeview(
            training_frame, columns=self.training_columns, show="headings"
        )
        training_column_settings = {
            "Model": {"anchor": "center", "min_width": 70, "weight": 0.0},
            "Variables": {"anchor": "w", "min_width": 440, "weight": 5.0},
            "N_var": {"anchor": "center", "min_width": 80, "weight": 0.0},
            "R2": {"anchor": "center", "min_width": 110, "weight": 1.0},
            "RMSE": {"anchor": "center", "min_width": 110, "weight": 1.0},
            "s": {"anchor": "center", "min_width": 110, "weight": 1.0},
            "MAE": {"anchor": "center", "min_width": 110, "weight": 1.0},
            "R2_adj": {"anchor": "center", "min_width": 90, "weight": 1.0},
            "VIF_max": {"anchor": "center", "min_width": 90, "weight": 0.5},
            "VIF_avg": {"anchor": "center", "min_width": 90, "weight": 0.5},
        }
        training_specs: list[_ResultsColumnLayout] = []
        for col in self.training_columns:
            heading_text = training_heading_map.get(col, col)
            settings = training_column_settings.get(
                col,
                {"anchor": "center", "min_width": 110, "weight": 1.0},
            )
            anchor = settings["anchor"]
            min_width = settings["min_width"]
            weight = settings.get("weight", 0.0)
            self.training_tree.heading(col, text=heading_text)
            self.training_tree.column(
                col,
                anchor=anchor,
                width=min_width,
                minwidth=20,
                stretch=False,
            )
            training_specs.append(_ResultsColumnLayout(col, min_width, weight))
        self._register_results_tree(self.training_tree, tuple(training_specs))

        training_scroll = ttk.Scrollbar(
            training_frame, orient="vertical", command=self.training_tree.yview
        )
        self.training_tree.configure(yscrollcommand=training_scroll.set)
        self.training_tree.grid(row=0, column=0, sticky="nsew", padx=(5, 0), pady=5)
        training_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 5), pady=5)

        internal_frame = ttk.LabelFrame(
            self.results_tab,
            text="Top models internal validation via cross-validation (CV)",
        )
        internal_frame.grid(row=2, column=0, sticky="nsew", padx=10, pady=5)
        internal_frame.grid_rowconfigure(0, weight=1)
        internal_frame.grid_columnconfigure(0, weight=1)
        self.internal_results_frame = internal_frame

        self.internal_results_columns = (
            "Model",
            "Variables",
            "N_var",
            "R2_loo",
            "RMSE_loo",
            "MAE_loo",
            "s_loo",
            "R2_kfold",
            "RMSE_kfold",
            "MAE_kfold",
            "s_kfold",
        )
        internal_heading_map = {
            "Variables": "Predictors",
            "R2_loo": f"{Q_SQUARED_SYMBOL} (LOO)",
            "RMSE_loo": "RMSE (LOO)",
            "MAE_loo": "MAE (LOO)",
            "s_loo": f"{RESULTS_STANDARD_ERROR_LABEL} (LOO)",
            "R2_kfold": f"{Q_SQUARED_SYMBOL} (k-fold)",
            "RMSE_kfold": "RMSE (k-fold)",
            "MAE_kfold": "MAE (k-fold)",
            "s_kfold": f"{RESULTS_STANDARD_ERROR_LABEL} (k-fold)",
            "N_var": "N pred",
        }
        self.internal_results_tree = ttk.Treeview(
            internal_frame, columns=self.internal_results_columns, show="headings"
        )
        internal_column_settings = {
            "Model": {"anchor": "center", "min_width": 70, "weight": 0.0},
            "Variables": {"anchor": "w", "min_width": 300, "weight": 5.0},
            "N_var": {"anchor": "center", "min_width": 80, "weight": 0.0},
            "R2_loo": {"anchor": "center", "min_width": 90, "weight": 1.0},
            "RMSE_loo": {"anchor": "center", "min_width": 105, "weight": 1.0},
            "MAE_loo": {"anchor": "center", "min_width": 105, "weight": 1.0},
            "s_loo": {"anchor": "center", "min_width": 80, "weight": 0.0},
            "R2_kfold": {"anchor": "center", "min_width": 105, "weight": 1.0},
            "RMSE_kfold": {"anchor": "center", "min_width": 115, "weight": 2.0},
            "MAE_kfold": {"anchor": "center", "min_width": 105, "weight": 1.0},
            "s_kfold": {"anchor": "center", "min_width": 90, "weight": 0.0},
        }
        internal_specs: list[_ResultsColumnLayout] = []
        for col in self.internal_results_columns:
            heading_text = internal_heading_map.get(col, col)
            settings = internal_column_settings.get(
                col,
                {"anchor": "center", "min_width": 110, "weight": 1.0},
            )
            anchor = settings["anchor"]
            min_width = settings["min_width"]
            weight = settings.get("weight", 0.0)
            self.internal_results_tree.heading(col, text=heading_text)
            self.internal_results_tree.column(
                col,
                anchor=anchor,
                width=min_width,
                minwidth=20,
                stretch=False,
            )
            internal_specs.append(_ResultsColumnLayout(col, min_width, weight))
        self._register_results_tree(self.internal_results_tree, tuple(internal_specs))

        internal_scroll = ttk.Scrollbar(
            internal_frame, orient="vertical", command=self.internal_results_tree.yview
        )
        self.internal_results_tree.configure(yscrollcommand=internal_scroll.set)
        self.internal_results_tree.grid(row=0, column=0, sticky="nsew", padx=(5, 0), pady=5)
        internal_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 5), pady=5)

        ext_frame = ttk.LabelFrame(self.results_tab, text="Top models external validation")
        ext_frame.grid(row=3, column=0, sticky="nsew", padx=10, pady=(5))
        ext_frame.grid_rowconfigure(0, weight=1)
        ext_frame.grid_columnconfigure(0, weight=1)
        self.external_results_frame = ext_frame

        self.external_results_columns = (
            "Model",
            "Variables",
            "N_var",
            "Q2F3_ext",
            "RMSE_ext",
            "MAE_ext",
            "Q2F2_ext",
            "Q2F1_ext",
        )
        ext_heading_map = {
            "Variables": "Predictors",
            "RMSE_ext": "RMSE (ext)",
            "MAE_ext": "MAE (ext)",
            "Q2F1_ext": f"{Q_SQUARED_SYMBOL}F1",
            "Q2F2_ext": f"{Q_SQUARED_SYMBOL}F2",
            "Q2F3_ext": f"{Q_SQUARED_SYMBOL}F3",
            "N_var": "N pred",
        }
        self.external_results_tree = ttk.Treeview(
            ext_frame, columns=self.external_results_columns, show="headings"
        )
        external_column_settings = {
            "Model": {"anchor": "center", "min_width": 70, "weight": 0.0},
            "Variables": {"anchor": "w", "min_width": 360, "weight": 5.0},
            "N_var": {"anchor": "center", "min_width": 80, "weight": 0.0},
            "Q2F1_ext": {"anchor": "center", "min_width": 110, "weight": 1.0},
            "RMSE_ext": {"anchor": "center", "min_width": 110, "weight": 1.0},
            "MAE_ext": {"anchor": "center", "min_width": 110, "weight": 1.0},
            "Q2F2_ext": {"anchor": "center", "min_width": 110, "weight": 1.0},
            "Q2F3_ext": {"anchor": "center", "min_width": 110, "weight": 1.0},
        }
        external_specs: list[_ResultsColumnLayout] = []
        for col in self.external_results_columns:
            heading_text = ext_heading_map.get(col, col)
            settings = external_column_settings.get(
                col,
                {"anchor": "center", "min_width": 110, "weight": 1.0},
            )
            anchor = settings["anchor"]
            min_width = settings["min_width"]
            weight = settings.get("weight", 0.0)
            self.external_results_tree.heading(col, text=heading_text)
            self.external_results_tree.column(
                col,
                anchor=anchor,
                width=min_width,
                minwidth=20,
                stretch=False,
            )
            external_specs.append(_ResultsColumnLayout(col, min_width, weight))
        self._register_results_tree(self.external_results_tree, tuple(external_specs))

        ext_scroll = ttk.Scrollbar(
            ext_frame, orient="vertical", command=self.external_results_tree.yview
        )
        self.external_results_tree.configure(yscrollcommand=ext_scroll.set)
        self.external_results_tree.grid(row=0, column=0, sticky="nsew", padx=(5, 0), pady=5)
        ext_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 5), pady=5)

        self.after_idle(self._initialize_notebook_layouts)

    def _initialize_help_tab(self) -> None:
        if getattr(self, "_help_tab_initialized", False):
            return

        self._help_tab_initialized = True
        content = ttk.Frame(self.help_tab, padding=40)
        content.pack(fill="both", expand=True)

        title_label = ttk.Label(
            content,
            text="About MLR-X",
            font=("TkDefaultFont", 16, "bold"),
            anchor="center",
        )
        title_label.pack(pady=(0, 10))

        description_label = ttk.Label(
            content,
            text="A Scalable Software for Multiple Linear Regression on Small and Large Datasets.",
            wraplength=600,
            justify="center",
        )
        description_label.pack(pady=(0, 20))

        version_label = ttk.Label(content, text="Version 1.0")
        version_label.pack(pady=(0, 0))

        release_label = ttk.Label(content, text="(Original release)")
        release_label.pack(pady=(0, 10))

        details_text = (
            "By Jackson J. Alcázar (Chile).\n"
            "This software is free and distributed under the GNU Affero General Public License v3.0 (AGPL-3.0).\n"
            "© 2025. All rights reserved."
        )
        details_label = ttk.Label(content, text=details_text, justify="center")
        details_label.pack(pady=(0, 15))

        homepage_label = ttk.Label(
            content,
            text=f"Homepage: {MLRX_HOMEPAGE_URL}",
            foreground="#0b57d0",
            cursor="hand2",
        )
        homepage_label.pack(pady=(0, 10))
        homepage_label.bind("<Button-1>", self._open_mlr_x_homepage)

        contact_label = ttk.Label(
            content,
            text="Contact: jjalcazar.dev@gmail.com",
            justify="center",
        )
        contact_label.pack()

        actions_frame = ttk.Frame(content)
        actions_frame.pack(pady=(30, 0), anchor="center")

        action_buttons = [
            ("Manual", self._open_manual_pdf),
            ("How to cite", self._open_citation_window),
            ("Report a bug", self._open_bug_report_page),
            ("Donate", self._open_donation_window),
        ]

        for text, command in action_buttons:
            button = ttk.Button(actions_frame, text=text, command=command)
            button.pack(pady=6)

    def _open_manual_pdf(self) -> None:
        webbrowser.open_new_tab(MANUAL_URL)

    def _open_citation_window(self) -> None:
        if self._citation_window is not None and tk.Toplevel.winfo_exists(self._citation_window):
            self._citation_window.deiconify()
            self._citation_window.lift()
            return

        window = tk.Toplevel(self)
        window.title("How to Cite")
        window.transient(self)
        window.resizable(False, False)

        content = ttk.Frame(window, padding=20)
        content.pack(fill="both", expand=True)
        self._center_dialog(window)
        window.grab_set()
        window.focus_set()

        message_label = ttk.Label(
            content,
            text=CITATION_TEXT,
            justify="center",
            wraplength=400,
        )
        message_label.pack(pady=(0, 15))

        buttons = ttk.Frame(content)
        buttons.pack(fill="x", pady=(0, 10))

        copy_btn = ttk.Button(
            buttons,
            text="Copy citation",
            command=self._copy_citation_to_clipboard,
        )
        copy_btn.pack(side="left", expand=True, padx=(0, 5))

        download_btn = ttk.Button(
            buttons,
            text="Download .bib",
            command=self._download_citation_bib,
        )
        download_btn.pack(side="left", expand=True, padx=(5, 0))

        def on_close() -> None:
            self._citation_window = None
            window.destroy()

        close_btn = ttk.Button(content, text="Close", command=on_close)
        close_btn.pack(pady=(5, 0))

        window.protocol("WM_DELETE_WINDOW", on_close)
        self._citation_window = window

    def _copy_citation_to_clipboard(self) -> None:
        self.clipboard_clear()
        self.clipboard_append(CITATION_TEXT)
        self.update_idletasks()
        messagebox.showinfo("Copied", "Citation copied to clipboard.")

    def _download_citation_bib(self) -> None:
        path = filedialog.asksaveasfilename(
            title="Save citation", defaultextension=".bib", filetypes=(("BibTeX", "*.bib"),)
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(CITATION_BIB)
        except OSError as exc:
            messagebox.showerror("Error", f"Could not save file: {exc}")
            return
        messagebox.showinfo("Saved", "BibTeX citation saved successfully.")

    def _open_bug_report_page(self) -> None:
        webbrowser.open_new_tab(BUG_REPORT_URL)

    def _open_donation_window(self) -> None:
        self._launch_paypal_donation()

    def _launch_paypal_donation(self) -> None:
        webbrowser.open_new_tab(PAYPAL_DONATION_URL)

    def _open_mlr_x_homepage(self, _event: Optional[tk.Event] = None) -> None:
        webbrowser.open_new_tab(MLRX_HOMEPAGE_URL)

    def _handle_first_map(self, _event):
        if self._results_layout_initialized:
            return
        # Schedule a layout pass once Tk finishes processing the map event so
        # the notebook pages report their actual allocated size.
        self.after(50, self._initialize_notebook_layouts)

    def _initialize_notebook_layouts(self):
        if self._results_layout_initialized or not hasattr(self, "notebook"):
            return
        if not self.notebook.winfo_ismapped():
            self.after(50, self._initialize_notebook_layouts)
            return
        # Ensure geometry calculations run without changing the active tab so
        # the startup experience remains visually stable.
        self.update_idletasks()
        self._refresh_results_tab_layout()
        self._results_layout_initialized = True
        self.unbind("<Map>")

    def _handle_tab_changed(self, _event):
        selected = self.notebook.select()
        self._current_tab_id = selected
        if selected == str(self.results_tab):
            self._refresh_results_tab_layout()

    def _refresh_results_tab_layout(self):
        self.results_tab.update_idletasks()
        for widget in (
            getattr(self, "training_frame", None),
            getattr(self, "internal_results_frame", None),
            getattr(self, "external_results_frame", None),
            getattr(self, "training_tree", None),
            getattr(self, "internal_results_tree", None),
            getattr(self, "external_results_tree", None),
        ):
            if widget is not None:
                widget.update_idletasks()

        # Ensure layout measurements are computed so all panes render correctly
        # when the application first appears on screen.
        self.update_idletasks()
        self._refresh_results_tree_columns()

    def _context_has_holdout(self, context: Optional[EPRSContext]) -> bool:
        if context is None:
            return False
        if context.test_df is not None and not context.test_df.empty:
            return True
        if context.external_df is not None and not context.external_df.empty:
            return True
        return False

    def _set_holdout_ready(self, ready: bool) -> None:
        if self.holdout_ready == ready:
            return
        self.holdout_ready = ready
        if hasattr(self, "diagnostics_tab"):
            self.diagnostics_tab.apply_holdout_default(ready)
        if hasattr(self, "visual_tab"):
            self.visual_tab.apply_holdout_default(ready)

    def _schedule_holdout_refresh(self, *_args):
        if self._holdout_refresh_job is not None:
            try:
                self.after_cancel(self._holdout_refresh_job)
            except Exception:  # noqa: BLE001
                pass
        self._holdout_refresh_job = self.after(300, self._refresh_holdout_status)

    def _cancel_holdout_refresh_job(self) -> None:
        if self._holdout_refresh_job is None:
            return
        try:
            self.after_cancel(self._holdout_refresh_job)
        except Exception:  # noqa: BLE001
            pass
        finally:
            self._holdout_refresh_job = None

    def _refresh_holdout_status(self) -> None:
        self._holdout_refresh_job = None
        self.ensure_holdout_data_available(show_alert=False)

    def _register_results_tree(
        self, tree: ttk.Treeview, specs: tuple[_ResultsColumnLayout, ...]
    ) -> None:
        self._results_tree_layouts[tree] = specs
        self._results_tree_last_widths.pop(tree, None)
        tree.bind("<Configure>", self._handle_results_tree_configure, add="+")

    def replace_results_tree_layout(
        self, tree: ttk.Treeview, specs: tuple[_ResultsColumnLayout, ...]
    ) -> None:
        if tree not in self._results_tree_layouts:
            self._register_results_tree(tree, specs)
            return

        self._results_tree_layouts[tree] = specs
        self._results_tree_last_widths.pop(tree, None)
        self._apply_results_tree_layout(tree)

    def update_results_tree_min_widths(
        self, tree: ttk.Treeview, widths: dict[str, int]
    ) -> None:
        specs = self._results_tree_layouts.get(tree)
        if not specs:
            return

        updated: list[_ResultsColumnLayout] = []
        changed = False
        for spec in specs:
            desired = max(widths.get(spec.name, spec.min_width), 20)
            if desired != spec.min_width:
                changed = True
            updated.append(_ResultsColumnLayout(spec.name, desired, spec.weight))

        if not changed:
            return

        self._results_tree_layouts[tree] = tuple(updated)
        self._results_tree_last_widths.pop(tree, None)
        self._apply_results_tree_layout(tree)

    def _handle_results_tree_configure(self, event: tk.Event) -> None:
        tree = event.widget
        if not isinstance(tree, ttk.Treeview):
            return
        if tree not in self._results_tree_layouts:
            return
        self._apply_results_tree_layout(tree, event.width)

    def _refresh_results_tree_columns(self) -> None:
        for tree, _ in list(self._results_tree_layouts.items()):
            if tree.winfo_exists():
                self._apply_results_tree_layout(tree)

    def _apply_results_tree_layout(
        self, tree: ttk.Treeview, width: Optional[int] = None
    ) -> None:
        specs = self._results_tree_layouts.get(tree)
        if not specs:
            return
        if width is None or width <= 1:
            width = tree.winfo_width()
        if width <= 1:
            return

        last_width = self._results_tree_last_widths.get(tree)
        if last_width == width:
            return

        # Reserve a small padding for the vertical scrollbar and tree borders.
        available = max(width - 16, 0)
        if available <= 0:
            return

        min_total = sum(max(spec.min_width, 20) for spec in specs)
        if min_total <= 0:
            return

        if available < min_total:
            scale = available / min_total if min_total else 1.0
            widths = [max(int(max(spec.min_width, 20) * scale), 20) for spec in specs]
            assigned = sum(widths)
            leftover = available - assigned
            idx = 0
            count = len(widths)
            while leftover > 0 and count:
                widths[idx % count] += 1
                leftover -= 1
                idx += 1
        else:
            widths = [max(spec.min_width, 20) for spec in specs]
            remain = available - sum(widths)
            if remain > 0:
                total_weight = sum(max(spec.weight, 0.0) for spec in specs)
                if total_weight > 0:
                    fractions: list[tuple[float, int]] = []
                    for idx, spec in enumerate(specs):
                        weight = max(spec.weight, 0.0)
                        if weight <= 0:
                            fractions.append((0.0, idx))
                            continue
                        share = remain * weight / total_weight
                        extra = int(share)
                        widths[idx] += extra
                        fractions.append((share - extra, idx))
                    assigned = sum(widths)
                    leftover = available - assigned
                    if leftover > 0:
                        fractions.sort(reverse=True)
                        if not fractions:
                            fractions = [(0.0, idx) for idx in range(len(widths))]
                        i = 0
                        while leftover > 0 and fractions:
                            idx = fractions[i % len(fractions)][1]
                            widths[idx] += 1
                            leftover -= 1
                            i += 1
                else:
                    idx = 0
                    count = len(widths)
                    while remain > 0 and count:
                        widths[idx % count] += 1
                        remain -= 1
                        idx += 1

        for spec, column_width in zip(specs, widths):
            tree.column(spec.name, width=max(column_width, 20))

        self._results_tree_last_widths[tree] = width

    def _update_split_controls(self):
        mode = self.split_mode.get()

        random_state = "normal" if mode == "random" else "disabled"
        self.random_test_entry.configure(state=random_state)

        manual_state = "normal" if mode == "manual" else "disabled"
        self.manual_train_entry.configure(state=manual_state)
        self.manual_test_entry.configure(state=manual_state)

        if hasattr(self, "validation_tab"):
            self.validation_tab.sync_split_mode(mode)

    def _update_report_threshold_label(self, *_: object) -> None:
        if not getattr(self, "report_threshold_label_var", None):
            return
        label_text = "Target metric cutoff:"
        self.report_threshold_label_var.set(label_text)

    def _toggle_clip_entries(self):
        state = "normal" if self.clip_enabled.get() else "disabled"
        self.clip_low_entry.configure(state=state)
        self.clip_high_entry.configure(state=state)

    def _toggle_constant_filter(self):
        state = "normal" if self.constant_filter_enabled.get() else "disabled"
        self.constant_threshold_spin.configure(state=state)

    def _open_seed_settings_dialog(self) -> None:
        if (
            self.seed_settings_dialog is not None
            and tk.Toplevel.winfo_exists(self.seed_settings_dialog)
        ):
            try:
                self.seed_settings_dialog.deiconify()
                self.seed_settings_dialog.lift()
            except tk.TclError:
                pass
            return

        window = tk.Toplevel(self)
        window.title("Seed settings")
        window.resizable(False, False)
        window.transient(self)
        self.seed_settings_dialog = window
        window.protocol("WM_DELETE_WINDOW", self._close_seed_settings_dialog)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)

        seed_frame = ttk.LabelFrame(container, text="Seed size")
        seed_frame.pack(fill="x", pady=(0, 10))
        seed_frame.columnconfigure(0, weight=1)

        ttk.Label(
            seed_frame,
            text="Choose how many predictors are included in each random seed:",
        ).pack(anchor="w", padx=10, pady=(8, 4))

        default_seed = self._calculate_seed_size_default()
        current_seed = self._safe_int(self.params_vars["seed_size"].get())
        if default_seed is not None and current_seed == default_seed:
            self.seed_size_mode.set("default")
        elif current_seed is not None:
            self.seed_size_mode.set("custom")
        else:
            self.seed_size_mode.set("default")

        default_label = "Max predictors per model / 2 (Default)"
        ttk.Radiobutton(
            seed_frame,
            text=default_label,
            variable=self.seed_size_mode,
            value="default",
            command=self._toggle_seed_settings_entries,
        ).pack(anchor="w", padx=10, pady=2)

        custom_seed_row = ttk.Frame(seed_frame)
        custom_seed_row.pack(anchor="w", padx=10, pady=(2, 8))
        ttk.Radiobutton(
            custom_seed_row,
            text="Custom seed size:",
            variable=self.seed_size_mode,
            value="custom",
            command=self._toggle_seed_settings_entries,
        ).pack(side="left")
        self.seed_size_entry = ttk.Entry(
            custom_seed_row,
            textvariable=self.seed_size_manual_var,
            width=10,
        )
        self.seed_size_entry.pack(side="left", padx=(6, 0))

        if self.seed_size_mode.get() == "custom" and current_seed is not None:
            self.seed_size_manual_var.set(str(current_seed))
        elif current_seed is None:
            self.seed_size_manual_var.set("")

        seed_status_row = ttk.Frame(seed_frame)
        seed_status_row.pack(fill="x", padx=10, pady=(0, 8))
        seed_status_row.columnconfigure(0, weight=1)
        ttk.Label(
            seed_status_row,
            textvariable=self.seed_size_applied_var,
            foreground="#666666",
        ).grid(row=0, column=1, sticky="e")

        reproducibility_frame = ttk.LabelFrame(container, text="Reproducibility")
        reproducibility_frame.pack(fill="x", pady=(0, 10))
        reproducibility_frame.columnconfigure(0, weight=1)
        ttk.Label(
            reproducibility_frame,
            text=(
                "Seeds are built at random; keep the same random state for reproducibility."
            ),
            wraplength=420,
        ).pack(anchor="w", padx=10, pady=(8, 4))

        current_random_state = self._safe_int(self.params_vars["random_state"].get())
        if current_random_state == 42 or current_random_state is None:
            self.random_state_mode.set("default")
        else:
            self.random_state_mode.set("custom")
        if current_random_state not in (None, 42):
            self.random_state_manual_var.set(str(current_random_state))
        elif self.random_state_mode.get() == "custom":
            self.random_state_manual_var.set("")

        ttk.Radiobutton(
            reproducibility_frame,
            text="Random state = 42 (Default)",
            variable=self.random_state_mode,
            value="default",
            command=self._toggle_seed_settings_entries,
        ).pack(anchor="w", padx=10, pady=2)

        custom_random_row = ttk.Frame(reproducibility_frame)
        custom_random_row.pack(anchor="w", padx=10, pady=(2, 8))
        ttk.Radiobutton(
            custom_random_row,
            text="Random state:",
            variable=self.random_state_mode,
            value="custom",
            command=self._toggle_seed_settings_entries,
        ).pack(side="left")
        self.random_state_entry = ttk.Entry(
            custom_random_row,
            textvariable=self.random_state_manual_var,
            width=10,
        )
        self.random_state_entry.pack(side="left", padx=(6, 0))

        reproducibility_status_row = ttk.Frame(reproducibility_frame)
        reproducibility_status_row.pack(fill="x", padx=10, pady=(0, 8))
        reproducibility_status_row.columnconfigure(0, weight=1)
        ttk.Label(
            reproducibility_status_row,
            textvariable=self.random_state_applied_var,
            foreground="#666666",
        ).grid(row=0, column=1, sticky="e")

        guard_frame = ttk.LabelFrame(container, text="Number of seeds guardrail")
        guard_frame.pack(fill="x", pady=(0, 10))
        guard_frame.columnconfigure(0, weight=1)
        ttk.Checkbutton(
            guard_frame,
            text="Allow fewer than 1000 seeds",
            variable=self.allow_small_seed_count,
            command=self._toggle_seed_minimum_enforcement,
        ).pack(anchor="w", padx=10, pady=8)

        guard_status_row = ttk.Frame(guard_frame)
        guard_status_row.pack(fill="x", padx=10, pady=(0, 8))
        guard_status_row.columnconfigure(0, weight=1)
        ttk.Label(
            guard_status_row,
            textvariable=self.guardrail_applied_var,
            foreground="#666666",
        ).grid(row=0, column=1, sticky="e")

        button_row = ttk.Frame(container)
        button_row.pack(fill="x")
        ttk.Button(button_row, text="Apply", command=self._apply_seed_settings).pack(
            side="right"
        )
        ttk.Button(
            button_row, text="Close", command=self._close_seed_settings_dialog
        ).pack(side="right", padx=(0, 8))

        self.seed_size_applied_var.set("")
        self.random_state_applied_var.set("")
        self.guardrail_applied_var.set("")
        self._toggle_seed_settings_entries()
        self._sync_seed_settings_controls()
        self._center_dialog(window)

    def _close_seed_settings_dialog(self) -> None:
        if self.seed_settings_dialog is None:
            return
        try:
            self.seed_settings_dialog.destroy()
        except tk.TclError:
            pass
        self.seed_settings_dialog = None
        self.seed_size_entry = None
        self.random_state_entry = None

    def _toggle_seed_settings_entries(self) -> None:
        if getattr(self, "seed_size_entry", None) is not None:
            seed_state = "normal" if self.seed_size_mode.get() == "custom" else "disabled"
            self.seed_size_entry.configure(state=seed_state)
        if getattr(self, "random_state_entry", None) is not None:
            random_state = (
                "normal" if self.random_state_mode.get() == "custom" else "disabled"
            )
            self.random_state_entry.configure(state=random_state)

    def _sync_seed_settings_controls(self) -> None:
        method_key = METHOD_DISPLAY_TO_KEY.get(self.method_choice.get(), "all_subsets")
        if method_key == "all_subsets" and self._restrict_seed_settings_to_eprs:
            if getattr(self, "seed_size_entry", None) is not None:
                self.seed_size_entry.configure(state="disabled")
            if getattr(self, "random_state_entry", None) is not None:
                self.random_state_entry.configure(state="disabled")
        else:
            self._toggle_seed_settings_entries()

    def _toggle_seed_minimum_enforcement(self) -> None:
        if not self.allow_small_seed_count.get():
            self._schedule_seed_minimum_enforcement()

    def _apply_seed_settings(self) -> None:
        if self.seed_size_mode.get() == "default":
            default_seed = self._calculate_seed_size_default()
            if default_seed is None:
                messagebox.showerror(
                    "Seed size",
                    "Please enter a valid integer for the maximum predictors per model.",
                )
                return
            self.params_vars["seed_size"].set(str(default_seed))
        else:
            seed_value = self._safe_int(self.seed_size_manual_var.get())
            if seed_value is None or seed_value <= 0:
                messagebox.showerror(
                    "Seed size",
                    "Please enter a positive integer for the seed size.",
                )
                return
            max_vars = self._safe_int(self.params_vars["max_vars"].get())
            if max_vars is None or max_vars <= 0:
                messagebox.showerror(
                    "Seed size",
                    "Please enter a valid integer for the maximum predictors per model.",
                )
                return
            if seed_value > max_vars:
                messagebox.showerror(
                    "Seed size",
                    "Seed size must be less than or equal to max predictors per model.",
                )
                return
            self.params_vars["seed_size"].set(str(seed_value))

        if self.random_state_mode.get() == "default":
            self.params_vars["random_state"].set("42")
        else:
            random_state = self._safe_int(self.random_state_manual_var.get())
            if random_state is None or random_state <= 0:
                messagebox.showerror(
                    "Random state",
                    "Please enter a positive integer for the random state.",
                )
                return
            self.params_vars["random_state"].set(str(random_state))

        self.seed_size_applied_var.set("Changes applied!")
        self.random_state_applied_var.set("Changes applied!")
        self.guardrail_applied_var.set("Changes applied!")
        self._update_seed_settings_summary()

    def _calculate_seed_size_default(self) -> Optional[int]:
        max_vars = self._safe_int(self.params_vars["max_vars"].get())
        if max_vars is None or max_vars <= 0:
            return None
        if max_vars % 2 == 0:
            return max_vars // 2
        return max_vars // 2 + 1

    def _update_seed_settings_summary(self) -> None:
        seed_size = self._safe_int(self.params_vars["seed_size"].get())
        random_state = self._safe_int(self.params_vars["random_state"].get())
        default_seed = self._calculate_seed_size_default()

        if self.seed_size_mode.get() == "default" and default_seed is not None:
            if seed_size != default_seed:
                self.params_vars["seed_size"].set(str(default_seed))
                seed_size = default_seed
            if self.seed_size_manual_var.get():
                self.seed_size_manual_var.set("")

        parts: list[str] = []
        if seed_size is not None and default_seed is not None and seed_size != default_seed:
            parts.append(f"Seed size: {seed_size}")
        if random_state is not None and random_state != 42:
            parts.append(f"Random state: {random_state}")

        self.seed_settings_summary_var.set("; ".join(parts))

    def _open_iterations_dialog(self) -> None:
        if (
            self.iterations_dialog is not None
            and tk.Toplevel.winfo_exists(self.iterations_dialog)
        ):
            try:
                self.iterations_dialog.deiconify()
                self.iterations_dialog.lift()
                self.iterations_dialog.focus_set()
            except Exception:  # noqa: BLE001
                pass
            return

        window = tk.Toplevel(self)
        self.iterations_dialog = window
        window.title("Iterations per seed")
        window.transient(self)
        window.grab_set()
        window.resizable(False, False)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)

        ttk.Label(
            container,
            text="Choose how to cap the iterations per seed:",
            font=("TkDefaultFont", 10, "bold"),
        ).pack(anchor="w", pady=(0, 8))

        radio_container = ttk.Frame(container)
        radio_container.pack(fill="both", expand=True)

        dialog_mode_var = tk.StringVar(self, value=self.iterations_mode_var.get())
        dialog_manual_var = tk.StringVar(self, value=self.manual_iterations_var.get())

        def _make_radio(parent: ttk.Frame, text: str, value: str) -> ttk.Radiobutton:
            return ttk.Radiobutton(
                parent,
                text=text,
                value=value,
                variable=dialog_mode_var,
                command=lambda: self._sync_iteration_controls(
                    mode_var=dialog_mode_var, manual_entry=manual_entry
                ),
            )

        _make_radio(
            radio_container,
            "Estimate and apply the maximum iterations per seed automatically (default)",
            ITERATION_MODE_AUTO,
        ).pack(anchor="w", pady=(0, 6))

        manual_row = ttk.Frame(radio_container)
        manual_row.pack(anchor="w", fill="x", pady=(0, 6))
        _make_radio(
            manual_row,
            "Set a maximum iterations per seed",
            ITERATION_MODE_MANUAL,
        ).pack(side="left", anchor="w")
        manual_entry = ttk.Entry(
            manual_row,
            textvariable=dialog_manual_var,
            width=10,
        )
        manual_entry.pack(side="left", padx=(8, 0))

        converge_row = ttk.Frame(radio_container)
        converge_row.pack(anchor="w", fill="x")
        _make_radio(
            converge_row,
            "Run until convergence",
            ITERATION_MODE_CONVERGE,
        ).pack(side="left", anchor="w")
        ttk.Label(
            converge_row,
            text="(Warning: this may take a long time)",
            foreground="#b58900",
        ).pack(side="left", padx=(6, 0))

        button_row = ttk.Frame(container)
        button_row.pack(fill="x", pady=(10, 0))
        ttk.Button(button_row, text="Cancel", command=window.destroy).pack(
            side="right"
        )
        ttk.Button(
            button_row,
            text="Apply",
            command=lambda: self._apply_iteration_choice(
                window, mode_var=dialog_mode_var, manual_var=dialog_manual_var
            ),
        ).pack(side="right", padx=(0, 8))

        self._sync_iteration_controls(mode_var=dialog_mode_var, manual_entry=manual_entry)
        window.update_idletasks()

        try:
            parent_x = self.winfo_rootx()
            parent_y = self.winfo_rooty()
            parent_w = self.winfo_width()
            parent_h = self.winfo_height()
            win_w = window.winfo_width()
            win_h = window.winfo_height()
            pos_x = parent_x + (parent_w - win_w) // 2
            pos_y = parent_y + (parent_h - win_h) // 2
            window.geometry(f"{win_w}x{win_h}+{pos_x}+{pos_y}")
        except Exception:  # noqa: BLE001
            pass

    def _refresh_iteration_mode_display(self, *_args) -> None:
        mode_value = (self.iterations_mode_var.get() or ITERATION_MODE_AUTO).lower()
        if mode_value not in {
            ITERATION_MODE_AUTO,
            ITERATION_MODE_MANUAL,
            ITERATION_MODE_CONVERGE,
        }:
            mode_value = ITERATION_MODE_AUTO
        self.iterations_mode_var.set(mode_value)
        self.iteration_mode_value_var.set(mode_value)

    def _set_iteration_mode_display_state(self, disabled: bool) -> None:
        if self.iteration_mode_label is None or self.iteration_mode_value_label is None:
            return

        state_token = "disabled" if disabled else "!disabled"
        for label in (self.iteration_mode_label, self.iteration_mode_value_label):
            try:
                label.state([state_token])
            except Exception:  # noqa: BLE001
                pass
            try:
                label.configure(
                    foreground=(
                        self._iteration_mode_disabled_fg
                        if disabled
                        else self._iteration_mode_normal_fg
                    )
                )
            except Exception:  # noqa: BLE001
                pass

    def _sync_iteration_controls(
        self,
        *,
        mode_var: Optional[tk.StringVar] = None,
        manual_entry: Optional[ttk.Entry] = None,
    ) -> None:
        mode_var = mode_var or self.iterations_mode_var
        mode = mode_var.get() or ITERATION_MODE_AUTO
        mode = mode.lower()
        if mode not in {ITERATION_MODE_AUTO, ITERATION_MODE_MANUAL, ITERATION_MODE_CONVERGE}:
            mode = ITERATION_MODE_AUTO
        mode_var.set(mode)

        manual_enabled = mode == ITERATION_MODE_MANUAL
        state = "normal" if manual_enabled else "disabled"
        if manual_entry is not None:
            try:
                manual_entry.configure(state=state)
            except Exception:  # noqa: BLE001
                pass
        elif self.iterations_dialog is not None:
            for child in self.iterations_dialog.winfo_children():
                if isinstance(child, ttk.Frame):
                    for entry in child.winfo_children():
                        if isinstance(entry, ttk.Entry):
                            try:
                                entry.configure(state=state)
                            except Exception:  # noqa: BLE001
                                pass

    def _apply_iteration_choice(
        self,
        dialog: tk.Toplevel,
        *,
        mode_var: Optional[tk.StringVar] = None,
        manual_var: Optional[tk.StringVar] = None,
    ) -> None:
        mode_var = mode_var or self.iterations_mode_var
        manual_var = manual_var or self.manual_iterations_var

        mode = mode_var.get() or ITERATION_MODE_AUTO
        mode = mode.lower()
        if mode not in {ITERATION_MODE_AUTO, ITERATION_MODE_MANUAL, ITERATION_MODE_CONVERGE}:
            mode = ITERATION_MODE_AUTO
        value: Optional[int] = None
        if mode == ITERATION_MODE_MANUAL:
            raw_value = manual_var.get().strip()
            if not raw_value:
                messagebox.showerror(
                    "Iterations per seed",
                    "Please enter a positive integer for the maximum iterations per seed.",
                )
                return
            try:
                value = int(float(raw_value))
            except (TypeError, ValueError):  # noqa: BLE001
                messagebox.showerror(
                    "Iterations per seed",
                    "Please enter a positive integer for the maximum iterations per seed.",
                )
                return
            if value <= 0:
                messagebox.showerror(
                    "Iterations per seed",
                    "Please enter a positive integer for the maximum iterations per seed.",
                )
                return
            manual_var.set(str(value))

        self.iterations_mode_var.set(mode)
        self.manual_iterations_var.set(manual_var.get())
        if value is None and mode != ITERATION_MODE_MANUAL:
            self.manual_iterations_var.set("")

        if self.summary_tab is not None:
            self.summary_tab.update_iteration_preferences(mode, value)

        try:
            dialog.destroy()
        except Exception:  # noqa: BLE001
            pass

    def _on_method_change(self, *_args):
        display_value = self.method_choice.get()
        method_key = METHOD_DISPLAY_TO_KEY.get(display_value, "all_subsets")
        enforce_seed_restrictions = (
            method_key == "all_subsets" and self._restrict_seed_settings_to_eprs
        )
        seed_state = "disabled" if enforce_seed_restrictions else "normal"
        button_state = "disabled" if enforce_seed_restrictions else "normal"

        if self.seed_entry is not None and self.seed_entry.winfo_exists():
            self.seed_entry.configure(state=seed_state)
        if self.seed_size_entry is not None and self.seed_size_entry.winfo_exists():
            self.seed_size_entry.configure(state=seed_state)
        if self.seed_recommend_btn is not None and self.seed_recommend_btn.winfo_exists():
            self.seed_recommend_btn.configure(state=button_state)
        if self.seed_settings_button is not None and self.seed_settings_button.winfo_exists():
            self.seed_settings_button.configure(state=button_state)
        if (
            self.seed_settings_dialog is not None
            and self.seed_settings_dialog.winfo_exists()
        ):
            self._sync_seed_settings_controls()
        elif self.seed_settings_dialog is not None:
            self.seed_settings_dialog = None
        if self.iterations_button is not None:
            self.iterations_button.configure(state=button_state)
        self._set_iteration_mode_display_state(button_state == "disabled")
        self._maybe_notify_method_efficiency()

        return method_key

    def _load_context_for_efficiency(self) -> EPRSContext:
        path = self.data_path_var.get()
        if not path:
            raise ValueError("Please select a dataset.")
        delimiter = self._get_delimiter(self.delimiter_var.get())
        split_settings = self._gather_split_settings()
        exclude_constant, constant_threshold = self._get_constant_filter()
        return load_dataset(
            path,
            delimiter=delimiter,
            split=split_settings,
            dependent_choice=self._get_dependent_choice(),
            non_variable_spec=self._get_non_variable_spec(),
            exclude_constant=exclude_constant,
            constant_threshold=constant_threshold,
            excluded_observations=self._get_excluded_observations_text(),
        )

    def _maybe_notify_method_efficiency(self) -> None:
        if self._efficiency_notify_job is not None:
            try:
                self.after_cancel(self._efficiency_notify_job)
            except Exception:  # noqa: BLE001
                pass
            self._efficiency_notify_job = None
        self._efficiency_notify_job = self.after(1000, self._notify_method_efficiency)

    def _reset_efficiency_notice_tracking(self) -> None:
        if self._efficiency_notify_job is not None:
            try:
                self.after_cancel(self._efficiency_notify_job)
            except Exception:  # noqa: BLE001
                pass
            self._efficiency_notify_job = None
        self._last_efficiency_notice = None
        self._last_efficiency_max_vars = None
        self._maybe_notify_method_efficiency()

    def _schedule_seed_minimum_enforcement(self) -> None:
        if self._seed_minimum_job is not None:
            try:
                self.after_cancel(self._seed_minimum_job)
            except Exception:  # noqa: BLE001
                pass
            self._seed_minimum_job = None
        self._seed_minimum_job = self.after(1000, self._enforce_seed_minimum)

    def _enforce_seed_minimum(self) -> None:
        self._seed_minimum_job = None
        if self.allow_small_seed_count.get():
            return
        raw_value = self.params_vars["n_seeds"].get().strip()
        if not raw_value:
            return
        try:
            seed_value = int(float(raw_value))
        except (TypeError, ValueError):
            return
        if seed_value < MIN_SEEDS:
            messagebox.showwarning(
                "Number of seeds",
                "Only values greater than 1000 are allowed. Resetting to 1000.",
            )
            self.params_vars["n_seeds"].set(str(MIN_SEEDS))

    def _notify_method_efficiency(self) -> None:
        self._efficiency_notify_job = None
        raw_max_vars = self.params_vars["max_vars"].get().strip()
        if not raw_max_vars:
            self._last_efficiency_notice = None
            return
        try:
            max_vars = int(raw_max_vars)
        except (TypeError, ValueError):
            return
        if max_vars <= 0:
            return
        if self._last_efficiency_max_vars is None or max_vars != self._last_efficiency_max_vars:
            self._last_efficiency_notice = None
            self._last_efficiency_max_vars = max_vars
        try:
            context = self._load_context_for_efficiency()
        except Exception as exc:  # noqa: BLE001
            if not isinstance(exc, FileNotFoundError):
                messagebox.showerror(
                    "Max predictors per model",
                    "Unable to compute combinations:\n" + str(exc),
                )
            return
        predictor_count = len(context.cols)
        if predictor_count <= 0:
            messagebox.showerror(
                "Max predictors per model",
                "No predictor columns were detected in the current configuration.",
            )
            return
        total_combos = _compute_combination_total(predictor_count, max_vars)
        threshold = _combination_efficiency_threshold(max_vars)
        method_key = METHOD_DISPLAY_TO_KEY.get(self.method_choice.get(), "all_subsets")
        notice_key = None
        if method_key == "eprs" and total_combos <= threshold:
            notice_key = "suggest_all_subsets"
        elif method_key == "all_subsets" and total_combos > threshold:
            notice_key = "suggest_eprs"

        if notice_key == self._last_efficiency_notice:
            return
        self._last_efficiency_notice = notice_key
        if notice_key == "suggest_all_subsets":
            messagebox.showinfo(
                "Search method guidance",
                'For the chosen maximum number of predictors, it is more efficient to use the "All subsets" method.',
            )
        elif notice_key == "suggest_eprs":
            messagebox.showinfo(
                "Search method guidance",
                "For the selected maximum number of predictors, it is more efficient to use the EPR-S method.",
            )

    def _set_seeds_to_predictors(self):
        path = self.data_path_var.get()
        try:
            delimiter = self._get_delimiter(self.delimiter_var.get())
            split_settings = self._gather_split_settings()
            exclude_constant, constant_threshold = self._get_constant_filter()
            context = load_dataset(
                path,
                delimiter=delimiter,
                split=split_settings,
                dependent_choice=self._get_dependent_choice(),
                non_variable_spec=self._get_non_variable_spec(),
                exclude_constant=exclude_constant,
                constant_threshold=constant_threshold,
                excluded_observations=self._get_excluded_observations_text(),
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror(
                "Number of seeds",
                "Unable to determine the number of predictor columns:\n" + str(exc),
            )
            return

        predictor_count = len(context.cols)
        if predictor_count <= 0:
            messagebox.showerror(
                "Number of seeds",
                "No predictor columns were detected in the current configuration.",
            )
            return

        self.params_vars["n_seeds"].set(str(predictor_count))

    def _set_seed_size_half(self):
        try:
            max_vars = int(self.params_vars["max_vars"].get())
        except (tk.TclError, ValueError):  # noqa: BLE001
            messagebox.showerror(
                "Seed size",
                "Please enter a valid integer for the maximum variables per model.",
            )
            return

        if max_vars <= 0:
            messagebox.showerror(
                "Seed size",
                "The maximum variables per model must be greater than zero.",
            )
            return

        if max_vars % 2 == 0:
            seed_size = max_vars // 2
        else:
            seed_size = max_vars // 2 + 1

        self.params_vars["seed_size"].set(str(seed_size))

    def _cancel_analysis(self):
        if self.worker_thread and self.worker_thread.is_alive():
            if not self.stop_event.is_set():
                self.stop_event.set()
                self.status_var.set("Cancelling...")
                self.cancel_button.configure(state="disabled")
                self._append_log("Cancel requested by user.\n")

    def _finalize_close(self):
        super().destroy()

    def _wait_for_worker_before_close(self):
        if self.worker_thread and self.worker_thread.is_alive():
            self.after(100, self._wait_for_worker_before_close)
            return
        self._finalize_close()

    def _on_close(self):
        self._cancel_analysis()
        if self.worker_thread and self.worker_thread.is_alive():
            self.after(100, self._wait_for_worker_before_close)
            return
        self._finalize_close()

    def _browse_file(self):
        path = filedialog.askopenfilename(
            title="Select dataset",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if path:
            self.data_path_var.set(path)

    def _browse_results_file(self):
        path = filedialog.askopenfilename(
            title="Select results file",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if path:
            self.results_load_path.set(path)

    def _load_results_file(self, *, select_tab: bool = True):
        path_text = self.results_load_path.get().strip()
        if not path_text:
            messagebox.showerror("Results error", "Please specify a results file to load.")
            return

        suppress_switch = not select_tab
        if suppress_switch:
            self._results_tab_suppression += 1

        try:
            path = Path(path_text)
            self.results_load_path.set(str(path))
            try:
                (
                    training_df,
                    internal_results,
                    external_results,
                    metadata,
                ) = self._read_results_file(path)
            except FileNotFoundError:
                messagebox.showerror("Results error", f"The file '{path}' was not found.")
                return
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror("Results error", f"Unable to load results:\n{exc}")
                return

            self.full_results_df = training_df
            self.last_results_df = training_df
            self.full_internal_results = [dict(row) for row in internal_results or []]
            self.full_external_results = [dict(row) for row in external_results or []]
            self.results_export_path = path

            if training_df.empty:
                self._append_log(
                    "No training models were found in the selected results file.\n"
                )

            metadata_models_found = self._safe_int(metadata.get("models_found"))
            if metadata_models_found is None:
                metadata_models_found = len(training_df) if training_df is not None else None
            self.summary_tab.update_models_found(metadata_models_found)
            metadata_models_reported = self._safe_int(metadata.get("models_reported"))
            if metadata_models_reported is None:
                metadata_models_reported = (
                    len(training_df) if training_df is not None else None
                )
            self.summary_tab.update_models_reported(metadata_models_reported)
            metadata_models_explored = self._safe_int(
                metadata.get("models_explored")
            )
            self.summary_tab.update_models_explored(metadata_models_explored)

            self._apply_metadata_from_results(metadata)
            self._cancel_holdout_refresh_job()
            self.last_results_metadata = dict(self.last_results_metadata or {})
            self.observation_cache.clear()
            self.correlation_cache.clear()

            self._prepare_context_for_loaded_results(allow_prompt=True)
            self.summary_tab.update_context(self.last_context, self.last_config)

            self._load_training_results(training_df)
            self.update_internal_results(internal_results)
            self.update_external_results(external_results)
            if not external_results:
                holdout_results = self._derive_holdout_results(self.last_results_df)
                if holdout_results:
                    self.update_external_results(holdout_results)
            self.validation_tab.update_sources(
                training_df, self.last_context, self.last_config
            )
            if self.validation_tab.available:
                self.notebook.tab(self.validation_tab, state="normal")
            else:
                self.notebook.tab(self.validation_tab, state="disabled")
            if self.last_results_df is not None and not self.last_results_df.empty:
                self.notebook.tab(self.summary_tab, state="normal")
                self.notebook.tab(self.diagnostics_tab, state="normal")
                self.notebook.tab(self.visual_tab, state="normal")
            else:
                self.summary_tab.prepare_for_new_run()
                self.notebook.tab(self.summary_tab, state="disabled")
                self.diagnostics_tab.prepare_for_new_run()
                self.notebook.tab(self.diagnostics_tab, state="disabled")
                self.visual_tab.prepare_for_new_run()
                self.variable_tab.prepare_for_new_run()
                self.notebook.tab(self.visual_tab, state="disabled")
            if select_tab:
                self.notebook.select(self.results_tab)
        finally:
            if suppress_switch:
                self._results_tab_suppression = max(0, self._results_tab_suppression - 1)

    def _get_results_filter_source(self) -> Optional[pd.DataFrame]:
        if self.full_results_df is not None and not self.full_results_df.empty:
            return self.full_results_df
        return self.last_results_df

    def _center_popup(self, window: tk.Toplevel, *, preserve_top: bool = False) -> None:
        window.update_idletasks()
        parent_x = self.winfo_rootx()
        parent_y = self.winfo_rooty()
        parent_width = self.winfo_width()
        parent_height = self.winfo_height()
        requested_width = window.winfo_reqwidth()
        requested_height = window.winfo_reqheight()
        window_width = max(window.winfo_width(), requested_width)
        window_height = max(window.winfo_height(), requested_height)

        x = parent_x + (parent_width // 2) - (window_width // 2)
        y = parent_y + (parent_height // 2) - (window_height // 2)

        if preserve_top:
            y = window.winfo_y()
        window.geometry(f"{window_width}x{window_height}+{int(x)}+{int(y)}")

    def _open_filter_dialog(self):
        if self._filter_window is not None and tk.Toplevel.winfo_exists(self._filter_window):
            self._filter_window.deiconify()
            self._filter_window.lift()
            return

        window = tk.Toplevel(self)
        window.title("Filter models")
        window.transient(self)
        window.resizable(False, False)
        window.protocol("WM_DELETE_WINDOW", self._close_filter_window)

        container = ttk.Frame(window, padding=12)
        container.pack(fill="both", expand=True)

        ttk.Label(container, text="Filter by variable").pack(anchor="w")

        entry_row = ttk.Frame(container)
        entry_row.pack(fill="x", pady=(6, 8))

        entry = ttk.Entry(entry_row, textvariable=self._filter_variable_entry)
        entry.pack(side="left", fill="x", expand=True)
        entry.focus_set()

        ttk.Button(entry_row, text="Add", command=self._add_filter_variable).pack(
            side="left", padx=(8, 0)
        )

        list_container = ttk.Frame(container, relief="groove", padding=6)
        list_container.pack(fill="x", expand=False)

        self._filter_list_frame = ttk.Frame(list_container)
        self._filter_list_frame.pack(fill="both", expand=True)

        self._filter_window = window
        self._refresh_filter_variable_list()

        ttk.Button(container, text="Apply", command=self._apply_variable_filter).pack(
            anchor="e", pady=(10, 0)
        )

        self._center_popup(window)

    def _refresh_filter_variable_list(self) -> None:
        if self._filter_list_frame is None:
            return

        for child in self._filter_list_frame.winfo_children():
            child.destroy()

        if not self._filter_variables:
            return

        for name, state in self._filter_variables:
            row = ttk.Frame(self._filter_list_frame)
            row.pack(fill="x", pady=2)

            ttk.Checkbutton(row, text=name, variable=state).pack(
                side="left", anchor="w"
            )
            ttk.Button(
                row,
                text="Remove",
                command=lambda item=name: self._remove_filter_variable(item),
                width=8,
            ).pack(side="right")

        if self._filter_window is not None and tk.Toplevel.winfo_exists(self._filter_window):
            self._filter_window.update_idletasks()
            self._center_popup(self._filter_window, preserve_top=True)

    def _remove_filter_variable(self, name: str) -> None:
        self._filter_variables = [item for item in self._filter_variables if item[0] != name]
        self._refresh_filter_variable_list()

    def _add_filter_variable(self) -> None:
        if len(self._filter_variables) >= 6:
            messagebox.showwarning(
                "Filter models", "You can select a maximum of six variables."
            )
            return

        name = self._filter_variable_entry.get().strip()
        if not name:
            messagebox.showwarning("Filter models", "Please enter a variable name to add.")
            return

        normalized = name.lower()
        for existing_name, state in self._filter_variables:
            if existing_name.lower() == normalized:
                state.set(True)
                self._filter_variable_entry.set("")
                return

        self._filter_variables.append((name, tk.BooleanVar(value=True)))
        self._filter_variable_entry.set("")
        self._refresh_filter_variable_list()

    def _apply_variable_filter(self) -> None:
        base_df = self._get_results_filter_source()
        if base_df is None or base_df.empty:
            messagebox.showinfo("Filter models", "There are no models to filter.")
            return

        required = [
            name.strip()
            for name, state in self._filter_variables
            if state.get() and name.strip()
        ]

        if not required:
            self._load_training_results(base_df)
            self.update_internal_results(self.full_internal_results, remember_source=False)
            self.update_external_results(self.full_external_results, remember_source=False)
            self._close_filter_window()
            return

        normalized_required = [name.lower() for name in required]

        filtered_rows: list[pd.Series] = []
        for _, row in base_df.iterrows():
            variables = self._normalize_variables(row.get("Variables"))
            variable_set = {item.lower() for item in variables}
            if all(name in variable_set for name in normalized_required):
                filtered_rows.append(row)

        filtered_df = pd.DataFrame(filtered_rows)
        if filtered_df.empty:
            messagebox.showinfo(
                "Filter models",
                "No models contain all selected variables. Adjust your selection and try again.",
            )
            return

        self._load_training_results(filtered_df)
        self.update_internal_results(self.full_internal_results, remember_source=False)
        self.update_external_results(self.full_external_results, remember_source=False)
        self._close_filter_window()

    def _close_filter_window(self) -> None:
        if self._filter_window is not None and tk.Toplevel.winfo_exists(self._filter_window):
            self._filter_window.destroy()
        self._filter_window = None
        self._filter_list_frame = None

    def _prepare_context_for_loaded_results(
        self, *, allow_prompt: bool = False, log_failure: bool = True
    ) -> bool:
        try:
            config = self._build_config(allow_defaults=True)
            split_settings = self._gather_split_settings()
            context = load_dataset(
                config.data_path,
                delimiter=config.delimiter,
                split=split_settings,
                dependent_choice=config.dependent_choice,
                non_variable_spec=config.non_variable_spec,
                exclude_constant=config.exclude_constant,
                constant_threshold=config.constant_threshold,
                excluded_observations=config.excluded_observations,
            )
        except Exception as exc:  # noqa: BLE001
            self.last_config = None
            self.last_context = None
            self.last_split_settings = None
            self._set_holdout_ready(False)
            if log_failure:
                self._append_log(
                    "Unable to prepare dataset context for loaded results. "
                    "Diagnostics and visualization require a compatible dataset.\n"
                )

            message = (
                "The dataset referenced by the loaded results could not be opened."
            )
            if allow_prompt:
                answer = messagebox.askyesno(
                    "Dataset required",
                    f"{message}\n\nWould you like to locate the dataset now?",
                )
                if answer:
                    new_path = filedialog.askopenfilename(
                        title="Select dataset",
                        filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                    )
                    if new_path:
                        self.data_path_var.set(new_path)
                        success = self._prepare_context_for_loaded_results(
                            allow_prompt=False, log_failure=False
                        )
                        if success:
                            self._append_log(
                                "Dataset context restored using the selected file.\n"
                            )
                        else:
                            messagebox.showerror(
                                "Dataset required",
                                "The selected dataset could not be loaded. "
                                "Diagnostics and visualization remain unavailable.",
                            )
                        return success
            else:
                detail = f"\n\nDetails: {exc}" if exc else ""
                messagebox.showerror(
                    "Dataset required",
                    f"{message}\nDiagnostics and visualization will remain unavailable.{detail}",
                )
            return False

        self.last_config = config
        self.last_context = context
        self.last_split_settings = split_settings
        self._set_holdout_ready(self._context_has_holdout(context))
        if (not self.holdout_ready) and self.external_test_path.get().strip():
            self.ensure_holdout_data_available(show_alert=False)
        return True

    def _clear_results_view(self):
        self.last_results_df = None
        self._training_order = []
        self.full_results_df = None
        self.last_internal_results = []
        self.last_external_results = []
        self.full_internal_results = []
        self.full_external_results = []
        self.observation_cache.clear()
        self.correlation_cache.clear()
        self.last_results_metadata = None
        self._cancel_holdout_refresh_job()
        self._set_holdout_ready(False)
        self._update_kfold_settings(False, None, None)

        self._filter_variables.clear()
        self._filter_variable_entry.set("")
        self._close_filter_window()

        if hasattr(self, "training_tree"):
            self.training_tree.delete(*self.training_tree.get_children())
        if hasattr(self, "internal_results_tree"):
            self.internal_results_tree.delete(*self.internal_results_tree.get_children())
        if hasattr(self, "external_results_tree"):
            self.external_results_tree.delete(*self.external_results_tree.get_children())

        self.validation_tab.prepare_for_new_run()
        self.summary_tab.prepare_for_new_run()
        self.diagnostics_tab.prepare_for_new_run()
        self.visual_tab.prepare_for_new_run()
        self.variable_tab.prepare_for_new_run()

        self.notebook.tab(self.validation_tab, state="disabled")
        self.notebook.tab(self.summary_tab, state="disabled")
        self.notebook.tab(self.diagnostics_tab, state="disabled")
        self.notebook.tab(self.visual_tab, state="disabled")

    def _get_dependent_choice(self) -> str:
        value = self.dependent_var_var.get().strip().lower()
        mapping = {
            "last column": "last",
            "first column": "first",
            "second column": "second",
            "third column": "third",
        }
        if value not in mapping:
            raise ValueError("Please select a valid dependent variable option.")
        return mapping[value]

    def _get_non_variable_spec(self) -> str:
        value = self.non_variable_var.get().strip().lower()
        mapping = {
            "none": "",
            "first column": "1",
            "first and second column": "1,2",
            "first, second, and third column": "1,2,3",
        }
        if value not in mapping:
            raise ValueError("Please select a valid non-variable option.")
        return mapping[value]

    def _get_constant_filter(self) -> tuple[bool, float]:
        enabled = self.constant_filter_enabled.get()
        raw_value = self.constant_threshold_var.get().strip()
        if not raw_value:
            if enabled:
                raise ValueError("Please enter a percentage threshold for near-constant predictors.")
            return False, 90.0
        try:
            value = float(raw_value)
        except ValueError as exc:  # noqa: BLE001
            if enabled:
                raise ValueError("Near-constant predictor threshold must be numeric.") from exc
            return False, 90.0
        if not 0 < value <= 100:
            if enabled:
                raise ValueError("Near-constant predictor threshold must be between 0 and 100.")
            return False, 90.0
        return enabled, value

    def _get_excluded_observations_text(self) -> str:
        value = self.exclude_obs_var.get().strip()
        if not value:
            return ""
        try:
            ids = _parse_id_entries(value)
        except ValueError as exc:  # noqa: BLE001
            raise ValueError(f"Exclude observations: {exc}") from exc
        normalized = self._ids_to_text(ids)
        self.exclude_obs_var.set(normalized)
        return normalized

    def _get_delimiter(self, value: str) -> str:
        delimiter = value.strip()
        if not delimiter:
            raise ValueError("Please specify a delimiter for the CSV file.")
        if delimiter == "\\t":
            return "\t"
        return delimiter

    @staticmethod
    def _serialize_delimiter_value(value: str) -> str:
        if not value:
            return ""
        name = DELIMITER_VALUE_TO_NAME.get(value)
        if name:
            return name
        codes = "-".join(str(ord(ch)) for ch in value)
        return codes

    @staticmethod
    def _normalize_delimiter_value(value: Optional[object]) -> str:
        if value in (None, ""):
            return ""

        if isinstance(value, dict):
            name_value = value.get("name")
            if isinstance(name_value, str):
                return MLRXApp._normalize_delimiter_value(name_value)
            codes_value = value.get("codes")
            if isinstance(codes_value, (list, tuple)):
                chars: list[str] = []
                for code in codes_value:
                    try:
                        chars.append(chr(int(code)))
                    except (TypeError, ValueError):
                        continue
                if chars:
                    return "".join(chars)
            raw = value.get("value")
            if isinstance(raw, str):
                return MLRXApp._normalize_delimiter_value(raw)
            return ""

        if isinstance(value, (list, tuple)):
            chars = []
            for code in value:
                try:
                    chars.append(chr(int(code)))
                except (TypeError, ValueError):
                    continue
            return "".join(chars)

        if isinstance(value, (int, np.integer)):
            try:
                return chr(int(value))
            except (OverflowError, ValueError):  # pragma: no cover - defensive
                return ""

        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return ""
            if stripped == "\\t":
                return "\t"
            lowered = stripped.lower()
            if lowered in DELIMITER_NAME_TO_VALUE:
                return DELIMITER_NAME_TO_VALUE[lowered]
            if len(stripped) == 1:
                return stripped
            parts = stripped.split("-")
            if all(part.strip().isdigit() for part in parts):
                chars: list[str] = []
                for part in parts:
                    try:
                        chars.append(chr(int(part.strip())))
                    except (ValueError, OverflowError):
                        return ""
                return "".join(chars)
            if stripped.isdigit():
                try:
                    return chr(int(stripped))
                except (OverflowError, ValueError):
                    return stripped
            return stripped

        return str(value)

    def _delimiter_to_ui(self, value: str) -> str:
        if value == "\t":
            return "\\t"
        return value

    @staticmethod
    def _format_numeric_value(value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, (int, np.integer)):
            return str(int(value))
        try:
            float_value = float(value)
        except (TypeError, ValueError):
            return str(value)
        if float_value.is_integer():
            return str(int(float_value))
        return f"{float_value:.12g}"

    @staticmethod
    def _normalize_id_iterable(values: object) -> list[str]:
        if values is None:
            return []
        if isinstance(values, str):
            raw_items = [segment.strip() for segment in values.split(",")]
        else:
            raw_items = [str(item).strip() for item in values]
        cleaned = [item for item in raw_items if item]

        def sort_key(item: str):
            return (not item.isdigit(), int(item) if item.isdigit() else item)

        return sorted(cleaned, key=sort_key)

    @staticmethod
    def _ids_to_text(values: object) -> str:
        cleaned = MLRXApp._normalize_id_iterable(values)
        return ", ".join(cleaned)

    def _metadata_from_split_settings(self, split_settings: Optional[dict]) -> dict:
        meta = {"mode": "none"}
        if not split_settings:
            return meta

        mode = split_settings.get("mode", "none")
        meta["mode"] = mode

        if mode == "random":
            try:
                fraction = float(split_settings.get("test_size", 0.0))
            except (TypeError, ValueError):
                fraction = 0.0
            meta["test_size_percent"] = fraction * 100.0
        elif mode == "manual":
            meta["train_ids"] = self._normalize_id_iterable(
                split_settings.get("train_ids")
            )
            meta["test_ids"] = self._normalize_id_iterable(
                split_settings.get("test_ids")
            )

        return meta

    @staticmethod
    def _split_settings_from_metadata_dict(meta_split: Optional[dict]) -> dict:
        if not meta_split:
            return {"mode": "none"}

        mode = meta_split.get("mode", "none")
        if mode == "random":
            percent = meta_split.get("test_size_percent", 0.0)
            try:
                fraction = float(percent) / 100.0
            except (TypeError, ValueError):
                fraction = 0.0
            return {"mode": "random", "test_size": fraction}
        if mode == "manual":
            train_ids = set(MLRXApp._normalize_id_iterable(meta_split.get("train_ids")))
            test_ids = set(MLRXApp._normalize_id_iterable(meta_split.get("test_ids")))
            return {"mode": "manual", "train_ids": train_ids, "test_ids": test_ids}
        return {"mode": "none"}

    def _sanitize_kfold_metadata(self, meta: Optional[dict]) -> dict:
        info = {"enabled": False, "folds": None, "repeats": None}
        if not isinstance(meta, dict):
            return info
        enabled = bool(meta.get("enabled"))
        folds = self._safe_int(meta.get("folds"))
        repeats = self._safe_int(meta.get("repeats"))
        if enabled and folds is not None and repeats is not None and folds >= 2 and repeats >= 1:
            info.update({"enabled": True, "folds": folds, "repeats": repeats})
        return info

    def _build_kfold_metadata(self, fallback: Optional[dict] = None) -> dict:
        base = self._sanitize_kfold_metadata(fallback)
        settings = getattr(self, "kfold_settings", None)
        if isinstance(settings, dict) and settings.get("enabled"):
            folds = self._safe_int(settings.get("folds"))
            repeats = self._safe_int(settings.get("repeats"))
            if folds is not None and repeats is not None and folds >= 2 and repeats >= 1:
                return {"enabled": True, "folds": folds, "repeats": repeats}

        validation_tab = getattr(self, "validation_tab", None)
        if validation_tab is not None:
            try:
                enabled = bool(validation_tab.use_kfold.get())
            except Exception:  # noqa: BLE001
                enabled = False
            if enabled:
                folds = self._safe_int(validation_tab.kfold_folds_var.get())
                repeats = self._safe_int(validation_tab.kfold_repeats_var.get())
                if folds is not None and repeats is not None and folds >= 2 and repeats >= 1:
                    return {"enabled": True, "folds": folds, "repeats": repeats}

        if base.get("enabled") and base.get("folds") and base.get("repeats"):
            return base
        return {"enabled": False, "folds": None, "repeats": None}

    def _apply_kfold_metadata(self, raw_meta: Optional[dict]) -> dict:
        info = self._sanitize_kfold_metadata(raw_meta)
        validation_tab = getattr(self, "validation_tab", None)
        if validation_tab is not None:
            try:
                validation_tab.use_kfold.set(info["enabled"])
                if info["enabled"]:
                    if info["folds"] is not None:
                        validation_tab.kfold_folds_var.set(info["folds"])
                    if info["repeats"] is not None:
                        validation_tab.kfold_repeats_var.set(info["repeats"])
                validation_tab._update_method_controls()
            except Exception:  # noqa: BLE001
                pass
        self._update_kfold_settings(info["enabled"], info.get("folds"), info.get("repeats"))
        return info

    def _update_kfold_settings(
        self, enabled: bool, folds: Optional[int], repeats: Optional[int]
    ) -> None:
        if enabled:
            folds_int = self._safe_int(folds)
            repeats_int = self._safe_int(repeats)
            if (
                folds_int is not None
                and repeats_int is not None
                and folds_int >= 2
                and repeats_int >= 1
            ):
                self.kfold_settings = {
                    "enabled": True,
                    "folds": folds_int,
                    "repeats": repeats_int,
                }
            else:
                self.kfold_settings = {"enabled": False, "folds": None, "repeats": None}
        else:
            self.kfold_settings = {"enabled": False, "folds": None, "repeats": None}

        if hasattr(self, "summary_tab"):
            try:
                self.summary_tab.info_vars["kfold"].set(
                    self.summary_tab._resolve_kfold_summary()
                )
            except Exception:  # noqa: BLE001
                pass

    def _build_metadata_dict(
        self,
        config: EPRSConfig,
        split_settings: Optional[dict],
        models_found: Optional[int] = None,
        models_reported: Optional[int] = None,
        models_explored: Optional[int] = None,
        *,
        avg_iterations_per_seed: Optional[object] = None,
        max_iterations_per_seed: Optional[object] = None,
    ) -> dict:
        delimiter_value = self._normalize_delimiter_value(config.delimiter)

        metadata: dict[str, object] = {
            "version": METADATA_VERSION,
            "dataset_path": config.data_path,
            "delimiter": self._serialize_delimiter_value(delimiter_value),
            "dependent": config.dependent_choice,
            "non_variable": config.non_variable_spec,
            "exclude_constant": bool(config.exclude_constant),
            "constant_threshold": float(config.constant_threshold),
            "excluded_observations": (config.excluded_observations or "").strip() or "none",
            "target_metric": config.target_metric,
            "top_models": int(config.export_limit),
            "method": config.method,
            "cov_type": getattr(config, "cov_type", COVARIANCE_DEFAULT_KEY),
            "iterations_mode": getattr(config, "iterations_mode", ITERATION_MODE_AUTO),
        }

        settings_values = [
            self._format_numeric_value(getattr(config, key, None))
            for key in SETTINGS_NUMERIC_KEYS
        ]
        metadata["settings_values"] = "#".join(settings_values)
        metadata["split"] = self._metadata_from_split_settings(split_settings)

        max_iterations = getattr(config, "max_iterations_per_seed", None)
        metadata["max_iterations_per_seed"] = _resolve_iterations_metadata_value(
            getattr(config, "method", "all_subsets"),
            getattr(config, "iterations_mode", ITERATION_MODE_AUTO),
            max_iterations,
            avg_iterations_per_seed,
            max_iterations_per_seed,
            formatter=self._format_numeric_value,
        )

        clip = config.clip_predictions
        if clip is not None:
            metadata["clip"] = {
                "enabled": True,
                "low": float(clip[0]),
                "high": float(clip[1]),
            }
        else:
            metadata["clip"] = {"enabled": False}

        metadata["kfold"] = self._build_kfold_metadata(metadata.get("kfold"))

        if models_found is not None:
            try:
                metadata["models_found"] = int(models_found)
            except (TypeError, ValueError):
                pass

        if models_reported is not None:
            try:
                metadata["models_reported"] = int(models_reported)
            except (TypeError, ValueError):
                pass

        if models_explored is not None:
            try:
                metadata["models_explored"] = int(models_explored)
            except (TypeError, ValueError):
                pass

        return metadata

    def _collect_metadata_for_export(self) -> dict:
        metadata: dict[str, object]
        config = self.last_config
        split_settings = self.last_split_settings

        if config is None:
            try:
                config = self._build_config(allow_defaults=True)
                split_settings = self._gather_split_settings()
            except Exception:  # noqa: BLE001
                config = None

        if config is not None:
            metadata = self._build_metadata_dict(
                config,
                split_settings,
                self.summary_tab.get_models_found(),
                self.summary_tab.get_models_reported(),
                self.summary_tab.get_models_explored(),
                avg_iterations_per_seed=self.summary_tab.get_avg_iterations_per_seed(),
                max_iterations_per_seed=self.summary_tab.get_max_iterations_per_seed(),
            )
        else:
            metadata = dict(self.last_results_metadata or {})
            metadata.setdefault("version", METADATA_VERSION)
            metadata.setdefault(
                "excluded_observations", (self.exclude_obs_var.get().strip() or "none")
            )
            if "method" not in metadata:
                metadata["method"] = "all_subsets"

        models_found_value = (
            self.summary_tab.get_models_found()
            if hasattr(self, "summary_tab")
            else None
        )
        if models_found_value is not None:
            metadata["models_found"] = models_found_value

        models_explored_value = (
            self.summary_tab.get_models_explored()
            if hasattr(self, "summary_tab")
            else None
        )
        if models_explored_value is not None:
            metadata["models_explored"] = models_explored_value

        metadata.pop("max_r2_calls", None)

        delimiter_value = self._normalize_delimiter_value(metadata.get("delimiter"))
        metadata["delimiter"] = (
            self._serialize_delimiter_value(delimiter_value) if delimiter_value else ""
        )
        split_meta = metadata.get("split")
        if isinstance(split_meta, dict) and split_meta.get("mode") == "external":
            normalized = self._normalize_delimiter_value(
                split_meta.get("external_delimiter")
            )
            split_meta["external_delimiter"] = (
                self._serialize_delimiter_value(normalized) if normalized else ""
            )

        # Ensure newly added dataset/settings options are mirrored in the metadata so
        # restored sessions match the current UI configuration even when the config
        # object cannot be rebuilt (e.g., when exporting loaded results).
        try:
            metadata["dependent"] = self._get_dependent_choice()
        except Exception:  # noqa: BLE001
            pass

        try:
            metadata["non_variable"] = self._get_non_variable_spec()
        except Exception:  # noqa: BLE001
            pass

        try:
            enabled, threshold = self._get_constant_filter()
        except Exception:  # noqa: BLE001
            enabled = self.constant_filter_enabled.get()
            try:
                threshold = float(self.constant_threshold_var.get())
            except Exception:  # noqa: BLE001
                threshold = metadata.get("constant_threshold", 90.0)
        metadata["exclude_constant"] = bool(enabled)
        metadata["constant_threshold"] = float(threshold)

        try:
            metadata["excluded_observations"] = (
                self._get_excluded_observations_text() or "none"
            )
        except Exception:  # noqa: BLE001
            metadata["excluded_observations"] = self.exclude_obs_var.get().strip() or "none"

        display_metric = self.target_metric_choice.get()
        metric_key = TARGET_METRIC_DISPLAY_TO_KEY.get(display_metric)
        if metric_key:
            metadata["target_metric"] = metric_key

        method_display = self.method_choice.get()
        method_key = METHOD_DISPLAY_TO_KEY.get(method_display)
        if method_key:
            metadata["method"] = method_key

        export_limit_var = self.params_vars.get("export_limit")
        if export_limit_var is not None:
            try:
                metadata["top_models"] = int(export_limit_var.get())
            except Exception:  # noqa: BLE001
                pass

        metadata["kfold"] = self._build_kfold_metadata(metadata.get("kfold"))

        def _resolve_minutes(primary: Optional[float], fallback: object) -> Optional[float]:
            for candidate in (primary, fallback):
                if candidate is None:
                    continue
                try:
                    numeric = float(candidate)
                except (TypeError, ValueError):
                    continue
                if np.isfinite(numeric):
                    return numeric
            return None

        search_minutes = _resolve_minutes(
            self.last_cpu_time_search_minutes, metadata.get("cpu_time_search_minutes")
        )
        total_minutes = _resolve_minutes(
            self.last_cpu_time_total_minutes, metadata.get("cpu_time_total_minutes")
        )
        if search_minutes is not None:
            metadata["cpu_time_search_minutes"] = float(search_minutes)
            metadata["cpu_time_search_seconds"] = float(search_minutes * 60.0)
        else:
            metadata.pop("cpu_time_search_minutes", None)
            metadata.pop("cpu_time_search_seconds", None)

        if total_minutes is not None:
            metadata["cpu_time_total_minutes"] = float(total_minutes)
            metadata["cpu_time_total_seconds"] = float(total_minutes * 60.0)
        else:
            metadata.pop("cpu_time_total_minutes", None)
            metadata.pop("cpu_time_total_seconds", None)

        if metadata.get("max_iterations_per_seed") in {None, ""}:
            metadata["max_iterations_per_seed"] = "none"

        return metadata

    def _read_results_file(
        self, path: Path
    ) -> tuple[pd.DataFrame, list[dict], list[dict], dict]:
        if not path.exists():
            raise FileNotFoundError

        metadata: dict = {}
        data_lines: list[str] = []

        inline_metadata_detected = False
        inline_delimiter: Optional[str] = None
        inline_column_count: Optional[int] = None
        inline_metadata_fields: int = 0

        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith(METADATA_PREFIX):
                    candidate = _deserialize_metadata(line)
                    if candidate:
                        metadata = candidate
                    continue

                if not data_lines and not line.strip():
                    continue

                if not data_lines:
                    header_line = line.rstrip("\n\r")
                    candidates = list(DELIMITER_NAME_TO_VALUE.values())
                    if ";" not in candidates:
                        candidates.append(";")
                    for delimiter in candidates:
                        header_values = _split_csv_line(header_line, delimiter)
                        metadata_candidate = _deserialize_metadata(header_values[-1])
                        if metadata_candidate:
                            inline_metadata_detected = True
                            inline_delimiter = delimiter
                            inline_column_count = len(header_values) - 1
                            inline_metadata_fields = 1
                            metadata = metadata_candidate
                            sanitized_header = _join_csv_line(header_values[:-1], delimiter)
                            line = sanitized_header + "\n"
                            break

                        metadata_candidate = _deserialize_metadata(header_values[-2]) if len(header_values) >= 2 else {}
                        if metadata_candidate and header_values[-1].strip() == "#":
                            inline_metadata_detected = True
                            inline_delimiter = delimiter
                            inline_column_count = len(header_values) - 2
                            inline_metadata_fields = 2
                            metadata = metadata_candidate
                            sanitized_header = _join_csv_line(header_values[:-2], delimiter)
                            line = sanitized_header + "\n"
                            break

                        metadata_label_lower = METADATA_HEADER_LABEL.strip().lower()
                        if (
                            len(header_values) >= 2
                            and header_values[-2].strip().lower() == metadata_label_lower
                        ):
                            inline_metadata_detected = True
                            inline_delimiter = delimiter
                            inline_column_count = len(header_values) - 2
                            inline_metadata_fields = 2
                            metadata_candidate = _deserialize_metadata(header_values[-1])
                            if metadata_candidate:
                                metadata = metadata_candidate
                            sanitized_header = _join_csv_line(header_values[:-2], delimiter)
                            line = sanitized_header + "\n"
                            break

                if inline_metadata_detected and inline_delimiter:
                    raw_line = line.rstrip("\n\r")
                    if raw_line:
                        row_values = _split_csv_line(raw_line, inline_delimiter)
                        if inline_metadata_fields == 0:
                            sanitized_row = raw_line
                        elif inline_column_count is None or len(row_values) >= inline_column_count + inline_metadata_fields:
                            sanitized_row = _join_csv_line(row_values[:-inline_metadata_fields], inline_delimiter)
                            line = sanitized_row + "\n"

                data_lines.append(line)

        if not data_lines:
            raise ValueError("The selected file does not contain any rows.")

        buffer = io.StringIO("".join(data_lines))
        try:
            df = pd.read_csv(buffer, sep=";")
        except Exception:  # noqa: BLE001
            buffer.seek(0)
            df = pd.read_csv(buffer)

        df = df.copy()
        if "Predictors" in df.columns and "Variables" not in df.columns:
            df = df.rename(columns={"Predictors": "Variables"})
        if "N_pred" in df.columns and "N_var" not in df.columns:
            df = df.rename(columns={"N_pred": "N_var"})
        metadata_column: Optional[str] = None
        for column in df.columns:
            if not isinstance(column, str):
                continue
            if column.strip().lower() in LEGACY_METADATA_COLUMNS:
                metadata_column = column
                break

        if metadata_column is not None:
            series = df[metadata_column]
            for value in series:
                candidate = _deserialize_metadata(value)
                if candidate:
                    metadata = candidate
                    break
            df = df.drop(columns=[metadata_column])

        if "Section" in df.columns:
            sections = df["Section"].astype(str).str.strip().str.lower()
        else:
            sections = None

        if sections is not None:
            training_raw = df.loc[sections == "training"].copy()
            internal_source = df.loc[sections == "internal"].copy()
            external_source = df.loc[sections == "external"].copy()
        else:
            training_raw = df.copy()
            internal_source = df.copy()
            external_source = df.copy()

        required_columns = {"Model", "Variables", "R2"}
        missing_columns = [col for col in required_columns if col not in training_raw.columns]
        if missing_columns:
            missing_text = ", ".join(missing_columns)
            raise ValueError(f"Training results are missing required columns: {missing_text}.")

        training_rows: list[dict] = []
        for _, row in training_raw.iterrows():
            model_value = self._safe_int(row.get("Model"))
            variables = self._normalize_variables(row.get("Variables"))
            if model_value is None:
                raise ValueError("Training results are missing model identifiers.")
            entry: dict[str, object] = {
                "Model": model_value,
                "Variables": variables,
                "N_var": len(variables),
                "R2": self._safe_float(row.get("R2")),
                "R2_adj": self._safe_float(row.get("R2_adj")),
                "RMSE": self._safe_float(row.get("RMSE")),
                "s": self._safe_float(row.get("s")),
                "MAE": self._safe_float(row.get("MAE")),
                "VIF_max": self._safe_float(row.get("VIF_max")),
                "VIF_avg": self._safe_float(row.get("VIF_avg")),
            }
            training_rows.append(entry)

        training_df = pd.DataFrame(training_rows)
        valid_models: set[int] = set()
        if not training_df.empty and "Model" in training_df.columns:
            valid_models = {
                model
                for model in (
                    self._safe_int(value) for value in training_df["Model"].tolist()
                )
                if model is not None
            }

        internal_rows: list[dict] = []
        for _, row in internal_source.iterrows():
            model_value = self._safe_int(row.get("Model"))
            if model_value is None or (valid_models and model_value not in valid_models):
                continue
            metrics = {
                "R2_loo": self._safe_float(row.get("R2_loo")),
                "RMSE_loo": self._safe_float(row.get("RMSE_loo")),
                "s_loo": self._safe_float(row.get("s_loo")),
                "MAE_loo": self._safe_float(row.get("MAE_loo")),
                "R2_kfold": self._safe_float(row.get("R2_kfold")),
                "RMSE_kfold": self._safe_float(row.get("RMSE_kfold")),
                "s_kfold": self._safe_float(row.get("s_kfold")),
                "MAE_kfold": self._safe_float(row.get("MAE_kfold")),
            }
            if all(value is None for value in metrics.values()):
                continue
            variables = self._normalize_variables(row.get("Variables"))
            internal_rows.append(
                {
                    "Model": model_value,
                    "Variables": variables,
                    "N_var": len(variables),
                    **metrics,
                }
            )

        external_rows: list[dict] = []
        for _, row in external_source.iterrows():
            model_value = self._safe_int(row.get("Model"))
            if model_value is None or (valid_models and model_value not in valid_models):
                continue
            metrics = {
                "Q2F1_ext": self._safe_float(row.get("Q2F1_ext")),
                "Q2F2_ext": self._safe_float(row.get("Q2F2_ext")),
                "Q2F3_ext": self._safe_float(row.get("Q2F3_ext")),
                "RMSE_ext": self._safe_float(row.get("RMSE_ext")),
                "s_ext": self._safe_float(row.get("s_ext")),
                "MAE_ext": self._safe_float(row.get("MAE_ext")),
            }
            if all(value is None for value in metrics.values()):
                continue
            variables = self._normalize_variables(row.get("Variables"))
            external_rows.append(
                {
                    "Model": model_value,
                    "Variables": variables,
                    "N_var": len(variables),
                    **metrics,
                }
            )

        return training_df, internal_rows, external_rows, metadata

    def _apply_metadata_from_results(self, metadata: dict) -> None:
        if not metadata:
            self.last_split_settings = {"mode": "none"}
            self.last_results_metadata = {}
            return

        dataset_path = metadata.get("dataset_path")
        if dataset_path:
            self.data_path_var.set(str(dataset_path))

        delimiter_value = self._normalize_delimiter_value(metadata.get("delimiter"))
        if delimiter_value:
            self.delimiter_var.set(self._delimiter_to_ui(delimiter_value))

        dependent = str(metadata.get("dependent", "")).lower()
        if dependent in DEPENDENT_TO_DISPLAY:
            self.dependent_var_var.set(DEPENDENT_TO_DISPLAY[dependent])

        non_variable_value = metadata.get("non_variable")
        if non_variable_value is not None:
            display = NON_VARIABLE_TO_DISPLAY.get(str(non_variable_value))
            if display:
                self.non_variable_var.set(display)

        exclude_constant = metadata.get("exclude_constant")
        if exclude_constant is not None:
            self.constant_filter_enabled.set(bool(exclude_constant))

        threshold_value = metadata.get("constant_threshold")
        if threshold_value is not None:
            self.constant_threshold_var.set(
                self._format_numeric_value(threshold_value)
            )
        self._toggle_constant_filter()

        excluded_value = metadata.get("excluded_observations")
        if excluded_value is not None:
            if isinstance(excluded_value, str):
                self.exclude_obs_var.set(excluded_value)
            else:
                self.exclude_obs_var.set(self._ids_to_text(excluded_value))
        else:
            self.exclude_obs_var.set("")

        target_metric_key = metadata.get("target_metric")
        if target_metric_key in TARGET_METRIC_DISPLAY:
            self.target_metric_choice.set(TARGET_METRIC_DISPLAY[target_metric_key])

        method_key = metadata.get("method")
        if isinstance(method_key, str):
            display_value = METHOD_KEY_TO_DISPLAY.get(method_key)
            if display_value:
                self.method_choice.set(display_value)

        iteration_mode = metadata.get("iterations_mode")
        manual_iterations_value: Optional[int] = None
        if isinstance(iteration_mode, str) and iteration_mode:
            normalized_mode = iteration_mode.lower()
            if normalized_mode in {
                ITERATION_MODE_AUTO,
                ITERATION_MODE_MANUAL,
                ITERATION_MODE_CONVERGE,
            }:
                self.iterations_mode_var.set(normalized_mode)
        manual_iter_raw = metadata.get("max_iterations_per_seed")
        try:
            if manual_iter_raw not in (None, ""):
                manual_iterations_value = int(float(manual_iter_raw))
                self.manual_iterations_var.set(str(manual_iterations_value))
        except Exception:  # noqa: BLE001
            self.manual_iterations_var.set("")
            manual_iterations_value = None
        if self.summary_tab is not None:
            self.summary_tab.update_iteration_preferences(
                self.iterations_mode_var.get(), manual_iterations_value
            )

        self.summary_tab.update_max_iterations_per_seed(
            metadata.get("max_iterations_per_seed")
        )

        cov_key = metadata.get("cov_type")
        if isinstance(cov_key, str):
            normalized = COVARIANCE_KEY_NORMALIZED.get(cov_key.lower(), cov_key)
            display_value = COVARIANCE_KEY_TO_DISPLAY.get(normalized)
            if display_value:
                self.cov_type_var.set(display_value)

        values_str = metadata.get("settings_values")
        if values_str:
            keys = list(SETTINGS_NUMERIC_KEYS)
            values = values_str.split("#")
            for key, value in zip(keys, values):
                if key not in self.params_vars:
                    continue
                var = self.params_vars[key]
                if value == "":
                    if isinstance(var, tk.StringVar):
                        var.set("")
                    continue
                try:
                    if isinstance(var, tk.StringVar):
                        var.set(value)
                    elif isinstance(var, tk.IntVar):
                        var.set(int(float(value)))
                    else:
                        var.set(float(value))
                except Exception:  # noqa: BLE001
                    pass

        clip_info = metadata.get("clip")
        if isinstance(clip_info, dict):
            enabled = bool(clip_info.get("enabled"))
            self.clip_enabled.set(enabled)
            low_value = clip_info.get("low")
            high_value = clip_info.get("high")
            try:
                if low_value is not None:
                    self.clip_low.set(float(low_value))
                if high_value is not None:
                    self.clip_high.set(float(high_value))
            except Exception:  # noqa: BLE001
                pass
        else:
            self.clip_enabled.set(False)
        self._toggle_clip_entries()

        split_meta = metadata.get("split")
        split_settings = self._split_settings_from_metadata_dict(split_meta)
        self.last_split_settings = split_settings
        mode = split_settings.get("mode", "none")
        self.split_mode.set(mode)
        self._update_split_controls()

        ext_delim_value = ""
        if mode == "random":
            percent = None
            if isinstance(split_meta, dict):
                percent = split_meta.get("test_size_percent")
            if percent is not None:
                try:
                    self.random_test_size.set(float(percent))
                except Exception:  # noqa: BLE001
                    pass
        elif mode == "manual":
            train_text = ""
            test_text = ""
            if isinstance(split_meta, dict):
                train_text = self._ids_to_text(split_meta.get("train_ids"))
                test_text = self._ids_to_text(split_meta.get("test_ids"))
            self.manual_train_ids.set(train_text)
            self.manual_test_ids.set(test_text)

        sanitized_metadata = dict(metadata)
        sanitized_metadata.pop("settings_keys", None)
        sanitized_metadata.pop("avg_iterations_per_seed", None)
        sanitized_metadata.pop("validation_csv", None)
        sanitized_metadata.pop("validation_delimiter", None)
        models_found_value = self._safe_int(metadata.get("models_found"))
        if models_found_value is not None:
            sanitized_metadata["models_found"] = models_found_value
        sanitized_metadata["delimiter"] = (
            self._serialize_delimiter_value(delimiter_value)
            if delimiter_value
            else ""
        )
        if isinstance(split_meta, dict):
            sanitized_split = dict(split_meta)
            if sanitized_split.get("mode") == "external":
                sanitized_split["external_delimiter"] = (
                    self._serialize_delimiter_value(ext_delim_value)
                    if ext_delim_value
                    else ""
                )
            sanitized_metadata["split"] = sanitized_split

        kfold_info = self._apply_kfold_metadata(metadata.get("kfold"))
        sanitized_metadata["kfold"] = kfold_info

        self.last_results_metadata = sanitized_metadata

        search_minutes = self._safe_float(
            sanitized_metadata.get("cpu_time_search_minutes")
        )
        if search_minutes is None:
            search_seconds = self._safe_float(
                sanitized_metadata.get("cpu_time_search_seconds")
            )
            if search_seconds is not None:
                search_minutes = search_seconds / 60.0

        total_minutes = self._safe_float(sanitized_metadata.get("cpu_time_total_minutes"))
        if total_minutes is None:
            total_seconds = self._safe_float(sanitized_metadata.get("cpu_time_total_seconds"))
            if total_seconds is not None:
                total_minutes = total_seconds / 60.0
        self.last_cpu_time_search_minutes = search_minutes if search_minutes is not None else None
        self.last_cpu_time_total_minutes = total_minutes if total_minutes is not None else None
        self.summary_tab.update_cpu_times(
            self.last_cpu_time_search_minutes, self.last_cpu_time_total_minutes
        )

    def _export_results_to_csv(self) -> Optional[Path]:
        source_df: Optional[pd.DataFrame] = self.full_results_df
        if source_df is None or source_df.empty:
            source_df = self.last_results_df

        if source_df is None or source_df.empty:
            return None

        export_path = self.results_export_path or Path("models.csv")

        columns = [
            "Model",
            "Predictors",
            "N_pred",
            "R2",
            "RMSE",
            "s",
            "MAE",
            "R2_adj",
            "VIF_max",
            "VIF_avg",
            "R2_loo",
            "RMSE_loo",
            "s_loo",
            "MAE_loo",
            "R2_kfold",
            "RMSE_kfold",
            "s_kfold",
            "MAE_kfold",
        "Q2F1_ext",
        "Q2F2_ext",
        "Q2F3_ext",
            "RMSE_ext",
            "s_ext",
            "MAE_ext",
        ]

        export_limit = self._get_export_limit()

        sorted_source = self._sort_training_dataframe(source_df)
        limited_source = sorted_source.head(export_limit).copy()

        rows: list[dict] = []
        export_model_ids: list[int] = []

        internal_map: dict[int, dict] = {}
        for row in self.last_internal_results:
            model_value = self._safe_int(row.get("Model"))
            if model_value is None:
                continue
            internal_map[model_value] = row

        external_map: dict[int, dict] = {}
        for row in self.last_external_results:
            model_value = self._safe_int(row.get("Model"))
            if model_value is None:
                continue
            external_map[model_value] = row

        for _, row in limited_source.iterrows():
            model_value = self._safe_int(row.get("Model"))
            if model_value is None:
                continue
            export_model_ids.append(model_value)
            variables_list = self._normalize_variables(row.get("Variables"))
            record: dict[str, object] = {
                "Model": model_value,
                "Predictors": ", ".join(variables_list) if variables_list else "-",
                "N_pred": len(variables_list),
                "R2": self._safe_float(row.get("R2")),
                "RMSE": self._safe_float(row.get("RMSE")),
                "s": self._safe_float(row.get("s")),
                "MAE": self._safe_float(row.get("MAE")),
                "R2_adj": self._safe_float(row.get("R2_adj")),
                "VIF_max": self._safe_float(row.get("VIF_max")),
                "VIF_avg": self._safe_float(row.get("VIF_avg")),
                "R2_loo": self._safe_float(row.get("R2_loo")),
                "RMSE_loo": self._safe_float(row.get("RMSE_loo")),
                "s_loo": self._safe_float(row.get("s_loo")),
                "MAE_loo": self._safe_float(row.get("MAE_loo")),
                "R2_kfold": self._safe_float(row.get("R2_kfold")),
                "RMSE_kfold": self._safe_float(row.get("RMSE_kfold")),
                "s_kfold": self._safe_float(row.get("s_kfold")),
                "MAE_kfold": self._safe_float(row.get("MAE_kfold")),
                "Q2F1_ext": self._safe_float(row.get("Q2F1_ext")),
                "Q2F2_ext": self._safe_float(row.get("Q2F2_ext")),
                "Q2F3_ext": self._safe_float(row.get("Q2F3_ext")),
                "RMSE_ext": self._safe_float(row.get("RMSE_ext")),
                "s_ext": self._safe_float(row.get("s_ext")),
                "MAE_ext": self._safe_float(row.get("MAE_ext")),
            }

            internal_row = internal_map.get(model_value)
            if internal_row:
                record.update(
                    {
                        "R2_loo": self._safe_float(internal_row.get("R2_loo")),
                        "RMSE_loo": self._safe_float(internal_row.get("RMSE_loo")),
                        "s_loo": self._safe_float(internal_row.get("s_loo")),
                        "MAE_loo": self._safe_float(internal_row.get("MAE_loo")),
                        "R2_kfold": self._safe_float(internal_row.get("R2_kfold")),
                        "RMSE_kfold": self._safe_float(internal_row.get("RMSE_kfold")),
                        "s_kfold": self._safe_float(internal_row.get("s_kfold")),
                        "MAE_kfold": self._safe_float(internal_row.get("MAE_kfold")),
                    }
                )

            external_row = external_map.get(model_value)
            if external_row:
                record.update(
                    {
                        "Q2F1_ext": self._safe_float(external_row.get("Q2F1_ext")),
                        "Q2F2_ext": self._safe_float(external_row.get("Q2F2_ext")),
                        "Q2F3_ext": self._safe_float(external_row.get("Q2F3_ext")),
                        "RMSE_ext": self._safe_float(external_row.get("RMSE_ext")),
                        "s_ext": self._safe_float(external_row.get("s_ext")),
                        "MAE_ext": self._safe_float(external_row.get("MAE_ext")),
                    }
                )

            rows.append(record)

        export_df = pd.DataFrame(rows)
        if export_df.columns.duplicated().any():
            export_df = export_df.loc[:, ~export_df.columns.duplicated(keep="last")]
        for column in columns:
            if column not in export_df.columns:
                export_df[column] = pd.NA
        export_df = export_df[columns]

        metadata = self._collect_metadata_for_export()
        metadata.setdefault("version", METADATA_VERSION)
        self.last_results_metadata = dict(metadata)

        try:
            _write_results_csv(export_df, metadata, export_path, sep=";", float_format="%.4f")
        except PermissionError:
            messagebox.showerror(
                "Results file in use",
                "Unable to save the analysis results because the destination file "
                f"'{export_path}' is being used by another application.",
            )
            return None
        except OSError as exc:
            messagebox.showerror(
                "Results file",
                f"Unable to save the analysis results:\n{exc}",
            )
            return None
        return export_path

    @staticmethod
    def _normalize_variables(value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            cleaned = value.strip()
            if not cleaned or cleaned == "-":
                return []
            parts = [part.strip() for part in cleaned.split(",")]
            return [part for part in parts if part]
        if isinstance(value, (list, tuple, set)):
            return [str(item) for item in value]
        if isinstance(value, pd.Series):
            return [str(item) for item in value.tolist()]
        try:
            if pd.isna(value):
                return []
        except TypeError:
            pass
        return [str(value)]

    @staticmethod
    def _variables_to_text(value: object) -> str:
        items = MLRXApp._normalize_variables(value)
        return ", ".join(items) if items else "-"

    @staticmethod
    def _safe_float(value: object) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: object) -> Optional[int]:
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    def _gather_split_settings(self) -> dict:
        mode = self.split_mode.get()
        if mode == "none":
            return {"mode": "none"}

        if mode == "random":
            try:
                test_size_percent = float(self.random_test_size.get())
            except (tk.TclError, ValueError) as exc:  # noqa: F841
                raise ValueError("Random split: please enter a numeric test size between 0 and 100.")
            if not 0.0 < test_size_percent < 100.0:
                raise ValueError("Random split: test size must be between 0 and 100.")
            test_size = test_size_percent / 100.0
            return {"mode": "random", "test_size": test_size}

        if mode == "manual":
            try:
                train_ids = _parse_id_entries(self.manual_train_ids.get())
                test_ids = _parse_id_entries(self.manual_test_ids.get())
            except ValueError as exc:  # noqa: BLE001
                raise ValueError(f"Manual split: {exc}") from exc
            return {"mode": "manual", "train_ids": train_ids, "test_ids": test_ids}

        raise ValueError("Unknown split mode selected.")

    def _preview_data(self):
        path = self.data_path_var.get()
        try:
            delimiter = self._get_delimiter(self.delimiter_var.get())
            split_settings = self._gather_split_settings()
            exclude_constant, constant_threshold = self._get_constant_filter()
            context = load_dataset(
                path,
                delimiter=delimiter,
                split=split_settings,
                dependent_choice=self._get_dependent_choice(),
                non_variable_spec=self._get_non_variable_spec(),
                exclude_constant=exclude_constant,
                constant_threshold=constant_threshold,
                excluded_observations=self._get_excluded_observations_text(),
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Dataset error", f"Unable to load dataset:\n{exc}")
            return

        testing_info = (
            f"Testing rows: {len(context.test_df):,}\n"
            if context.test_df is not None
            else "Testing rows: not configured\n"
        )
        info = (
            f"Total observations: {len(context.df_full):,}\n"
            f"Training rows: {len(context.train_df):,}\n"
            f"{testing_info}"
            f"Predictor columns: {len(context.cols):,}\n"
            f"Response column: {context.target_column}\n"
            f"First predictors: {', '.join(context.cols[:5]) + ('...' if len(context.cols) > 5 else '')}"
        )
        messagebox.showinfo("Dataset preview", info)

    def _center_dialog(self, dialog: tk.Toplevel) -> None:
        dialog.update_idletasks()
        self.update_idletasks()
        dlg_width = dialog.winfo_width() or dialog.winfo_reqwidth()
        dlg_height = dialog.winfo_height() or dialog.winfo_reqheight()
        parent_width = self.winfo_width() or self.winfo_reqwidth()
        parent_height = self.winfo_height() or self.winfo_reqheight()
        parent_root_x = self.winfo_rootx()
        parent_root_y = self.winfo_rooty()
        x = parent_root_x + max(0, int((parent_width - dlg_width) / 2))
        y = parent_root_y + max(0, int((parent_height - dlg_height) / 2))
        dialog.geometry(f"+{x}+{y}")

    def _show_configuration_warning(self, message: str) -> bool:
        dialog = tk.Toplevel(self)
        dialog.title("Configuration warning")
        dialog.transient(self)
        dialog.grab_set()

        result = {"continue": False}

        def _continue() -> None:
            result["continue"] = True
            dialog.destroy()

        def _cancel() -> None:
            dialog.destroy()

        dialog.protocol("WM_DELETE_WINDOW", _cancel)

        content = ttk.Frame(dialog, padding=10)
        content.grid(sticky="nsew")
        dialog.columnconfigure(0, weight=1)
        dialog.rowconfigure(0, weight=1)

        message_label = ttk.Label(content, text=message, justify="left", wraplength=420)
        message_label.grid(row=0, column=0, columnspan=2, sticky="w")

        button_frame = ttk.Frame(content)
        button_frame.grid(row=1, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(button_frame, text="Cancel analysis", command=_cancel).grid(
            row=0, column=0, padx=(0, 6)
        )
        ttk.Button(button_frame, text="Continue", command=_continue).grid(
            row=0, column=1
        )

        self._center_dialog(dialog)
        self.wait_window(dialog)
        return result["continue"]

    def _prompt_output_destination(self) -> Optional[Path]:
        dialog = tk.Toplevel(self)
        dialog.title("Select results destination")
        dialog.transient(self)
        dialog.grab_set()

        selection_var = tk.StringVar(value="default")
        default_path = Path("models.csv")
        existing = self.results_export_path if getattr(self, "results_export_path", None) else default_path
        path_var = tk.StringVar(value=str(existing))

        def _update_state(*_args):
            state = "normal" if selection_var.get() == "custom" else "disabled"
            entry.configure(state=state)
            browse_btn.configure(state=state)

        def _browse():
            initial = path_var.get().strip() or str(default_path)
            chosen = filedialog.asksaveasfilename(
                parent=dialog,
                title="Select results file",
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
                initialfile=os.path.basename(initial),
                initialdir=os.path.dirname(initial) if os.path.dirname(initial) else os.getcwd(),
            )
            if chosen:
                path_var.set(chosen)

        result: dict[str, Optional[Path]] = {"path": None}

        def _confirm():
            choice = selection_var.get()
            if choice == "custom":
                text = path_var.get().strip()
                if not text:
                    messagebox.showerror(
                        "Results destination",
                        "Please specify a file path for the results.",
                        parent=dialog,
                    )
                    return
                result["path"] = Path(text)
            else:
                result["path"] = default_path
            dialog.destroy()

        def _cancel():
            result["path"] = None
            dialog.destroy()

        dialog.protocol("WM_DELETE_WINDOW", _cancel)

        content = ttk.Frame(dialog, padding=10)
        content.grid(sticky="nsew")
        dialog.columnconfigure(0, weight=1)
        dialog.rowconfigure(0, weight=1)

        default_btn = ttk.Radiobutton(
            content,
            text="Save to models.csv (default)",
            variable=selection_var,
            value="default",
            command=_update_state,
        )
        default_btn.grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 5))

        custom_btn = ttk.Radiobutton(
            content,
            text="Save to a custom file",
            variable=selection_var,
            value="custom",
            command=_update_state,
        )
        custom_btn.grid(row=1, column=0, columnspan=3, sticky="w")

        entry = ttk.Entry(content, textvariable=path_var, width=40, state="disabled")
        entry.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(5, 5))
        browse_btn = ttk.Button(content, text="Browse", command=_browse, state="disabled")
        browse_btn.grid(row=2, column=2, sticky="ew", padx=(5, 0), pady=(5, 5))

        button_frame = ttk.Frame(content)
        button_frame.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(10, 0))
        button_frame.columnconfigure(0, weight=1)

        ttk.Button(button_frame, text="Cancel", command=_cancel).grid(row=0, column=0, sticky="e", padx=(0, 5))
        ttk.Button(button_frame, text="OK", command=_confirm).grid(row=0, column=1, sticky="e")

        _update_state()
        entry.focus_set()
        self._center_dialog(dialog)
        self.wait_window(dialog)
        return result["path"]

    def _start_analysis(self):
        if self.worker_thread and self.worker_thread.is_alive():
            messagebox.showinfo("In progress", "An analysis is already running.")
            return

        destination = self._prompt_output_destination()
        if destination is None:
            return

        self._set_holdout_ready(False)
        try:
            config = self._build_config()
            split_settings = self._gather_split_settings()
            context = load_dataset(
                config.data_path,
                delimiter=config.delimiter,
                split=split_settings,
                dependent_choice=config.dependent_choice,
                non_variable_spec=config.non_variable_spec,
                exclude_constant=config.exclude_constant,
                constant_threshold=config.constant_threshold,
                excluded_observations=config.excluded_observations,
            )
            self._set_holdout_ready(self._context_has_holdout(context))
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Configuration error", str(exc))
            return

        total_combos = _compute_combination_total(len(context.cols), config.max_vars)
        threshold = _combination_efficiency_threshold(config.max_vars)
        if config.method == "eprs" and total_combos <= threshold:
            proceed = self._show_configuration_warning(
                "The current configuration is outside the recommended EPR-S thresholds. "
                "The analysis will proceed, but it is recommended to use the 'All subsets' "
                "method for improved efficiency."
            )
            if not proceed:
                return
        if config.method == "all_subsets" and total_combos > threshold:
            proceed = self._show_configuration_warning(
                "This configuration may require substantial computation time. "
                "It is recommended to switch to the EPR-S method for better efficiency."
            )
            if not proceed:
                return

        output_path = Path(destination)
        self.results_export_path = output_path
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:  # noqa: BLE001
            pass

        try:
            with open(output_path, "a", encoding="utf-8", newline=""):
                pass
        except PermissionError:
            messagebox.showerror(
                "Results file in use",
                "Unable to write the analysis results because the destination file "
                f"'{output_path}' is being used by another application.",
            )
            return
        except OSError as exc:
            messagebox.showerror(
                "Results file",
                f"Unable to prepare the destination file:\n{exc}",
            )
            return

        self.last_cpu_time_search_minutes = None
        self.last_cpu_time_total_minutes = None
        self.full_results_df = None
        self.observation_cache.clear()
        self.correlation_cache.clear()

        self._clear_log()
        header_text = self._format_run_header(config, context, split_settings)
        if header_text:
            self._append_log(header_text)
        self._append_log("Analysis initiated with the user-provided configuration.\n")
        self._append_log("Starting analysis...\n")
        self.status_var.set(self._format_running_status(1))
        self.run_button.configure(state="disabled")
        self.cancel_button.configure(state="normal")
        self.stop_event.clear()
        self.notebook.tab(self.validation_tab, state="disabled")
        self.notebook.tab(self.summary_tab, state="disabled")
        self.notebook.tab(self.diagnostics_tab, state="disabled")
        self.notebook.tab(self.visual_tab, state="disabled")
        self.training_tree.delete(*self.training_tree.get_children())
        self.update_internal_results([])
        self.update_external_results([])
        self.validation_tab.prepare_for_new_run()
        self.summary_tab.prepare_for_new_run()
        self.diagnostics_tab.prepare_for_new_run()
        self.visual_tab.prepare_for_new_run()
        self.variable_tab.prepare_for_new_run()
        self.last_results_df = None
        self._training_order = []
        self.full_results_df = None
        self.last_results_metadata = None
        self.last_context = context
        self.last_config = config
        self.last_split_settings = split_settings
        self.summary_tab.update_context(context, config)
        total_iterations = self._estimate_total_iterations(config, context)
        self.total_seeds = total_iterations
        self.processed_seeds = 0
        self.last_progress_percent = None
        self.completed_progress_steps = 0
        self.current_progress_stage = 1
        self.progress_started = False
        self.progress_step_var.set(self._format_progress_steps())
        self._update_progress(0, total_iterations)
        self._set_progress_disabled_appearance(False)

        def worker():
            try:
                runner = run_all_subsets if config.method == "all_subsets" else run_eprs
                result = runner(
                    context,
                    config,
                    progress_callback=lambda msg: self.queue.put(("log", msg)),
                    progress_hook=lambda done, total, stage=1: self.queue.put(
                        ("progress", (done, total, stage))
                    ),
                    stop_event=self.stop_event,
                )
                self.queue.put(("done", result))
            except AnalysisCancelled:
                self.queue.put(("cancelled", None))
            except Exception as exc:  # noqa: BLE001
                self.queue.put(("error", str(exc)))

        self.worker_thread = threading.Thread(target=worker, daemon=True)
        self.worker_thread.start()
        self.after(200, self._process_queue)

    def _process_queue(self):
        while True:
            try:
                event, payload = self.queue.get_nowait()
            except queue.Empty:
                break

            if event == "log":
                self._append_log(payload + "\n")
            elif event == "progress":
                if isinstance(payload, tuple) and len(payload) == 3:
                    done, total, stage = payload
                else:
                    done, total = payload
                    stage = 1
                self.progress_started = True
                self._update_progress(done, total, stage=stage)
            elif event == "done":
                self._handle_results(payload)
            elif event == "error":
                self._handle_error(payload)
            elif event == "cancelled":
                self._handle_cancelled()

        if self.worker_thread and self.worker_thread.is_alive():
            self.after(200, self._process_queue)

    def _handle_results(self, result: dict):
        self.run_button.configure(state="normal")
        self.cancel_button.configure(state="disabled")
        self._update_progress(
            self.total_seeds, self.total_seeds, stage=self.progress_total_steps
        )
        self.status_var.set("Finished")
        self._set_holdout_ready(self._context_has_holdout(self.last_context))

        cpu_search = float(result.get("cpu_time_search", 0.0))
        cpu_total = float(result.get("cpu_time_total", 0.0))
        self.last_cpu_time_search_minutes = (
            cpu_search if np.isfinite(cpu_search) else None
        )
        self.last_cpu_time_total_minutes = cpu_total if np.isfinite(cpu_total) else None
        self.summary_tab.update_cpu_times(
            self.last_cpu_time_search_minutes, self.last_cpu_time_total_minutes
        )
        self.summary_tab.update_max_iterations_per_seed(result.get("max_r2_calls"))
        self.summary_tab.update_avg_iterations_per_seed(result.get("avg_r2_calls"))
        self.summary_tab.update_models_found(result.get("models_found"))
        results_df = result.get("results_df")
        models_reported = len(results_df.index) if results_df is not None else 0
        self.summary_tab.update_models_reported(models_reported)
        self.summary_tab.update_models_explored(result.get("models_explored"))
        threshold_display = "-"
        metric_label = TARGET_METRIC_DISPLAY.get(
            getattr(self.last_config, "target_metric", None), R_SQUARED_SYMBOL
        )
        comparator = ">="
        if self.last_config is not None:
            threshold_value = getattr(self.last_config, "tm_cutoff", None)
            if threshold_value is not None:
                threshold_display = f"{threshold_value:.2f}"
            if getattr(self.last_config, "target_metric", None) == "RMSE_loo":
                comparator = "<="
        summary_lines = [
            "Analysis complete.",
            f"Total models explored: {result.get('models_explored', 0)}",
            f"Models with {metric_label} {comparator} {threshold_display}: {result['models_found']}",
            (
                "Filtrated and reported models: "
                f"{len(results_df.index) if results_df is not None else 0}"
            ),
        ]
        self.full_results_df = results_df
        self.last_results_df = results_df
        if self.results_export_path is None:
            self.results_export_path = Path("models.csv")

        export_path: Optional[Path] = None
        if results_df is not None and not results_df.empty:
            self._load_training_results(results_df)
            holdout_results = self._derive_holdout_results(self.last_results_df)
            self.validation_tab.update_sources(results_df, self.last_context, self.last_config)
            if self.validation_tab.available:
                self.notebook.tab(self.validation_tab, state="normal")
            else:
                self.notebook.tab(self.validation_tab, state="disabled")
            self.notebook.tab(self.diagnostics_tab, state="normal")
            self.notebook.tab(self.visual_tab, state="normal")
            if holdout_results:
                self.update_external_results(holdout_results)
            self.notebook.select(self.results_tab)
            if self.last_config is not None:
                metadata_seed = self._build_metadata_dict(
                    self.last_config,
                    self.last_split_settings,
                    result.get("models_found"),
                    models_reported,
                    result.get("models_explored"),
                    avg_iterations_per_seed=result.get("avg_r2_calls"),
                    max_iterations_per_seed=result.get("max_r2_calls"),
                )
                self.last_results_metadata = metadata_seed
            export_path = self._export_results_to_csv()
            if export_path:
                summary_lines.append(f"Results file: {export_path}")
                self.results_load_path.set(str(export_path))
        else:
            summary_lines.append("No models met the reporting threshold.")
            self._update_training_order(None)
            self.training_tree.delete(*self.training_tree.get_children())
            self.update_internal_results([])
            self.validation_tab.prepare_for_new_run()
            self.notebook.tab(self.validation_tab, state="disabled")
            self.summary_tab.prepare_for_new_run()
            self.notebook.tab(self.summary_tab, state="disabled")
            self.diagnostics_tab.prepare_for_new_run()
            self.notebook.tab(self.diagnostics_tab, state="disabled")
            self.visual_tab.prepare_for_new_run()
            self.variable_tab.prepare_for_new_run()
            self.notebook.tab(self.visual_tab, state="disabled")
            self.full_results_df = None
            messagebox.showinfo(
                "Model search",
                "No models were generated for the selected configuration.",
            )

        cpu_total_seconds = cpu_total * 60.0
        cpu_search_seconds = cpu_search * 60.0
        cpu_vif = float(result.get("cpu_time_vif", max(cpu_total - cpu_search, 0.0)))
        cpu_vif_seconds = cpu_vif * 60.0

        summary_lines.append("")
        summary_lines.append(
            "Model search CPU time: "
            f"{cpu_search_seconds:.2f} s ({cpu_search:.2f} min)."
        )
        if cpu_vif > 0:
            summary_lines.append(
                "Post-processing CPU time: "
                f"{cpu_vif_seconds:.2f} s ({cpu_vif:.2f} min)."
            )
        summary_lines.append(
            "Total CPU time: "
            f"{cpu_total_seconds:.2f} s ({cpu_total:.2f} min)."
        )

        self._append_log("\n".join(summary_lines) + "\n")
        self._mark_progress_step_complete(self.progress_total_steps)
        self.progress_step_var.set(self._format_progress_steps())
        self.last_progress_percent = None

    def _handle_error(self, message: str):
        self.status_var.set("Error")
        self.run_button.configure(state="normal")
        self.cancel_button.configure(state="disabled")
        self.validation_tab.prepare_for_new_run()
        self.notebook.tab(self.validation_tab, state="disabled")
        self.summary_tab.prepare_for_new_run()
        self.notebook.tab(self.summary_tab, state="disabled")
        self.diagnostics_tab.prepare_for_new_run()
        self.notebook.tab(self.diagnostics_tab, state="disabled")
        self.visual_tab.prepare_for_new_run()
        self.variable_tab.prepare_for_new_run()
        self.notebook.tab(self.visual_tab, state="disabled")
        self.update_internal_results([])
        self.full_results_df = None
        self._update_progress(self.processed_seeds, self.total_seeds)
        self._append_log(f"Error: {message}\n")
        self.last_progress_percent = None
        messagebox.showerror("Execution error", message)

    def _handle_cancelled(self):
        self.status_var.set("Cancelled")
        self.run_button.configure(state="normal")
        self.cancel_button.configure(state="disabled")
        self.validation_tab.prepare_for_new_run()
        self.notebook.tab(self.validation_tab, state="disabled")
        self.summary_tab.prepare_for_new_run()
        self.notebook.tab(self.summary_tab, state="disabled")
        self.diagnostics_tab.prepare_for_new_run()
        self.notebook.tab(self.diagnostics_tab, state="disabled")
        self.visual_tab.prepare_for_new_run()
        self.variable_tab.prepare_for_new_run()
        self.notebook.tab(self.visual_tab, state="disabled")
        self.training_tree.delete(*self.training_tree.get_children())
        self._update_training_order(None)
        self.update_internal_results([])
        self.update_external_results([])
        self.full_results_df = None
        self._update_progress(self.processed_seeds, self.total_seeds)
        self._append_log("Analysis cancelled by user.\n")
        self.last_progress_percent = None
        self._set_progress_disabled_appearance(True)

    def _get_export_limit(self) -> int:
        limit: Optional[int] = None

        if hasattr(self, "params_vars") and "export_limit" in self.params_vars:
            try:
                limit = int(self.params_vars["export_limit"].get())
            except Exception:  # noqa: BLE001
                limit = None

        if (limit is None or limit <= 0) and self.last_config is not None:
            limit = int(self.last_config.export_limit)

        if limit is None or limit <= 0:
            limit = DEFAULT_EXPORT_LIMIT

        return limit

    def _load_training_results(self, df: pd.DataFrame):
        if df is None or df.empty:
            self.last_results_df = df
            self.training_tree.delete(*self.training_tree.get_children())
            self._update_training_order(df)
            if hasattr(self, "summary_tab"):
                self.summary_tab.update_training_results(pd.DataFrame())
                self.summary_tab.prepare_for_new_run()
                self.notebook.tab(self.summary_tab, state="disabled")
            return

        working_df = df.copy()
        sorted_df = self._sort_training_dataframe(working_df)
        limit = self._get_export_limit()
        limited_df = sorted_df.head(limit).copy()
        self.last_results_df = limited_df
        self._update_training_order(limited_df)
        self._populate_observation_cache(limited_df)
        self._render_training_tree(limited_df)
        if hasattr(self, "visual_tab"):
            self.visual_tab.update_training_results(limited_df)
        if hasattr(self, "variable_tab"):
            self.variable_tab.update_results(limited_df)
        if hasattr(self, "diagnostics_tab"):
            self.diagnostics_tab.update_training_results(limited_df)
        if hasattr(self, "summary_tab"):
            self.summary_tab.update_training_results(limited_df)
            self.summary_tab.update_context(self.last_context, self.last_config)
            self.notebook.tab(self.summary_tab, state="normal")

    def _populate_observation_cache(self, df: Optional[pd.DataFrame]):
        if df is None or df.empty:
            self.observation_cache = {}
            self.correlation_cache = {}
            return

        if self.last_context is None or self.last_config is None:
            return

        self.observation_cache = {}
        self.correlation_cache = {}

        first_row = df.iloc[0]
        model_value = self._safe_int(first_row.get("Model"))
        if model_value is None:
            return

        variables = self._normalize_variables(first_row.get("Variables"))
        if not variables:
            return

        diagnostics_df, hat_threshold = compute_observation_diagnostics(
            self.last_context,
            self.last_config,
            variables,
        )
        if diagnostics_df.empty:
            return

        diagnostics_copy = diagnostics_df.copy()
        self.observation_cache[model_value] = (diagnostics_copy, hat_threshold)

        corr_df = self._compute_correlation_matrix(variables)
        if corr_df is not None and not corr_df.empty:
            self.correlation_cache[(model_value, self._CORR_CACHE_PREDICTORS)] = corr_df.copy()

    def register_external_holdout_dataset(self, dataset: pd.DataFrame):
        context = self.last_context
        if context is None:
            return

        target_col = context.target_column

        normalized = dataset.copy()
        normalized.columns = [
            col.strip() if isinstance(col, str) else col for col in normalized.columns
        ]

        if target_col not in normalized.columns or normalized.empty:
            context.external_df = None
            context.external_X_np = None
            context.external_y_np = None
            self._set_holdout_ready(self._context_has_holdout(context))
            return

        normalized = normalized.reset_index(drop=True)
        observation_column = context.observation_column
        id_column = context.id_column

        if observation_column in normalized.columns:
            normalized[observation_column] = normalized[observation_column].reset_index(
                drop=True
            )
        else:
            start_value = 1
            train_df = context.train_df
            if train_df is not None and observation_column in train_df.columns:
                try:
                    existing = pd.to_numeric(
                        train_df[observation_column], errors="coerce"
                    )
                    max_existing = existing.max()
                    if pd.notna(max_existing):
                        start_value = int(float(max_existing)) + 1
                    else:
                        start_value = len(train_df) + 1
                except Exception:  # noqa: BLE001
                    start_value = len(train_df) + 1
            normalized.insert(
                0, observation_column, range(start_value, start_value + len(normalized))
            )

        if id_column != observation_column:
            if id_column in normalized.columns:
                normalized[id_column] = normalized[id_column].reset_index(drop=True)
            else:
                normalized.insert(1, id_column, range(1, len(normalized) + 1))

        context.external_df = normalized
        context.external_X_np = None
        context.external_y_np = None

        self._set_holdout_ready(self._context_has_holdout(context))

        self.observation_cache.clear()
        self.correlation_cache.clear()

        if (
            hasattr(self, "diagnostics_tab")
            and self.diagnostics_tab.available
            and self.diagnostics_tab.current_model_id is not None
        ):
            self.diagnostics_tab._handle_model_change()

        if (
            hasattr(self, "visual_tab")
            and self.visual_tab.available
            and self.visual_tab.current_model_id is not None
        ):
            self.visual_tab._handle_model_change()

    def ensure_holdout_data_available(self, show_alert: bool = True) -> bool:
        try:
            _config, context = self.ensure_testing_context(allow_defaults=True)
        except Exception as exc:  # noqa: BLE001
            if show_alert:
                messagebox.showerror("Testing data", f"Unable to prepare dataset:\n{exc}")
            self._set_holdout_ready(False)
            return False

        if self._context_has_holdout(context):
            self._set_holdout_ready(True)
            return True

        path = self.external_test_path.get().strip()
        if not path:
            if show_alert:
                messagebox.showwarning(
                    "Testing data",
                    "Testing CSV is not configured.",
                )
            self._set_holdout_ready(False)
            return False

        try:
            delimiter = self._get_delimiter(self.external_delimiter_var.get())
        except ValueError as exc:  # noqa: BLE001
            if show_alert:
                messagebox.showwarning("Testing data", str(exc))
            self._set_holdout_ready(False)
            return False

        try:
            ext_df = pd.read_csv(path, delimiter=delimiter)
        except Exception as exc:  # noqa: BLE001
            if show_alert:
                messagebox.showwarning(
                    "Testing data",
                    f"Unable to read testing CSV:\n{exc}",
                )
            self._set_holdout_ready(False)
            return False

        ext_df.columns = [col.strip() if isinstance(col, str) else col for col in ext_df.columns]

        target_col = context.target_column
        if target_col not in ext_df.columns or ext_df.empty:
            if show_alert:
                messagebox.showwarning(
                    "Testing data",
                    "Testing CSV must include the dependent column and at least one row.",
                )
            self._set_holdout_ready(False)
            return False

        self.register_external_holdout_dataset(ext_df)

        if not self._context_has_holdout(self.last_context):
            if show_alert:
                messagebox.showwarning(
                    "Testing data",
                    "Testing dataset could not be prepared for analysis.",
                )
            self._set_holdout_ready(False)
            return False

        self._set_holdout_ready(True)
        return True

    def get_top_training_model_ids(self) -> list[int]:
        if self.last_results_df is None or self.last_results_df.empty:
            return []

        top_ids: list[int] = []
        limit = self._get_export_limit()
        for value in self.last_results_df["Model"].tolist():
            model_id = self._safe_int(value)
            if model_id is None:
                continue
            top_ids.append(model_id)
            if len(top_ids) >= limit:
                break

        return top_ids

    def get_observation_diagnostics(
        self, model_id: int
    ) -> tuple[Optional[pd.DataFrame], float]:
        cached = self.observation_cache.get(model_id)
        if cached:
            return cached

        if (
            self.last_context is None
            or self.last_config is None
            or self.last_results_df is None
            or self.last_results_df.empty
        ):
            return None, float("nan")

        row = self.last_results_df[self.last_results_df["Model"] == model_id]
        if row.empty:
            return None, float("nan")

        variables = self._normalize_variables(row.iloc[0].get("Variables"))
        if not variables:
            return None, float("nan")

        diagnostics_df, hat_threshold = compute_observation_diagnostics(
            self.last_context,
            self.last_config,
            variables,
        )
        if diagnostics_df.empty:
            return None, float("nan")

        diagnostics_copy = diagnostics_df.copy()
        self.observation_cache[model_id] = (diagnostics_copy, hat_threshold)
        return diagnostics_copy, hat_threshold

    def _compute_correlation_matrix(
        self, variables: list[str], include_target: bool = False
    ) -> Optional[pd.DataFrame]:
        if self.last_context is None:
            return None
        if not variables:
            return None
        context = self.last_context
        if include_target:
            target_col = context.target_column
            columns = list(dict.fromkeys([*variables, target_col]))
            source_df = context.train_df
        else:
            columns = list(variables)
            source_df = context.X
        try:
            subset = source_df.loc[:, columns]
        except KeyError:
            return None
        if subset.empty:
            return None
        try:
            corr = subset.astype(float, copy=False).corr()
        except Exception:  # noqa: BLE001
            return None
        if corr.empty:
            return None
        corr = corr.reindex(index=columns, columns=columns)
        return corr

    def get_model_correlation(
        self, model_id: int, include_target: bool = False
    ) -> Optional[pd.DataFrame]:
        cache_key = (
            model_id,
            self._CORR_CACHE_WITH_TARGET if include_target else self._CORR_CACHE_PREDICTORS,
        )
        cached = self.correlation_cache.get(cache_key)
        if cached is not None:
            return cached.copy()

        if self.last_results_df is None or self.last_results_df.empty:
            return None

        row = self.last_results_df[self.last_results_df["Model"] == model_id]
        if row.empty:
            return None

        variables = self._normalize_variables(row.iloc[0].get("Variables"))
        if not variables:
            return None

        corr = self._compute_correlation_matrix(variables, include_target=include_target)
        if corr is None or corr.empty:
            return None

        corr_copy = corr.copy()
        self.correlation_cache[cache_key] = corr_copy
        return corr_copy

    def _derive_holdout_results(self, df: Optional[pd.DataFrame]) -> list[dict]:
        if df is None or df.empty:
            return []

        context = self.last_context
        if context is None:
            return []

        if not self._context_has_holdout(context):
            return []

        config = self.last_config
        results: list[dict] = []
        for _, row in df.iterrows():
            model_value = self._safe_int(row.get("Model"))
            if model_value is None:
                continue
            variables = self._normalize_variables(row.get("Variables"))
            if not variables:
                continue
            holdout_metrics = _compute_holdout_metrics(context, variables, config)
            if not holdout_metrics:
                continue
            results.append(
                {
                    "Model": model_value,
                    "Variables": variables,
                    "N_var": len(variables),
                    **holdout_metrics,
                }
            )

        return results

    def update_internal_results(self, results: list[dict], *, remember_source: bool = True):
        results = results or []
        if self.last_results_df is not None and not self.last_results_df.empty:
            valid_models = set(self.get_top_training_model_ids())
            if valid_models:
                filtered: list[dict] = []
                for row in results:
                    model_value = self._safe_int(row.get("Model"))
                    if model_value is None or model_value not in valid_models:
                        continue
                    normalized = dict(row)
                    normalized["Model"] = model_value
                    filtered.append(normalized)
                results = filtered

        if remember_source:
            self.full_internal_results = [dict(row) for row in results]

        sorted_internal = self._sort_internal_results(results)
        self.last_internal_results = sorted_internal
        self._render_internal_tree(sorted_internal)
        if hasattr(self, "summary_tab"):
            self.summary_tab.update_internal_results(sorted_internal)

        if self.last_results_df is not None and not self.last_results_df.empty:
            self._export_results_to_csv()

        if sorted_internal and not self._results_tab_suppression:
            self.notebook.select(self.results_tab)

    def update_external_results(self, results: list[dict], *, remember_source: bool = True):
        results = results or []
        if self.last_results_df is not None and not self.last_results_df.empty:
            valid_models = set(self.get_top_training_model_ids())
            if valid_models:
                filtered: list[dict] = []
                for row in results:
                    model_value = self._safe_int(row.get("Model"))
                    if model_value is None or model_value not in valid_models:
                        continue
                    normalized = dict(row)
                    normalized["Model"] = model_value
                    filtered.append(normalized)
                results = filtered

        if remember_source:
            self.full_external_results = [dict(row) for row in results]

        sorted_external = self._sort_external_results(results)
        self.last_external_results = sorted_external
        self._render_external_tree(sorted_external)
        if hasattr(self, "summary_tab"):
            self.summary_tab.update_external_results(sorted_external)

        if self.last_results_df is not None and not self.last_results_df.empty:
            self._export_results_to_csv()

        if sorted_external and not self._results_tab_suppression:
            self.notebook.select(self.results_tab)

    def _handle_sort_change(self, event=None):  # noqa: D401 - Tkinter callback
        """Refresh the results tables whenever the sort selection changes."""

        self._resort_results_tables()
        if self.last_results_df is not None and not self.last_results_df.empty:
            self._export_results_to_csv()

    def _resort_results_tables(self):
        if self.last_results_df is not None and not self.last_results_df.empty:
            sorted_df = self._sort_training_dataframe(self.last_results_df)
            self.last_results_df = sorted_df
            self._update_training_order(sorted_df)
            self._render_training_tree(sorted_df)
            if hasattr(self, "visual_tab"):
                self.visual_tab.update_training_results(sorted_df)
            if hasattr(self, "variable_tab"):
                self.variable_tab.update_results(sorted_df)
            if hasattr(self, "diagnostics_tab"):
                self.diagnostics_tab.update_training_results(sorted_df)
            if hasattr(self, "summary_tab"):
                self.summary_tab.update_training_results(sorted_df)
        else:
            self.training_tree.delete(*self.training_tree.get_children())
            self._update_training_order(None)
            if hasattr(self, "visual_tab"):
                self.visual_tab.update_training_results(pd.DataFrame())
            if hasattr(self, "variable_tab"):
                self.variable_tab.update_results(pd.DataFrame())
            if hasattr(self, "diagnostics_tab"):
                self.diagnostics_tab.update_training_results(pd.DataFrame())
            if hasattr(self, "summary_tab"):
                self.summary_tab.update_training_results(pd.DataFrame())

        if self.last_internal_results:
            sorted_internal = self._sort_internal_results(self.last_internal_results)
            self.last_internal_results = sorted_internal
            self._render_internal_tree(sorted_internal)
            if hasattr(self, "summary_tab"):
                self.summary_tab.update_internal_results(sorted_internal)
        else:
            self.internal_results_tree.delete(*self.internal_results_tree.get_children())

        if self.last_external_results:
            sorted_external = self._sort_external_results(self.last_external_results)
            self.last_external_results = sorted_external
            self._render_external_tree(sorted_external)
            if hasattr(self, "summary_tab"):
                self.summary_tab.update_external_results(sorted_external)
        else:
            self.external_results_tree.delete(*self.external_results_tree.get_children())

    def _sort_training_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        metric_label = self.results_sort_var.get()
        metric = RESULTS_SORT_DISPLAY_TO_KEY.get(metric_label) or RESULTS_SORT_LEGACY_DISPLAY_TO_KEY.get(metric_label, "R2")
        ascending_metrics = {"RMSE", "s", "MAE", "N var", "VIFmax", "VIFavg"}

        column_map = {
            "R2": "R2",
            "adj-R2": "R2_adj",
            "R2 adj": "R2_adj",
            "RMSE": "RMSE",
            "s": "s",
            "MAE": "MAE",
            "N var": "N_var",
            "VIFmax": "VIF_max",
            "VIFavg": "VIF_avg",
        }
        sort_column = column_map.get(metric, "R2")
        ascending = metric in ascending_metrics

        sorted_df = df.copy()
        if sort_column == "N_var" and sort_column not in sorted_df.columns:
            sorted_df[sort_column] = [
                len(self._normalize_variables(value)) for value in sorted_df.get("Variables", [])
            ]

        if sort_column not in sorted_df.columns:
            return sorted_df

        sort_values = pd.to_numeric(sorted_df[sort_column], errors="coerce")
        if "Model" in sorted_df.columns:
            model_values = pd.to_numeric(sorted_df["Model"], errors="coerce")
        else:
            model_values = pd.Series(np.nan, index=sorted_df.index)

        sorted_df = sorted_df.assign(_sort_key=sort_values, _model_key=model_values)
        sorted_df = sorted_df.sort_values(
            by=["_sort_key", "_model_key"],
            ascending=[ascending, True],
            na_position="last",
            kind="mergesort",
        )
        sorted_df = sorted_df.drop(columns=["_sort_key", "_model_key"], errors="ignore")
        return sorted_df.reset_index(drop=True)

    def _get_training_metric_map(self, column: str) -> dict[int, Optional[float]]:
        if self.last_results_df is None or column not in self.last_results_df.columns:
            return {}

        metric_map: dict[int, Optional[float]] = {}
        for _, row in self.last_results_df.iterrows():
            model_value = self._safe_int(row.get("Model"))
            if model_value is None:
                continue
            metric_map[model_value] = self._safe_float(row.get(column))

        return metric_map

    def _update_training_order(self, df: Optional[pd.DataFrame]) -> None:
        order: list[int] = []
        if df is not None and not df.empty and "Model" in df.columns:
            for value in df["Model"].tolist():
                model_id = self._safe_int(value)
                if model_id is not None:
                    order.append(model_id)
        self._training_order = order

    def _get_internal_metric_map(self, column: str) -> dict[int, Optional[float]]:
        metric_map: dict[int, Optional[float]] = {}
        for row in getattr(self, "last_internal_results", []):
            model_value = self._safe_int(row.get("Model"))
            if model_value is None:
                continue
            metric_map[model_value] = self._safe_float(row.get(column))
        return metric_map

    def _get_training_order_map(self) -> dict[int, int]:
        if not self._training_order:
            return {}

        return {model_id: idx for idx, model_id in enumerate(self._training_order)}

    def _sort_results_by_training_order(
        self, results: list[dict], fallback_sort: Callable[[list[dict]], list[dict]]
    ) -> list[dict]:
        order_map = self._get_training_order_map()
        if not order_map:
            return fallback_sort(results)

        enumerated_rows = list(enumerate(results))

        def sort_key(item: tuple[int, dict]):
            original_index, row = item
            model_value = self._safe_int(row.get("Model"))
            if model_value is None:
                return (1, float("inf"), original_index)
            order_value = order_map.get(model_value)
            if order_value is None:
                return (1, float("inf"), original_index)
            return (0, order_value, original_index)

        sorted_rows = sorted(enumerated_rows, key=sort_key)
        return [row for _, row in sorted_rows]

    def _render_training_tree(self, df: pd.DataFrame):
        self.training_tree.delete(*self.training_tree.get_children())

        if df is None or df.empty:
            return

        def fmt(value: Optional[float]) -> str:
            if value is None or pd.isna(value):
                return "-"
            return f"{float(value):.4f}"

        limit = self._get_export_limit()
        for _, row in df.head(limit).iterrows():
            variables_iter = self._normalize_variables(row.get("Variables"))
            variables_text = ", ".join(variables_iter) if variables_iter else "-"
            n_vars = len(variables_iter)
            model_value = self._safe_int(row.get("Model"))
            values = [
                "-" if model_value is None else int(model_value),
                variables_text,
                n_vars,
                fmt(row.get("R2")),
                fmt(row.get("RMSE")),
                fmt(row.get("MAE")),
                fmt(row.get("s")),
                fmt(row.get("R2_adj")),
                fmt(row.get("VIF_max")),
                fmt(row.get("VIF_avg")),
            ]
            self.training_tree.insert("", "end", values=values)

    def _sort_internal_results(self, results: list[dict]) -> list[dict]:
        if not results:
            return []

        metric_label = self.results_sort_var.get()
        metric = RESULTS_SORT_DISPLAY_TO_KEY.get(metric_label) or RESULTS_SORT_LEGACY_DISPLAY_TO_KEY.get(metric_label, "R2")
        allowed_metrics = {"R2", "RMSE", "s", "MAE", "N var", "Q2F2", "Q2F1"}
        training_order_metrics = {"VIFmax", "VIFavg"}

        if metric in training_order_metrics:
            return self._sort_results_by_training_order(
                results,
                lambda rows: self._sort_internal_results_by_metric(rows, "R2"),
            )

        if metric not in allowed_metrics:
            metric = "R2"

        return self._sort_internal_results_by_metric(results, metric)

    def _sort_internal_results_by_metric(self, results: list[dict], metric: str) -> list[dict]:
        ascending_metrics = {"RMSE", "s", "MAE", "N var"}
        ascending = metric in ascending_metrics

        def compute_value(row: dict) -> tuple[Optional[float], Optional[float]]:
            if metric == "R2":
                loo_value = self._safe_float(row.get("R2_loo"))
                if loo_value is not None:
                    return loo_value, self._safe_float(row.get("R2_kfold"))
                return self._safe_float(row.get("R2_kfold")), None
            if metric == "RMSE":
                loo_value = self._safe_float(row.get("RMSE_loo"))
                if loo_value is not None:
                    return loo_value, self._safe_float(row.get("RMSE_kfold"))
                return self._safe_float(row.get("RMSE_kfold")), None
            if metric == "s":
                loo_value = self._safe_float(row.get("s_loo"))
                if loo_value is not None:
                    return loo_value, self._safe_float(row.get("s_kfold"))
                return self._safe_float(row.get("s_kfold")), None
            if metric == "MAE":
                loo_value = self._safe_float(row.get("MAE_loo"))
                if loo_value is not None:
                    return loo_value, self._safe_float(row.get("MAE_kfold"))
                return self._safe_float(row.get("MAE_kfold")), None
            if metric == "N var":
                n_var_value = row.get("N_var")
                if n_var_value is None:
                    variables = self._normalize_variables(row.get("Variables"))
                    return float(len(variables)), None
                return float(n_var_value), None
            return None, None

        def sort_key(row: dict):
            primary, secondary = compute_value(row)
            missing_primary = primary is None or (
                isinstance(primary, float) and pd.isna(primary)
            )
            primary_numeric = float(primary) if not missing_primary else 0.0
            sort_primary = primary_numeric if ascending else -primary_numeric

            missing_secondary = secondary is None or (
                isinstance(secondary, float) and pd.isna(secondary)
            )
            secondary_numeric = float(secondary) if not missing_secondary else 0.0
            sort_secondary = secondary_numeric if ascending else -secondary_numeric

            model_value = self._safe_int(row.get("Model"))
            model_key = model_value if model_value is not None else float("inf")
            return (missing_primary, sort_primary, missing_secondary, sort_secondary, model_key)

        return sorted(results, key=sort_key)

    def _render_internal_tree(self, rows: list[dict]):
        self.internal_results_tree.delete(*self.internal_results_tree.get_children())

        if not rows:
            return

        def fmt(value: Optional[float]) -> str:
            if value is None or pd.isna(value):
                return "-"
            return f"{float(value):.4f}"

        limit = self._get_export_limit()
        for row in rows[:limit]:
            variables_iter = self._normalize_variables(row.get("Variables"))
            variables_text = ", ".join(variables_iter) if variables_iter else "-"
            n_vars = len(variables_iter)
            model_value = self._safe_int(row.get("Model"))
            values = [
                "-" if model_value is None else int(model_value),
                variables_text,
                n_vars,
                fmt(row.get("R2_loo")),
                fmt(row.get("RMSE_loo")),
                fmt(row.get("MAE_loo")),
                fmt(row.get("s_loo")),
                fmt(row.get("R2_kfold")),
                fmt(row.get("RMSE_kfold")),
                fmt(row.get("MAE_kfold")),
                fmt(row.get("s_kfold")),
            ]
            self.internal_results_tree.insert("", "end", values=values)

    def _sort_external_results(self, results: list[dict]) -> list[dict]:
        if not results:
            return []

        metric_label = self.results_sort_var.get()
        metric = RESULTS_SORT_DISPLAY_TO_KEY.get(metric_label) or RESULTS_SORT_LEGACY_DISPLAY_TO_KEY.get(metric_label, "R2")
        allowed_metrics = {"R2", "RMSE", "s", "MAE", "N var", "Q2F2", "Q2F1"}
        training_order_metrics = {"VIFmax", "VIFavg"}

        if metric in training_order_metrics:
            return self._sort_results_by_training_order(
                results,
                lambda rows: self._sort_external_results_by_metric(rows, "R2"),
            )

        if metric not in allowed_metrics:
            metric = "R2"

        return self._sort_external_results_by_metric(results, metric)

    def _sort_external_results_by_metric(self, results: list[dict], metric: str) -> list[dict]:
        ascending_metrics = {"RMSE", "s", "MAE", "N var"}
        ascending = metric in ascending_metrics

        def compute_value(row: dict) -> Optional[float]:
            if metric == "R2":
                return self._safe_float(row.get("Q2F3_ext"))
            if metric == "RMSE":
                return self._safe_float(row.get("RMSE_ext"))
            if metric == "s":
                return self._safe_float(row.get("s_ext"))
            if metric == "MAE":
                return self._safe_float(row.get("MAE_ext"))
            if metric == "Q2F2":
                return self._safe_float(row.get("Q2F2_ext"))
            if metric == "Q2F1":
                return self._safe_float(row.get("Q2F1_ext"))
            if metric == "N var":
                n_var_value = row.get("N_var")
                if n_var_value is None:
                    variables = self._normalize_variables(row.get("Variables"))
                    return float(len(variables))
                return float(n_var_value)
            return None

        def sort_key(row: dict):
            value = compute_value(row)
            missing = value is None or (isinstance(value, float) and pd.isna(value))
            numeric_value = float(value) if value is not None else 0.0
            sort_value = numeric_value if ascending else -numeric_value
            model_value = self._safe_int(row.get("Model"))
            model_key = model_value if model_value is not None else float("inf")
            return (missing, sort_value, model_key)

        return sorted(results, key=sort_key)

    def _render_external_tree(self, rows: list[dict]):
        self.external_results_tree.delete(*self.external_results_tree.get_children())

        if not rows:
            return

        def fmt(value: Optional[float]) -> str:
            if value is None or pd.isna(value):
                return "-"
            return f"{float(value):.4f}"

        limit = self._get_export_limit()
        for row in rows[:limit]:
            variables_iter = self._normalize_variables(row.get("Variables"))
            variables_text = ", ".join(variables_iter) if variables_iter else "-"
            n_vars = len(variables_iter)
            model_value = self._safe_int(row.get("Model"))
            values = [
                "-" if model_value is None else int(model_value),
                variables_text,
                n_vars,
                fmt(row.get("Q2F3_ext")),
                fmt(row.get("RMSE_ext")),
                fmt(row.get("MAE_ext")),
                fmt(row.get("Q2F2_ext")),
                fmt(row.get("Q2F1_ext")),
            ]
            self.external_results_tree.insert("", "end", values=values)

    def _format_progress_text(self, percent: int) -> str:
        return f"Progress: {percent}%"

    def _format_running_status(self, stage: int) -> str:
        if stage >= 2:
            return "Running (VIF calculations)"
        method_display = self.method_choice.get()
        method_key = METHOD_DISPLAY_TO_KEY.get(method_display, "")
        if method_key == "all_subsets":
            return "Running (model evaluation)"
        return "Running (model searching)"

    def _format_progress_steps(self, steps: Optional[int] = None) -> str:
        current_steps = self.completed_progress_steps if steps is None else steps
        return f"({current_steps}/{self.progress_total_steps})"

    def _current_progress_step_display(self) -> int:
        if self.progress_started:
            return max(self.completed_progress_steps, self.current_progress_stage)
        return self.completed_progress_steps

    def _configure_progress_styles(self) -> None:
        style = ttk.Style()
        label_fg = style.lookup("TLabel", "foreground") or ""
        disabled_fg = (
            style.lookup("TLabel", "foreground", ("disabled",))
            or style.lookup("TButton", "foreground", ("disabled",))
            or style.lookup("TEntry", "foreground", ("disabled",))
            or "gray50"
        )
        bar_bg = style.lookup("Horizontal.TProgressbar", "background") or ""
        disabled_bar_bg = "#b8b8b8"
        disabled_trough = style.lookup("Horizontal.TProgressbar", "troughcolor") or "#e6e6e6"

        style.configure(self._progress_label_style, foreground=label_fg)
        style.configure(self._progress_label_disabled_style, foreground=disabled_fg)
        style.configure(self._progressbar_style, background=bar_bg)
        style.configure(
            self._progressbar_disabled_style,
            background=disabled_bar_bg,
            troughcolor=disabled_trough,
        )

    def _set_progress_disabled_appearance(self, disabled: bool) -> None:
        if not self.progress_bar or not self.progress_label or not self.progress_stage_label:
            return
        label_style = (
            self._progress_label_disabled_style if disabled else self._progress_label_style
        )
        bar_style = (
            self._progressbar_disabled_style if disabled else self._progressbar_style
        )
        self.progress_stage_label.configure(style=label_style)
        self.progress_label.configure(style=label_style)
        self.progress_bar.configure(style=bar_style)

    def _mark_progress_step_complete(self, step_index: int) -> None:
        step_index = max(0, min(step_index, self.progress_total_steps))
        if step_index > self.completed_progress_steps:
            self.completed_progress_steps = step_index
            self.progress_step_var.set(
                self._format_progress_steps(self._current_progress_step_display())
            )

    def _update_progress(self, processed: int, total: int, *, stage: int = 1):
        total = max(0, int(total))
        processed = max(0, int(processed))
        if total > 0 and processed > total:
            processed = total
        self.processed_seeds = processed
        self.total_seeds = total
        stage = max(1, min(int(stage), self.progress_total_steps))
        if stage != self.current_progress_stage:
            self.status_var.set(self._format_running_status(stage))
            self.current_progress_stage = stage
            self.last_progress_percent = None
        if stage > 1:
            self._mark_progress_step_complete(stage - 1)
        percent = (processed * 100) // total if total else 0
        percent = max(0, min(100, percent))

        if percent == 100:
            self._mark_progress_step_complete(stage)
            if stage >= self.progress_total_steps:
                self.status_var.set("Finished")
            elif self.current_progress_stage == stage:
                next_stage = min(stage + 1, self.progress_total_steps)
                self.status_var.set(self._format_running_status(next_stage))
                self.current_progress_stage = next_stage
                self.last_progress_percent = None
        else:
            self.progress_step_var.set(
                self._format_progress_steps(self._current_progress_step_display())
            )

        if percent == self.last_progress_percent:
            return

        self.progress_var.set(self._format_progress_text(percent))
        if self.progress_bar is not None:
            try:
                self.progress_bar["value"] = percent
            except tk.TclError:
                pass
        self.last_progress_percent = percent

    def _estimate_total_iterations(self, config: EPRSConfig, context: EPRSContext) -> int:
        if config.method == "all_subsets":
            predictor_count = len(context.cols)
            max_size = min(config.max_vars, predictor_count)
            if predictor_count <= 0 or max_size <= 0:
                return 1
            total = _compute_combination_total(predictor_count, max_size)
            return max(int(total), 1)
        return max(int(config.n_seeds), 1)

    def _format_run_header(
        self,
        config: EPRSConfig,
        context: EPRSContext,
        split_settings: dict,
    ) -> str:
        lines: list[str] = [
            "EPRS-S Model Explorer - Author: Dr. Jackson J. Alcazar",
            "Please cite: Example Article Title (doi.org/xxx)",
            "",
            "Selected configuration:",
        ]

        delimiter_display = "\\t" if config.delimiter == "\t" else config.delimiter
        lines.append(f"Dataset: {config.data_path}")
        lines.append(f"CSV delimiter: {delimiter_display}")
        lines.append(f"Target column: {context.target_column}")
        lines.append(f"Predictor columns: {len(context.cols)}")
        method_display = METHOD_KEY_TO_DISPLAY.get(config.method, config.method)
        lines.append(f"Method: {method_display}")
        if config.exclude_constant:
            lines.append(
                "Near-constant filter: enabled (threshold "
                f"{config.constant_threshold:.0f}% of identical values)"
            )
        else:
            lines.append("Near-constant filter: disabled")
        if config.excluded_observations:
            lines.append(f"Excluded observations: {config.excluded_observations}")
        else:
            lines.append("Excluded observations: none")
        lines.append(f"Training rows: {len(context.train_df)}")
        testing_rows = len(context.test_df) if context.test_df is not None else 0
        if context.test_df is not None:
            lines.append(f"Testing rows: {testing_rows}")
        else:
            lines.append("Testing rows: not configured")

        lines.extend(
            [
                f"Number of seeds: {config.n_seeds}",
                "Covariance type: "
                + _format_covariance_type_label(getattr(config, "cov_type", "-")),
                f"Seed size: {config.seed_size}",
                f"Max predictors per model: {config.max_vars}",
                "Target metric: "
                + TARGET_METRIC_DISPLAY.get(config.target_metric, config.target_metric),
                f"Significance level: {config.signif_lvl:.4f}",
                f"Correlation threshold: {config.corr_threshold:.2f}",
                f"VIF threshold: {config.vif_threshold:.2f}",
                "Reporting "
                + TARGET_METRIC_DISPLAY.get(config.target_metric, config.target_metric)
                + " cutoff: "
                + _format_threshold_display(getattr(config, "tm_cutoff", None)),
                f"CPU cores to use: {config.n_jobs}",
            ]
        )

        if config.clip_predictions:
            low, high = config.clip_predictions
            lines.append(f"Prediction clipping: enabled ({low} to {high})")
        else:
            lines.append("Prediction clipping: disabled")

        split_mode = split_settings.get("mode", "none")

        def _preview_ids(values: list[str]) -> str:
            if not values:
                return "None"
            preview = ", ".join(values[:5])
            if len(values) > 5:
                preview += ", ..."
            return preview

        if split_mode == "none":
            lines.append("Split mode: None (all rows used for training)")
        elif split_mode == "random":
            percent = float(split_settings.get("test_size", 0.0)) * 100.0
            lines.append(f"Split mode: Random ({percent:.0f}% of rows reserved for testing)")
        elif split_mode == "manual":
            train_ids = sorted(str(x) for x in split_settings.get("train_ids", set()))
            test_ids = sorted(str(x) for x in split_settings.get("test_ids", set()))
            lines.append(
                f"Split mode: Manual (train IDs: {len(train_ids)}, test IDs: {len(test_ids)})"
            )
            if train_ids:
                lines.append(f"  Train IDs preview: {_preview_ids(train_ids)}")
            if test_ids:
                lines.append(f"  Test IDs preview: {_preview_ids(test_ids)}")
        else:
            lines.append(f"Split mode: {split_mode}")

        return "\n".join(lines) + "\n\n"

    def _append_log(self, _text: str) -> None:
        return

    def _clear_log(self):
        self.last_progress_percent = None
        self._update_progress(0, self.total_seeds)

    def _build_config(
        self,
        allow_defaults: bool = False,
        *,
        validate_dataset: bool = True,
    ) -> EPRSConfig:
        data_path = self.data_path_var.get().strip()
        if not data_path:
            raise ValueError("Please select a dataset.")
        if validate_dataset and not Path(data_path).exists():
            raise FileNotFoundError(f"Dataset not found: {data_path}")

        clip = None
        if self.clip_enabled.get():
            low = self.clip_low.get()
            high = self.clip_high.get()
            if low >= high:
                raise ValueError("Prediction clip: lower bound must be smaller than upper bound.")
            clip = (float(low), float(high))

        params = {key: var.get() for key, var in self.params_vars.items()}
        defaults = EPRSConfig()

        excluded_text = self._get_excluded_observations_text()

        method_display = self.method_choice.get()
        method_key = METHOD_DISPLAY_TO_KEY.get(method_display)
        if method_key is None:
            raise ValueError("Please select a valid method.")
        params["method"] = method_key

        selected_cov_display = self.cov_type_var.get()
        params["cov_type"] = COVARIANCE_DISPLAY_TO_KEY.get(
            selected_cov_display, COVARIANCE_DEFAULT_KEY
        )

        required_int_fields = {
            "max_vars": "Max predictors per model",
            "n_seeds": "Number of seeds",
            "seed_size": "Seed size",
            "random_state": "Random state",
        }

        for key, label in required_int_fields.items():
            raw_value = params.get(key)
            allow_blank = method_key == "all_subsets" and key in {"n_seeds", "seed_size"}
            if raw_value in ("", None):
                if allow_defaults or allow_blank:
                    params[key] = getattr(defaults, key)
                    continue
                raise ValueError(f"{label} is required.")
            try:
                params[key] = int(raw_value)
            except (TypeError, ValueError) as exc:  # noqa: BLE001
                if allow_defaults or allow_blank:
                    params[key] = getattr(defaults, key)
                    continue
                raise ValueError(f"{label} must be a positive integer.") from exc
            if params[key] <= 0:
                if allow_defaults or allow_blank:
                    params[key] = max(1, getattr(defaults, key))
                    continue
                raise ValueError(f"{label} must be a positive integer.")
        if params["n_seeds"] < MIN_SEEDS and not self.allow_small_seed_count.get():
            messagebox.showwarning(
                "Number of seeds",
                "Only values greater than 1000 are allowed. Resetting to 1000.",
            )
            params["n_seeds"] = MIN_SEEDS
            self.params_vars["n_seeds"].set(str(MIN_SEEDS))
        params["allow_small_seed_count"] = self.allow_small_seed_count.get()

        params["data_path"] = data_path
        params["delimiter"] = self._get_delimiter(self.delimiter_var.get())
        params["dependent_choice"] = self._get_dependent_choice()
        params["non_variable_spec"] = self._get_non_variable_spec()
        exclude_constant, constant_threshold = self._get_constant_filter()
        params["exclude_constant"] = exclude_constant
        params["constant_threshold"] = constant_threshold
        params["excluded_observations"] = excluded_text
        params["clip_predictions"] = clip

        target_display = self.target_metric_choice.get()
        try:
            params["target_metric"] = TARGET_METRIC_DISPLAY_TO_KEY[target_display]
        except KeyError as exc:  # noqa: B904
            raise ValueError("Please select a valid target metric.") from exc

        threshold_value = params.get("tm_cutoff", defaults.tm_cutoff)
        if isinstance(threshold_value, str) and threshold_value.strip().lower() == "none":
            params["tm_cutoff"] = None
        else:
            if threshold_value in ("", None):
                if allow_defaults:
                    threshold_value = defaults.tm_cutoff
                else:
                    raise ValueError("Target metric cutoff is required.")
            try:
                threshold_float = float(threshold_value)
            except (TypeError, ValueError) as exc:  # noqa: BLE001
                raise ValueError("Target metric cutoff must be a number.") from exc
            if params["target_metric"] == "RMSE_loo" and threshold_float <= 0:
                raise ValueError("Target metric cutoff must be greater than zero for RMSE (LOO).")
            params["tm_cutoff"] = threshold_float

        export_limit = params.get("export_limit", 0)
        if export_limit in ("", None):
            if allow_defaults:
                export_limit = defaults.export_limit
            else:
                raise ValueError("Top models to report must be a positive integer.")
        if int(export_limit) <= 0:
            raise ValueError("Top models to report must be a positive integer.")
        params["export_limit"] = int(export_limit)

        n_jobs_value = params.get("n_jobs", defaults.n_jobs)
        if n_jobs_value in ("", None):
            if allow_defaults:
                n_jobs_value = defaults.n_jobs
            else:
                raise ValueError("CPU cores to use must be a positive integer.")
        try:
            n_jobs_int = int(n_jobs_value)
        except (TypeError, ValueError) as exc:  # noqa: BLE001
            raise ValueError("CPU cores to use must be a positive integer.") from exc
        if n_jobs_int <= 0:
            if allow_defaults:
                n_jobs_int = max(1, defaults.n_jobs)
            else:
                raise ValueError("CPU cores to use must be a positive integer.")
        params["n_jobs"] = n_jobs_int

        mode_value = (self.iterations_mode_var.get() or ITERATION_MODE_AUTO).lower()
        if mode_value not in {
            ITERATION_MODE_AUTO,
            ITERATION_MODE_MANUAL,
            ITERATION_MODE_CONVERGE,
        }:
            mode_value = ITERATION_MODE_AUTO
        params["iterations_mode"] = mode_value
        params["max_iterations_per_seed"] = None
        if mode_value == ITERATION_MODE_MANUAL:
            manual_value = self.manual_iterations_var.get().strip()
            if not manual_value:
                raise ValueError(
                    "Please enter a positive integer for the maximum iterations per seed.",
                )
            try:
                parsed_manual = int(float(manual_value))
            except (TypeError, ValueError) as exc:  # noqa: BLE001
                raise ValueError(
                    "The maximum iterations per seed must be a positive integer.",
                ) from exc
            if parsed_manual <= 0:
                raise ValueError(
                    "The maximum iterations per seed must be a positive integer.",
                )
            params["max_iterations_per_seed"] = parsed_manual

        return EPRSConfig(**params)

    def _save_configuration_file(self) -> None:
        try:
            config = self._build_config(validate_dataset=False)
            split_settings = self._gather_split_settings()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Configuration error", str(exc))
            return

        default_output = self.results_export_path or Path("models.csv")
        destination = filedialog.asksaveasfilename(
            title="Save configuration",
            defaultextension=".conf",
            filetypes=(("Configuration files", "*.conf"), ("All files", "*.*")),
        )
        if not destination:
            return

        try:
            write_configuration_file(
                Path(destination),
                config,
                split_settings,
                output_path=default_output,
            )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror(
                "Save configuration",
                f"Unable to save the configuration file:\n{exc}",
            )
            return

        messagebox.showinfo("Configuration saved", f"Configuration saved to {destination}")

    def _apply_configuration_to_setup(
        self, config: EPRSConfig, split_settings: dict, output_path: Path
    ) -> None:
        self.results_export_path = output_path

        delimiter_value = self._normalize_delimiter_value(config.delimiter)
        self.delimiter_var.set(self._delimiter_to_ui(delimiter_value))
        self.data_path_var.set(str(config.data_path))

        dependent_display = DEPENDENT_TO_DISPLAY.get(
            config.dependent_choice, DEPENDENT_TO_DISPLAY["last"]
        )
        self.dependent_var_var.set(dependent_display)

        non_variable_display = NON_VARIABLE_TO_DISPLAY.get(config.non_variable_spec)
        if non_variable_display:
            self.non_variable_var.set(non_variable_display)

        self.constant_filter_enabled.set(bool(config.exclude_constant))
        self.constant_threshold_var.set(
            self._format_numeric_value(config.constant_threshold)
        )
        self._toggle_constant_filter()

        excluded_value = config.excluded_observations or ""
        self.exclude_obs_var.set(excluded_value)

        target_display = TARGET_METRIC_DISPLAY.get(
            config.target_metric, TARGET_METRIC_DISPLAY["R2"]
        )
        self.target_metric_choice.set(target_display)

        method_display = METHOD_KEY_TO_DISPLAY.get(config.method)
        if method_display:
            self.method_choice.set(method_display)

        self.params_vars["max_vars"].set(self._format_numeric_value(config.max_vars))
        self.params_vars["n_seeds"].set(self._format_numeric_value(config.n_seeds))
        self.params_vars["seed_size"].set(self._format_numeric_value(config.seed_size))
        self.params_vars["random_state"].set(self._format_numeric_value(config.random_state))
        self.params_vars["signif_lvl"].set(config.signif_lvl)
        self.params_vars["corr_threshold"].set(config.corr_threshold)
        self.params_vars["vif_threshold"].set(config.vif_threshold)
        self.params_vars["tm_cutoff"].set(config.tm_cutoff)
        self.params_vars["export_limit"].set(config.export_limit)
        self.params_vars["n_jobs"].set(config.n_jobs)
        self.allow_small_seed_count.set(bool(config.allow_small_seed_count))
        self._update_seed_settings_summary()

        normalized_cov = COVARIANCE_KEY_NORMALIZED.get(
            config.cov_type.lower(), config.cov_type
        )
        cov_display = COVARIANCE_KEY_TO_DISPLAY.get(normalized_cov)
        if cov_display:
            self.cov_type_var.set(cov_display)

        valid_mode = config.iterations_mode
        if valid_mode not in {
            ITERATION_MODE_AUTO,
            ITERATION_MODE_MANUAL,
            ITERATION_MODE_CONVERGE,
        }:
            valid_mode = ITERATION_MODE_AUTO
        self.iterations_mode_var.set(valid_mode)
        manual_iterations = config.max_iterations_per_seed
        if manual_iterations is not None:
            self.manual_iterations_var.set(str(manual_iterations))
        else:
            self.manual_iterations_var.set("")

        if config.clip_predictions:
            low, high = config.clip_predictions
            self.clip_enabled.set(True)
            self.clip_low.set(low)
            self.clip_high.set(high)
        else:
            self.clip_enabled.set(False)
            self.clip_low.set(0.0)
            self.clip_high.set(1.0)
        self._toggle_clip_entries()

        split_meta = self._metadata_from_split_settings(split_settings)
        mode = split_meta.get("mode", "none")
        self.split_mode.set(mode)
        self._update_split_controls()

        if mode == "random":
            percent = split_meta.get("test_size_percent")
            if percent is not None:
                try:
                    self.random_test_size.set(float(percent))
                except Exception:  # noqa: BLE001
                    pass
        elif mode == "manual":
            self.manual_train_ids.set(self._ids_to_text(split_meta.get("train_ids")))
            self.manual_test_ids.set(self._ids_to_text(split_meta.get("test_ids")))

        self.last_split_settings = split_settings

    def _load_configuration_file(self) -> None:
        config_path = filedialog.askopenfilename(
            title="Load configuration",
            defaultextension=".conf",
            filetypes=(("Configuration files", "*.conf"), ("All files", "*.*")),
        )
        if not config_path:
            return

        try:
            config, split_settings, output_path = parse_configuration_file(config_path)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Load configuration", f"Unable to load the configuration file:\n{exc}")
            return

        self._apply_configuration_to_setup(config, split_settings, output_path)
        self.last_config = config
        self.last_context = None
        messagebox.showinfo("Configuration loaded", f"Loaded configuration from {config_path}")

    def ensure_testing_context(self, allow_defaults: bool = False) -> tuple[EPRSConfig, EPRSContext]:
        """Return a configuration/context pair for testing tabs."""

        config = self._build_config(allow_defaults=allow_defaults)
        split_settings = self._gather_split_settings()
        if (
            self.last_config is not None
            and self.last_context is not None
            and self.last_split_settings == split_settings
            and self.last_config == config
        ):
            return self.last_config, self.last_context

        context = load_dataset(
            config.data_path,
            delimiter=config.delimiter,
            split=split_settings,
            dependent_choice=config.dependent_choice,
            non_variable_spec=config.non_variable_spec,
            exclude_constant=config.exclude_constant,
            constant_threshold=config.constant_threshold,
            excluded_observations=config.excluded_observations,
        )
        self.last_config = config
        self.last_context = context
        self.last_split_settings = split_settings
        return config, context


class ValidationTab(ttk.Frame):
    def __init__(self, notebook: ttk.Notebook, master_app: "MLRXApp"):
        super().__init__(notebook)
        self.master_app = master_app
        self.results_df: Optional[pd.DataFrame] = None
        self.context: Optional[EPRSContext] = None
        self.config: Optional[EPRSConfig] = None

        self.dataset_path_var = master_app.external_test_path
        self.delimiter_var = master_app.external_delimiter_var

        self.selection_mode = tk.StringVar(value="top")
        self.top_models_var = tk.StringVar(value="1000")
        self.multiple_models_var = tk.StringVar()

        self.internal_status_var = tk.StringVar(value="Awaiting analysis")
        self.external_status_var = tk.StringVar(value="Awaiting analysis")

        self.use_loo = tk.BooleanVar(value=True)
        self.use_kfold = tk.BooleanVar(value=False)
        self.kfold_folds_var = tk.IntVar(value=10)
        self.kfold_repeats_var = tk.IntVar(value=5)

        self.selection_buttons: list[ttk.Radiobutton] = []
        self.available = False
        self.pending_context = False
        self.external_disabled_by_split = False

        self._build_ui()
        self.set_available(False)

    def _build_ui(self):
        padding = {"padx": 10, "pady": 5}

        selection_frame = ttk.LabelFrame(self, text="Model selection")
        selection_frame.pack(fill="x", padx=10, pady=(10, 5))

        options = [
            ("Top models", "top"),
            ("Multiple models", "multiple"),
            ("All models", "all"),
        ]
        for idx, (label, value) in enumerate(options):
            btn = ttk.Radiobutton(
                selection_frame,
                text=label,
                variable=self.selection_mode,
                value=value,
                command=self._update_selection_controls,
            )
            btn.grid(row=idx, column=0, sticky="w", **padding)
            self.selection_buttons.append(btn)

        self.top_entry = ttk.Entry(selection_frame, textvariable=self.top_models_var, width=10)
        self.top_entry.grid(row=0, column=1, sticky="w", **padding)

        multiple_row = ttk.Frame(selection_frame)
        multiple_row.grid(row=1, column=1, sticky="w", **padding)
        self.multiple_entry = ttk.Entry(multiple_row, textvariable=self.multiple_models_var, width=30)
        self.multiple_entry.pack(side="left")
        ttk.Label(
            multiple_row,
            text="Comma-separated IDs, supports ranges (e.g., 1-5,7,9).",
        ).pack(side="left", padx=(8, 0))

        selection_frame.columnconfigure(1, weight=1)

        internal_frame = ttk.LabelFrame(self, text="Internal validation")
        internal_frame.pack(fill="x", padx=10, pady=(0, 5))
        internal_frame.columnconfigure(0, weight=1)
        internal_frame.columnconfigure(1, weight=1)
        internal_frame.columnconfigure(2, weight=1)

        self.loo_check = ttk.Checkbutton(
            internal_frame,
            text="Leave-one-out (LOO)",
            variable=self.use_loo,
            command=self._update_method_controls,
        )
        self.loo_check.grid(row=0, column=0, sticky="w", **padding)

        kfold_row = ttk.Frame(internal_frame)
        kfold_row.grid(row=1, column=0, columnspan=2, sticky="w", **padding)
        self.kfold_check = ttk.Checkbutton(
            kfold_row,
            text="k-fold cross-validation",
            variable=self.use_kfold,
            command=self._update_method_controls,
        )
        self.kfold_check.pack(side="left")

        kfold_params = ttk.Frame(kfold_row)
        kfold_params.pack(side="left", padx=(10, 0))
        ttk.Label(kfold_params, text="Folds:").pack(side="left")
        self.kfold_folds_entry = ttk.Entry(
            kfold_params, textvariable=self.kfold_folds_var, width=6, state="disabled"
        )
        self.kfold_folds_entry.pack(side="left", padx=(4, 10))
        ttk.Label(kfold_params, text="Repeats:").pack(side="left")
        self.kfold_repeats_entry = ttk.Entry(
            kfold_params, textvariable=self.kfold_repeats_var, width=6, state="disabled"
        )
        self.kfold_repeats_entry.pack(side="left")

        internal_actions = ttk.Frame(internal_frame)
        internal_actions.grid(row=3, column=0, columnspan=3, sticky="we", padx=10, pady=(10, 5))
        internal_actions.columnconfigure(0, weight=0)
        internal_actions.columnconfigure(1, weight=1)

        self.internal_button = ttk.Button(
            internal_actions,
            text="Run internal validation",
            command=self._run_internal_validation,
        )
        self.internal_button.grid(row=0, column=0, sticky="w")

        ttk.Label(internal_actions, textvariable=self.internal_status_var).grid(
            row=0, column=1, sticky="e"
        )

        external_frame = ttk.LabelFrame(self, text="External validation")
        external_frame.pack(fill="x", padx=10, pady=(0, 10))
        external_frame.grid_columnconfigure(1, weight=1)

        ttk.Label(external_frame, text="Testing CSV:").grid(
            row=0, column=0, sticky="w", padx=10, pady=(8, 2)
        )
        self.dataset_entry = ttk.Entry(external_frame, textvariable=self.dataset_path_var, width=40)
        self.dataset_entry.grid(row=0, column=1, sticky="ew", padx=5, pady=(8, 2))
        self.dataset_browse = ttk.Button(
            external_frame,
            text="Browse",
            command=self._browse_dataset,
        )
        self.dataset_browse.grid(row=0, column=2, sticky="w", padx=(0, 10), pady=(8, 2))

        ttk.Label(external_frame, text="CSV delimiter:").grid(
            row=1, column=0, sticky="w", padx=10, pady=(2, 8)
        )
        self.delimiter_box = ttk.Combobox(
            external_frame,
            textvariable=self.delimiter_var,
            values=[";", ",", "	", "|"],
            width=6,
            state="readonly",
        )
        self.delimiter_box.grid(row=1, column=1, sticky="w", padx=5, pady=(2, 8))

        external_actions = ttk.Frame(external_frame)
        external_actions.grid(row=2, column=0, columnspan=3, sticky="we", padx=10, pady=(0, 8))
        external_actions.columnconfigure(1, weight=1)

        self.external_button = ttk.Button(
            external_actions,
            text="Run external validation",
            command=self._run_external_validation,
        )
        self.external_button.grid(row=0, column=0, sticky="w")

        ttk.Label(external_actions, textvariable=self.external_status_var).grid(
            row=0, column=1, sticky="e"
        )

        self._update_selection_controls()
        self._update_method_controls()
        self.sync_split_mode(self.master_app.split_mode.get())

    def prepare_for_new_run(self):
        self.results_df = None
        self.context = None
        self.config = None
        self.pending_context = False
        self.selection_mode.set("top")
        self.top_models_var.set("1000")
        self.multiple_models_var.set("")
        self.use_loo.set(True)
        self.use_kfold.set(False)
        self.kfold_folds_var.set(10)
        self.kfold_repeats_var.set(5)
        self.internal_status_var.set("Awaiting analysis")
        self.external_status_var.set("Awaiting analysis")
        self.set_available(False)
        self.master_app._update_kfold_settings(False, None, None)

    def update_sources(
        self,
        results_df: Optional[pd.DataFrame],
        context: Optional[EPRSContext],
        config: Optional[EPRSConfig],
    ):
        filtered_df = results_df
        if results_df is not None and not results_df.empty:
            top_models = self.master_app.get_top_training_model_ids()
            if top_models and "Model" in results_df.columns:
                order_map = {mid: idx for idx, mid in enumerate(top_models)}
                normalized_ids = results_df["Model"].apply(self.master_app._safe_int)
                order_series = normalized_ids.map(order_map)
                mask = order_series.notna()
                filtered_df = results_df.loc[mask].copy()
                filtered_df["Model"] = normalized_ids.loc[mask].astype(int)
                filtered_df["__order"] = order_series.loc[mask].astype(int)
                filtered_df = filtered_df.sort_values("__order").drop(columns="__order")
        self.results_df = filtered_df
        self.context = context
        self.config = config
        self.pending_context = False

        if config is not None:
            self.update_iteration_preferences(
                getattr(config, "iterations_mode", ITERATION_MODE_AUTO),
                getattr(config, "max_iterations_per_seed", None),
            )

        has_results = bool(filtered_df is not None and not filtered_df.empty)
        has_context = bool(context is not None and config is not None)
        if has_results and not has_context:
            self.pending_context = True
        if has_results:
            self.selection_mode.set("top")
            self.top_models_var.set("1000")
        self.set_available(has_results)
        if not has_results:
            self.internal_status_var.set("Awaiting analysis")
            self.external_status_var.set("Awaiting analysis")

    def set_available(self, enabled: bool):
        self.available = enabled
        state = "normal" if enabled else "disabled"
        for btn in self.selection_buttons:
            btn.configure(state=state)
        if enabled:
            self.internal_button.configure(state="normal")
            self.internal_status_var.set("Ready")
        else:
            self.internal_button.configure(state="disabled")
            self.external_button.configure(state="disabled")
            self.internal_status_var.set("Awaiting analysis")
            self.external_status_var.set("Awaiting analysis")
            self.top_entry.configure(state="disabled")
            self.multiple_entry.configure(state="disabled")
        self._update_selection_controls()
        self._update_method_controls()
        self.sync_split_mode(self.master_app.split_mode.get())

    def update_iteration_preferences(
        self, mode: Optional[str], manual_value: Optional[object]
    ) -> None:
        normalized = (mode or ITERATION_MODE_AUTO).lower()
        if normalized not in {
            ITERATION_MODE_AUTO,
            ITERATION_MODE_MANUAL,
            ITERATION_MODE_CONVERGE,
        }:
            normalized = ITERATION_MODE_AUTO

        self.iterations_mode = normalized
        try:
            self.manual_iterations_per_seed = (
                None
                if manual_value in (None, "")
                else int(float(manual_value))
            )
        except Exception:  # noqa: BLE001
            self.manual_iterations_per_seed = None

    def _update_selection_controls(self):
        if not self.available:
            self.top_entry.configure(state="disabled")
            self.multiple_entry.configure(state="disabled")
            return

        mode = self.selection_mode.get()
        self.top_entry.configure(state="normal" if mode == "top" else "disabled")
        self.multiple_entry.configure(state="normal" if mode == "multiple" else "disabled")

    def _update_method_controls(self):
        base_state = "normal" if self.available else "disabled"
        self.loo_check.configure(state=base_state)
        self.kfold_check.configure(state=base_state)

        entry_state = "normal" if (self.available and self.use_kfold.get()) else "disabled"
        self.kfold_folds_entry.configure(state=entry_state)
        self.kfold_repeats_entry.configure(state=entry_state)

    def sync_split_mode(self, mode: str):
        disabled = mode in {"random", "manual"}
        self.external_disabled_by_split = disabled
        entry_state = "disabled" if disabled else "normal"
        delimiter_state = "disabled" if disabled else "readonly"
        button_state = "disabled" if (disabled or not self.available) else "normal"

        self.dataset_entry.configure(state=entry_state)
        self.dataset_browse.configure(state=entry_state)
        self.delimiter_box.configure(state=delimiter_state)
        self.external_button.configure(state=button_state)

        if disabled:
            self.external_status_var.set("External dataset disabled by split")
        elif not self.available:
            self.external_status_var.set("Awaiting analysis")
        else:
            self.external_status_var.set("Ready")

    def _browse_dataset(self):
        path = filedialog.askopenfilename(
            title="Select external testing dataset",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if path:
            self.dataset_path_var.set(path)

    def _resolve_model_selection(self) -> list[int]:
        if self.results_df is None or self.results_df.empty:
            raise ValueError("No models are available for evaluation.")

        raw_ids = [self.master_app._safe_int(value) for value in self.results_df["Model"].tolist()]
        raw_ids = [mid for mid in raw_ids if mid is not None]
        top_ids = self.master_app.get_top_training_model_ids()
        if top_ids:
            available = [mid for mid in top_ids if mid in raw_ids]
        else:
            available = raw_ids
        available_set = set(available)
        if not available_set:
            raise ValueError("No models are available for evaluation.")

        mode = self.selection_mode.get()

        if mode == "top":
            value = self.top_models_var.get().strip()
            if not value:
                raise ValueError("Enter how many top models to evaluate.")
            try:
                count = int(value)
            except ValueError as exc:  # noqa: BLE001
                raise ValueError("Top models: please enter a valid integer count.") from exc
            if count <= 0:
                raise ValueError("Top models count must be a positive integer.")
            return available[:count]

        if mode == "multiple":
            value = self.multiple_models_var.get().strip()
            if not value:
                raise ValueError("Provide at least one model ID to evaluate.")
            try:
                ids = sorted(int(token) for token in _parse_id_entries(value))
            except ValueError as exc:  # noqa: BLE001
                raise ValueError(f"Multiple models: {exc}") from exc
            missing = [mid for mid in ids if mid not in available_set]
            if missing:
                raise ValueError("Models not found: " + ", ".join(str(mid) for mid in missing))
            return ids

        if mode == "all":
            return available

        raise ValueError("Unknown selection mode.")

    def _run_internal_validation(self):
        if (not self.available) or self.results_df is None or self.results_df.empty:
            messagebox.showinfo(
                "Internal validation",
                "Internal validation is not available for the current run.",
            )
            return

        if self.context is None or self.config is None:
            try:
                config, context = self.master_app.ensure_testing_context()
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror(
                    "Internal validation",
                    "Unable to prepare the dataset for evaluation:\n" + str(exc),
                )
                self.internal_status_var.set("Configuration required")
                return
            self.config = config
            self.context = context
            self.pending_context = False
            self.update_iteration_preferences(
                getattr(config, "iterations_mode", ITERATION_MODE_AUTO),
                getattr(config, "max_iterations_per_seed", None),
            )

        try:
            model_ids = self._resolve_model_selection()
        except ValueError as exc:  # noqa: BLE001
            messagebox.showerror("Internal validation", str(exc))
            return

        use_loo = bool(self.use_loo.get())
        use_kfold = bool(self.use_kfold.get())

        if not (use_loo or use_kfold):
            messagebox.showerror(
                "Internal validation", "Select at least one evaluation method (LOO and/or k-fold)."
            )
            return

        folds = repeats = None
        if use_kfold:
            folds_text = self.kfold_folds_entry.get().strip()
            repeats_text = self.kfold_repeats_entry.get().strip()
            if not folds_text or not repeats_text:
                messagebox.showerror(
                    "Internal validation", "Specify both folds and repeats for k-fold evaluation."
                )
                return
            try:
                folds = int(folds_text)
                repeats = int(repeats_text)
            except ValueError:
                messagebox.showerror("Internal validation", "k-fold parameters must be integers.")
                return
            if folds < 2:
                messagebox.showerror(
                    "Internal validation", "k-fold: the number of folds must be at least 2."
                )
                return
            if repeats < 1:
                messagebox.showerror(
                    "Internal validation", "k-fold: repeats must be at least 1."
                )
                return
            if self.context.X_np.shape[0] < folds:
                messagebox.showerror(
                    "Internal validation", "k-fold: folds cannot exceed the available samples."
                )
                return

        method_descriptions: list[str] = []
        if use_loo:
            method_descriptions.append("LOO")
        if use_kfold and folds is not None and repeats is not None:
            method_descriptions.append(f"k-fold (folds={folds}, repeats={repeats})")
        if not method_descriptions:
            method_descriptions.append("-")

        self.master_app._update_kfold_settings(
            bool(use_kfold and folds is not None and repeats is not None),
            folds,
            repeats,
        )

        models_preview = ", ".join(str(mid) for mid in model_ids[:10])
        if len(model_ids) > 10:
            models_preview += ", ..."
        if not models_preview:
            models_preview = "-"
        log_lines = [
            "Internal validation started:",
            f"  Models: {models_preview} (total {len(model_ids)})",
            f"  Methods: {', '.join(method_descriptions)}",
        ]
        self.master_app._append_log("\n".join(log_lines) + "\n")

        rows: list[dict] = []
        errors: list[str] = []

        def _has_loo_metrics(row: dict[str, object]) -> bool:
            return all(key in row for key in ("R2_loo", "RMSE_loo", "s_loo", "MAE_loo"))

        def _has_kfold_metrics(row: dict[str, object]) -> bool:
            return all(
                key in row for key in ("R2_kfold", "RMSE_kfold", "s_kfold", "MAE_kfold")
            )

        existing_results: dict[int, dict[str, object]] = {}
        for row in getattr(self.master_app, "last_internal_results", []) or []:
            model_value = self.master_app._safe_int(row.get("Model"))
            if model_value is None:
                continue
            existing_results[model_value] = dict(row)

        for model_id in model_ids:
            subset = self.results_df[self.results_df["Model"] == model_id]
            if subset.empty:
                errors.append(f"Model {model_id} is not available in the current results.")
                continue

            record = subset.iloc[0]
            variables = list(record["Variables"])
            result_row: dict[str, object] = {"Model": int(record["Model"]), "Variables": variables}

            existing_row = existing_results.get(int(record["Model"]))
            kfold_matches = bool(
                existing_row
                and use_kfold
                and folds is not None
                and repeats is not None
                and existing_row.get("_kfold_folds") == folds
                and existing_row.get("_kfold_repeats") == repeats
            )
            if existing_row:
                result_row.update(existing_row)

            if existing_row and not kfold_matches:
                for key in ("R2_kfold", "RMSE_kfold", "s_kfold", "MAE_kfold"):
                    result_row.pop(key, None)
                result_row.pop("_kfold_folds", None)
                result_row.pop("_kfold_repeats", None)

            method_success = bool(
                existing_row
                and (
                    _has_loo_metrics(existing_row)
                    or (kfold_matches and _has_kfold_metrics(existing_row))
                )
            )

            if use_loo and not (existing_row and _has_loo_metrics(existing_row)):
                try:
                    result_row.update(self._evaluate_model_loo(variables))
                    method_success = True
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"Model {model_id} (LOO): {exc}")

            if (
                use_kfold
                and folds is not None
                and repeats is not None
                and not (kfold_matches and existing_row and _has_kfold_metrics(existing_row))
            ):
                try:
                    result_row.update(self._evaluate_model_kfold(variables, folds, repeats))
                    result_row["_kfold_folds"] = folds
                    result_row["_kfold_repeats"] = repeats
                    method_success = True
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"Model {model_id} (k-fold): {exc}")

            if method_success:
                rows.append(result_row)

        self.master_app.update_internal_results(rows)

        if rows:
            method_labels = []
            if use_loo:
                method_labels.append("LOO")
            if use_kfold:
                method_labels.append("k-fold")
            self.internal_status_var.set(
                f"Evaluated {len(rows)} model(s) with {', '.join(method_labels)}."
            )
            self.master_app._append_log(
                f"Internal validation completed: evaluated {len(rows)} model(s) with {', '.join(method_descriptions)}.\n"
            )
        else:
            self.internal_status_var.set("No models were evaluated.")
            self.master_app._append_log(
                "Internal validation completed: no models were evaluated.\n"
            )

        if errors:
            preview = "; ".join(errors[:3])
            if len(errors) > 3:
                preview += "; ..."
            self.master_app._append_log(
                "Internal validation warnings: " + preview + "\n"
            )

    def _run_external_validation(self):
        if not self.available or self.results_df is None or self.results_df.empty:
            messagebox.showinfo(
                "External validation",
                "Run an analysis to generate models before performing external validation.",
            )
            return

        if self.context is None or self.config is None:
            try:
                config, context = self.master_app.ensure_testing_context()
            except Exception as exc:  # noqa: BLE001
                messagebox.showerror(
                    "External validation",
                    "Unable to prepare the dataset for evaluation:\n" + str(exc),
                )
                self.external_status_var.set("Configuration required")
                return
            self.config = config
            self.context = context
            self.pending_context = False
            self.update_iteration_preferences(
                getattr(config, "iterations_mode", ITERATION_MODE_AUTO),
                getattr(config, "max_iterations_per_seed", None),
            )

        dataset_path = self.dataset_path_var.get().strip()
        if not dataset_path:
            messagebox.showerror("External validation", "Please select a CSV file to evaluate.")
            return

        self.master_app.external_test_path.set(dataset_path)
        self.master_app.external_delimiter_var.set(self.delimiter_var.get())

        try:
            delimiter = self.master_app._get_delimiter(self.delimiter_var.get())
        except ValueError as exc:  # noqa: BLE001
            messagebox.showerror("External validation", str(exc))
            return

        try:
            ext_df = pd.read_csv(dataset_path, delimiter=delimiter)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("External validation", f"Unable to read dataset:\n{exc}")
            return

        ext_df.columns = [col.strip() if isinstance(col, str) else col for col in ext_df.columns]

        if ext_df.empty:
            messagebox.showerror(
                "External validation",
                "The external dataset does not contain any rows to evaluate.",
            )
            return

        target_col = self.context.target_column
        if target_col not in ext_df.columns:
            messagebox.showerror(
                "External validation",
                f"The external dataset is missing the dependent variable column: {target_col}",
            )
            return

        self.master_app.register_external_holdout_dataset(ext_df)

        try:
            model_ids = self._resolve_model_selection()
        except ValueError as exc:  # noqa: BLE001
            messagebox.showerror("External validation", str(exc))
            return

        models_preview = ", ".join(str(mid) for mid in model_ids[:10])
        if len(model_ids) > 10:
            models_preview += ", ..."
        if not models_preview:
            models_preview = "-"
        log_lines = [
            "External validation started:",
            f"  Dataset: {dataset_path}",
            f"  Models: {models_preview} (total {len(model_ids)})",
        ]
        self.master_app._append_log("\n".join(log_lines) + "\n")

        results = []
        errors: list[str] = []

        for model_id in model_ids:
            subset = self.results_df[self.results_df["Model"] == model_id]
            if subset.empty:
                errors.append(f"Model {model_id} is not available in the current results.")
                continue

            variables = list(self.master_app._normalize_variables(subset.iloc[0]["Variables"]))  # noqa: SLF001
            try:
                metrics = self._compute_external_metrics(variables, ext_df)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Model {model_id}: {exc}")
                continue

            results.append({
                "Model": int(model_id),
                "Variables": variables,
                **metrics,
            })

        if results:
            status_message = f"Evaluated {len(results)} model(s) on external data."
        else:
            status_message = "No models were evaluated."

        if errors:
            preview = "; ".join(errors[:3])
            if len(errors) > 3:
                preview += "; ..."
            messagebox.showwarning(
                "External validation",
                "Some models could not be evaluated: " + preview,
            )

        self.external_status_var.set(status_message)
        self.master_app.update_external_results(results)
        self.master_app._append_log("External validation completed.\n")

    def _clip_predictions(self, preds: np.ndarray) -> np.ndarray:
        if self.config and self.config.clip_predictions is not None:
            lo, hi = self.config.clip_predictions
            return np.clip(preds, lo, hi)
        return preds

    def _evaluate_model_loo(self, variables: list[str]) -> dict[str, float]:
        if self.context is None:
            raise ValueError("No dataset is available for internal validation.")

        return _evaluate_model_loo(
            self.context, variables, getattr(self.config, "clip_predictions", None)
        )

    def _evaluate_model_kfold(
        self, variables: list[str], folds: int, repeats: int
    ) -> dict[str, float]:
        if self.context is None:
            raise ValueError("No dataset is available for internal validation.")

        X = take(self.context, variables)
        y = self.context.y_np.astype(float)

        rng = np.random.default_rng(42)
        metrics = []
        sse_total = 0.0
        dof_total = 0.0
        s_values: list[float] = []
        param_count = len(variables) + 1
        for _ in range(repeats):
            indices = np.arange(X.shape[0])
            rng.shuffle(indices)
            fold_sizes = np.full(folds, X.shape[0] // folds, dtype=int)
            fold_sizes[: X.shape[0] % folds] += 1
            current = 0
            for fold_size in fold_sizes:
                start, stop = current, current + fold_size
                test_idx = indices[start:stop]
                train_idx = np.concatenate((indices[:start], indices[stop:]))
                current = stop

                if not len(train_idx) or not len(test_idx):
                    raise ValueError("k-fold: insufficient data for the requested folds.")

                X_train, X_test = X[train_idx], X[test_idx]
                y_train, y_test = y[train_idx], y[test_idx]

                design_train = np.c_[np.ones(X_train.shape[0]), X_train]
                try:
                    coef, *_ = np.linalg.lstsq(design_train, y_train, rcond=None)
                except np.linalg.LinAlgError as exc:  # noqa: BLE001
                    raise ValueError("k-fold: failed to fit the model.") from exc

                preds = np.c_[np.ones(X_test.shape[0]), X_test] @ coef
                preds = self._clip_predictions(preds)

                residuals = y_test - preds
                mse = mean_squared_error(y_test, preds)
                rmse = float(np.sqrt(mse))
                mae = float(mean_absolute_error(y_test, preds))
                r2 = float(r2_score(y_test, preds))

                s_fold = _compute_standard_error(residuals, param_count)
                if np.isfinite(s_fold):
                    s_values.append(float(s_fold))

                sse = float(np.dot(residuals, residuals))
                if np.isfinite(sse):
                    sse_total += sse
                dof = residuals.size - max(param_count, 1)
                if dof <= 0:
                    dof = residuals.size
                if dof > 0 and np.isfinite(dof):
                    dof_total += float(dof)

                metrics.append((r2, rmse, mae))

        if not metrics:
            raise ValueError("k-fold: no evaluations were performed.")

        r2_values, rmse_values, mae_values = zip(*metrics)
        if dof_total > 0:
            s_kfold = float(math.sqrt(sse_total / dof_total)) if sse_total >= 0 else float("nan")
        elif s_values:
            s_kfold = float(np.nanmean(s_values))
        else:
            s_kfold = float("nan")
        if (not np.isfinite(s_kfold)) and s_values:
            s_kfold = float(np.nanmean(s_values))
        return {
            "R2_kfold": float(np.mean(r2_values)),
            "RMSE_kfold": float(np.mean(rmse_values)),
            "s_kfold": s_kfold,
            "MAE_kfold": float(np.mean(mae_values)),
        }

    def _compute_external_metrics(
        self,
        variables: list[str],
        ext_df: pd.DataFrame,
    ) -> dict:
        if self.context is None or self.config is None:
            raise ValueError("Training context is unavailable.")

        target_col = self.context.target_column
        if target_col not in ext_df.columns:
            raise ValueError(
                f"Dependent variable '{target_col}' is not present in the external dataset."
            )

        missing = [v for v in variables if v not in ext_df.columns]
        if missing:
            raise ValueError(
                "Missing predictor columns for external evaluation: " + ", ".join(missing)
            )

        Xm = take(self.context, variables)
        if Xm.size == 0:
            raise ValueError("Model has no predictors to evaluate.")
        exog = np.c_[np.ones(Xm.shape[0], dtype=Xm.dtype), Xm]
        model = sm.OLS(self.context.y_np, exog).fit()

        ext_matrix = ext_df.loc[:, variables].to_numpy(dtype=np.float64, copy=False)
        if ext_matrix.size == 0:
            raise ValueError("External dataset does not contain values for the selected predictors.")
        ext_exog = np.c_[np.ones(ext_matrix.shape[0], dtype=ext_matrix.dtype), ext_matrix]
        preds = ext_exog @ model.params
        if self.config.clip_predictions is not None:
            lo, hi = self.config.clip_predictions
            preds = np.clip(preds, lo, hi)

        y_true = ext_df.loc[:, target_col].to_numpy(dtype=np.float64, copy=False)
        residuals = y_true - preds
        param_count = len(variables) + 1
        sum_squared_errors = float(np.dot(residuals, residuals))
        train_mean = (
            float(np.nanmean(self.context.y_np)) if self.context.y_np.size else float("nan")
        )
        test_mean = float(np.nanmean(y_true)) if y_true.size else float("nan")
        n_train = int(self.context.y_np.size)
        n_ext = int(y_true.size)
        if n_train > 0 and np.isfinite(train_mean):
            train_centered = np.asarray(self.context.y_np, dtype=np.float64) - train_mean
            train_centered_sum = float(np.dot(train_centered, train_centered))
        else:
            train_centered_sum = float("nan")

        def _safe_q2(denominator: float) -> float:
            if not np.isfinite(sum_squared_errors) or not np.isfinite(denominator):
                return float("nan")
            if denominator <= 0:
                return float("nan")
            return float(1.0 - (sum_squared_errors / denominator))

        denom_f1 = float(np.dot(y_true - train_mean, y_true - train_mean))
        denom_f2 = float(np.dot(y_true - test_mean, y_true - test_mean))

        q2_ext = _safe_q2(denom_f2)

        if (
            not np.isfinite(sum_squared_errors)
            or n_ext <= 0
            or n_train <= 0
            or not np.isfinite(train_centered_sum)
        ):
            q2f3_ext = float("nan")
        else:
            mse_ext = sum_squared_errors / n_ext if n_ext else float("nan")
            denom_f3 = train_centered_sum / n_train if n_train else float("nan")
            if (
                not np.isfinite(mse_ext)
                or not np.isfinite(denom_f3)
                or denom_f3 <= 0
            ):
                q2f3_ext = float("nan")
            else:
                q2f3_ext = float(1.0 - (mse_ext / denom_f3))

        return {
            "R2_ext": q2_ext,
            "RMSE_ext": float(np.sqrt(mean_squared_error(y_true, preds))),
            "s_ext": _compute_standard_error(residuals, param_count),
            "MAE_ext": float(mean_absolute_error(y_true, preds)),
            "Q2F1_ext": _safe_q2(denom_f1),
            "Q2F2_ext": q2_ext,
            "Q2F3_ext": q2f3_ext,
        }



class SummaryTab(ttk.Frame):
    ASCII_ART: tuple[str, ...] = (
        "##   # ##     #####        ##  ##      ##     #### ",
        "### ## ##     ##  ##        ####     ####    ##  ##",
        "## # # ##     #####  #####   ##        ##    ##  ##",
        "##   # ##     ##  ##        ####       ##    ##  ##",
        "##   # ###### ##  ##       ##  ##      ## ##  #### ",
    )

    LICENSE_BLOCK: tuple[str, ...] = (
        "-------------------------------------------------------------",
        "This software is free and distributed under the",
        "GNU Affero General Public License v3.0 (AGPL-3.0).",
        "© 2025, Jackson J. Alcázar. All rights reserved.",
        "-------------------------------------------------------------",
    )

    RESULT_SECTION_MARKER: str = "  "

    INFO_FIELDS: tuple[tuple[str, str], ...] = (
        ("Regression", "regression"),
        ("Covariance type", "cov_type"),
        ("Search method", "search_method"),
        ("Max iterations/seed", "iterations"),
        ("Used target metric", "target_metric"),
        ("Total models explored", "models_explored"),
        (
            f"Models with {R_SQUARED_SYMBOL} >= {{tm_cutoff}}",
            "models_found",
        ),
        ("Filtrated and reported models", "models_reported"),
        ("Significance level", "significance"),
        ("No. observations", "nobs"),
        ("Dependent variable", "dependent"),
        ("Df model (No. predictors)", "df_model"),
        ("Df residuals", "df_resid"),
        ("Predictors", "predictors"),
        ("K-fold (folds × repeats)", "kfold"),
        ("Model search CPU time", "cpu_search"),
        ("Total CPU time", "cpu_total"),
    )

    ADDITIONAL_METRICS: tuple[tuple[str, str], ...] = (
        ("F-statistic", "f_stat"),
        ("Prob (F-statistic)", "f_pvalue"),
        ("Log-Likelihood", "log_like"),
        ("AIC", "aic"),
        ("BIC", "bic"),
        ("Omnibus", "omnibus"),
        ("Prob(Omnibus)", "omnibus_p"),
        ("Skew", "skew"),
        ("Kurtosis", "kurtosis"),
        ("Durbin-Watson", "durbin_watson"),
        ("Jarque-Bera (JB)", "jarque_bera"),
        ("Prob(JB)", "jarque_bera_p"),
        ("Cond. No", "cond_no"),
        ("CCC", "lin_ccc"),
    )

    TRAINING_LABELS: dict[str, str] = {
        "R2": f"{R_SQUARED_SYMBOL} (train)",
        "R2_adj": f"adj-{R_SQUARED_SYMBOL}",
        "RMSE": "RMSE (train)",
        "MAE": "MAE (train)",
        "s": f"{STANDARD_ERROR_SYMBOL} (train)",
        "VIF_max": "VIFmax",
        "VIF_avg": "VIFavg",
        "r_max": "|r|max",
    }

    INTERNAL_ORDER: tuple[str, ...] = (
        "R2_loo",
        "RMSE_loo",
        "MAE_loo",
        "s_loo",
        "R2_kfold",
        "RMSE_kfold",
        "MAE_kfold",
        "s_kfold",
    )

    INTERNAL_LABELS: dict[str, str] = {
        "R2_loo": f"{Q_SQUARED_SYMBOL} (LOO)",
        "RMSE_loo": "RMSE (LOO)",
        "MAE_loo": "MAE (LOO)",
        "s_loo": f"{STANDARD_ERROR_SYMBOL} (LOO)",
        "R2_kfold": f"{Q_SQUARED_SYMBOL} (k-fold)",
        "RMSE_kfold": "RMSE (k-fold)",
        "MAE_kfold": "MAE (k-fold)",
        "s_kfold": f"{STANDARD_ERROR_SYMBOL} (k-fold)",
    }

    EXTERNAL_ORDER: tuple[str, ...] = (
        "Q2F3_ext",
        "RMSE_ext",
        "MAE_ext",
        "Q2F2_ext",
        "Q2F1_ext",
    )

    EXTERNAL_LABELS: dict[str, str] = {
        "Q2F1_ext": f"{Q_SQUARED_SYMBOL}F1",
        "Q2F2_ext": f"{Q_SQUARED_SYMBOL}F2",
        "Q2F3_ext": f"{Q_SQUARED_SYMBOL}F3",
        "RMSE_ext": "RMSE (ext)",
        "MAE_ext": "MAE (ext)",
    }

    def __init__(self, notebook: ttk.Notebook, master_app: "MLRXApp"):
        super().__init__(notebook)
        self.master_app = master_app
        self.results_df: Optional[pd.DataFrame] = None
        self.internal_results: list[dict] = []
        self.external_results: list[dict] = []
        self.context: Optional[EPRSContext] = None
        self.config: Optional[EPRSConfig] = None
        self.available = False
        self._y_randomization_results: dict[int, dict[str, YRandomizationResult]] = {}

        self.model_var = tk.StringVar()
        self.model_status_var = tk.StringVar(value="Select a model to summarize.")
        self.use_recommended_covariance = tk.BooleanVar(value=False)
        self.export_button: Optional[ttk.Button] = None
        self.use_recommended_check: Optional[ttk.Checkbutton] = None
        self.info_vars: dict[str, tk.StringVar] = {
            key: tk.StringVar(value="-") for _, key in self.INFO_FIELDS
        }
        self.additional_metric_vars: dict[str, tk.StringVar] = {
            key: tk.StringVar(value="-") for _, key in self.ADDITIONAL_METRICS
        }
        self.equation_with_errors_var = tk.StringVar(
            value="Equation unavailable (pending model selection)."
        )
        self.equation_without_errors_var = tk.StringVar(
            value="Equation unavailable (pending model selection)."
        )
        self.summary_text: Optional[tk.Text] = None
        self.coefficient_rows: list[tuple[str, ...]] = []
        self.training_metrics: list[tuple[str, str, str]] = []
        self.internal_metrics: list[tuple[str, str, str]] = []
        self.external_metrics: list[tuple[str, str]] = []
        self._cpu_search_minutes: Optional[float] = None
        self._cpu_total_minutes: Optional[float] = None
        self._avg_iterations_per_seed: Optional[float] = None
        self._max_iterations_per_seed: Optional[float] = None
        self._iteration_mode: str = ITERATION_MODE_AUTO
        self._manual_iterations_per_seed: Optional[int] = None
        self._models_found: Optional[int] = None
        self._models_reported: Optional[int] = None
        self._models_explored: Optional[int] = None
        self._title_width = 62
        self._title_center_width = 84
        self._training_metric_order: list[str] = []
        self._covariance_diagnosis: Optional[dict[str, Any]] = None
        self._significance_relation = "<"

        self._build_ui()
        self.set_available(False)

    def _build_ui(self) -> None:
        selection_frame = ttk.LabelFrame(self, text="Model selection")
        selection_frame.pack(fill="x", padx=10, pady=(10, 5))
        selection_frame.columnconfigure(2, weight=1)
        selection_frame.columnconfigure(3, weight=0)
        selection_frame.columnconfigure(4, weight=0)

        ttk.Label(selection_frame, text="Model:").grid(
            row=0, column=0, sticky="w", padx=5, pady=5
        )
        self.model_combo = ttk.Combobox(
            selection_frame, textvariable=self.model_var, state="disabled", width=12
        )
        self.model_combo.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        self.model_combo.bind("<<ComboboxSelected>>", lambda _e: self._refresh_summary())

        ttk.Label(selection_frame, textvariable=self.model_status_var, foreground="#666666").grid(
            row=0, column=2, sticky="w", padx=5, pady=5
        )

        self.use_recommended_check = ttk.Checkbutton(
            selection_frame,
            text="Use recommended covariance  ",
            variable=self.use_recommended_covariance,
            command=self._on_recommended_covariance_toggle,
            state="disabled",
        )
        self.use_recommended_check.grid(row=0, column=3, sticky="w", padx=5, pady=5)

        self.export_button = ttk.Button(
            selection_frame,
            text="Export Summary...",
            command=self._export_summary,
            state="disabled",
        )
        self.export_button.grid(row=0, column=4, sticky="e", padx=5, pady=5)

        text_container = ttk.Frame(self)
        text_container.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        text_container.columnconfigure(0, weight=1)
        text_container.rowconfigure(0, weight=1)

        vscroll = ttk.Scrollbar(text_container, orient="vertical")
        hscroll = ttk.Scrollbar(text_container, orient="horizontal")

        self.summary_text = tk.Text(
            text_container,
            wrap="none",
            state="disabled",
            background="#ffffff",
            foreground="#1a1a1a",
            relief="solid",
            borderwidth=1,
            padx=10,
            pady=8,
        )
        try:
            fixed_font = tkfont.nametofont("TkFixedFont")
        except tk.TclError:
            fixed_font = tkfont.nametofont("TkDefaultFont")
        self.summary_text.configure(font=fixed_font)
        self.summary_text.grid(row=0, column=0, sticky="nsew")

        vscroll.configure(command=self.summary_text.yview)
        hscroll.configure(command=self.summary_text.xview)
        self.summary_text.configure(
            yscrollcommand=vscroll.set,
            xscrollcommand=hscroll.set,
        )
        vscroll.grid(row=0, column=1, sticky="ns")
        hscroll.grid(row=1, column=0, sticky="ew")

        self._render_summary_text()

    def set_available(self, available: bool) -> None:
        self.available = available
        state = "readonly" if available else "disabled"
        self.model_combo.configure(state=state)
        if self.export_button is not None:
            self.export_button.configure(state="normal" if available else "disabled")
        if self.use_recommended_check is not None:
            self.use_recommended_check.configure(
                state="normal" if available else "disabled"
            )

    def prepare_for_new_run(self) -> None:
        self.results_df = None
        self.internal_results = []
        self.external_results = []
        self.context = None
        self.config = None
        self._avg_iterations_per_seed = None
        self._max_iterations_per_seed = None
        self._iteration_mode = ITERATION_MODE_AUTO
        self._manual_iterations_per_seed = None
        self._models_found = None
        self._models_explored = None
        self._cpu_total_minutes = None
        self._covariance_diagnosis = None
        self.model_var.set("")
        self.model_combo.configure(values=())
        self.model_status_var.set("Select a model to summarize.")
        self._training_metric_order = []
        self._reset_info()
        self._populate_coefficients([])
        self._set_equation_placeholder("Equation unavailable (pending model selection.)")
        for key in self.additional_metric_vars:
            self.additional_metric_vars[key].set("-")
        self._set_training_metrics([])
        self._set_internal_metrics(None)
        self._set_external_metrics(None)
        self._y_randomization_results.clear()
        self._render_summary_text()
        self.set_available(False)

    def update_context(
        self, context: Optional[EPRSContext], config: Optional[EPRSConfig]
    ) -> None:
        self.context = context
        self.config = config
        if config is not None:
            self.update_iteration_preferences(
                getattr(config, "iterations_mode", ITERATION_MODE_AUTO),
                getattr(config, "max_iterations_per_seed", None),
            )
        self._refresh_summary()

    def update_training_results(self, df: Optional[pd.DataFrame]) -> None:
        if df is None or df.empty:
            self.results_df = None
            self.model_combo.configure(values=())
            self.model_var.set("")
            self.model_status_var.set("Select a model to summarize.")
            self._training_metric_order = []
            self._refresh_summary()
            self.set_available(False)
            return

        self.results_df = df.copy()
        model_ids: list[str] = []
        for value in df.get("Model", []):
            model_value = self.master_app._safe_int(value)
            if model_value is None:
                continue
            model_ids.append(str(int(model_value)))
        self.model_combo.configure(values=tuple(model_ids))
        if model_ids:
            previous = self.model_var.get()
            if previous and previous in model_ids:
                self.model_combo.set(previous)
            else:
                self.model_combo.set(model_ids[0])
            self.set_available(True)
        else:
            self.model_combo.set("")
            self.set_available(False)

        columns = [
            column
            for column in self.results_df.columns
            if column not in {"Model", "Variables", "N_var"}
        ]

        def move_after(items: list[str], target: str, anchor: str) -> list[str]:
            if target not in items or anchor not in items:
                return items
            filtered = [item for item in items if item != target]
            anchor_index = filtered.index(anchor)
            filtered.insert(anchor_index + 1, target)
            return filtered

        columns = move_after(columns, "s", "MAE")
        columns = move_after(columns, "R2_adj", "s")
        self._training_metric_order = columns
        self._refresh_summary()

    def update_internal_results(self, results: list[dict]) -> None:
        self.internal_results = [dict(row) for row in results or []]
        self._refresh_summary()

    def update_external_results(self, results: list[dict]) -> None:
        self.external_results = [dict(row) for row in results or []]
        self._refresh_summary()

    def update_avg_iterations_per_seed(self, value: Optional[object]) -> None:
        try:
            numeric = float(value)
            if np.isfinite(numeric):
                self._avg_iterations_per_seed = numeric
            else:
                self._avg_iterations_per_seed = None
        except (TypeError, ValueError):
            self._avg_iterations_per_seed = None
        resolved_description = self._resolve_search_method_description()
        self.info_vars["search_method"].set(resolved_description)
        self.info_vars["iterations"].set(self._resolve_iterations_per_seed_detail())

    def update_max_iterations_per_seed(self, value: Optional[object]) -> None:
        try:
            numeric = float(value)
            if np.isfinite(numeric):
                self._max_iterations_per_seed = numeric
            else:
                self._max_iterations_per_seed = None
        except (TypeError, ValueError):
            self._max_iterations_per_seed = None
        resolved_description = self._resolve_search_method_description()
        self.info_vars["search_method"].set(resolved_description)
        self.info_vars["iterations"].set(self._resolve_iterations_per_seed_detail())

    def update_iteration_preferences(
        self, mode: Optional[str], manual_value: Optional[object]
    ) -> None:
        normalized = (mode or ITERATION_MODE_AUTO).lower()
        if normalized not in {
            ITERATION_MODE_AUTO,
            ITERATION_MODE_MANUAL,
            ITERATION_MODE_CONVERGE,
        }:
            normalized = ITERATION_MODE_AUTO
        self._iteration_mode = normalized

        manual_numeric: Optional[int] = None
        try:
            if manual_value not in (None, ""):
                manual_numeric = int(float(manual_value))
                if manual_numeric <= 0:
                    manual_numeric = None
        except Exception:  # noqa: BLE001
            manual_numeric = None
        self._manual_iterations_per_seed = manual_numeric

        self.info_vars["search_method"].set(self._resolve_search_method_description())

    def update_y_randomization_result(self, result: YRandomizationResult) -> None:
        if result is None:
            return
        store = self._y_randomization_results.setdefault(result.model_id, {})
        store[result.metric_key] = result
        self._refresh_summary()

    def sync_y_randomization_results(
        self, results: Optional[dict[int, dict[str, YRandomizationResult]]]
    ) -> None:
        if not results:
            self._y_randomization_results.clear()
        else:
            self._y_randomization_results = {
                model_id: dict(store)
                for model_id, store in results.items()
                if store
            }
        self._refresh_summary()

    def _reset_info(self) -> None:
        self.info_vars["regression"].set("OLS")
        self.info_vars["search_method"].set(self._resolve_search_method_description())
        self.info_vars["target_metric"].set(self._resolve_target_metric_label())
        self.info_vars["models_found"].set(self._format_models_found())
        self.info_vars["models_reported"].set(self._format_models_reported())
        self.info_vars["models_explored"].set(
            self._format_models_explored()
        )
        self._significance_relation = "<"
        self.info_vars["significance"].set(self._resolve_significance_level())
        self.info_vars["cpu_search"].set(self._format_cpu_time(self._cpu_search_minutes))
        self.info_vars["cpu_total"].set(self._format_cpu_time(self._cpu_total_minutes))
        self.info_vars["nobs"].set("-")
        self.info_vars["df_resid"].set("-")
        self.info_vars["df_model"].set("-")
        self.info_vars["predictors"].set("-")
        dependent = getattr(self.context, "target_column", None)
        if not dependent and self.config is not None:
            dependent = getattr(self.config, "dependent_choice", None) or "-"
        self.info_vars["dependent"].set(dependent or "-")
        self.info_vars["kfold"].set(self._resolve_kfold_summary())
        self.info_vars["cov_type"].set("-")
        self._covariance_diagnosis = None
        self.info_vars["iterations"].set(self._resolve_iterations_per_seed_detail())

    def _on_recommended_covariance_toggle(self) -> None:
        self._refresh_summary()

    def update_models_found(self, value: Optional[int]) -> None:
        try:
            self._models_found = int(value) if value is not None else None
        except (TypeError, ValueError):
            self._models_found = None
        self.info_vars["models_found"].set(self._format_models_found())
        self._render_summary_text()

    def get_models_found(self) -> Optional[int]:
        return self._models_found

    def _format_models_found(self) -> str:
        if self._models_found is None:
            return "-"
        return self._format_count(self._models_found)

    def update_models_reported(self, value: Optional[int]) -> None:
        try:
            self._models_reported = int(value) if value is not None else None
        except (TypeError, ValueError):
            self._models_reported = None
        self.info_vars["models_reported"].set(self._format_models_reported())
        self._render_summary_text()

    def get_models_reported(self) -> Optional[int]:
        return self._models_reported

    def _format_models_reported(self) -> str:
        if self._models_reported is None:
            return "-"
        return self._format_count(self._models_reported)

    def update_models_explored(self, value: Optional[int]) -> None:
        try:
            self._models_explored = int(value) if value is not None else None
        except (TypeError, ValueError):
            self._models_explored = None
        self.info_vars["models_explored"].set(self._format_models_explored())
        self._render_summary_text()

    def get_models_explored(self) -> Optional[int]:
        return self._models_explored

    def _format_models_explored(self) -> str:
        if self._models_explored is None:
            return "-"
        return self._format_count(self._models_explored)

    def get_avg_iterations_per_seed(self) -> Optional[float]:
        return self._avg_iterations_per_seed

    def get_max_iterations_per_seed(self) -> Optional[float]:
        return self._max_iterations_per_seed

    def _format_y_randomization_summary(self, model_id: Optional[int]) -> str:
        if model_id is None:
            return "Unavailable (select a model first)."
        store = self._y_randomization_results.get(model_id)
        if not store:
            return "Not determined."

        parts: list[str] = []
        metric_labels: tuple[tuple[str, str], ...] = (
            ("R2", R_SQUARED_SYMBOL),
            ("R2_loo", f"{Q_SQUARED_SYMBOL} (LOO)"),
        )
        for metric_key, label in metric_labels:
            result = store.get(metric_key)
            if result is None:
                if metric_key != "R2_loo":
                    parts.append(f"{label}: Not determined.")
                continue
            if not result.completed:
                total = self._format_count(result.permutations_requested)
                done = self._format_count(result.completed_permutations)
                parts.append(
                    f"{label}: Running ({done}/{total} permutations)."
                )
                continue
            p_value = self._format_p_value(result.p_value)
            total = self._format_count(result.permutations_requested)
            segments = [
                f"p-value {p_value} using {label} ({total} permutations)",
            ]
            parts.append(". ".join(segments))

        return "\n".join(parts) if parts else "Not determined."

    def _refresh_summary(self) -> None:
        model_id = self._get_selected_model_id()
        self._covariance_diagnosis = None
        self.info_vars["search_method"].set(self._resolve_search_method_description())
        self.info_vars["target_metric"].set(self._resolve_target_metric_label())
        self.info_vars["models_found"].set(self._format_models_found())
        self.info_vars["significance"].set(self._resolve_significance_level())
        self.info_vars["kfold"].set(self._resolve_kfold_summary())
        if model_id is None or self.results_df is None or self.results_df.empty:
            self._reset_info()
            self._populate_coefficients([])
            self._set_equation_placeholder("Equation unavailable (pending model selection.)")
            self._set_training_metrics([])
            self._set_internal_metrics(None)
            self._set_external_metrics(None)
            for key in self.additional_metric_vars:
                self.additional_metric_vars[key].set("-")
            self._render_summary_text()
            return

        row = self._get_training_row(model_id)
        if row is None:
            self.model_status_var.set("Selected model not found in results.")
            self._reset_info()
            self._populate_coefficients([])
            self._set_equation_placeholder("Equation unavailable (model not found.)")
            self._set_training_metrics([])
            self._set_internal_metrics(None)
            self._set_external_metrics(None)
            for key in self.additional_metric_vars:
                self.additional_metric_vars[key].set("-")
            self._render_summary_text()
            return

        variables = self.master_app._normalize_variables(row.get("Variables"))
        preview = ", ".join(variables[:5]) if variables else "-"
        if variables and len(variables) > 5:
            preview += ", ..."
        self.model_status_var.set(f"Predictors ({len(variables)}): {preview}")

        self.info_vars["regression"].set("OLS")
        self.info_vars["search_method"].set(self._resolve_search_method_description())
        self.info_vars["target_metric"].set(self._resolve_target_metric_label())
        self.info_vars["iterations"].set(self._resolve_iterations_per_seed_detail())
        self.info_vars["models_found"].set(self._format_models_found())
        self.info_vars["significance"].set(self._resolve_significance_level())
        self.info_vars["cpu_total"].set(self._format_cpu_time(self._cpu_total_minutes))
        self.info_vars["predictors"].set(", ".join(variables) if variables else "-")
        dependent = getattr(self.context, "target_column", None)
        if not dependent and self.config is not None:
            dependent = getattr(self.config, "dependent_choice", None)
        self.info_vars["dependent"].set(dependent or "-")
        self.info_vars["kfold"].set(self._resolve_kfold_summary())

        training_metrics = [
            (
                column,
                self._get_training_label(column),
                self._format_metric_value(row.get(column)),
            )
            for column in self._training_metric_order
        ]
        r_max_value = self._compute_r_max(variables)
        training_metrics.append(
            (
                "r_max",
                self.TRAINING_LABELS.get("r_max", "|r|max"),
                self._format_metric_value(r_max_value),
            )
        )
        self._set_training_metrics(training_metrics)

        self._set_internal_metrics(self._find_metric_row(self.internal_results, model_id))
        self._set_external_metrics(self._find_metric_row(self.external_results, model_id))

        if not self.context or getattr(self.context, "y_np", None) is None:
            self.info_vars["nobs"].set("-")
            self.info_vars["df_resid"].set("-")
            self.info_vars["df_model"].set(f"- ({len(variables)})")
            self.info_vars["cov_type"].set("-")
            self._update_significance_relation_from_result(None)
            self.info_vars["significance"].set(self._resolve_significance_level())
            self._populate_coefficients([])
            self._set_equation_placeholder(
                "Equation unavailable (dataset context required)."
            )
            for key in self.additional_metric_vars:
                self.additional_metric_vars[key].set("-")
            self._render_summary_text()
            return

        try:
            result, param_names = self._fit_model(variables)
        except Exception:
            self.info_vars["nobs"].set("-")
            self.info_vars["df_resid"].set("-")
            self.info_vars["df_model"].set(f"- ({len(variables)})")
            self.info_vars["cov_type"].set("-")
            self._update_significance_relation_from_result(None)
            self.info_vars["significance"].set(self._resolve_significance_level())
            self._populate_coefficients([])
            self._set_equation_placeholder("Equation unavailable (model could not be fitted.)")
            for key in self.additional_metric_vars:
                self.additional_metric_vars[key].set("-")
            self._render_summary_text()
            return

        self.info_vars["nobs"].set(self._format_count(result.nobs))
        self.info_vars["df_resid"].set(self._format_count(result.df_resid))
        df_model_value = self._format_count(result.df_model)
        predictor_count = len(variables)
        if df_model_value == str(predictor_count):
            df_model_display = df_model_value
        else:
            df_model_display = f"{df_model_value} ({predictor_count})"
        self.info_vars["df_model"].set(df_model_display)
        cov_label = _format_covariance_type_label(getattr(result, "cov_type", "-"))
        if self.use_recommended_covariance.get() and self._covariance_diagnosis:
            cov_label = f"{cov_label} (Recommended)"
        self.info_vars["cov_type"].set(cov_label or "-")

        standardized_result, standardized_actual = self._compute_standardized_result(variables)
        self._populate_coefficients(
            self._build_coefficients_rows(result, param_names, standardized_result)
        )
        self._update_equations(result, param_names, dependent or "Dependent")
        self.info_vars["significance"].set(self._resolve_significance_level())
        self._update_additional_metrics(
            result,
            variables,
            standardized_result=standardized_result,
            standardized_actual=standardized_actual,
        )

    def _format_count(self, value: float) -> str:
        if value is None or not np.isfinite(value):
            return "-"
        if abs(value - round(value)) < 1e-9:
            return str(int(round(value)))
        return f"{value:.4f}"

    def _find_metric_row(self, rows: list[dict], model_id: int) -> Optional[dict]:
        for row in rows:
            model_value = self.master_app._safe_int(row.get("Model"))
            if model_value == model_id:
                return row
        return None

    def _set_internal_metrics(self, row: Optional[dict]) -> None:
        if row:
            enabled, folds, _repeats = self._get_kfold_settings()
            metrics: list[tuple[str, str, str]] = []
            for column in self.INTERNAL_ORDER:
                if column not in row:
                    continue
                label = self.INTERNAL_LABELS.get(column, column)
                if (
                    column in {"R2_kfold", "RMSE_kfold", "s_kfold", "MAE_kfold"}
                    and enabled
                    and folds is not None
                ):
                    label = label.replace("k-fold", f"{folds}-fold")
                metrics.append((column, label, self._format_metric_value(row.get(column))))
        else:
            metrics = []

        self.internal_metrics = metrics

    def _set_external_metrics(self, row: Optional[dict]) -> None:
        if row:
            metrics = [
                (
                    self.EXTERNAL_LABELS.get(column, column),
                    self._format_metric_value(row.get(column)),
                )
                for column in self.EXTERNAL_ORDER
                if column in row
            ]
        else:
            metrics = []

        self.external_metrics = metrics

    def update_cpu_times(
        self,
        search_minutes: Optional[float],
        total_minutes: Optional[float],
    ) -> None:
        self._cpu_search_minutes = self._coerce_cpu_minutes(search_minutes)
        self._cpu_total_minutes = self._coerce_cpu_minutes(total_minutes)
        self.info_vars["cpu_search"].set(
            self._format_cpu_time(self._cpu_search_minutes)
        )
        self.info_vars["cpu_total"].set(self._format_cpu_time(self._cpu_total_minutes))
        self._render_summary_text()

    @staticmethod
    def _coerce_cpu_minutes(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        return numeric if np.isfinite(numeric) else None

    def _export_summary(self) -> None:
        if self.summary_text is None:
            messagebox.showwarning(
                "Export summary", "Summary information is not available to export.",
            )
            return

        content = self.summary_text.get("1.0", "end-1c").strip()
        if not content:
            messagebox.showwarning(
                "Export summary", "Summary information is not available to export.",
            )
            return

        model_id = self._get_selected_model_id()
        default_name = (
            f"model_{model_id}_summary.txt" if model_id is not None else "model_summary.txt"
        )
        path = filedialog.asksaveasfilename(
            title="Save summary",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=default_name,
        )
        if not path:
            return

        try:
            Path(path).write_text(content + "\n", encoding="utf-8")
        except OSError as exc:
            messagebox.showerror(
                "Export summary", f"Unable to export summary information:\n{exc}",
            )
            return

        self.master_app._append_log(f"Summary exported to {path}.\n")

    def _set_training_metrics(self, metrics: list[tuple[str, str, str]]) -> None:
        self.training_metrics = list(metrics)

    def _format_metric_value(self, value: object) -> str:
        if value is None:
            return "-"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return str(value)
        if not np.isfinite(numeric):
            return "-"
        return f"{numeric:.4f}"

    def _format_percent_value(self, value: object) -> str:
        if value is None:
            return "-"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "-"
        if not np.isfinite(numeric):
            return "-"
        return f"{numeric:.2f}%"

    def _format_r2_threshold(self, value: object) -> str:
        if value is None:
            return "none"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "none"
        if not np.isfinite(numeric):
            return "none"
        return f"{numeric:.2f}"

    def _format_p_value(self, value: object) -> str:
        if value is None:
            return "-"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "-"
        if not np.isfinite(numeric):
            return "-"
        if numeric < 0.0001:
            return f"{numeric:.1e}"
        return self._format_metric_value(numeric)

    def _format_cpu_time(self, value: Optional[float]) -> str:
        if value is None:
            return "-"
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return "-"
        if not np.isfinite(numeric):
            return "-"
        total_seconds = numeric * 60.0
        return f"{total_seconds:.2f} s ({numeric:.2f} min)"

    def _format_confint_quantile_label(self, value: float) -> str:
        if not np.isfinite(value):
            return "-"
        text = f"{value:.6f}".rstrip("0").rstrip(".")
        return text if text else "0"

    def _get_kfold_settings(self) -> tuple[bool, Optional[int], Optional[int]]:
        settings = getattr(self.master_app, "kfold_settings", None)
        if isinstance(settings, dict) and settings.get("enabled"):
            folds = self.master_app._safe_int(settings.get("folds"))
            repeats = self.master_app._safe_int(settings.get("repeats"))
            if folds is not None and repeats is not None:
                return True, folds, repeats

        validation_tab = getattr(self.master_app, "validation_tab", None)
        if validation_tab is not None:
            try:
                enabled = bool(validation_tab.use_kfold.get())
            except Exception:  # noqa: BLE001
                enabled = False
            if enabled:
                folds = self.master_app._safe_int(validation_tab.kfold_folds_var.get())
                repeats = self.master_app._safe_int(validation_tab.kfold_repeats_var.get())
                if folds is not None and repeats is not None:
                    return True, folds, repeats

        return False, None, None

    def _resolve_search_method_description(self) -> str:
        config = self.config
        if config is None:
            return "-"
        method_key = getattr(config, "method", None)
        display_value = METHOD_KEY_TO_DISPLAY.get(method_key, None)
        if isinstance(display_value, str) and display_value:
            base_display = display_value.split(" (", 1)[0].strip()
        else:
            base_display = str(method_key).strip() if method_key else ""
        if not base_display:
            base_display = "-"
        if method_key == "eprs":
            max_predictors = self._resolve_max_predictors()
            details: list[str] = [
                f"{base_display} (Max predictors per model: {max_predictors})"
            ]
            seeds = getattr(config, "n_seeds", None)
            if seeds is not None:
                details.append(f"Number of seeds: {seeds}")
            seed_size = getattr(config, "seed_size", None)
            if seed_size is not None:
                details.append(f"Seed size: {seed_size}")
            iter_detail = self._resolve_iterations_per_seed_value()
            if iter_detail:
                details.append(f"Max iterations/seed: {iter_detail}")
            return "\n".join(details)
        elif method_key == "all_subsets":
            max_predictors = self._resolve_max_predictors()
            base_display = f"{base_display or 'All subsets'} (Max predictors per model: {max_predictors})"

        return base_display or "-"

    def _resolve_iterations_per_seed_detail(self) -> Optional[str]:
        iter_detail = self._resolve_iterations_per_seed_value()
        if iter_detail is None:
            return "-"
        return str(iter_detail)

    def _resolve_iterations_per_seed_value(self) -> Optional[str]:
        mode = getattr(self, "_iteration_mode", ITERATION_MODE_AUTO) or ITERATION_MODE_AUTO
        mode = mode.lower()
        config_mode = None
        config_value = None
        if self.config is not None:
            config_mode = getattr(self.config, "iterations_mode", None)
            config_value = getattr(self.config, "max_iterations_per_seed", None)
        if mode not in {
            ITERATION_MODE_AUTO,
            ITERATION_MODE_MANUAL,
            ITERATION_MODE_CONVERGE,
        }:
            mode = config_mode or ITERATION_MODE_AUTO

        if mode == ITERATION_MODE_CONVERGE:
            return "Until converge"

        if mode == ITERATION_MODE_MANUAL:
            manual_value = self._manual_iterations_per_seed
            if manual_value is None:
                manual_value = config_value
            if manual_value is not None:
                formatted = self._format_count(manual_value)
                return f"{formatted} (Mode: Manual)"
            return None

        auto_limit = getattr(self, "_max_iterations_per_seed", None)
        if auto_limit is None:
            auto_limit = config_value
        if auto_limit is not None:
            formatted = self._format_count(auto_limit)
            return f"{formatted} (Mode: Auto)"
        return None

    def _resolve_target_metric_label(self) -> str:
        config = self.config
        if config is None:
            return "target metric"
        target_key = getattr(config, "target_metric", None)
        if target_key is None:
            return "target metric"
        return TARGET_METRIC_DISPLAY.get(target_key, str(target_key))

    def _resolve_max_predictors(self) -> str:
        config = self.config
        if config is None:
            return "-"
        max_vars = getattr(config, "max_vars", None)
        try:
            numeric = int(max_vars)
        except (TypeError, ValueError):
            return "-"
        if numeric <= 0:
            return "-"
        return str(numeric)

    def _resolve_significance_level(self) -> str:
        config = self.config
        if config is None:
            return "-"
        signif = getattr(config, "signif_lvl", None)
        try:
            numeric = float(signif)
        except (TypeError, ValueError):
            return "-"
        if not np.isfinite(numeric):
            return "-"
        text = f"{numeric:.6f}".rstrip("0").rstrip(".")
        formatted = text if text else "0"
        relation = getattr(self, "_significance_relation", "<") or "<"
        if relation not in {"<", "=", ">"}:
            relation = "<"
        return f"{relation}{formatted}"

    def _get_significance_alpha(self) -> float:
        default_alpha = 0.05
        config = self.config
        if config is None:
            return default_alpha
        signif = getattr(config, "signif_lvl", None)
        try:
            numeric = float(signif)
        except (TypeError, ValueError):
            return default_alpha
        if 0 < numeric < 1:
            return numeric
        return default_alpha

    def _update_significance_relation_from_result(self, result: Any) -> None:
        relation = "<"
        if result is not None:
            alpha = self._get_significance_alpha()
            pvalues = getattr(result, "pvalues", None)
            try:
                values = np.asarray(pvalues, dtype=float).reshape(-1)
            except Exception:
                values = np.asarray([], dtype=float)
            if values.size:
                finite = values[np.isfinite(values)]
                if finite.size:
                    tolerance = max(1e-9, alpha * 1e-6)
                    max_p = float(np.max(finite))
                    if max_p < alpha - tolerance:
                        relation = "<"
                    elif max_p <= alpha + tolerance:
                        relation = "="
                    else:
                        relation = ">"
        self._significance_relation = relation

    def _resolve_kfold_summary(self) -> str:
        enabled, folds, repeats = self._get_kfold_settings()
        if not enabled or folds is None or repeats is None:
            return "-"
        repeat_label = "repeat" if repeats == 1 else "repeats"
        return f"{folds} folds × {repeats} {repeat_label}"

    def _compute_r_max(self, variables: list[str]) -> Optional[float]:
        context = self.context
        if context is None:
            return None
        abs_corr = getattr(context, "abs_corr", None)
        if abs_corr is None or not isinstance(abs_corr, pd.DataFrame):
            return None
        subset = [var for var in variables if var in abs_corr.index and var in abs_corr.columns]
        if not subset:
            return None
        matrix = abs_corr.loc[subset, subset].to_numpy(dtype=float)
        if matrix.size == 0:
            return None
        np.fill_diagonal(matrix, np.nan)
        with np.errstate(invalid="ignore"):
            try:
                max_abs = float(np.nanmax(matrix))
            except ValueError:
                return None
        if not np.isfinite(max_abs):
            return None
        return max_abs

    def _get_training_row(self, model_id: int) -> Optional[pd.Series]:
        if self.results_df is None or self.results_df.empty:
            return None
        for _, candidate in self.results_df.iterrows():
            model_value = self.master_app._safe_int(candidate.get("Model"))
            if model_value == model_id:
                return candidate
        return None

    def _get_selected_model_id(self) -> Optional[int]:
        text = self.model_var.get().strip()
        if not text:
            return None
        return self.master_app._safe_int(text)

    def _get_training_label(self, column: str) -> str:
        return self.TRAINING_LABELS.get(column, column)

    def _fit_model(self, variables: list[str]):
        if self.context is None or getattr(self.context, "y_np", None) is None:
            raise ValueError("Context is not available.")
        Xm = take(self.context, variables) if variables else np.empty((len(self.context.y_np), 0))
        exog = np.c_[np.ones((Xm.shape[0], 1), dtype=Xm.dtype), Xm]
        base_result = sm.OLS(self.context.y_np, exog).fit()
        config = self.config if isinstance(self.config, EPRSConfig) else None
        use_recommended = bool(self.use_recommended_covariance.get())
        if use_recommended:
            diagnosis = _recommend_covariance_type(base_result)
            notes = diagnosis.setdefault("notes", [])
            cov_type = diagnosis.get("cov_type") or "nonrobust"
            cov_kwds = diagnosis.get("cov_kwds") or {}
            if not isinstance(cov_kwds, dict):
                cov_kwds = {}
            if cov_type == "nonrobust":
                result = base_result
            else:
                try:
                    result = base_result.get_robustcov_results(cov_type=cov_type, **cov_kwds)
                except Exception:  # noqa: BLE001
                    result = base_result
                    cov_type = "nonrobust"
                    notes.append("Unable to apply recommended covariance; using nonrobust instead.")
            applied_cov_type = getattr(result, "cov_type", cov_type)
            diagnosis["applied_cov_type"] = applied_cov_type
            self._covariance_diagnosis = diagnosis
        else:
            result = _apply_covariance_type(base_result, config, self.context, exog)
            self._covariance_diagnosis = None
        self._update_significance_relation_from_result(result)
        param_names = ["Intercept"] + list(variables)
        return result, param_names

    def _build_coefficients_rows(
        self, result, param_names: list[str], standardized_result=None
    ) -> list[tuple[str, ...]]:
        params = result.params
        std_err = result.bse
        t_values = result.tvalues
        p_values = result.pvalues
        ci = result.conf_int(alpha=self._get_significance_alpha())
        contribution_by_param: dict[str, float] = {}
        if standardized_result is not None:
            try:
                std_params = np.asarray(standardized_result.params, dtype=np.float64)
                if std_params.size == len(param_names):
                    if "Intercept" in param_names:
                        intercept_idx = param_names.index("Intercept")
                        std_coefs = np.delete(std_params, intercept_idx)
                        names = [name for name in param_names if name != "Intercept"]
                    else:
                        std_coefs = std_params
                        names = list(param_names)
                    abs_vals = np.abs(std_coefs)
                    total = float(np.sum(abs_vals))
                    if total > 0.0 and len(names) == len(abs_vals):
                        for name, value in zip(names, abs_vals):
                            contribution_by_param[name] = float(value / total * 100.0)
            except Exception:
                contribution_by_param = {}
        rows: list[tuple[str, ...]] = []
        for idx, name in enumerate(param_names):
            coef_text = self._format_metric_value(params[idx])
            std_err_text = self._format_metric_value(std_err[idx])
            t_text = self._format_metric_value(t_values[idx])
            p_text = self._format_p_value(p_values[idx])
            ci_low = self._format_metric_value(ci[idx, 0])
            ci_high = self._format_metric_value(ci[idx, 1])
            contrib_text = self._format_percent_value(contribution_by_param.get(name))
            rows.append(
                (name, coef_text, std_err_text, t_text, p_text, ci_low, ci_high, contrib_text)
            )
        return rows

    def _populate_coefficients(self, rows: list[tuple[str, ...]]) -> None:
        self.coefficient_rows = list(rows)

    def _set_equation_placeholder(self, message: str) -> None:
        self.equation_with_errors_var.set(message)
        self.equation_without_errors_var.set(message)

    def _update_equations(
        self, result, param_names: list[str], dependent_label: str
    ) -> None:
        params = result.params
        std_err = result.bse
        names = list(param_names)
        terms_with_errors: list[tuple[str, str]] = []
        terms_without_errors: list[tuple[str, str]] = []

        ordered_indices = list(range(len(names)))
        if "Intercept" in names:
            intercept_idx = names.index("Intercept")
            ordered_indices = [idx for idx in ordered_indices if idx != intercept_idx] + [
                intercept_idx
            ]

        for idx in ordered_indices:
            name = names[idx]
            coef = float(params[idx])
            err = float(std_err[idx])
            if name == "Intercept":
                base = f"({abs(coef):.4f} ± {err:.4f})"
                simple = f"{abs(coef):.4f}"
            else:
                base = f"({abs(coef):.4f} ± {err:.4f}) {name}"
                simple = f"{abs(coef):.4f} {name}"
            sign = "-" if coef < 0 else "+"
            terms_with_errors.append((sign, base))
            terms_without_errors.append((sign, simple))

        equation_with_errors = self._combine_equation_terms(
            dependent_label, terms_with_errors
        )
        equation_without_errors = self._combine_equation_terms(
            dependent_label, terms_without_errors
        )
        self.equation_with_errors_var.set(equation_with_errors)
        self.equation_without_errors_var.set(equation_without_errors)

    def _combine_equation_terms(
        self, dependent_label: str, terms: list[tuple[str, str]]
    ) -> str:
        if not terms:
            return f"{dependent_label} = 0"
        pieces: list[str] = []
        first_sign, first_term = terms[0]
        prefix = "-" if first_sign == "-" else ""
        pieces.append(f"{dependent_label} = {prefix}{first_term}")
        for sign, term in terms[1:]:
            pieces.append(f" {sign} {term}")
        return "".join(pieces)

    def _compute_standardized_result(
        self, variables: list[str]
    ) -> tuple[Optional[Any], Optional[np.ndarray]]:
        if self.context is None:
            return None, None
        try:
            y_values = np.asarray(self.context.y_np, dtype=np.float64)
        except Exception:
            return None, None
        if y_values.size == 0:
            return None, None
        try:
            y_std = self._standardize_vector(y_values)
            Xm = (
                take(self.context, variables)
                if variables
                else np.empty((y_std.shape[0], 0), dtype=np.float64)
            )
            Xm = np.asarray(Xm, dtype=np.float64)
            X_std = self._standardize_matrix(Xm)
            exog_std = np.c_[np.ones((X_std.shape[0], 1), dtype=np.float64), X_std]
            standardized_result = sm.OLS(y_std, exog_std).fit()
            standardized_actual = y_std
        except Exception:
            return None, None

        if self.use_recommended_covariance.get() and self._covariance_diagnosis:
            cov_type = self._covariance_diagnosis.get("applied_cov_type")
            if not cov_type:
                cov_type = self._covariance_diagnosis.get("cov_type")
            cov_type_key = str(cov_type or "").strip().lower()
            if cov_type_key and cov_type_key != "nonrobust":
                cov_kwds = self._covariance_diagnosis.get("cov_kwds") or {}
                if not isinstance(cov_kwds, dict):
                    cov_kwds = {}
                try:
                    standardized_result = standardized_result.get_robustcov_results(
                        cov_type=cov_type,
                        **cov_kwds,
                    )
                except Exception:
                    pass
        return standardized_result, standardized_actual

    def _update_additional_metrics(
        self,
        result,
        variables: list[str],
        *,
        standardized_result=None,
        standardized_actual: Optional[np.ndarray] = None,
    ) -> None:
        if standardized_result is None or standardized_actual is None:
            standardized_result, standardized_actual = self._compute_standardized_result(
                variables
            )

        metric_values: dict[str, float] = {
            key: float("nan") for _, key in self.ADDITIONAL_METRICS
        }

        def _try_assign(key: str, candidate: Any, *, allow_overwrite: bool = False) -> None:
            if candidate is None:
                return
            try:
                numeric = float(candidate)
            except (TypeError, ValueError):
                return
            if not np.isfinite(numeric):
                return
            current = metric_values.get(key)
            if allow_overwrite or current is None or not np.isfinite(current):
                metric_values[key] = numeric

        def _collect_metrics(
            metrics_source,
            actual_for_ccc: Optional[np.ndarray],
            overwrite: Collection[str] = (),
        ) -> None:
            if metrics_source is None:
                return
            def _safe_to_float(value: Any) -> float:
                try:
                    return float(value)
                except (TypeError, ValueError):
                    return float("nan")

            def _extract_test_values(result_obj: Any, field_order: Sequence[Any]) -> list[Any]:
                if result_obj is None:
                    return [float("nan")] * len(field_order)

                try:
                    as_sequence = list(result_obj)  # type: ignore[arg-type]
                except TypeError:
                    as_sequence = None

                extracted: list[Any] = []
                for idx, options in enumerate(field_order):
                    if isinstance(options, str):
                        options = (options,)
                    value: Any = None
                    for attr in options:
                        if hasattr(result_obj, attr):
                            value = getattr(result_obj, attr)
                            break
                    if value is None and as_sequence is not None and idx < len(as_sequence):
                        value = as_sequence[idx]
                    if value is None:
                        value = float("nan")
                    extracted.append(value)
                return extracted

            resid_array: Optional[np.ndarray]
            resid = getattr(metrics_source, "resid", None)
            if resid is not None:
                try:
                    resid_array = np.asarray(resid, dtype=np.float64).reshape(-1)
                    if resid_array.size == 0:
                        resid_array = None
                except Exception:
                    resid_array = None
            else:
                resid_array = None

            omni_stat = float("nan")
            omni_p = float("nan")
            jb_stat = float("nan")
            jb_p = float("nan")
            skew = float("nan")
            kurtosis = float("nan")

            if resid_array is not None and resid_array.size >= 8:
                try:
                    omni_stat, omni_p = stattools_omni_normtest(resid_array)
                except Exception:
                    omni_stat, omni_p = float("nan"), float("nan")
            if resid_array is not None:
                try:
                    jb_stat, jb_p, skew, kurtosis = stattools_jarque_bera(resid_array)
                except Exception:
                    jb_stat, jb_p, skew, kurtosis = (
                        float("nan"),
                        float("nan"),
                        float("nan"),
                        float("nan"),
                    )

            if (not np.isfinite(omni_stat)) or (not np.isfinite(omni_p)):
                try:
                    omni_result = metrics_source.omni_normtest()
                except Exception:
                    omni_result = None
                else:
                    fallback_stat, fallback_p = _extract_test_values(
                        omni_result,
                        (
                            ("statistic", "stat"),
                            ("pvalue", "p_value", "prob"),
                        ),
                    )
                    if not np.isfinite(omni_stat):
                        omni_stat = _safe_to_float(fallback_stat)
                    if not np.isfinite(omni_p):
                        omni_p = _safe_to_float(fallback_p)

            if (
                not np.isfinite(jb_stat)
                or not np.isfinite(jb_p)
                or not np.isfinite(skew)
                or not np.isfinite(kurtosis)
            ):
                try:
                    jb_result = metrics_source.jarque_bera()
                except Exception:
                    jb_result = None
                else:
                    fallback_stat, fallback_p, fallback_skew, fallback_kurt = _extract_test_values(
                        jb_result,
                        (
                            ("statistic", "stat", "jb_stat"),
                            ("pvalue", "p_value", "prob"),
                            ("skew", "skewness"),
                            ("kurtosis", "excess_kurtosis", "kurt"),
                        ),
                    )
                    if not np.isfinite(jb_stat):
                        jb_stat = _safe_to_float(fallback_stat)
                    if not np.isfinite(jb_p):
                        jb_p = _safe_to_float(fallback_p)
                    if not np.isfinite(skew):
                        skew = _safe_to_float(fallback_skew)
                    if not np.isfinite(kurtosis):
                        kurtosis = _safe_to_float(fallback_kurt)

            if resid_array is not None:
                try:
                    durbin_watson = sm.stats.durbin_watson(resid_array)
                except Exception:
                    durbin_watson = float("nan")
            elif resid is not None:
                try:
                    durbin_watson = sm.stats.durbin_watson(resid)
                except Exception:
                    durbin_watson = float("nan")
            else:
                durbin_watson = float("nan")

            fitted = getattr(metrics_source, "fittedvalues", None)
            if fitted is not None and actual_for_ccc is not None:
                lin_ccc_value = self._compute_lin_ccc(actual_for_ccc, fitted)
            else:
                lin_ccc_value = float("nan")

            values = {
                "f_stat": getattr(metrics_source, "fvalue", float("nan")),
                "f_pvalue": getattr(metrics_source, "f_pvalue", float("nan")),
                "log_like": getattr(metrics_source, "llf", float("nan")),
                "aic": getattr(metrics_source, "aic", float("nan")),
                "bic": getattr(metrics_source, "bic", float("nan")),
                "omnibus": omni_stat,
                "omnibus_p": omni_p,
                "skew": skew,
                "kurtosis": kurtosis,
                "durbin_watson": durbin_watson,
                "jarque_bera": jb_stat,
                "jarque_bera_p": jb_p,
                "cond_no": getattr(metrics_source, "condition_number", float("nan")),
                "lin_ccc": lin_ccc_value,
            }

            for key, candidate in values.items():
                _try_assign(key, candidate, allow_overwrite=key in overwrite)

        if standardized_result is not None and standardized_actual is not None:
            _collect_metrics(standardized_result, standardized_actual)

        context_actual = (
            np.asarray(self.context.y_np, dtype=np.float64)
            if self.context is not None
            else None
        )
        _collect_metrics(
            result,
            context_actual,
            overwrite={"log_like", "aic", "bic"},
        )

        for key, var in self.additional_metric_vars.items():
            value = metric_values.get(key)
            if key in {"f_pvalue", "omnibus_p", "jarque_bera_p"}:
                display = self._format_p_value(value)
            else:
                display = self._format_metric_value(value)
            var.set(display)

        self._render_summary_text()

    def _render_summary_text(self) -> None:
        if self.summary_text is None:
            return

        selected_model_id = self._get_selected_model_id()
        info_rows: list[tuple[str, str]] = []
        for label, key in self.INFO_FIELDS:
            if key == "kfold":
                continue
            if key == "iterations":
                continue
            value = self.info_vars[key].get()
            display_label = label
            if key == "models_found":
                metric_label = self._resolve_target_metric_label()
                threshold_display = "-"
                if self.config is not None:
                    threshold = getattr(self.config, "tm_cutoff", None)
                    if threshold is not None:
                        threshold_display = self._format_r2_threshold(threshold)
                display_label = (
                    label.replace("{metric target}", metric_label).replace(
                        "{tm_cutoff}", threshold_display
                    )
                )
            if key == "predictors" and selected_model_id is not None:
                display_label = f"{label} (Model {selected_model_id})"
            if key == "predictors" and value:
                value = self._wrap_text_block(value, 60)
            info_rows.append((display_label, value))
            if key == "predictors":
                summary_label = "Y-Randomization"
                if selected_model_id is not None:
                    summary_label = f"{summary_label} (Model {selected_model_id})"
                info_rows.append(
                    (
                        summary_label,
                        self._format_y_randomization_summary(selected_model_id),
                    )
                )
        if self.config and getattr(self.config, "clip_predictions", None) is not None:
            try:
                low, high = self.config.clip_predictions
            except (TypeError, ValueError):  # noqa: BLE001
                clip_text = "-"
            else:
                clip_text = f"{self._format_metric_value(low)} to {self._format_metric_value(high)}"
            info_rows.append(("Predictions clipped", clip_text))
        info_table = self._build_table(
            [],
            info_rows,
            [28, 56],
            "Information not available.",
        )
        alpha = self._get_significance_alpha()
        lower_quantile = alpha / 2.0
        upper_quantile = 1.0 - lower_quantile
        coefficients_table = self._build_table(
            [
                "Term ",
                "  Coef.",
                "Std. Err.",
                "  \U0001D461",
                "   \U0001D45D",
                f"   [{self._format_confint_quantile_label(lower_quantile)}",
                f" {self._format_confint_quantile_label(upper_quantile)}]",
                "% Contrib.",
            ],
            self.coefficient_rows,
            [10, 8, 8, 7, 7, 9, 9, 10],
            "Coefficients not available.",
            extra_padding=1,
            column_align=[
                "left",
                "right",
                "center",
                "right",
                "right",
                "right",
                "right",
                "right",
            ],
            header_align=[
                "center",
                "center",
                "right",
                "center",
                "center",
                "center",
                "center",
                "center",
            ],
        )
        coefficients_width = self._measure_block_width(coefficients_table)

        result_marker = self.RESULT_SECTION_MARKER

        covariance_heading = ""
        covariance_table_block = ""
        covariance_details_block = ""
        if self.use_recommended_covariance.get() and self._covariance_diagnosis:
            diagnosis = self._covariance_diagnosis
            tests = diagnosis.get("tests") or {}

            def _diag_value(key: str, *, p_value: bool = False, count: bool = False) -> str:
                value = tests.get(key)
                if value is None:
                    return "-"
                if p_value:
                    return self._format_p_value(value)
                if count:
                    try:
                        numeric = float(value)
                    except (TypeError, ValueError):  # noqa: BLE001
                        return "-"
                    return self._format_count(numeric)
                return self._format_metric_value(value)

            recommended_label = _format_covariance_type_label(diagnosis.get("cov_type", "-"))
            applied_label = _format_covariance_type_label(
                diagnosis.get("applied_cov_type", diagnosis.get("cov_type", "-"))
            )
            diag_rows = [
                ("Recommended type", recommended_label or "-"),
                ("Applied type", applied_label or "-"),
                ("Breusch–Pagan p-value", _diag_value("p_bp", p_value=True)),
                ("White test p-value", _diag_value("p_white", p_value=True)),
                ("Leverage threshold", _diag_value("lev_threshold")),
                ("Maximum leverage", _diag_value("max_leverage")),
                (
                    "High leverage count",
                    _diag_value("n_high_leverage", count=True),
                ),
            ]
            covariance_table_block = self._build_table(
                [],
                diag_rows,
                [32, 22],
                "Diagnostics not available.",
                extra_padding=1,
                column_align=["left", "right"],
            )
            heading_width = max(
                self._measure_block_width(covariance_table_block),
                len(f"{result_marker}COVARIANCE DIAGNOSTICS"),
            )
            covariance_heading = self._build_section_heading(
                "Covariance Diagnostics",
                width=heading_width,
                marker=result_marker,
            )
            detail_lines: list[str] = []
            reasons = diagnosis.get("reasons") or []
            notes = diagnosis.get("notes") or []
            if reasons:
                detail_lines.append("Reasons:")
                detail_lines.extend([f"  - {text}" for text in reasons])
            if notes:
                if detail_lines:
                    detail_lines.append("")
                detail_lines.append("Notes:")
                detail_lines.extend([f"  - {text}" for text in notes])
            if detail_lines:
                detail_text = "\n".join(detail_lines)
                covariance_details_block = self._wrap_text_block(detail_text, 90)

        if self.training_metrics:
            primary_keys = {"R2", "RMSE", "MAE", "s"}
            secondary_keys = {"R2_adj", "VIF_max", "VIF_avg", "r_max"}

            def collect_group(keys: set[str]) -> list[tuple[str, str, str]]:
                return [item for item in self.training_metrics if item[0] in keys]

            primary_metrics = collect_group(primary_keys)
            secondary_metrics = collect_group(secondary_keys)
            used_keys = {item[0] for item in primary_metrics + secondary_metrics}
            remaining_metrics = [
                item for item in self.training_metrics if item[0] not in used_keys
            ]

            def build_training_block(
                metrics: list[tuple[str, str, str]]
            ) -> Optional[str]:
                if not metrics:
                    return None
                headers = [label for _column, label, _value in metrics]
                values = [value for _column, _label, value in metrics]
                table = self._build_table(
                    headers,
                    [tuple(values)],
                    None,
                    "Metrics not available.",
                    extra_padding=2,
                    data_align="center",
                    header_align=["center"] * len(headers),
                )
                width = self._measure_block_width(table)
                border = "-" * width if width > 0 else ""
                if border:
                    return "\n".join([border, table, border])
                return table

            training_blocks = [
                block
                for block in (
                    build_training_block(primary_metrics),
                    build_training_block(secondary_metrics),
                    build_training_block(remaining_metrics),
                )
                if block
            ]
            training_table = "\n".join(training_blocks) if training_blocks else ""
        else:
            training_table = ""
        if not training_table:
            training_table = "Metrics not available."
        training_table_width = self._measure_block_width(training_table)
        training_heading_width = max(
            training_table_width, len(f"{result_marker}TRAINING")
        )

        if self.internal_metrics:
            column_order: list[str] = []
            metric_map: dict[str, dict[str, str]] = {}
            method_order: list[str] = []
            method_labels: dict[str, str] = {}

            def _append_fold_asterisk(label: str) -> str:
                match = re.search(r"fold\b", label, flags=re.IGNORECASE)
                if not match:
                    return f"{label}*"
                end = match.end()
                return f"{label[:end]}*{label[end:]}"

            for column, label, value in self.internal_metrics:
                if column.endswith("_loo"):
                    method_key = "loo"
                elif column.endswith("_kfold"):
                    method_key = "kfold"
                else:
                    method_key = column

                match = re.search(r"\(([^()]*)\)\s*$", label)
                if match:
                    base_label = label[: match.start()].rstrip()
                    method_label = match.group(1).strip()
                else:
                    base_label = label
                    method_label = method_key.replace("_", " ").strip().upper()

                if not base_label:
                    base_label = label or column
                if method_key not in method_order:
                    method_order.append(method_key)
                if method_key not in method_labels:
                    resolved_label = method_label or method_key.replace("_", " ").strip().upper()
                    if method_key == "kfold":
                        resolved_label = _append_fold_asterisk(resolved_label)
                    method_labels[method_key] = resolved_label
                if base_label not in column_order:
                    column_order.append(base_label)
                metric_map.setdefault(base_label, {})[method_key] = value

            if column_order and method_order:
                headers = [""] + column_order
                column_align = ["center"] * len(headers)
                table_rows: list[tuple[str, ...]] = []
                for method_key in method_order:
                    row_values: list[str] = [method_labels.get(method_key, method_key)]
                    for base_label in column_order:
                        row_values.append(metric_map.get(base_label, {}).get(method_key, "-"))
                    table_rows.append(tuple(row_values))
                internal_table = self._build_table(
                    headers,
                    table_rows,
                    None,
                    "Metrics not available.",
                    extra_padding=2,
                    data_align="center",
                    column_align=column_align,
                    header_align=column_align,
                )
            else:
                internal_table = "Metrics not available."
        else:
            internal_table = "Metrics not available."
        internal_table_width = self._measure_block_width(internal_table)
        internal_heading_width = max(
            internal_table_width, len(f"{result_marker}INTERNAL VALIDATION")
        )

        if self.external_metrics:
            external_headers = [label for label, _value in self.external_metrics]
            external_values = [value for _label, value in self.external_metrics]
            external_table = self._build_table(
                external_headers,
                [tuple(external_values)],
                None,
                "Metrics not available.",
                extra_padding=2,
                data_align="center",
            )
        else:
            external_table = "Metrics not available."
        external_table_width = self._measure_block_width(external_table)
        external_heading_width = max(
            external_table_width, len(f"{result_marker}EXTERNAL VALIDATION")
        )

        additional_rows = [
            (label, self.additional_metric_vars[key].get())
            for label, key in self.ADDITIONAL_METRICS
        ]
        half = max((len(additional_rows) + 1) // 2, 1)
        left_entries = additional_rows[:half]
        right_entries = additional_rows[half:]
        if len(left_entries) < half:
            left_entries.extend([("", "")] * (half - len(left_entries)))
        if len(right_entries) < half:
            right_entries.extend([("", "")] * (half - len(right_entries)))
        combined_rows: list[tuple[str, ...]] = []
        for idx in range(half):
            left_label, left_value = left_entries[idx]
            right_label, right_value = right_entries[idx]
            combined_rows.append(
                (
                    left_label,
                    left_value,
                    "",
                    right_label,
                    right_value,
                )
            )
        additional_table = self._build_table(
            [],
            combined_rows,
            [22, 10, 1, 22, 10],
            "Metrics not available.",
            extra_padding=0,
            column_spacing=" ",
            column_align=["left", "right", "left", "left", "right"],
        )
        additional_table_width = self._measure_block_width(additional_table)
        additional_heading_width = max(
            additional_table_width, len(f"{result_marker}TRAINING ADDITIONAL METRICS")
        )

        target_title_width = max(self._title_center_width, self._title_width)

        summary_title = self._pad_block_to_width(
            self._build_title_line("Summary View", symbol="/", width=30),
            target_title_width,
        )
        model_log_title = self._pad_block_to_width(
            self._build_title_line("Model Overview", width=self._title_width),
            target_title_width,
        )
        coefficients_title = self._pad_block_to_width(
            self._build_title_line("Coefficients Table", width=self._title_width),
            target_title_width,
        )
        equation_heading_width = self._title_width
        equation_content_width = 90
        equation_with_errors_title = self._pad_block_to_width(
            self._build_title_line(
                "Equation (coefficients ± error)",
                width=equation_heading_width,
            ),
            target_title_width,
        )
        equation_with_errors_text = self._wrap_text_block(
            self.equation_with_errors_var.get(), equation_content_width
        )
        equation_without_errors_title = self._pad_block_to_width(
            self._build_title_line(
                "Equation (coefficients only)",
                width=equation_heading_width,
            ),
            target_title_width,
        )
        equation_without_errors_text = self._wrap_text_block(
            self.equation_without_errors_var.get(), equation_content_width
        )
        results_title = self._pad_block_to_width(
            self._build_title_line("Results Metrics", width=self._title_width),
            target_title_width,
        )

        ascii_block = self._pad_block_to_width(
            "\n".join(self.ASCII_ART), target_title_width
        )
        license_block = self._pad_block_to_width(
            "\n".join(self.LICENSE_BLOCK), target_title_width
        )

        sections: list[str] = [
            ascii_block,
            license_block,
            "",
            summary_title,
            "",
            model_log_title,
            info_table,
        ]
        if covariance_heading and covariance_table_block:
            sections.append("")
            sections.append(
                self._pad_block_to_width(
                    covariance_heading, target_title_width, align="left"
                )
            )
            sections.append(
                self._pad_block_to_width(
                    covariance_table_block, target_title_width, align="left"
                )
            )
            if covariance_details_block:
                sections.append(
                    self._pad_block_to_width(
                        covariance_details_block, target_title_width, align="left"
                    )
                )
        sections.extend(
            [
                "",
                coefficients_title,
                coefficients_table,
                "",
                equation_with_errors_title,
                equation_with_errors_text,
                "",
                equation_without_errors_title,
                equation_without_errors_text,
                "",
                results_title,
            ]
        )
        training_heading = self._build_section_heading(
            "Training",
            width=training_heading_width,
            marker=result_marker,
        )
        training_title_line = training_heading.splitlines()[0] if training_heading else ""
        sections.append(
            self._pad_block_to_width(
                training_title_line, target_title_width, align="left"
            )
        )
        sections.append(
            self._pad_block_to_width(
                training_table, target_title_width, align="left"
            )
        )
        sections.append("")

        internal_heading = self._build_section_heading(
            "Internal Validation",
            width=internal_heading_width,
            marker=result_marker,
        )
        internal_border = "-" * internal_heading_width
        sections.append(
            self._pad_block_to_width(
                internal_heading, target_title_width, align="left"
            )
        )
        sections.append(
            self._pad_block_to_width(
                internal_table, target_title_width, align="left"
            )
        )

        sections.append(
            self._pad_block_to_width(
                internal_border, target_title_width, align="left"
            )
        )
        kfold_repeat_note = ""
        kfold_enabled, _folds, repeats = self._get_kfold_settings()
        if kfold_enabled and repeats is not None:
            if repeats == 1:
                repeat_message = "* Average of 1 independent repetition."
            else:
                repeat_message = (
                    f"* Average of {repeats} independent repetitions."
                )
            kfold_repeat_note = repeat_message
        if kfold_repeat_note:
            sections.append(
                self._pad_block_to_width(
                    kfold_repeat_note, target_title_width, align="left"
                )
            )
        sections.append("")

        external_heading = self._build_section_heading(
            "External Validation",
            width=external_heading_width,
            marker=result_marker,
        )
        external_border = "-" * external_heading_width
        sections.append(
            self._pad_block_to_width(
                external_heading, target_title_width, align="left"
            )
        )
        sections.append(
            self._pad_block_to_width(
                external_table, target_title_width, align="left"
            )
        )
        sections.append(
            self._pad_block_to_width(
                external_border, target_title_width, align="left"
            )
        )
        sections.append("")

        additional_heading = self._build_section_heading(
            "Training Additional Metrics",
            width=additional_heading_width,
            marker=result_marker,
        )
        additional_border = "-" * additional_heading_width
        sections.append(
            self._pad_block_to_width(
                additional_heading, target_title_width, align="left"
            )
        )
        sections.append(
            self._pad_block_to_width(
                additional_table, target_title_width, align="left"
            )
        )
        sections.append(
            self._pad_block_to_width(
                additional_border, target_title_width, align="left"
            )
        )

        notation_candidates: tuple[tuple[str, str, str], ...] = (
            ("Coef.", "Coefficient estimate", "Coef."),
            ("Std. Err.", "Standard error of the coefficient", "Std. Err."),
            ("\U0001D461", "t-statistic for the coefficient", "\U0001D461"),
            ("\U0001D45D", "Two-tailed p-value for the coefficient", "\U0001D45D"),
            ("R²", "Coefficient of determination", "R²"),
            ("adj-R²", "Adjusted coefficient of determination", "adj-R²"),
            (
                f"{Q_SQUARED_SYMBOL}F1",
                "External predictive ability (F1, centered on the training mean)",
                f"{Q_SQUARED_SYMBOL}F1",
            ),
            (
                f"{Q_SQUARED_SYMBOL}F2",
                "External predictive ability (F2, equivalent to external R²)",
                f"{Q_SQUARED_SYMBOL}F2",
            ),
            (
                f"{Q_SQUARED_SYMBOL}F3",
                "External predictive ability (F3, scaled by the training variance)",
                f"{Q_SQUARED_SYMBOL}F3",
            ),
            ("RMSE", "Root mean squared error", "RMSE"),
            ("MAE", "Mean absolute error", "MAE"),
            (
                STANDARD_ERROR_SYMBOL,
                "Residual standard error",
                STANDARD_ERROR_SYMBOL,
            ),
            (
                "VIFmax/avg",
                "Maximum and average variance inflation factor",
                "VIFmax",
            ),
            ("|r|max", "Maximum absolute correlation among predictors", "|r|max"),
            ("LOO", "Leave-one-out cross-validation", "LOO"),
            ("k-fold", "k-fold cross-validation", "k-fold"),
            ("AIC", "Akaike information criterion", "AIC"),
            ("BIC", "Bayesian information criterion", "BIC"),
            ("Cond. No", "Condition number of the design matrix", "Cond. No"),
            ("CCC", "Lin's concordance correlation coefficient", "CCC"),
            ("Df", "Degrees of freedom", "Df "),
            ("OLS", "Ordinary least squares", "OLS"),
            ("HC1", "Heteroskedasticity-consistent covariance", "HC1"),
            (
                "HC2",
                "Heteroskedasticity-consistent covariance with leverage adjustment",
                "HC2",
            ),
            (
                "HC3",
                "Heteroskedasticity-consistent covariance with strong leverage correction",
                "HC3",
            ),
        )

        search_text = "\n".join(sections)
        notation_rows: list[tuple[int, str, str]] = []
        for label, description, token in notation_candidates:
            if token in search_text:
                notation_rows.append((search_text.index(token), label, description))

        notation_rows.sort(key=lambda item: item[0])
        if notation_rows:
            hc_labels = {"HC1", "HC2", "HC3"}
            notation_rows = [
                row for row in notation_rows if row[1] not in hc_labels
            ] + [row for row in notation_rows if row[1] in hc_labels]

        if notation_rows:
            notation_table = self._build_table(
                [],
                [
                    (
                        label,
                        self._wrap_text_block(description, 70),
                    )
                    for _idx, label, description in notation_rows
                ],
                [14, 60],
                "Notation not available.",
                extra_padding=2,
                column_align=["left", "left"],
            )
            notation_table_width = self._measure_block_width(notation_table)
            notation_heading_width = max(
                notation_table_width, len(f"{result_marker}NOTATION")
            )
            notation_heading = self._build_section_heading(
                "Notation", width=notation_heading_width, marker=result_marker
            )
            sections.append("")
            sections.append(
                self._pad_block_to_width(
                    notation_heading, target_title_width, align="left"
                )
            )
            sections.append(
                self._pad_block_to_width(
                    notation_table, target_title_width, align="left"
                )
            )

        content = "\n".join(sections).rstrip() + "\n"
        self.summary_text.configure(state="normal")
        self.summary_text.delete("1.0", "end")
        self.summary_text.insert("1.0", content)
        self.summary_text.configure(state="disabled")
        self.summary_text.yview_moveto(0.0)

    def _build_table(
        self,
        headers: list[str],
        rows: list[tuple[str, ...]],
        widths: Optional[list[int]],
        empty_message: str,
        extra_padding: int = 2,
        column_spacing: str = "  ",
        *,
        data_align: str = "left",
        column_align: Optional[list[str]] = None,
        header_align: Optional[list[str]] = None,
    ) -> str:
        min_widths = list(widths or [])
        column_count = max(len(headers), len(min_widths), 1)
        for row in rows:
            column_count = max(column_count, len(row))

        if not rows:
            fallback = [empty_message] + [""] * (max(column_count - 1, 0))
            normalized_rows: list[tuple[str, ...]] = [tuple(fallback)]
        else:
            normalized_rows = [
                tuple(
                    str(row[idx]) if idx < len(row) and row[idx] is not None else ""
                    for idx in range(column_count)
                )
                for row in rows
            ]

        if len(min_widths) < column_count:
            min_widths.extend([0] * (column_count - len(min_widths)))

        if column_align is not None and len(column_align) < column_count:
            column_align = column_align + [data_align] * (column_count - len(column_align))
        if header_align is not None and len(header_align) < column_count:
            header_align = header_align + [data_align] * (column_count - len(header_align))

        computed_widths: list[int] = []
        for idx in range(column_count):
            header_len = len(headers[idx]) if idx < len(headers) else 0
            data_len = max((len(row[idx]) for row in normalized_rows), default=0)
            base = min_widths[idx] if idx < len(min_widths) else 0
            width = max(base, header_len, data_len)
            if idx < column_count - 1:
                width += max(extra_padding, 0)
            computed_widths.append(width)

        lines: list[str] = []
        if headers:
            header_cells: list[str] = []
            for idx in range(column_count):
                header_text = headers[idx] if idx < len(headers) else ""
                width = computed_widths[idx]
                align_mode = data_align
                if header_align is not None and idx < len(header_align):
                    align_mode = header_align[idx] or data_align
                elif column_align is not None and idx < len(column_align):
                    align_mode = column_align[idx] or data_align
                if align_mode == "center":
                    header_cells.append(header_text.center(width))
                elif align_mode == "right":
                    header_cells.append(header_text.rjust(width))
                else:
                    header_cells.append(header_text.ljust(width))
            lines.append(column_spacing.join(header_cells).rstrip())

        def _format_cell(text: str, width: int, align_mode: str) -> str:
            if align_mode == "center":
                return text.center(width)
            if align_mode == "right":
                return text.rjust(width)
            return text.ljust(width)

        for row in normalized_rows:
            formatted_columns: list[list[str]] = []
            column_specs: list[tuple[int, str]] = []
            max_lines = 0
            for idx in range(column_count):
                cell = row[idx]
                width = computed_widths[idx]
                align_mode = data_align
                if column_align is not None and idx < len(column_align):
                    align_mode = column_align[idx] or data_align
                segments = cell.splitlines() or [""]
                formatted = [_format_cell(segment, width, align_mode) for segment in segments]
                formatted_columns.append(formatted)
                column_specs.append((width, align_mode))
                if len(formatted) > max_lines:
                    max_lines = len(formatted)

            for idx, (width, align_mode) in enumerate(column_specs):
                column_lines = formatted_columns[idx]
                pad_value = _format_cell("", width, align_mode)
                while len(column_lines) < max_lines:
                    column_lines.append(pad_value)

            for line_idx in range(max_lines):
                pieces = [column[line_idx] for column in formatted_columns]
                lines.append(column_spacing.join(pieces).rstrip())

        return "\n".join(lines)

    @staticmethod
    def _measure_block_width(block: str) -> int:
        return max((len(line) for line in block.splitlines()), default=0)

    def _pad_block_to_width(
        self,
        block: str,
        target_width: int,
        *,
        align: str = "center",
    ) -> str:
        if target_width <= 0:
            return block
        padded_lines: list[str] = []
        for line in block.splitlines():
            if not line:
                padded_lines.append("")
                continue
            if len(line) >= target_width:
                padded_lines.append(line)
                continue
            if align == "left":
                padded_lines.append(line.ljust(target_width))
            elif align == "right":
                padded_lines.append(line.rjust(target_width))
            else:
                total_padding = target_width - len(line)
                left_padding = total_padding // 2
                right_padding = total_padding - left_padding
                padded_lines.append(
                    f"{' ' * left_padding}{line}{' ' * right_padding}"
                )
        return "\n".join(padded_lines)

    @staticmethod
    def _wrap_text_block(text: str, width: int) -> str:
        if width <= 0:
            return text
        wrapped_lines: list[str] = []
        for line in text.splitlines() or [""]:
            segments = textwrap.wrap(
                line,
                width=width,
                break_long_words=False,
                break_on_hyphens=False,
            )
            if not segments:
                wrapped_lines.append("")
            else:
                wrapped_lines.extend(segments)
        return "\n".join(wrapped_lines)

    def _build_title_line(
        self, title: str, symbol: str = "=", width: Optional[int] = None
    ) -> str:
        width = width or self._title_width
        title_text = title.upper()
        border = symbol * width
        if symbol == "-":
            return "\n".join([border, title_text.center(width), border])
        inner_width = max(width - 2, 1)
        return "\n".join(
            [border, f"{symbol}{title_text.center(inner_width)}{symbol}", border]
        )

    def _build_section_heading(
        self,
        title: str,
        symbol: str = "-",
        width: Optional[int] = None,
        *,
        center_title: bool = False,
        marker: str = "",
    ) -> str:
        width = width or self._title_width
        title_text = f"{marker}{title.upper()}"
        width = max(width, len(title_text))
        if center_title:
            title_line = title_text.center(width)
        else:
            title_line = title_text.ljust(width)
        border = symbol * width
        return "\n".join([title_line, border])

    @staticmethod
    def _standardize_vector(values: np.ndarray) -> np.ndarray:
        if values.size == 0:
            return values.astype(np.float64, copy=False)
        arr = np.asarray(values, dtype=np.float64)
        mean = float(np.mean(arr))
        centered = arr - mean
        std = float(np.std(arr))
        if not np.isfinite(std) or std == 0.0:
            return centered
        return centered / std

    @staticmethod
    def _standardize_matrix(values: np.ndarray) -> np.ndarray:
        if values.size == 0:
            return values.astype(np.float64, copy=False)
        arr = np.asarray(values, dtype=np.float64)
        means = np.mean(arr, axis=0)
        centered = arr - means
        std = np.std(arr, axis=0)
        safe_std = np.where(np.isfinite(std) & (std != 0.0), std, 1.0)
        return centered / safe_std

    @staticmethod
    def _compute_lin_ccc(actual: np.ndarray, predicted: np.ndarray) -> float:
        if actual.size == 0 or predicted.size == 0 or actual.shape != predicted.shape:
            return float("nan")
        actual = np.asarray(actual, dtype=np.float64)
        predicted = np.asarray(predicted, dtype=np.float64)
        if not np.all(np.isfinite(actual)) or not np.all(np.isfinite(predicted)):
            return float("nan")
        mean_actual = float(np.mean(actual))
        mean_pred = float(np.mean(predicted))
        var_actual = float(np.var(actual, ddof=1))
        var_pred = float(np.var(predicted, ddof=1))
        if var_actual == 0.0 and var_pred == 0.0:
            return float("nan")
        try:
            covariance = float(np.cov(actual, predicted, ddof=1)[0, 1])
        except Exception:
            return float("nan")
        denominator = var_actual + var_pred + (mean_actual - mean_pred) ** 2
        if denominator == 0.0:
            return float("nan")
        return 2.0 * covariance / denominator

class ObservationDiagnosticsTab(ttk.Frame):
    BASE_COLUMN_LAYOUT: tuple[tuple[str, str, int, int], ...] = (
        ("Observation", "Observation", 80, 12),
        ("Set", "Set", 70, 8),
        ("Actual", "Actual", 60, 12),
        ("Predicted", "Predicted", 70, 12),
        ("Predicted_LOO", "Predicted (LOO)", 90, 12),
        ("Residual", "Residual", 60, 12),
        ("Residual_LOO", "Residual (LOO)", 90, 12),
        ("Z_value", "Z value", 60, 10),
        ("Leverage", "Leverage", 80, 10),
        ("StdPredResid", "Std. residual", 120, 12),
        ("StdPredResid_LOO", "Std. residual (LOO)", 150, 12),
    )

    FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
        ("Training only", "training"),
        ("Training and testing", "both"),
        ("Testing only", "testing"),
    )

    def __init__(self, notebook: ttk.Notebook, master_app: "MLRXApp"):
        super().__init__(notebook)
        self.master_app = master_app
        self.results_df: Optional[pd.DataFrame] = None
        self.current_model_id: Optional[int] = None
        self.current_variables: list[str] = []
        self.current_observation_df: Optional[pd.DataFrame] = None
        self.current_correlation_df: Optional[pd.DataFrame] = None
        self.current_correlation_with_target_df: Optional[pd.DataFrame] = None
        self.available = False

        self.model_var = tk.StringVar()
        self.model_status_var = tk.StringVar(value="Select a model to inspect.")
        self.filter_var = tk.StringVar(value="training")

        self.option_controls: list[tk.Widget] = []
        self._extra_label_column: Optional[str] = None
        self.obs_export_button: Optional[ttk.Button] = None
        self.corr_export_button: Optional[ttk.Button] = None
        self.corr_tree: Optional[ttk.Treeview] = None
        self.corr_columns: list[str] = []
        self.corr_include_target_var = tk.BooleanVar(value=False)
        self.corr_include_target_check: Optional[ttk.Checkbutton] = None

        self._build_ui()
        self.set_available(False)

    def _build_ui(self):
        selection_frame = ttk.LabelFrame(self, text="Model selection")
        selection_frame.pack(fill="x", padx=10, pady=(10, 5))
        selection_frame.columnconfigure(2, weight=1)

        ttk.Label(selection_frame, text="Model:").grid(
            row=0, column=0, sticky="w", padx=5, pady=5
        )
        self.model_combo = ttk.Combobox(
            selection_frame,
            textvariable=self.model_var,
            state="disabled",
            width=12,
        )
        self.model_combo.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        self.model_combo.bind("<<ComboboxSelected>>", self._handle_model_change)

        ttk.Label(
            selection_frame, textvariable=self.model_status_var, foreground="#666666"
        ).grid(row=0, column=2, sticky="w", padx=5, pady=5)

        ttk.Label(selection_frame, text="Dataset:").grid(
            row=1, column=0, sticky="w", padx=5, pady=(0, 5)
        )
        filter_values = [label for label, _value in self.FILTER_OPTIONS]
        self.filter_combo = ttk.Combobox(
            selection_frame,
            state="disabled",
            values=filter_values,
            width=20,
        )
        self.filter_combo.grid(row=1, column=1, sticky="w", padx=5, pady=(0, 5))
        if filter_values:
            self.filter_combo.set(self.FILTER_OPTIONS[0][0])
        self.filter_combo.bind("<<ComboboxSelected>>", self._handle_filter_change)
        self.option_controls.append(self.filter_combo)

        diagnostics_frame = ttk.LabelFrame(self, text="Observation diagnostics")
        diagnostics_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        diagnostics_frame.columnconfigure(0, weight=1)
        diagnostics_frame.rowconfigure(1, weight=1)

        obs_controls = ttk.Frame(diagnostics_frame)
        obs_controls.grid(row=0, column=0, sticky="e", padx=5, pady=(5, 0))
        self.obs_export_button = ttk.Button(
            obs_controls,
            text="Export CSV",
            command=self._export_observations_csv,
            state="disabled",
            width=12,
        )
        self.obs_export_button.pack(anchor="e")

        self.columns: list[str] = []
        self.column_headings: dict[str, str] = {}
        self.numeric_columns = {
            "Actual",
            "Predicted",
            "Predicted_LOO",
            "Residual",
            "Residual_LOO",
            "Z_value",
            "Leverage",
            "StdPredResid",
            "StdPredResid_LOO",
        }

        self.observation_tree = ttk.Treeview(
            diagnostics_frame,
            columns=(),
            show="headings",
        )
        try:
            self._data_font = tkfont.nametofont(self.observation_tree.cget("font"))
        except tk.TclError:  # pragma: no cover - fallback when font unavailable
            self._data_font = tkfont.nametofont("TkDefaultFont")

        vscroll = ttk.Scrollbar(
            diagnostics_frame, orient="vertical", command=self.observation_tree.yview
        )
        self.observation_tree.configure(yscrollcommand=vscroll.set)
        self.observation_tree.grid(row=1, column=0, sticky="nsew", padx=(5, 0), pady=5)
        vscroll.grid(row=1, column=1, sticky="ns", padx=(0, 5), pady=5)

        self._configure_observation_columns()

        corr_frame = ttk.LabelFrame(self, text="Model variable correlation matrix")
        corr_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        corr_frame.columnconfigure(0, weight=1)
        corr_frame.rowconfigure(1, weight=1)

        corr_controls = ttk.Frame(corr_frame)
        corr_controls.grid(row=0, column=0, sticky="ew", padx=5, pady=(5, 0))
        corr_controls.columnconfigure(0, weight=1)
        self.corr_include_target_check = ttk.Checkbutton(
            corr_controls,
            text="Include dependent variable",
            variable=self.corr_include_target_var,
            command=self._handle_correlation_include_target_toggle,
            state="disabled",
        )
        self.corr_include_target_check.grid(row=0, column=0, sticky="w", padx=(0, 10))
        self.corr_export_button = ttk.Button(
            corr_controls,
            text="Export CSV",
            command=self._export_correlation_csv,
            state="disabled",
            width=12,
        )
        self.corr_export_button.grid(row=0, column=1, sticky="e")

        self.corr_tree = ttk.Treeview(corr_frame, show="headings")
        self.corr_tree.grid(row=1, column=0, sticky="nsew", padx=(5, 0), pady=5)
        corr_vscroll = ttk.Scrollbar(
            corr_frame, orient="vertical", command=self.corr_tree.yview
        )
        corr_vscroll.grid(row=1, column=1, sticky="ns", padx=(0, 5), pady=5)
        corr_hscroll = ttk.Scrollbar(
            corr_frame, orient="horizontal", command=self.corr_tree.xview
        )
        corr_hscroll.grid(row=2, column=0, sticky="ew", padx=(5, 0), pady=(0, 5))
        self.corr_tree.configure(
            yscrollcommand=corr_vscroll.set,
            xscrollcommand=corr_hscroll.set,
        )

    def prepare_for_new_run(self):
        self.results_df = None
        self.current_model_id = None
        self.current_variables = []
        self.current_observation_df = None
        self.current_correlation_df = None
        self.current_correlation_with_target_df = None
        self.model_var.set("")
        self.model_status_var.set("Select a model to inspect.")
        self.filter_var.set("training")
        if hasattr(self, "filter_combo"):
            self.filter_combo.set(self.FILTER_OPTIONS[0][0])
        self._configure_observation_columns()
        self._clear_table()
        self._reset_correlation()
        self.set_available(False)

    def _configure_observation_columns(self, df: Optional[pd.DataFrame] = None) -> None:
        if not hasattr(self, "observation_tree"):
            return

        base_layout = list(self.BASE_COLUMN_LAYOUT)
        base_columns = [identifier for identifier, *_ in base_layout]
        base_headings = {
            identifier: heading for identifier, heading, *_ in base_layout
        }
        layout_map = {
            identifier: (min_width, float(weight))
            for identifier, _heading, min_width, weight in base_layout
        }

        extra_column = None
        if df is not None:
            for column in df.columns:
                if column not in base_columns:
                    extra_column = column
                    break

        self._extra_label_column = extra_column

        columns: list[str] = ["Observation"]
        if extra_column:
            columns.append(extra_column)
        columns.extend([column for column in base_columns if column != "Observation"])

        self.columns = columns
        self.column_headings = dict(base_headings)
        if extra_column:
            self.column_headings[extra_column] = extra_column

        self.observation_tree.configure(columns=columns)

        diagnostics_specs: list[_ResultsColumnLayout] = []
        for column in columns:
            if column == extra_column and extra_column is not None:
                heading_text = self.column_headings.get(column, column)
                self.observation_tree.heading(column, text=heading_text, anchor="center")
                self.observation_tree.column(
                    column,
                    anchor="center",
                    width=120,
                    minwidth=20,
                    stretch=False,
                )
                diagnostics_specs.append(_ResultsColumnLayout(column, 120, 0.0))
                continue

            heading_text = self.column_headings.get(column, column)
            min_width, weight = layout_map.get(column, (80, 0.0))
            self.observation_tree.heading(column, text=heading_text, anchor="center")
            anchor = "center" if column in self.numeric_columns else "center"
            self.observation_tree.column(
                column,
                anchor=anchor,
                width=min_width,
                minwidth=20,
                stretch=False,
            )
            diagnostics_specs.append(
                _ResultsColumnLayout(column, min_width, float(weight))
            )

        self.master_app.replace_results_tree_layout(  # type: ignore[attr-defined]
            self.observation_tree,
            tuple(diagnostics_specs),
        )

    def set_available(self, available: bool):
        self.available = available
        combo_state = "readonly" if available else "disabled"
        self.model_combo.configure(state=combo_state)
        for widget in self.option_controls:
            widget.configure(state=combo_state)
        if not available:
            self.model_status_var.set("Select a model to inspect.")
        self._sync_correlation_toggle()
        self._update_export_buttons()

    def update_training_results(self, df: Optional[pd.DataFrame]):
        if df is None or df.empty:
            self.results_df = None
            self.model_combo.configure(values=())
            self.model_var.set("")
            self._clear_table()
            self._reset_correlation()
            self.set_available(False)
            return

        self.results_df = df.copy()
        models: list[str] = []
        for value in df["Model"].tolist():
            try:
                models.append(str(int(value)))
            except (TypeError, ValueError):
                continue
        self.model_combo.configure(values=tuple(models))
        if models:
            previous = self.model_var.get()
            if previous and previous in models:
                self.model_combo.set(previous)
            else:
                self.model_combo.set(models[0])
            self.set_available(True)
            self.apply_holdout_default(self.master_app.holdout_ready)
            self._handle_model_change()
        else:
            self.model_var.set("")
            self._clear_table()
            self._reset_correlation()
            self.set_available(False)

    def _handle_model_change(self, _event=None):
        if not self.available:
            return
        model_text = self.model_var.get().strip()
        if not model_text:
            self._clear_table()
            self._reset_correlation()
            self.model_status_var.set("Select a model to inspect.")
            return
        try:
            model_id = int(model_text)
        except ValueError:
            self.model_status_var.set("Invalid model identifier.")
            self._clear_table()
            self._reset_correlation()
            return

        if self.results_df is None or self.results_df.empty:
            self.model_status_var.set("No training results available.")
            self._clear_table()
            self._reset_correlation()
            return

        row = self.results_df[self.results_df["Model"] == model_id]
        if row.empty:
            self.model_status_var.set("Model not found in results.")
            self._clear_table()
            self._reset_correlation()
            return

        variables = self.master_app._normalize_variables(row.iloc[0]["Variables"])  # noqa: SLF001
        if not variables:
            self.model_status_var.set("Model does not include predictor variables.")
            self._clear_table()
            self._reset_correlation()
            return

        diagnostics_df, _hat_threshold = self.master_app.get_observation_diagnostics(model_id)

        if diagnostics_df is None or diagnostics_df.empty:
            self.model_status_var.set("Diagnostics unavailable for this model.")
            self._clear_table()
            self._reset_correlation()
            return

        self.current_model_id = model_id
        self.current_variables = variables
        self._configure_observation_columns(diagnostics_df)
        self.current_observation_df = diagnostics_df.copy()
        corr_df = self.master_app.get_model_correlation(model_id)
        self.current_correlation_df = corr_df.copy() if corr_df is not None else None
        corr_with_target = self.master_app.get_model_correlation(
            model_id, include_target=True
        )
        self.current_correlation_with_target_df = (
            corr_with_target.copy() if corr_with_target is not None else None
        )
        self._sync_correlation_toggle()

        preview = ", ".join(variables[:5])
        if len(variables) > 5:
            preview += ", ..."
        self.model_status_var.set(f"Predictors ({len(variables)}): {preview}")

        self._render_observation_table()
        self._render_correlation_table()

    def _handle_filter_change(self, _event=None):
        mapping = {label: value for label, value in self.FILTER_OPTIONS}
        selection = self.filter_combo.get()
        desired = mapping.get(selection, "training")
        if desired in {"both", "testing"}:
            if not self.master_app.ensure_holdout_data_available():  # noqa: SLF001
                self.filter_var.set("training")
                if hasattr(self, "filter_combo"):
                    self.filter_combo.set(self.FILTER_OPTIONS[0][0])
                return
        self.filter_var.set(desired)
        self._render_observation_table()

    def apply_holdout_default(self, ready: bool) -> None:
        value_to_label = {value: label for label, value in self.FILTER_OPTIONS}
        if ready:
            if self.filter_var.get() == "training":
                self.filter_var.set("both")
                label = value_to_label.get("both")
                if label and hasattr(self, "filter_combo"):
                    self.filter_combo.set(label)
                if self.current_observation_df is not None:
                    self._render_observation_table()
        else:
            if self.filter_var.get() != "training":
                self.filter_var.set("training")
                label = value_to_label.get("training")
                if label and hasattr(self, "filter_combo"):
                    self.filter_combo.set(label)
                if self.current_observation_df is not None:
                    self._render_observation_table()

    def _apply_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        mode = self.filter_var.get()
        if mode == "training":
            return df[df["Set"] == "Training"].copy()
        if mode == "testing":
            return df[df["Set"] == "Testing"].copy()
        return df.copy()

    def _clear_table(self):
        if hasattr(self, "observation_tree"):
            self.observation_tree.delete(*self.observation_tree.get_children())
            for identifier, heading in self.column_headings.items():
                self.observation_tree.heading(identifier, text=heading, anchor="center")
            self._update_column_widths({})

    def _reset_correlation(self) -> None:
        self.current_correlation_df = None
        self.current_correlation_with_target_df = None
        self.corr_include_target_var.set(False)
        self._clear_correlation_table()
        self._update_export_buttons()
        self._sync_correlation_toggle()

    def _sync_correlation_toggle(self) -> None:
        if self.corr_include_target_check is None:
            return
        has_target = (
            self.available
            and self.current_correlation_with_target_df is not None
            and not self.current_correlation_with_target_df.empty
        )
        if not has_target:
            self.corr_include_target_var.set(False)
        state = "normal" if has_target else "disabled"
        self.corr_include_target_check.configure(state=state)

    def _render_observation_table(self):
        self._clear_table()
        if self.current_observation_df is None or self.current_observation_df.empty:
            self._update_export_buttons()
            return

        filtered = self._apply_filter(self.current_observation_df)
        column_texts: dict[str, list[str]] = {identifier: [] for identifier in self.columns}
        rows: list[list[str]] = []
        for _, row in filtered.iterrows():
            row_values: list[str] = []
            for identifier in self.columns:
                if identifier in self.numeric_columns:
                    formatted = self._format_float(row.get(identifier))
                else:
                    formatted = self._format_text(row.get(identifier))
                row_values.append(formatted)
                column_texts[identifier].append(formatted)
            rows.append(row_values)

        for identifier in self.numeric_columns:
            if identifier not in column_texts:
                continue
            aligned_values, heading_text = self._align_numeric_column(
                identifier, column_texts[identifier]
            )
            column_texts[identifier] = aligned_values
            column_index = self.columns.index(identifier)
            for row_idx, text in enumerate(aligned_values):
                rows[row_idx][column_index] = text
            self.observation_tree.heading(identifier, text=heading_text, anchor="center")

        for values in rows:
            self.observation_tree.insert("", "end", values=values)

        self._update_column_widths(column_texts)
        self._update_export_buttons()

    def _format_float(self, value: Optional[float]) -> str:
        if value is None:
            return "-"
        try:
            float_value = float(value)
        except (TypeError, ValueError):
            return "-"
        if not np.isfinite(float_value):
            return "-"
        return f"{float_value:.4f}"

    def _format_text(self, value: Optional[object]) -> str:
        if value is None:
            return "-"
        if isinstance(value, str):
            text = value.strip()
            return text if text else "-"
        if pd.isna(value):
            return "-"
        return str(value)

    def _align_numeric_column(
        self, identifier: str, values: list[str]
    ) -> tuple[list[str], str]:
        heading = self.column_headings.get(identifier, identifier)
        if not values:
            return values, heading

        formatted: list[str] = []
        for text in values:
            if text is None:
                formatted.append("-")
            else:
                stripped = text.strip()
                formatted.append(stripped if stripped else "-")

        def _integer_length(text: str) -> int:
            dot_index = text.find(".")
            if dot_index == -1:
                return len(text)
            return dot_index

        max_len_overall = 0
        max_int_len = 0
        for item in formatted:
            max_len_overall = max(max_len_overall, len(item))
            max_int_len = max(max_int_len, _integer_length(item))

        if max_len_overall == 0:
            return formatted, heading

        aligned_rows: list[str] = []
        for item in formatted:
            int_len = _integer_length(item)
            left_spaces = max_int_len - int_len
            if left_spaces < 0:
                left_spaces = 0
            right_spaces = max_len_overall - (left_spaces + len(item))
            if right_spaces < 0:
                right_spaces = 0
            aligned_rows.append(" " * left_spaces + item + " " * right_spaces)

        header_centered = f"{heading:^{max_len_overall}}"
        return aligned_rows, header_centered

    def _update_column_widths(self, column_texts: dict[str, list[str]]) -> None:
        if not hasattr(self, "observation_tree"):
            return

        data_font = getattr(self, "_data_font", None)
        if data_font is None:
            try:
                data_font = tkfont.nametofont(self.observation_tree.cget("font"))
            except tk.TclError:  # pragma: no cover - fallback for uncommon themes
                data_font = tkfont.nametofont("TkDefaultFont")

        try:
            heading_font = tkfont.nametofont("TkHeadingFont")
        except tk.TclError:  # pragma: no cover - fallback if heading font missing
            heading_font = data_font

        widths: dict[str, int] = {}
        for identifier in self.columns:
            texts = column_texts.get(identifier, [])
            data_width = max((data_font.measure(text) for text in texts), default=0)
            heading_info = self.observation_tree.heading(identifier)
            heading_text = heading_info.get("text", "") if isinstance(heading_info, dict) else ""
            heading_width = heading_font.measure(heading_text)
            desired = max(data_width, heading_width) + 16
            widths[identifier] = max(int(desired), 20)

        self.master_app.update_results_tree_min_widths(self.observation_tree, widths)

    def _clear_correlation_table(self) -> None:
        if not self.corr_tree:
            return
        self.corr_tree.delete(*self.corr_tree.get_children())
        if self.corr_columns:
            for column in self.corr_columns:
                self.corr_tree.heading(column, text=column, anchor="center")
        self.corr_tree.configure(columns=())
        self.corr_columns = []

    def _render_correlation_table(self) -> None:
        if not self.corr_tree:
            return

        self.corr_tree.delete(*self.corr_tree.get_children())

        df = self._get_active_correlation_df()
        if df is None or df.empty:
            self.corr_tree.configure(columns=())
            self.corr_columns = []
            self._update_export_buttons()
            return

        df = df.copy()
        columns = ["Variable"] + list(df.columns)
        self.corr_columns = columns
        self.corr_tree.configure(columns=columns)

        for column in columns:
            heading_text = column
            anchor = "w" if column == "Variable" else "center"
            min_width = 140 if column == "Variable" else 90
            self.corr_tree.heading(column, text=heading_text, anchor="center")
            self.corr_tree.column(
                column,
                anchor=anchor,
                width=min_width,
                minwidth=min_width,
                stretch=False,
            )

        rows: list[list[str]] = []
        column_labels = df.columns.tolist()
        for row_idx, (index_label, row_series) in enumerate(df.iterrows()):
            values = [str(index_label)]
            for col_idx, column_name in enumerate(column_labels):
                if col_idx > row_idx:
                    values.append("")
                else:
                    values.append(self._format_float(row_series[column_name]))
            self.corr_tree.insert("", "end", values=values)
            rows.append(values)

        self._update_correlation_widths(columns, rows)
        self._update_export_buttons()

    def _update_correlation_widths(
        self, columns: list[str], rows: list[list[str]]
    ) -> None:
        if not self.corr_tree or not columns:
            return
        try:
            data_font = tkfont.nametofont(self.corr_tree.cget("font"))
        except tk.TclError:
            data_font = tkfont.nametofont("TkDefaultFont")
        try:
            heading_font = tkfont.nametofont("TkHeadingFont")
        except tk.TclError:
            heading_font = data_font

        for idx, column in enumerate(columns):
            texts = [row[idx] for row in rows] if rows else []
            data_width = max((data_font.measure(text) for text in texts), default=0)
            heading_info = self.corr_tree.heading(column)
            heading_text = heading_info.get("text", "") if isinstance(heading_info, dict) else ""
            heading_width = heading_font.measure(heading_text)
            desired = max(data_width, heading_width) + 16
            min_width = 140 if column == "Variable" else 90
            self.corr_tree.column(column, width=max(int(desired), min_width))

    def _update_export_buttons(self) -> None:
        obs_button = self.obs_export_button
        if obs_button is not None:
            obs_enabled = False
            if self.available and self.current_observation_df is not None:
                filtered = self._apply_filter(self.current_observation_df)
                obs_enabled = not filtered.empty
            obs_button.configure(state="normal" if obs_enabled else "disabled")

        corr_button = self.corr_export_button
        if corr_button is not None:
            corr_df = self._get_active_correlation_df()
            corr_enabled = (
                self.available
                and corr_df is not None
                and not corr_df.empty
            )
            corr_button.configure(state="normal" if corr_enabled else "disabled")

    def _get_active_correlation_df(self) -> Optional[pd.DataFrame]:
        if (
            self.corr_include_target_var.get()
            and self.current_correlation_with_target_df is not None
            and not self.current_correlation_with_target_df.empty
        ):
            return self.current_correlation_with_target_df
        return self.current_correlation_df

    def _handle_correlation_include_target_toggle(self) -> None:
        if self.corr_include_target_var.get():
            if (
                self.current_correlation_with_target_df is None
                or self.current_correlation_with_target_df.empty
            ):
                self.corr_include_target_var.set(False)
                return
        self._render_correlation_table()
        self._update_export_buttons()

    def _export_observations_csv(self) -> None:
        if not self.available or self.current_observation_df is None:
            messagebox.showwarning(
                "Export diagnostics", "No observation diagnostics are available to export.",
            )
            return

        filtered = self._apply_filter(self.current_observation_df)
        if filtered.empty:
            messagebox.showwarning(
                "Export diagnostics",
                "The current observation diagnostics table does not contain rows to export.",
            )
            return

        default_name = (
            f"model_{self.current_model_id}_diagnostics.csv"
            if self.current_model_id is not None
            else "observation_diagnostics.csv"
        )
        path = filedialog.asksaveasfilename(
            title="Save observation diagnostics",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=default_name,
        )
        if not path:
            return
        try:
            filtered.to_csv(path, sep=";", index=False)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror(
                "Export diagnostics", f"Unable to export observation diagnostics:\n{exc}",
            )

    def _export_correlation_csv(self) -> None:
        if not self.available:
            messagebox.showwarning(
                "Export correlation",
                "No model variable correlation matrix is available to export.",
            )
            return

        corr_df = self._get_active_correlation_df()
        if corr_df is None or corr_df.empty:
            messagebox.showwarning(
                "Export correlation",
                "No model variable correlation matrix is available to export.",
            )
            return

        default_name = (
            f"model_{self.current_model_id}_correlation.csv"
            if self.current_model_id is not None
            else "correlation_matrix.csv"
        )
        path = filedialog.asksaveasfilename(
            title="Save correlation matrix",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=default_name,
        )
        if not path:
            return

        export_df = corr_df.copy()
        mask = np.tril(np.ones(export_df.shape, dtype=bool))
        export_df = export_df.where(mask)
        export_df.insert(0, "Variable", export_df.index.astype(str))
        try:
            export_df.to_csv(path, sep=";", index=False, na_rep="", float_format="%.4f")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror(
                "Export correlation", f"Unable to export correlation matrix:\n{exc}",
            )


@dataclass(frozen=True)
class _PointIdentity:
    dataset: Optional[str]
    unique_key: object
    display_label: object
    x_value: Optional[float] = None
    y_value: Optional[float] = None
    extras: dict[str, object] = field(default_factory=dict, compare=False)

    def display_value(self) -> str:
        for candidate in (self.display_label, self.unique_key):
            if candidate is None:
                continue
            text = str(candidate).strip()
            if text:
                return text
        return "?"


@dataclass
class _ScatterCollectionInfo:
    collection: PathCollection
    observation_ids: np.ndarray
    x: np.ndarray
    y: np.ndarray
    sizes: np.ndarray


@dataclass
class _VisibleAnnotation:
    artist: Any
    offset: tuple[float, float]


@dataclass
class _DragState:
    observation_id: object
    annotation: _VisibleAnnotation
    point_pixels: tuple[float, float]
    button: MouseButton
    origin: tuple[float, float]
    click_offset: tuple[float, float]
    moved: bool = False


@dataclass
class YRandomizationResult:
    model_id: int
    metric_key: str
    permutations_requested: int
    metrics: list[float]
    actual_value: float
    p_value: float
    interpretation: str
    actual_line_color_label: str
    bin_color_label: str
    completed_permutations: int
    completed: bool = True
    chart_added: bool = False

    @property
    def metric_label(self) -> str:
        return "R2" if self.metric_key == "R2" else "Q2 (LOO)"


class _InteractiveLabelManager:
    """Coordinates shared label visibility and per-plot positions."""

    _DEFAULT_OFFSET: tuple[float, float] = (10.0, 10.0)

    def __init__(self, canvas: FigureCanvasTkAgg, label_formatter: Callable[[object], str]):
        self.canvas = canvas
        self.figure = canvas.figure
        self._format_label = label_formatter
        self._visibility: dict[object, bool] = {}
        self._positions: dict[tuple[str, object], tuple[float, float]] = {}
        self._active_plot_key: Optional[str] = None
        self._active_axes: Optional[Axes] = None
        self._current_collections: list[_ScatterCollectionInfo] = []
        self._current_annotations: dict[object, _VisibleAnnotation] = {}
        self._point_lookup: dict[object, list[tuple[_ScatterCollectionInfo, int]]] = {}
        self._drag_state: Optional[_DragState] = None
        self._cid_press = self.figure.canvas.mpl_connect("button_press_event", self._on_press)
        self._cid_release = self.figure.canvas.mpl_connect("button_release_event", self._on_release)
        self._cid_motion = self.figure.canvas.mpl_connect("motion_notify_event", self._on_motion)

    def reset(self) -> None:
        self._visibility.clear()
        self._positions.clear()
        self.suspend()

    def suspend(self) -> None:
        self._active_plot_key = None
        self._active_axes = None
        self._current_collections.clear()
        self._current_annotations.clear()
        self._point_lookup.clear()
        self._drag_state = None

    def begin_plot(self, plot_key: str, axes: Axes) -> None:
        self._active_plot_key = plot_key
        self._active_axes = axes
        self._current_collections = []
        self._current_annotations = {}
        self._point_lookup = {}
        self._drag_state = None

    def register_collection(
        self,
        plot_key: str,
        collection: PathCollection,
        observation_ids: np.ndarray,
        x_values: np.ndarray,
        y_values: np.ndarray,
    ) -> None:
        if plot_key != self._active_plot_key:
            return
        if observation_ids.size == 0:
            return
        sizes = np.asarray(collection.get_sizes(), dtype=float)
        if sizes.size == 1 and observation_ids.size > 1:
            sizes = np.full(observation_ids.shape, sizes[0], dtype=float)
        info = _ScatterCollectionInfo(
            collection=collection,
            observation_ids=np.asarray(observation_ids, dtype=object),
            x=np.asarray(x_values, dtype=float),
            y=np.asarray(y_values, dtype=float),
            sizes=sizes,
        )
        self._current_collections.append(info)

    def complete_plot(self) -> None:
        if self._active_axes is None:
            return
        self._point_lookup = {}
        for info in self._current_collections:
            for index, observation_id in enumerate(info.observation_ids):
                self._point_lookup.setdefault(observation_id, []).append((info, index))
        for observation_id, visible in self._visibility.items():
            if visible:
                self._ensure_annotation(observation_id)

    def _ensure_annotation(self, observation_id: object) -> None:
        if self._active_axes is None or self._active_plot_key is None:
            return
        if observation_id in self._current_annotations:
            return
        entries = self._point_lookup.get(observation_id)
        if not entries:
            return
        info, index = entries[0]
        x = float(info.x[index])
        y = float(info.y[index])
        offset = self._positions.get((self._active_plot_key, observation_id), self._DEFAULT_OFFSET)
        annotation = self._active_axes.annotate(
            self._format_label(observation_id),
            xy=(x, y),
            xycoords="data",
            xytext=offset,
            textcoords="offset pixels",
            fontsize=9,
            ha="left",
            va="bottom",
            bbox=dict(boxstyle="round,pad=0.3", fc=(1.0, 1.0, 1.0, 0.0), ec="#666666"),
            arrowprops=dict(arrowstyle="-", lw=0.8, color="#666666"),
        )
        annotation.set_zorder(10)
        self._current_annotations[observation_id] = _VisibleAnnotation(
            artist=annotation,
            offset=(float(offset[0]), float(offset[1])),
        )

    def _remove_annotation(self, observation_id: object) -> None:
        entry = self._current_annotations.pop(observation_id, None)
        if entry is None or self._active_plot_key is None:
            return
        self._positions[(self._active_plot_key, observation_id)] = (
            float(entry.offset[0]),
            float(entry.offset[1]),
        )
        entry.artist.remove()

    def _toggle_visibility(self, observation_id: object) -> None:
        current = self._visibility.get(observation_id, False)
        self._visibility[observation_id] = not current
        if current:
            self._remove_annotation(observation_id)
        else:
            self._ensure_annotation(observation_id)
        self.canvas.draw_idle()

    def _hide_all_annotations(self) -> None:
        if not self._current_annotations and not any(self._visibility.values()):
            return
        for observation_id in list(self._current_annotations):
            self._remove_annotation(observation_id)
        for observation_id in list(self._visibility):
            self._visibility[observation_id] = False
        self._drag_state = None
        self.canvas.draw_idle()

    def _on_press(self, event) -> None:
        if event.button == MouseButton.RIGHT:
            self._hide_all_annotations()
            return
        if event.button != MouseButton.LEFT:
            return
        if self._active_axes is None or event.inaxes != self._active_axes:
            return
        for observation_id, entry in reversed(self._current_annotations.items()):
            annotation = entry.artist
            contains, _ = annotation.contains(event)
            if contains:
                if event.x is None or event.y is None:
                    continue
                point_x, point_y = self._active_axes.transData.transform(annotation.xy)
                offset_x, offset_y = entry.offset
                self._drag_state = _DragState(
                    observation_id=observation_id,
                    annotation=entry,
                    point_pixels=(float(point_x), float(point_y)),
                    button=MouseButton.LEFT,
                    origin=(float(event.x), float(event.y)),
                    click_offset=(
                        float(event.x) - (float(point_x) + offset_x),
                        float(event.y) - (float(point_y) + offset_y),
                    ),
                )
                return
        hit = self._locate_point(event)
        if hit is not None:
            self._toggle_visibility(hit)

    def _on_motion(self, event) -> None:
        if self._drag_state is None:
            return
        if event.x is None or event.y is None:
            return
        dx = (
            float(event.x)
            - self._drag_state.point_pixels[0]
            - self._drag_state.click_offset[0]
        )
        dy = (
            float(event.y)
            - self._drag_state.point_pixels[1]
            - self._drag_state.click_offset[1]
        )
        if not self._drag_state.moved:
            distance = math.hypot(float(event.x) - self._drag_state.origin[0], float(event.y) - self._drag_state.origin[1])
            if distance > 1.0:
                self._drag_state.moved = True
        entry = self._drag_state.annotation
        entry.artist.set_position((dx, dy))
        entry.offset = (dx, dy)
        if self._active_plot_key is not None:
            self._positions[(self._active_plot_key, self._drag_state.observation_id)] = (dx, dy)
        self.canvas.draw_idle()

    def _on_release(self, event) -> None:
        if self._drag_state is not None and event.button == self._drag_state.button:
            if self._active_plot_key is not None:
                entry = self._drag_state.annotation
                self._positions[(self._active_plot_key, self._drag_state.observation_id)] = (
                    float(entry.offset[0]),
                    float(entry.offset[1]),
                )
            moved = self._drag_state.moved
            button = self._drag_state.button
            self._drag_state = None
            return
        if event.button == MouseButton.RIGHT:
            self._hide_all_annotations()

    def _locate_point(self, event) -> Optional[object]:
        if self._active_axes is None:
            return None
        if event.xdata is None or event.ydata is None:
            return None
        best: Optional[tuple[object, float]] = None
        figure_dpi = float(self.figure.dpi)
        transform = self._active_axes.transData
        for info in self._current_collections:
            if info.observation_ids.size == 0:
                continue
            for index, observation_id in enumerate(info.observation_ids):
                px, py = transform.transform((info.x[index], info.y[index]))
                dx = float(event.x) - float(px)
                dy = float(event.y) - float(py)
                distance = math.hypot(dx, dy)
                size = info.sizes[index if info.sizes.size > 1 else 0]
                radius_points = math.sqrt(max(size, 0.0) / math.pi)
                radius_pixels = radius_points * (figure_dpi / 72.0)
                if distance <= radius_pixels:
                    if best is None or distance < best[1]:
                        best = (observation_id, distance)
        return best[0] if best is not None else None


class _AxisLabelDialog(simpledialog.Dialog):
    def __init__(
        self,
        parent: tk.Widget,
        title: str,
        initial_x: str,
        initial_y: str,
        initial_font_size: Optional[float] = None,
    ):
        self.initial_x = initial_x or ""
        self.initial_y = initial_y or ""
        self.initial_font_size = initial_font_size
        self.result: Optional[tuple[str, str, Optional[float]]] = None
        self._active_entry: Optional[ttk.Entry] = None
        super().__init__(parent, title)

    def body(self, master: tk.Misc):  # type: ignore[override]
        ttk.Label(master, text="X axis label:").grid(row=0, column=0, sticky="w", padx=5, pady=(5, 0))
        self.x_entry = ttk.Entry(master, width=30)
        self.x_entry.grid(row=0, column=1, sticky="w", padx=5, pady=(5, 0))
        self.x_entry.insert(0, self.initial_x)
        self.x_entry.bind("<FocusIn>", lambda _event: self._set_active_entry(self.x_entry))

        ttk.Label(master, text="Y axis label:").grid(row=1, column=0, sticky="w", padx=5, pady=(5, 5))
        self.y_entry = ttk.Entry(master, width=30)
        self.y_entry.grid(row=1, column=1, sticky="w", padx=5, pady=(5, 5))
        self.y_entry.insert(0, self.initial_y)
        self.y_entry.bind("<FocusIn>", lambda _event: self._set_active_entry(self.y_entry))

        ttk.Label(master, text="Font size:").grid(row=2, column=0, sticky="w", padx=5)
        self.font_size_var = tk.StringVar()
        if self.initial_font_size is not None and math.isfinite(self.initial_font_size):
            self.font_size_var.set(f"{self.initial_font_size:g}")
        font_spin = ttk.Spinbox(
            master,
            from_=6,
            to=48,
            increment=1,
            width=5,
            textvariable=self.font_size_var,
        )
        font_spin.grid(row=2, column=1, sticky="w", padx=5, pady=(0, 5))

        button_frame = ttk.Frame(master)
        button_frame.grid(row=3, column=0, columnspan=2, sticky="w", padx=5, pady=(0, 5))

        ttk.Label(button_frame, text="Insert:").grid(row=0, column=0, sticky="w")
        sup_button = ttk.Button(
            button_frame,
            text="Superscript",
            command=lambda: self._insert_markup("^"),
        )
        sup_button.grid(row=0, column=1, padx=(5, 0))
        sub_button = ttk.Button(
            button_frame,
            text="Subscript",
            command=lambda: self._insert_markup("_"),
        )
        sub_button.grid(row=0, column=2, padx=(5, 0))

        self._set_active_entry(self.x_entry)

        return self.x_entry

    def apply(self):  # type: ignore[override]
        x_value = self.x_entry.get().strip()
        y_value = self.y_entry.get().strip()
        font_value = self.font_size_var.get().strip()
        font_size: Optional[float]
        if font_value:
            try:
                parsed = float(font_value)
            except ValueError:
                font_size = None
            else:
                if math.isfinite(parsed):
                    font_size = max(6.0, min(48.0, parsed))
                else:
                    font_size = None
        else:
            font_size = None
        self.result = (x_value, y_value, font_size)

    def _set_active_entry(self, entry: ttk.Entry) -> None:
        self._active_entry = entry

    def _insert_markup(self, op: str) -> None:
        entry = self._active_entry if self._active_entry is not None else self.x_entry
        entry.focus_set()

        try:
            has_selection = bool(entry.selection_present())
        except tk.TclError:
            has_selection = False

        if has_selection:
            try:
                start = entry.index("sel.first")
                end = entry.index("sel.last")
                selected_text = entry.selection_get()
            except tk.TclError:
                start = entry.index(tk.INSERT)
                end = start
                selected_text = ""
        else:
            start = entry.index(tk.INSERT)
            end = start
            selected_text = ""

        replacement = f"{op}{{{selected_text}}}"
        entry.delete(start, end)
        entry.insert(start, replacement)
        inner_start = start + len(op) + 1
        inner_end = inner_start + len(selected_text)
        entry.selection_range(inner_start, inner_end)
        entry.icursor(inner_end)



class VariableExplorerTab(ttk.Frame):
    COLOR_CHOICES: tuple[tuple[str, Optional[str]], ...] = (
        ("Default", None),
        ("Blue", "#1f77b4"),
        ("Orange", "#ff7f0e"),
        ("Green", "#2ca02c"),
        ("Red", "#d62728"),
        ("Purple", "#9467bd"),
        ("Gray", "#7f7f7f"),
        ("Black", "#000000"),
    )

    MARKER_CHOICES: tuple[tuple[str, str], ...] = (
        ("Circle", "o"),
        ("Square", "s"),
        ("Triangle", "^"),
        ("Diamond", "D"),
        ("Plus", "P"),
        ("Cross", "X"),
    )

    MAX_TICKS: int = 1000
    MAX_AXIS_TICKS: int = 7
    MIN_AXIS_TICKS: int = 5

    LEGEND_LOCATION_CHOICES: tuple[tuple[str, Union[str, int]], ...] = (
        ("1", 2),
        ("2", 1),
        ("3", 3),
        ("4", 4),
    )

    BASE_LABEL_MODE_OPTIONS: tuple[tuple[str, str], ...] = (
        ("Observations", "observations"),
    )

    ASPECT_RATIO_OPTIONS: tuple[tuple[str, tuple[int, int]], ...] = (
        ("1", (1, 1)),
        ("2", (4, 3)),
        ("3", (16, 9)),
        ("4", (9, 16)),
    )

    _DEFAULT_DATASET_LABEL = "Dataset"

    def __init__(self, notebook: ttk.Notebook, master_app: "MLRXApp"):
        super().__init__(notebook)
        self.master_app = master_app
        self.dataset_path_var = tk.StringVar(value=self.master_app.data_path_var.get())
        self.dataset_status_var = tk.StringVar(value="Load a dataset to visualize variables.")
        self.axis_source_var = tk.StringVar(value="all")
        self.model_id_var = tk.StringVar(value="1")
        self.x_axis_var = tk.StringVar()
        self.y_axis_var = tk.StringVar()
        self.legend_var = tk.BooleanVar(value=False)
        self.legend_location_var = tk.StringVar(value="1")
        self.linear_fit_var = tk.BooleanVar(value=False)
        self.linear_fit_color_var = tk.StringVar(value="Default")
        self.r2_value_entry: Optional[ttk.Entry] = None
        self.gridline_var = tk.BooleanVar(value=True)
        self.partial_residuals_var = tk.BooleanVar(value=False)
        self.compute_r2_var = tk.BooleanVar(value=False)
        self.r2_value_var = tk.StringVar(value="")
        self.point_size_var = tk.DoubleVar(value=50.0)
        self.marker_color_var = tk.StringVar(value="Default")
        self.marker_style_var = tk.StringVar(value="Circle")
        self.histogram_var = tk.BooleanVar(value=False)
        self.bar_count_var = tk.StringVar(value="4")
        self.bin_mode_var = tk.StringVar(value="count")
        self.bin_edges_var = tk.StringVar(value="")
        self.bin_linewidth_var = tk.StringVar(value="1.2")
        self.bar_color_var = tk.StringVar(value="Default")
        self.histogram_check: Optional[ttk.Checkbutton] = None
        self.legend_check: Optional[ttk.Checkbutton] = None
        self.linear_fit_check: Optional[ttk.Checkbutton] = None
        self.r2_check: Optional[ttk.Checkbutton] = None
        self.gridline_check: Optional[ttk.Checkbutton] = None
        self.bin_settings_button: Optional[ttk.Button] = None
        self.bar_color_label: Optional[ttk.Label] = None
        self.bar_color_combo: Optional[ttk.Combobox] = None
        self.marker_color_combo: Optional[ttk.Combobox] = None
        self.marker_style_combo: Optional[ttk.Combobox] = None
        self.marker_size_label: Optional[ttk.Label] = None
        self.marker_size_scale: Optional[ttk.Scale] = None
        self.label_mode_var = tk.StringVar(value="observations")
        self._label_mode_options: list[tuple[str, str]] = list(
            self.BASE_LABEL_MODE_OPTIONS
        )
        self._non_variable_label_column: Optional[str] = None
        self.aspect_ratio_var = tk.StringVar(value="1")
        self.plot_status_var = tk.StringVar(value="")
        self._latest_axis_options: list[tuple[str, str]] = []
        self.xmin_var = tk.StringVar()
        self.xmax_var = tk.StringVar()
        self.ymin_var = tk.StringVar()
        self.ymax_var = tk.StringVar()
        self.x_tick_step_var = tk.StringVar()
        self.y_tick_step_var = tk.StringVar()
        self.dataset_filter_mode_var = tk.StringVar(value="current")
        self.model_range_start_var = tk.StringVar(value="1")
        self.model_range_end_var = tk.StringVar(value="")
        self.custom_xlabel: Optional[str] = None
        self.custom_ylabel: Optional[str] = None
        self.axis_label_fontsize: Optional[float] = None
        self._current_context: Optional[EPRSContext] = None
        self._results_df: Optional[pd.DataFrame] = None
        self._model_variables: dict[int, list[str]] = {}
        self._current_model_id: Optional[int] = None
        self._axis_default_context: Optional[tuple[Any, ...]] = None
        self._axis_user_override: dict[str, bool] = {"x": False, "y": False}
        self._axis_parameters: dict[str, Optional[AxisParameters]] = {"x": None, "y": None}
        self._stored_limits: dict[str, Optional[tuple[float, float]]] = {"x": None, "y": None}
        self._in_resize = False
        self._resize_cid: Optional[int] = None
        self._current_axis_labels: tuple[str, str] = ("", "")
        self._legend_loc: Optional[Union[str, int]] = None
        self._label_manager: Optional[_InteractiveLabelManager] = None
        self._histogram_label_draggables: list[Any] = []
        self._filter_dialog: Optional[tk.Toplevel] = None
        self._filter_dialog_controls: dict[str, Any] = {}
        self._bin_settings_dialog: Optional[tk.Toplevel] = None
        self._bin_settings_controls: dict[str, Any] = {}
        self.partial_residuals_check: Optional[ttk.Checkbutton] = None
        self._data_path_trace = self.master_app.data_path_var.trace_add(
            "write", self._sync_dataset_path
        )
        self._build_ui()
        self._update_model_option_state()

    def destroy(self):  # type: ignore[override]
        try:
            self.master_app.data_path_var.trace_remove("write", self._data_path_trace)
        except Exception:  # noqa: BLE001
            pass
        self._close_filter_dialog()
        self._close_bin_settings_dialog()
        super().destroy()

    def _build_ui(self) -> None:
        dataset_frame = ttk.LabelFrame(self, text="Dataset and Results")
        dataset_frame.pack(fill="x", padx=10, pady=10)
        dataset_frame.columnconfigure(1, weight=1)

        ttk.Label(dataset_frame, text="CSV file:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        path_entry = ttk.Entry(dataset_frame, textvariable=self.dataset_path_var)
        path_entry.grid(row=0, column=1, sticky="ew", padx=5, pady=5)

        button_frame = ttk.Frame(dataset_frame)
        button_frame.grid(row=0, column=2, sticky="e", padx=5, pady=5)
        ttk.Button(button_frame, text="Browse", command=self._browse_dataset).pack(side="left")
        ttk.Button(button_frame, text="Load", command=self._load_dataset).pack(
            side="left", padx=(5, 0)
        )

        ttk.Label(dataset_frame, text="Results file:").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        results_entry = ttk.Entry(dataset_frame, textvariable=self.master_app.results_load_path)
        results_entry.grid(row=1, column=1, sticky="ew", padx=5, pady=5)

        results_button_frame = ttk.Frame(dataset_frame)
        results_button_frame.grid(row=1, column=2, sticky="e", padx=5, pady=5)
        ttk.Button(
            results_button_frame,
            text="Browse",
            command=self.master_app._browse_results_file,
        ).pack(side="left")
        ttk.Button(
            results_button_frame,
            text="Load",
            command=lambda: self.master_app._load_results_file(select_tab=False),
        ).pack(side="left", padx=(5, 0))

        ttk.Label(dataset_frame, textvariable=self.dataset_status_var, foreground="#666666").grid(
            row=2, column=0, columnspan=3, sticky="w", padx=5, pady=(0, 5)
        )

        self._build_selection_controls()
        self._build_plot_area()

    def _build_selection_controls(self) -> None:
        selection_frame = ttk.Frame(self)
        selection_frame.pack(fill="x", padx=10, pady=(0, 10))
        selection_frame.columnconfigure(8, weight=1)

        ttk.Radiobutton(
            selection_frame,
            text="All variables",
            value="all",
            variable=self.axis_source_var,
            command=self._handle_source_change,
        ).grid(row=0, column=0, sticky="w", padx=5)

        self.model_radio = ttk.Radiobutton(
            selection_frame,
            text="Only variables of model:",
            value="model",
            variable=self.axis_source_var,
            command=self._handle_source_change,
            state="disabled",
        )
        self.model_radio.grid(row=0, column=1, sticky="w", padx=(15, 5))

        self.model_entry = ttk.Entry(selection_frame, textvariable=self.model_id_var, width=8)
        self.model_entry.grid(row=0, column=2, sticky="w", padx=(5, 15))
        self.model_entry.configure(state="disabled")
        self.model_entry.bind("<Return>", lambda _e: self._refresh_model_variables())
        self.model_entry.bind("<FocusOut>", lambda _e: self._refresh_model_variables())

        ttk.Label(selection_frame, text="X axis:").grid(row=0, column=3, sticky="w", padx=(0, 5))
        self.x_axis_combo = ttk.Combobox(
            selection_frame,
            textvariable=self.x_axis_var,
            state="disabled",
            width=18,
        )
        self.x_axis_combo.grid(row=0, column=4, sticky="w", padx=(0, 10))
        self.x_axis_combo.bind("<<ComboboxSelected>>", self._update_plot)

        ttk.Label(selection_frame, text="Y axis:").grid(row=0, column=5, sticky="w", padx=(0, 5))
        self.y_axis_combo = ttk.Combobox(
            selection_frame,
            textvariable=self.y_axis_var,
            state="disabled",
            width=18,
        )
        self.y_axis_combo.grid(row=0, column=6, sticky="w", padx=(0, 5))
        self.y_axis_combo.bind("<<ComboboxSelected>>", self._update_plot)

        self.histogram_check = ttk.Checkbutton(
            selection_frame,
            text="Histogram",
            variable=self.histogram_var,
            command=self._handle_histogram_toggle,
        )
        self.histogram_check.grid(row=0, column=7, sticky="w", padx=(10, 0))

        spacer = ttk.Frame(selection_frame)
        spacer.grid(row=0, column=8, sticky="ew")

    def _build_plot_area(self) -> None:
        plot_frame = ttk.Frame(self)
        plot_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        plot_frame.columnconfigure(0, weight=1)
        plot_frame.columnconfigure(1, weight=0)
        plot_frame.rowconfigure(0, weight=1)

        figure_container = ttk.Frame(plot_frame)
        figure_container.grid(row=0, column=0, sticky="nsew", padx=(5, 0), pady=5)
        self.figure = Figure(figsize=(5, 5))
        self.ax = self.figure.add_subplot(111)
        self._enforce_axis_ratio(self.ax)
        self.canvas = FigureCanvasTkAgg(self.figure, master=figure_container)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)
        try:
            self._resize_cid = self.canvas.mpl_connect("resize_event", self._handle_canvas_resize)
        except Exception:  # noqa: BLE001
            self._resize_cid = None
        self._label_manager = _InteractiveLabelManager(self.canvas, self._format_point_label)

        self._build_option_controls(plot_frame)
        self._clear_plot()

    def _close_filter_dialog(self) -> None:
        dialog = self._filter_dialog
        if dialog is not None:
            try:
                if dialog.grab_current() is dialog:
                    dialog.grab_release()
            except Exception:  # noqa: BLE001
                pass
            try:
                if dialog.winfo_exists():
                    dialog.destroy()
            except Exception:  # noqa: BLE001
                pass
        self._filter_dialog = None
        self._filter_dialog_controls.clear()

    def _open_bin_settings_dialog(self) -> None:
        if self._bin_settings_dialog is not None and self._bin_settings_dialog.winfo_exists():
            try:
                self._bin_settings_dialog.focus_set()
                return
            except Exception:  # noqa: BLE001
                pass

        dialog = tk.Toplevel(self)
        dialog.title("Set histogram bins")
        dialog.transient(self.winfo_toplevel())
        dialog.resizable(False, False)
        dialog.protocol("WM_DELETE_WINDOW", self._close_bin_settings_dialog)

        self._bin_settings_dialog = dialog
        controls: dict[str, Any] = {}
        self._bin_settings_controls = controls

        container = ttk.Frame(dialog, padding=10)
        container.pack(fill="both", expand=True)

        mode_frame = ttk.LabelFrame(container, text="Bin mode")
        mode_frame.pack(fill="x", pady=(0, 10))

        count_radio = ttk.Radiobutton(
            mode_frame,
            text="Number of bins",
            value="count",
            variable=self.bin_mode_var,
            command=self._sync_bin_dialog_controls,
        )
        count_radio.grid(row=0, column=0, sticky="w")

        count_entry = ttk.Entry(mode_frame, textvariable=self.bar_count_var, width=8)
        count_entry.grid(row=0, column=1, sticky="w", padx=(8, 0))
        controls["count_entry"] = count_entry

        range_radio = ttk.Radiobutton(
            mode_frame,
            text="Value range (comma-separated edges)",
            value="range",
            variable=self.bin_mode_var,
            command=self._sync_bin_dialog_controls,
        )
        range_radio.grid(row=1, column=0, sticky="w", pady=(6, 0))

        range_entry = ttk.Entry(mode_frame, textvariable=self.bin_edges_var, width=40)
        range_entry.grid(row=1, column=1, sticky="w", padx=(8, 0), pady=(6, 0))
        controls["range_entry"] = range_entry

        linewidth_frame = ttk.LabelFrame(container, text="Outline")
        linewidth_frame.pack(fill="x", pady=(0, 10))

        ttk.Label(linewidth_frame, text="Line width:").grid(row=0, column=0, sticky="w")
        linewidth_combo = ttk.Combobox(
            linewidth_frame,
            textvariable=self.bin_linewidth_var,
            values=["None", "0.5", "1", "1.2", "1.5", "2"],
            width=8,
            state="readonly",
        )
        linewidth_combo.grid(row=0, column=1, sticky="w", padx=(8, 0))
        controls["linewidth_combo"] = linewidth_combo

        apply_btn = ttk.Button(container, text="Apply", command=self._apply_bin_settings)
        apply_btn.pack(anchor="e")
        controls["apply_btn"] = apply_btn

        self._center_dialog(dialog)
        self._sync_bin_dialog_controls()

    def _center_dialog(self, dialog: tk.Toplevel) -> None:
        try:
            dialog.update_idletasks()
            master = self.winfo_toplevel()
            master.update_idletasks()
            master_x = master.winfo_rootx()
            master_y = master.winfo_rooty()
            master_w = master.winfo_width()
            master_h = master.winfo_height()
            width = dialog.winfo_reqwidth()
            height = dialog.winfo_reqheight()
            x = master_x + (master_w // 2) - (width // 2)
            y = master_y + (master_h // 2) - (height // 2)
            dialog.geometry(f"{width}x{height}+{x}+{y}")
        except Exception:  # noqa: BLE001
            pass

    def _sync_bin_dialog_controls(self) -> None:
        controls = self._bin_settings_controls
        mode = self.bin_mode_var.get()
        count_state = "normal" if mode == "count" else "disabled"
        range_state = "normal" if mode == "range" else "disabled"
        try:
            entry = controls.get("count_entry")
            if entry:
                entry.configure(state=count_state)
        except Exception:  # noqa: BLE001
            pass
        try:
            range_entry = controls.get("range_entry")
            if range_entry:
                range_entry.configure(state=range_state)
        except Exception:  # noqa: BLE001
            pass

    def _close_bin_settings_dialog(self) -> None:
        dialog = self._bin_settings_dialog
        if dialog is not None:
            try:
                dialog.destroy()
            except Exception:  # noqa: BLE001
                pass
        self._bin_settings_dialog = None
        self._bin_settings_controls.clear()

    def _apply_bin_settings(self) -> None:
        mode = self.bin_mode_var.get()
        if mode == "count":
            try:
                value = int(float(self.bar_count_var.get()))
            except (TypeError, ValueError):
                messagebox.showerror("Invalid bins", "Enter a valid number of bins (>=2).")
                return
            if value < 2:
                messagebox.showerror("Invalid bins", "Number of bins must be at least 2.")
                return
            self.bar_count_var.set(str(value))
        else:
            edges = self._parse_bin_edges_text(self.bin_edges_var.get())
            if edges is None:
                messagebox.showerror(
                    "Invalid range",
                    "Provide at least two numeric, comma-separated edge values in ascending order.",
                )
                return
            self.bin_edges_var.set(", ".join(str(edge) for edge in edges))

        linewidth_raw = self.bin_linewidth_var.get().strip()
        if linewidth_raw:
            if linewidth_raw.lower() == "none":
                self.bin_linewidth_var.set("None")
            else:
                try:
                    float(linewidth_raw)
                except (TypeError, ValueError):
                    messagebox.showerror("Invalid line width", "Line width must be numeric or 'None'.")
                    return

        self._close_bin_settings_dialog()
        if self.histogram_var.get():
            self._update_plot()

    def _parse_bin_edges_text(self, text: str) -> Optional[list[float]]:
        parts = [p.strip() for p in text.split(",") if p.strip()]
        edges: list[float] = []
        for part in parts:
            try:
                edges.append(float(part))
            except (TypeError, ValueError):
                return None
        if len(edges) < 2:
            return None
        edges = sorted(set(edges))
        if len(edges) < 2:
            return None
        return edges

    def _build_option_controls(self, plot_frame: ttk.Frame) -> None:
        controls_frame = ttk.LabelFrame(plot_frame, text="Plot options")
        controls_frame.grid(row=0, column=1, sticky="ns", padx=(10, 5), pady=5)
        controls_frame.columnconfigure(0, weight=1)

        legend_row = ttk.Frame(controls_frame)
        legend_row.pack(fill="x", padx=5, pady=(5, 0))
        legend_btn = ttk.Checkbutton(
            legend_row,
            text="Show legend",
            variable=self.legend_var,
            command=lambda: (self._sync_legend_controls(), self._update_plot()),
        )
        legend_btn.pack(side="left")
        self.legend_check = legend_btn
        label_mode_frame = ttk.Frame(legend_row)
        label_mode_frame.pack(side="left", padx=(12, 0))
        ttk.Label(label_mode_frame, text="Label mode:").pack(side="left")
        self.label_mode_combo = ttk.Combobox(
            label_mode_frame,
            state="readonly",
            width=10,
        )
        self.label_mode_combo.pack(side="left", padx=(5, 0))
        self.label_mode_combo.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._handle_label_mode_change(self.label_mode_combo.get()),
        )
        self._refresh_label_mode_options()

        self.legend_loc_buttons = []
        legend_loc_row = ttk.Frame(controls_frame)
        legend_loc_row.pack(fill="x", padx=5, pady=(2, 0))
        ttk.Label(legend_loc_row, text="Legend location:").pack(side="left")
        for label, _loc in self.LEGEND_LOCATION_CHOICES:
            btn = ttk.Radiobutton(
                legend_loc_row,
                text=label,
                value=label,
                variable=self.legend_location_var,
                command=self._handle_legend_location_change,
            )
            btn.pack(side="left", padx=(5, 0))
            self.legend_loc_buttons.append(btn)
        self._sync_legend_controls()

        line_group = ttk.Frame(controls_frame)
        line_group.pack(fill="x", padx=5, pady=(10, 0))

        linear_btn = ttk.Checkbutton(
            line_group,
            text="Show linear fit",
            variable=self.linear_fit_var,
            command=self._update_plot,
        )
        linear_btn.grid(row=0, column=0, sticky="w")
        self.linear_fit_check = linear_btn

        linear_color_values = [label for label, _value in self.COLOR_CHOICES]
        linear_color_width = LINEAR_FIT_COLOR_WIDTH
        self.linear_fit_color_combo = ttk.Combobox(
            line_group,
            textvariable=self.linear_fit_color_var,
            values=linear_color_values,
            state="readonly",
            width=linear_color_width,
        )
        self.linear_fit_color_combo.grid(row=0, column=1, sticky="w", padx=(8, 0))
        self.linear_fit_color_combo.bind("<<ComboboxSelected>>", lambda _e: self._update_plot())

        r2_check = ttk.Checkbutton(
            line_group,
            text=f"Compute {R_SQUARED_SYMBOL}:",
            variable=self.compute_r2_var,
            command=self._update_plot,
        )
        r2_check.grid(row=1, column=0, sticky="w", pady=(4, 0))
        self.r2_check = r2_check

        combo_width_setting = self.linear_fit_color_combo.cget("width")
        try:
            r2_entry_width = int(combo_width_setting)
        except (TypeError, ValueError):
            r2_entry_width = linear_color_width
        r2_value_entry = ttk.Entry(
            line_group,
            textvariable=self.r2_value_var,
            width=r2_entry_width,
            state="readonly",
            justify="center",
        )
        r2_value_entry.grid(row=1, column=1, sticky="w", padx=(8, 0), pady=(4, 0))
        self.r2_value_entry = r2_value_entry

        gridline_row = ttk.Frame(controls_frame)
        gridline_row.pack(fill="x", padx=5, pady=(6, 0))
        self.gridline_check = ttk.Checkbutton(
            gridline_row,
            text="Show gridline",
            variable=self.gridline_var,
            command=self._update_plot,
        )
        self.gridline_check.pack(side="left")

        self.partial_residuals_check = ttk.Checkbutton(
            gridline_row,
            text="Partial residuals",
            variable=self.partial_residuals_var,
            command=self._handle_partial_residual_toggle,
        )
        self.partial_residuals_check.pack(side="left", padx=(10, 0))

        bar_options_row = ttk.Frame(controls_frame)
        bar_options_row.pack(fill="x", padx=5, pady=(4, 0))

        self.bin_settings_button = ttk.Button(
            bar_options_row,
            text="Set bins",
            command=self._open_bin_settings_dialog,
            state="disabled",
        )
        self.bin_settings_button.pack(side="left")

        self.bar_color_label = ttk.Label(bar_options_row, text="Bin color:")
        self.bar_color_label.pack(side="left", padx=(10, 0))
        bar_color_values = [label for label, _value in self.COLOR_CHOICES]
        self.bar_color_combo = ttk.Combobox(
            bar_options_row,
            textvariable=self.bar_color_var,
            state="disabled",
            values=bar_color_values,
            width=7,
        )
        self.bar_color_combo.pack(side="left", padx=(3, 0))
        self.bar_color_combo.bind("<<ComboboxSelected>>", lambda _e: self._update_plot())

        self._update_histogram_control_state()

        marker_size_label = ttk.Label(controls_frame, text="Marker size:")
        marker_size_label.pack(anchor="w", padx=5, pady=(10, 0))
        self.marker_size_label = marker_size_label
        size_scale = ttk.Scale(
            controls_frame,
            from_=20,
            to=200,
            variable=self.point_size_var,
            orient="horizontal",
            command=lambda _value: self._update_plot(),
        )
        size_scale.pack(fill="x", padx=5, pady=(0, 5))
        self.marker_size_scale = size_scale

        self._sync_histogram_controls()

        limits_frame = ttk.Frame(controls_frame)
        limits_frame.pack(fill="x", padx=5, pady=(5, 0))
        limits_frame.columnconfigure(1, weight=1)
        limits_frame.columnconfigure(3, weight=1)

        entries: list[tuple[str, tk.StringVar, Optional[str]]] = [
            ("X min", self.xmin_var, "x"),
            ("X max", self.xmax_var, "x"),
            ("X tick step", self.x_tick_step_var, None),
            ("Y min", self.ymin_var, "y"),
            ("Y max", self.ymax_var, "y"),
            ("Y tick step", self.y_tick_step_var, None),
        ]

        for idx, (label_text, var, axis) in enumerate(entries):
            row = idx % 3
            col = 0 if idx < 3 else 2
            entry_col = col + 1
            ttk.Label(limits_frame, text=label_text).grid(
                row=row,
                column=col,
                sticky="w",
                padx=(0, 5),
                pady=(0 if row == 0 else 5, 0),
            )
            entry = ttk.Entry(limits_frame, textvariable=var, width=8)
            entry.grid(
                row=row,
                column=entry_col,
                sticky="w",
                padx=(0, 10 if col == 0 else 0),
                pady=(0 if row == 0 else 5, 0),
            )
            self._bind_axis_entry(entry, axis)

        color_row = ttk.Frame(controls_frame)
        color_row.pack(fill="x", padx=5, pady=(10, 0))
        ttk.Label(color_row, text="Marker color:", width=12, anchor="w").pack(side="left")
        color_values = [label for label, _value in self.COLOR_CHOICES]
        color_combo = ttk.Combobox(
            color_row,
            textvariable=self.marker_color_var,
            state="readonly",
            values=color_values,
            width=7,
        )
        color_combo.pack(side="left", padx=(3, 0))
        color_combo.bind("<<ComboboxSelected>>", lambda _e: self._update_plot())
        self.marker_color_combo = color_combo

        marker_row = ttk.Frame(controls_frame)
        marker_row.pack(fill="x", padx=5, pady=(5, 0))
        ttk.Label(marker_row, text="Marker style:", width=12, anchor="w").pack(side="left")
        marker_values = [label for label, _value in self.MARKER_CHOICES]
        marker_combo = ttk.Combobox(
            marker_row,
            textvariable=self.marker_style_var,
            state="readonly",
            values=marker_values,
            width=7,
        )
        marker_combo.pack(side="left", padx=(3, 0))
        marker_combo.bind("<<ComboboxSelected>>", lambda _e: self._update_plot())
        self.marker_style_combo = marker_combo

        ratio_row = ttk.Frame(controls_frame)
        ratio_row.pack(fill="x", padx=5, pady=(0, 5))
        ttk.Label(ratio_row, text="Aspect ratio:").pack(side="left")
        for value, _dims in self.ASPECT_RATIO_OPTIONS:
            label_text = value
            ttk.Radiobutton(
                ratio_row,
                text=label_text,
                value=value,
                variable=self.aspect_ratio_var,
                command=self._handle_aspect_ratio_change,
            ).pack(side="left", padx=(8, 0))

        axis_button = ttk.Button(
            controls_frame,
            text="Set axis labels...",
            command=self._prompt_axis_labels,
        )
        axis_button.pack(fill="x", padx=5, pady=(5, 0))

        save_button = ttk.Button(
            controls_frame,
            text="Save current plot",
            command=self._save_current_plot,
        )
        save_button.pack(fill="x", padx=5, pady=(5, 0))

        self.create_dataset_button = ttk.Button(
            controls_frame,
            text="Create filtered dataset",
            command=self._open_filtered_dataset_dialog,
        )
        self.create_dataset_button.pack(fill="x", padx=5, pady=(5, 5))

        self._sync_partial_residual_controls()
        self.after_idle(self._sync_r2_entry_width)

    def _open_filtered_dataset_dialog(self) -> None:
        if self._current_context is None:
            messagebox.showinfo(
                "Filtered dataset",
                "Load a dataset before creating a filtered dataset.",
            )
            return

        if not self._model_variables:
            messagebox.showinfo(
                "Filtered dataset",
                "Model results are required before creating a filtered dataset.",
            )
            return

        existing_dialog = self._filter_dialog
        if existing_dialog is not None:
            try:
                if existing_dialog.winfo_exists():
                    try:
                        existing_dialog.lift()
                        existing_dialog.focus_force()
                    except Exception:  # noqa: BLE001
                        pass
                    return
            except Exception:  # noqa: BLE001
                pass
            self._filter_dialog = None
            self._filter_dialog_controls.clear()

        dialog = tk.Toplevel(self)
        dialog.title("Create filtered dataset")
        dialog.transient(self.winfo_toplevel())
        dialog.resizable(False, False)
        try:
            dialog.grab_set()
        except Exception:  # noqa: BLE001
            pass

        container = ttk.Frame(dialog, padding=10)
        container.pack(fill="both", expand=True)

        ttk.Label(container, text="Select variables based on:").pack(
            anchor="w", pady=(0, 6)
        )

        current_btn = ttk.Radiobutton(
            container,
            text="Current model",
            value="current",
            variable=self.dataset_filter_mode_var,
            command=self._handle_dataset_filter_mode_change,
        )
        current_btn.pack(anchor="w")

        range_btn = ttk.Radiobutton(
            container,
            text="Model range",
            value="range",
            variable=self.dataset_filter_mode_var,
            command=self._handle_dataset_filter_mode_change,
        )
        range_btn.pack(anchor="w", pady=(4, 0))

        range_frame = ttk.Frame(container)
        range_frame.pack(fill="x", padx=(20, 0), pady=(4, 0))

        from_label = ttk.Label(range_frame, text="From")
        from_label.grid(row=0, column=0, padx=(0, 5))

        start_entry = ttk.Entry(
            range_frame,
            textvariable=self.model_range_start_var,
            width=8,
        )
        start_entry.grid(row=0, column=1)

        to_label = ttk.Label(range_frame, text="to")
        to_label.grid(row=0, column=2, padx=(8, 5))

        end_entry = ttk.Entry(
            range_frame,
            textvariable=self.model_range_end_var,
            width=8,
        )
        end_entry.grid(row=0, column=3)

        button_row = ttk.Frame(container)
        button_row.pack(fill="x", pady=(10, 0))

        create_button = ttk.Button(
            button_row,
            text="Create",
            command=self._handle_filtered_dataset_create,
        )
        create_button.pack(side="right")

        ttk.Button(
            button_row,
            text="Cancel",
            command=self._close_filter_dialog,
        ).pack(side="right", padx=(0, 5))

        self._filter_dialog = dialog
        self._filter_dialog_controls = {
            "range_button": range_btn,
            "from_entry": start_entry,
            "to_entry": end_entry,
            "from_label": from_label,
            "to_label": to_label,
        }
        dialog.protocol("WM_DELETE_WINDOW", self._close_filter_dialog)
        self._handle_dataset_filter_mode_change()

        try:
            self.master_app._center_dialog(dialog)
        except Exception:  # noqa: BLE001
            dialog.update_idletasks()
            parent = dialog.master
            if parent is not None:
                try:
                    parent.update_idletasks()
                    parent_x = parent.winfo_rootx()
                    parent_y = parent.winfo_rooty()
                    parent_w = parent.winfo_width() or parent.winfo_reqwidth()
                    parent_h = parent.winfo_height() or parent.winfo_reqheight()
                    dlg_w = dialog.winfo_width() or dialog.winfo_reqwidth()
                    dlg_h = dialog.winfo_height() or dialog.winfo_reqheight()
                    pos_x = parent_x + max(0, int((parent_w - dlg_w) / 2))
                    pos_y = parent_y + max(0, int((parent_h - dlg_h) / 2))
                    dialog.geometry(f"+{pos_x}+{pos_y}")
                except Exception:  # noqa: BLE001
                    pass

    def _sync_dataset_path(self, *_args) -> None:
        self.dataset_path_var.set(self.master_app.data_path_var.get())

    def _browse_dataset(self) -> None:
        initial = self.dataset_path_var.get().strip() or self.master_app.data_path_var.get()
        path = filedialog.askopenfilename(
            title="Select dataset",
            initialfile=os.path.basename(initial) if initial else None,
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if path:
            self.dataset_path_var.set(path)

    def _load_dataset(self) -> None:
        path = self.dataset_path_var.get().strip()
        if not path:
            messagebox.showwarning("Dataset", "Please provide a dataset path.")
            return
        try:
            delimiter = self.master_app._get_delimiter(self.master_app.delimiter_var.get())
            split_settings = self.master_app._gather_split_settings()
            exclude_constant, constant_threshold = self.master_app._get_constant_filter()
            context = load_dataset(
                path,
                delimiter=delimiter,
                split=split_settings,
                dependent_choice=self.master_app._get_dependent_choice(),
                non_variable_spec=self.master_app._get_non_variable_spec(),
                exclude_constant=exclude_constant,
                constant_threshold=constant_threshold,
                excluded_observations=self.master_app._get_excluded_observations_text(),
            )
        except Exception as exc:  # noqa: BLE001
            self.dataset_status_var.set(f"Error loading dataset: {exc}")
            messagebox.showerror("Dataset error", f"Unable to load dataset:\n{exc}")
            return

        self._apply_context(context)
        self.dataset_status_var.set(f"Dataset loaded ({len(context.train_df):,} training rows).")
        self._update_axis_choices()
        self._update_plot()

    def _apply_context(self, context: EPRSContext) -> None:
        self._current_context = context
        self._reset_axis_state()
        if self._label_manager is not None:
            self._label_manager.reset()
        candidate_label = getattr(context, "primary_non_variable_column", None)
        if candidate_label and candidate_label not in context.train_df.columns:
            candidate_label = None
        self._non_variable_label_column = candidate_label
        self._refresh_label_mode_options()

    def _reset_axis_state(self) -> None:
        self._axis_default_context = None
        self._axis_user_override = {"x": False, "y": False}
        self._axis_parameters = {"x": None, "y": None}
        self._stored_limits = {"x": None, "y": None}
        self.custom_xlabel = None
        self.custom_ylabel = None
        self.axis_label_fontsize = None
        self.xmin_var.set("")
        self.xmax_var.set("")
        self.ymin_var.set("")
        self.ymax_var.set("")
        self.x_tick_step_var.set("")
        self.y_tick_step_var.set("")
        self.r2_value_var.set("")

    def _handle_source_change(self) -> None:
        if self.axis_source_var.get() == "model":
            if not self._model_variables:
                messagebox.showinfo(
                    "Model variables",
                    "Model results are required before selecting model variables.",
                )
                self.axis_source_var.set("all")
                return
            self.model_entry.configure(state="normal")
            self._refresh_model_variables()
        else:
            self.model_entry.configure(state="disabled")
            self._current_model_id = None
            self._update_axis_choices()
            self._update_plot()
        self._sync_partial_residual_controls()

    def _handle_partial_residual_toggle(self) -> None:
        if self.axis_source_var.get() != "model" or not self._model_variables:
            if self.partial_residuals_var.get():
                self.partial_residuals_var.set(False)
            return
        self._update_plot()

    def _handle_histogram_toggle(self) -> None:
        if self.histogram_var.get() and self.partial_residuals_var.get():
            self.partial_residuals_var.set(False)
        if self.histogram_var.get():
            self.legend_var.set(False)
            self.linear_fit_var.set(False)
            self.compute_r2_var.set(False)
            self.gridline_var.set(False)
        self._update_histogram_control_state()
        self._sync_histogram_controls()
        self._sync_partial_residual_controls()
        self._update_axis_choices()
        if not self.histogram_var.get():
            self._restore_default_axis_selection()
        self._update_plot()

    def _update_histogram_control_state(self) -> None:
        enabled = self.histogram_var.get()
        if self.bin_settings_button is not None:
            try:
                state = "normal" if enabled else "disabled"
                self.bin_settings_button.configure(state=state)
            except Exception:  # noqa: BLE001
                pass
        if self.bar_color_combo is not None:
            try:
                color_state = "readonly" if enabled else "disabled"
                self.bar_color_combo.configure(state=color_state)
            except Exception:  # noqa: BLE001
                pass
        if self.bar_color_label is not None:
            try:
                color_label_state = "normal" if enabled else "disabled"
                self.bar_color_label.configure(state=color_label_state)
            except Exception:  # noqa: BLE001
                pass

    def _sync_histogram_controls(self) -> None:
        histogram = self.histogram_var.get()
        legend_state = "normal" if not histogram else "disabled"
        marker_state = "readonly" if not histogram else "disabled"
        marker_size_state = tk.NORMAL if not histogram else tk.DISABLED

        if histogram:
            if self.legend_var.get():
                self.legend_var.set(False)
            if self.linear_fit_var.get():
                self.linear_fit_var.set(False)
            if self.compute_r2_var.get():
                self.compute_r2_var.set(False)

        if self.legend_check is not None:
            try:
                self.legend_check.configure(state=legend_state)
            except Exception:  # noqa: BLE001
                pass

        if self.linear_fit_check is not None:
            try:
                self.linear_fit_check.configure(state=legend_state)
            except Exception:  # noqa: BLE001
                pass

        if self.linear_fit_color_combo is not None:
            try:
                color_state = "readonly" if not histogram else "disabled"
                self.linear_fit_color_combo.configure(state=color_state)
            except Exception:  # noqa: BLE001
                pass

        if self.r2_check is not None:
            try:
                self.r2_check.configure(state=legend_state)
            except Exception:  # noqa: BLE001
                pass

        if self.r2_value_entry is not None:
            try:
                r2_state = "readonly" if not histogram else "disabled"
                self.r2_value_entry.configure(state=r2_state)
            except Exception:  # noqa: BLE001
                pass

        if self.gridline_check is not None:
            try:
                self.gridline_check.configure(state=legend_state)
            except Exception:  # noqa: BLE001
                pass

        if self.marker_color_combo is not None:
            try:
                self.marker_color_combo.configure(state=marker_state)
            except Exception:  # noqa: BLE001
                pass

        if self.marker_style_combo is not None:
            try:
                self.marker_style_combo.configure(state=marker_state)
            except Exception:  # noqa: BLE001
                pass
        if self.marker_size_scale is not None:
            try:
                self.marker_size_scale.configure(state=marker_size_state)
            except Exception:  # noqa: BLE001
                pass
        if self.marker_size_label is not None:
            try:
                label_state = tk.NORMAL if not histogram else tk.DISABLED
                self.marker_size_label.configure(state=label_state)
            except Exception:  # noqa: BLE001
                pass

        if self.label_mode_combo is not None:
            try:
                mode_state = "readonly" if not histogram else "disabled"
                self.label_mode_combo.configure(state=mode_state)
            except Exception:  # noqa: BLE001
                pass

        self._sync_legend_controls()

    def _clear_histogram_label_draggables(self) -> None:
        for draggable in list(self._histogram_label_draggables):
            try:
                draggable.disconnect()
            except Exception:  # noqa: BLE001
                pass
        self._histogram_label_draggables.clear()

    def _sync_partial_residual_controls(self) -> None:
        check = self.partial_residuals_check
        allow = (
            self.axis_source_var.get() == "model"
            and bool(self._model_variables)
            and not self.histogram_var.get()
        )
        if check is not None:
            state = "normal" if allow else "disabled"
            try:
                check.configure(state=state)
            except Exception:  # noqa: BLE001
                pass
        if not allow and self.partial_residuals_var.get():
            self.partial_residuals_var.set(False)

    def _handle_dataset_filter_mode_change(self) -> None:
        if not self.model_range_start_var.get().strip():
            self.model_range_start_var.set("1")

        controls = self._filter_dialog_controls
        if not controls:
            return

        allow_range_entries = self.dataset_filter_mode_var.get() == "range"
        entry_state = "normal" if allow_range_entries else "disabled"
        label_state = "normal" if allow_range_entries else "disabled"

        for key in ("from_entry", "to_entry"):
            widget = controls.get(key)
            if widget is not None:
                try:
                    widget.configure(state=entry_state)
                except Exception:  # noqa: BLE001
                    widget.configure(state="disabled")

        for key in ("from_label", "to_label"):
            widget = controls.get(key)
            if widget is not None:
                try:
                    widget.configure(state=label_state)
                except Exception:  # noqa: BLE001
                    widget.configure(state="disabled")

    def _handle_filtered_dataset_create(self) -> None:
        if self._create_filtered_dataset():
            self._close_filter_dialog()

    def _create_filtered_dataset(self) -> bool:
        if self._current_context is None:
            messagebox.showinfo(
                "Filtered dataset",
                "Load a dataset before creating a filtered dataset.",
            )
            return False

        if not self._model_variables:
            messagebox.showinfo(
                "Filtered dataset",
                "Model results are required before creating a filtered dataset.",
            )
            return False

        context = self._current_context
        df = context.train_df
        if df.empty:
            messagebox.showinfo(
                "Filtered dataset",
                "The training dataset is empty. Nothing to export.",
            )
            return False

        mode = self.dataset_filter_mode_var.get()
        selected_models: list[int]
        missing_models: list[int] = []

        if mode == "range":
            start_text = self.model_range_start_var.get().strip()
            end_text = self.model_range_end_var.get().strip()
            if not start_text or not end_text:
                messagebox.showinfo(
                    "Model range",
                    "Provide both the start and end model identifiers.",
                )
                return False
            try:
                start_id = int(start_text)
                end_id = int(end_text)
            except ValueError:
                messagebox.showerror(
                    "Model range",
                    "Model identifiers must be numeric values.",
                )
                return False
            if start_id > end_id:
                messagebox.showerror(
                    "Model range",
                    "The start model identifier must be less than or equal to the end identifier.",
                )
                return False
            model_ids = list(range(start_id, end_id + 1))
            available: list[int] = []
            for model_id in model_ids:
                if model_id in self._model_variables:
                    available.append(model_id)
                else:
                    missing_models.append(model_id)
            if not available:
                messagebox.showerror(
                    "Model range",
                    "None of the specified models were found in the results.",
                )
                return False
            selected_models = available
        else:
            model_text = self.model_id_var.get().strip()
            if not model_text:
                messagebox.showinfo(
                    "Filtered dataset",
                    "Enter a model identifier before creating the dataset.",
                )
                return False
            try:
                model_id = int(model_text)
            except ValueError:
                messagebox.showerror(
                    "Filtered dataset",
                    "The model identifier must be numeric.",
                )
                return False
            if model_id not in self._model_variables:
                messagebox.showerror(
                    "Filtered dataset",
                    "The specified model was not found in the results.",
                )
                return False
            selected_models = [model_id]

        variable_order: list[str] = []
        seen_variables: set[str] = set()
        for model_id in selected_models:
            for variable in self._model_variables.get(model_id, []):
                if variable not in seen_variables:
                    seen_variables.add(variable)
                    variable_order.append(variable)

        if not variable_order:
            messagebox.showinfo(
                "Filtered dataset",
                "The selected model set does not contain any variables to export.",
            )
            return False

        columns_to_keep: list[str] = []
        observation_column = context.observation_column
        if observation_column and observation_column in df.columns:
            columns_to_keep.append(observation_column)
        id_column = context.id_column
        target_column = context.target_column
        if (
            id_column
            and id_column != observation_column
            and id_column in df.columns
            and id_column not in columns_to_keep
        ):
            columns_to_keep.append(id_column)

        non_variable_columns = list(getattr(context, "non_variable_columns", ()))
        for column in non_variable_columns:
            if column == target_column:
                continue
            if column in df.columns and column not in columns_to_keep:
                columns_to_keep.append(column)

        included_variables = [var for var in variable_order if var in df.columns]
        if not included_variables:
            messagebox.showinfo(
                "Filtered dataset",
                "None of the selected variables are available in the dataset.",
            )
            return False

        for variable in included_variables:
            if variable == target_column:
                continue
            if variable not in columns_to_keep:
                columns_to_keep.append(variable)

        if (
            target_column
            and target_column in df.columns
            and target_column not in columns_to_keep
        ):
            columns_to_keep.append(target_column)

        export_columns = [
            column for column in columns_to_keep if column != observation_column
        ]
        if not export_columns:
            messagebox.showinfo(
                "Filtered dataset",
                "No columns are available to export after removing observations.",
            )
            return False

        default_name = (
            f"model_{selected_models[0]}_dataset.csv"
            if len(selected_models) == 1
            else "model_range_dataset.csv"
        )
        path = filedialog.asksaveasfilename(
            title="Save filtered dataset",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=default_name,
        )
        if not path:
            return False

        try:
            df.loc[:, export_columns].to_csv(path, index=False, sep=";")
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Filtered dataset", f"Unable to save dataset:\n{exc}")
            return False

        if missing_models:
            missing_text = ", ".join(str(model) for model in missing_models)
            messagebox.showwarning(
                "Filtered dataset",
                "Filtered dataset saved, but these models were not found in the results: "
                f"{missing_text}.",
            )
        else:
            messagebox.showinfo(
                "Filtered dataset",
                f"Filtered dataset saved to:\n{path}",
            )

        return True

    def _refresh_model_variables(self) -> None:
        if self.axis_source_var.get() != "model":
            return
        model_text = self.model_id_var.get().strip()
        if not model_text:
            self.plot_status_var.set("Enter a model identifier to use model variables.")
            self._current_model_id = None
            self._update_axis_choices(options=[])
            self._update_plot()
            return
        try:
            model_id = int(model_text)
        except ValueError:
            self.plot_status_var.set("Invalid model identifier.")
            self._current_model_id = None
            self._update_axis_choices(options=[])
            self._update_plot()
            return

        variables = self._model_variables.get(model_id)
        if not variables:
            self.plot_status_var.set("Model not found in results.")
            self._current_model_id = None
            self._update_axis_choices(options=[])
            self._update_plot()
            return

        self._current_model_id = model_id
        self.plot_status_var.set(f"Using model {model_id} ({len(variables)} variables).")
        self._update_axis_choices()
        self._update_plot()

    def _update_axis_choices(self, options: Optional[list[tuple[str, str]]] = None) -> None:
        context = self._current_context
        if context is None:
            self.x_axis_combo.configure(values=(), state="disabled")
            self.y_axis_combo.configure(values=(), state="disabled")
            self._refresh_label_mode_options()
            return

        if options is None:
            options = self._build_axis_options(context)

        self._latest_axis_options = list(options)

        if self.histogram_var.get():
            histogram_options = [
                (label, value) for label, value in options if value != "__observations__"
            ]
            x_labels = [label for label, _value in histogram_options]
            if x_labels:
                self.x_axis_combo.configure(values=x_labels, state="readonly")
                default_x = x_labels[0]
                if self.x_axis_var.get() not in x_labels:
                    self.x_axis_var.set(default_x)
                    self.x_axis_combo.set(default_x)
            else:
                self.x_axis_combo.configure(values=(), state="disabled")
                self.x_axis_var.set("")
            self.y_axis_combo.configure(values=("Frequency",), state="readonly")
            self.y_axis_var.set("Frequency")
            self.y_axis_combo.set("Frequency")
            self._refresh_label_mode_options()
            return

        labels = [label for label, _value in options]
        self.x_axis_combo.configure(values=labels, state="readonly")
        self.y_axis_combo.configure(values=labels, state="readonly")

        default_x = labels[0] if labels else ""
        default_y = labels[1] if len(labels) > 1 else (labels[0] if labels else "")

        if self.x_axis_var.get() not in labels and default_x:
            self.x_axis_var.set(default_x)
            self.x_axis_combo.set(default_x)
        if self.y_axis_var.get() not in labels and default_y:
            self.y_axis_var.set(default_y)
            self.y_axis_combo.set(default_y)

        if not labels:
            self.x_axis_combo.configure(state="disabled")
            self.y_axis_combo.configure(state="disabled")

        self._refresh_label_mode_options()

    def _restore_default_axis_selection(self) -> None:
        labels = [label for label, _value in getattr(self, "_latest_axis_options", [])]
        if not labels:
            return

        default_x = labels[0]
        default_y = labels[1] if len(labels) > 1 else labels[0]

        if default_x:
            self.x_axis_var.set(default_x)
            try:
                self.x_axis_combo.set(default_x)
            except Exception:  # noqa: BLE001
                pass

        if default_y:
            self.y_axis_var.set(default_y)
            try:
                self.y_axis_combo.set(default_y)
            except Exception:  # noqa: BLE001
                pass

        self._refresh_label_mode_options()

    def _build_axis_options(self, context: EPRSContext) -> list[tuple[str, str]]:
        options: list[tuple[str, str]] = [("Observations", "__observations__")]
        existing = {"__observations__"}
        if context.target_column:
            options.append((str(context.target_column), context.target_column))
            existing.add(context.target_column)

        if self.axis_source_var.get() == "model" and self._current_model_id is not None:
            variables = self._model_variables.get(self._current_model_id, [])
        else:
            variables = list(context.cols)

        for var in variables:
            if var in existing:
                continue
            options.append((str(var), var))
            existing.add(var)
        return options

    def _refresh_label_mode_options(self) -> None:
        if self.histogram_var.get():
            options = [("Interval and %", "histogram_interval_percent")]
            self._label_mode_options = options
            self.label_mode_var.set("histogram_interval_percent")
            if hasattr(self, "label_mode_combo") and self.label_mode_combo is not None:
                try:
                    self.label_mode_combo.configure(values=["Interval and %"])
                    self.label_mode_combo.set("Interval and %")
                    self.label_mode_combo.configure(state="disabled")
                except Exception:  # noqa: BLE001
                    pass
            return

        options = list(self.BASE_LABEL_MODE_OPTIONS)
        extra_label = self._non_variable_label_column
        if extra_label:
            options.insert(1, (extra_label, f"non_variable:{extra_label}"))

        current_labels = getattr(self, "_current_axis_labels", ("", ""))
        current_x_label = current_labels[0] if len(current_labels) > 0 else ""
        current_y_label = current_labels[1] if len(current_labels) > 1 else ""

        x_display = (current_x_label or self.x_axis_var.get() or "").strip()
        y_display = (current_y_label or self.y_axis_var.get() or "").strip()

        excluded_labels = {"observations", "n obs"}
        if x_display and x_display.lower() not in excluded_labels:
            options.append((x_display, "x"))
        if y_display and y_display.lower() not in excluded_labels:
            options.append((y_display, "y"))

        self._label_mode_options = options

        current_value = self.label_mode_var.get() or "observations"
        valid_values = {value for _, value in options}
        if current_value not in valid_values:
            current_value = "observations"
            self.label_mode_var.set(current_value)

        if hasattr(self, "label_mode_combo") and self.label_mode_combo is not None:
            labels = [label for label, _value in options]
            self.label_mode_combo.configure(values=labels)
            label_lookup = {value: label for label, value in options}
            display_label = label_lookup.get(current_value)
            if not display_label and labels:
                display_label = labels[0]
                self.label_mode_var.set(options[0][1])
            if display_label:
                self.label_mode_combo.set(display_label)
            try:
                self.label_mode_combo.configure(state="readonly")
            except Exception:  # noqa: BLE001
                pass

    def _handle_label_mode_change(self, selection: str) -> None:
        lookup = {label: value for label, value in self._label_mode_options}
        mode = lookup.get(selection, "observations")
        self.label_mode_var.set(mode)
        if self._label_manager is not None:
            self._label_manager.suspend()
        self._update_plot()

    def _handle_aspect_ratio_change(self) -> None:
        if self.ax is None:
            return
        self._apply_axis_formatting()
        self.canvas.draw_idle()

    def _refresh_bar_options(self, data_count: int) -> None:
        max_available = max(2, data_count if data_count > 1 else 2)
        values = [str(i) for i in range(2, max_available + 1)]
        if not values:
            values = ["2"]
        current = self.bar_count_var.get()
        if current not in values:
            preferred = "4"
            if preferred in values:
                self.bar_count_var.set(preferred)
            else:
                default_index = min(8, len(values) - 1)
                self.bar_count_var.set(values[default_index])

    def _resolve_bar_count(self, max_count: int) -> int:
        try:
            value = int(float(self.bar_count_var.get()))
        except (TypeError, ValueError):
            value = max_count if max_count >= 2 else 2
        upper_bound = max(2, max_count if max_count > 1 else 2)
        value = max(2, value)
        if value > upper_bound:
            value = upper_bound
        return value

    def _resolve_histogram_bins(self, max_count: int) -> Optional[Union[int, Sequence[float]]]:
        if self.bin_mode_var.get() == "range":
            edges = self._parse_bin_edges_text(self.bin_edges_var.get())
            return edges
        return self._resolve_bar_count(max_count)

    def _plot_histogram(self, x_values: np.ndarray, bar_color: str) -> bool:
        if x_values.size == 0:
            self._clear_plot("No data points available for the histogram.")
            return False

        self._clear_histogram_label_draggables()
        self._histogram_label_draggables = []
        bins = self._resolve_histogram_bins(len(x_values))
        if bins is None:
            self._clear_plot("No valid bin configuration provided.")
            return False

        hist, bin_edges = np.histogram(x_values, bins=bins)
        total = hist.sum()
        if total <= 0:
            self._clear_plot("No data points available for the histogram.")
            return False

        percentages = (hist / total) * 100.0
        widths = np.diff(bin_edges)
        starts = bin_edges[:-1]
        linewidth_raw = self.bin_linewidth_var.get().strip()
        linewidth: Optional[float]
        if linewidth_raw.lower() == "none":
            linewidth = None
        else:
            try:
                linewidth = float(linewidth_raw) if linewidth_raw else 1.2
            except (TypeError, ValueError):
                linewidth = 1.2
        edgecolor = "none" if linewidth is None or linewidth <= 0 else "black"
        linewidth_value = 0 if linewidth is None or linewidth <= 0 else linewidth
        containers = self.ax.bar(
            starts,
            hist,
            width=widths,
            align="edge",
            facecolor=bar_color,
            edgecolor=edgecolor,
            linewidth=linewidth_value,
            alpha=0.8,
            label=self._DEFAULT_DATASET_LABEL,
            zorder=3,
        )

        for rect in containers:
            rect.set_facecolor(bar_color)
            rect.set_edgecolor(edgecolor)
            rect.set_linewidth(max(rect.get_linewidth(), linewidth_value))
            rect.set_alpha(0.8)
            rect.set_zorder(3)

        def _format_interval_bound(value: float) -> str:
            if not math.isfinite(value):
                return ""
            text = f"{value:.2f}"
            return "0.00" if text == "-0.00" else text

        tick_positions = starts + (widths / 2.0)
        interval_labels = []
        for idx in range(len(bin_edges) - 1):
            left = _format_interval_bound(bin_edges[idx])
            right = _format_interval_bound(bin_edges[idx + 1])
            closing = "]" if idx == len(bin_edges) - 2 else ")"
            interval = f"[{left}, {right}{closing}"
            interval_labels.append(interval)
        self.ax.set_xticks(tick_positions)
        self.ax.set_xticklabels(interval_labels)

        for idx, (rect, percent) in enumerate(zip(containers, percentages)):
            if percent <= 0:
                continue
            height = rect.get_height()
            label_text = interval_labels[idx]
            annotation = self.ax.annotate(
                f"{label_text}: {percent:.1f}%",
                xy=(rect.get_x() + rect.get_width() / 2.0, height),
                xycoords="data",
                xytext=(0.0, 4.0),
                textcoords="offset points",
                fontsize=9,
                ha="center",
                va="bottom",
                bbox=dict(boxstyle="round,pad=0.3", fc=(1.0, 1.0, 1.0, 0.9), ec="#666666"),
                arrowprops=dict(arrowstyle="-", lw=0.8, color="#666666"),
            )
            annotation.set_zorder(5)
            try:
                draggable = annotation.draggable(True)
            except Exception:  # noqa: BLE001
                draggable = None
            if draggable is not None:
                self._histogram_label_draggables.append(draggable)

        x_extent = np.concatenate([starts, starts + widths])
        if x_extent.size == 0:
            x_extent = np.array([0.0, 1.0])

        y_axis_values = np.concatenate([hist, np.array([0])])

        self._apply_axis_defaults(
            x_extent,
            y_axis_values,
            update_x=not self._axis_user_override["x"],
            update_y=not self._axis_user_override["y"],
        )
        self.ax.set_ylim(bottom=0)

        return True

    def _prompt_axis_labels(self) -> None:
        if self._current_context is None:
            messagebox.showinfo("Axis labels", "Load a dataset before setting axis labels.")
            return
        base_x = self.custom_xlabel or self._current_axis_labels[0] or self.x_axis_var.get() or "X"
        base_y = self.custom_ylabel or self._current_axis_labels[1] or self.y_axis_var.get() or "Y"
        dialog = _AxisLabelDialog(
            self,
            title="Axis labels",
            initial_x=base_x,
            initial_y=base_y,
            initial_font_size=self.axis_label_fontsize,
        )
        if dialog.result is None:
            return
        x_label, y_label, font_size = dialog.result
        self.custom_xlabel = x_label or None
        self.custom_ylabel = y_label or None
        self.axis_label_fontsize = font_size
        self._update_plot()

    def _save_current_plot(self) -> None:
        if self._current_context is None:
            messagebox.showinfo("Save plot", "Load a dataset before saving the plot.")
            return
        path = filedialog.asksaveasfilename(
            title="Save plot",
            defaultextension=".png",
            filetypes=(
                ("PNG", "*.png"),
                ("TIFF", "*.tiff"),
                ("PDF", "*.pdf"),
                ("SVG", "*.svg"),
                ("All files", "*.*"),
            ),
        )
        if not path:
            return
        try:
            current_dpi = float(self.figure.dpi)
            target_dpi = current_dpi if current_dpi >= 1000 else 1000
            self.figure.savefig(path, bbox_inches="tight", dpi=target_dpi)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Save plot", f"Unable to save plot:\n{exc}")

    def _get_selected_axis_keys(self) -> Optional[tuple[str, str]]:
        context = self._current_context
        if context is None:
            return None
        options = self._build_axis_options(context)
        mapping = {label: value for label, value in options}
        if self.histogram_var.get():
            x_key = mapping.get(self.x_axis_var.get())
            if x_key is None:
                return None
            return x_key, "__observations__"

        x_key = mapping.get(self.x_axis_var.get())
        y_key = mapping.get(self.y_axis_var.get())
        if x_key is None or y_key is None:
            return None
        return x_key, y_key

    def _clear_plot(self, message: str = "") -> None:
        self._clear_histogram_label_draggables()
        self.ax.clear()
        self._enforce_axis_ratio(self.ax)
        self.ax.set_xticks([])
        self.ax.set_yticks([])
        if message:
            self.ax.text(
                0.5,
                0.5,
                message,
                ha="center",
                va="center",
                transform=self.ax.transAxes,
                color="#666666",
            )
        self.plot_status_var.set(message)
        self.canvas.draw_idle()
        if self._label_manager is not None:
            self._label_manager.suspend()
        self.r2_value_var.set("")
        self._axis_parameters = {"x": None, "y": None}
        self._stored_limits = {"x": None, "y": None}

    def _update_plot(self, *_args) -> None:
        context = self._current_context
        if context is None:
            self._clear_plot("Load a dataset to visualize variables.")
            return

        axis_keys = self._get_selected_axis_keys()
        if axis_keys is None:
            self._clear_plot("Select valid axes to display the plot.")
            return

        self.r2_value_var.set("")
        x_key, y_key = axis_keys
        x_label = self.x_axis_var.get()
        y_label = self.y_axis_var.get()

        data = self._resolve_axis_data(context, x_key, y_key)
        if data is None:
            self._clear_plot("Unable to resolve the selected variables.")
            return

        x_values, y_values, identities, label_overrides = data
        if label_overrides is not None:
            x_override, y_override = label_overrides
            if x_override:
                x_label = x_override
            if y_override:
                y_label = y_override
        mask = np.isfinite(x_values) & np.isfinite(y_values)
        if not mask.any():
            self._clear_plot("No finite data points for the selected axes.")
            return

        x_values = x_values[mask]
        y_values = y_values[mask]
        identities = identities[mask]

        axis_context = (
            id(context),
            self.axis_source_var.get(),
            self._current_model_id,
            x_key,
            y_key,
        )
        if axis_context != self._axis_default_context:
            self._axis_default_context = axis_context
            self._axis_user_override = {"x": False, "y": False}
            self._axis_parameters = {"x": None, "y": None}
            self._stored_limits = {"x": None, "y": None}

        self._clear_histogram_label_draggables()
        self.ax.clear()
        self._enforce_axis_ratio(self.ax)
        self._apply_gridlines(self.ax)
        self._refresh_label_mode_options()

        self._refresh_bar_options(len(x_values))

        if self.histogram_var.get():
            bar_color = self._resolve_bar_color()
            self._current_axis_labels = self._apply_axis_labels(x_label, "Frequency")
            if not self._plot_histogram(x_values, bar_color):
                return
            if self.legend_var.get():
                self._show_legend()
            else:
                self._legend_loc = None
            if self._label_manager is not None:
                self._label_manager.suspend()
            self._apply_axis_formatting()
            self.canvas.draw_idle()
            return

        marker_color = self._resolve_marker_color()
        marker_style = self._resolve_marker_style()
        point_size = float(self.point_size_var.get())

        self._current_axis_labels = self._apply_axis_labels(x_label, y_label)

        collection = self.ax.scatter(
            x_values,
            y_values,
            s=point_size,
            color=marker_color,
            alpha=0.8,
            marker=marker_style,
            label=self._DEFAULT_DATASET_LABEL,
        )

        if self._label_manager is not None:
            self._label_manager.begin_plot("variable_explorer", self.ax)
            self._label_manager.register_collection(
                "variable_explorer",
                collection,
                identities,
                x_values,
                y_values,
            )

        self._apply_axis_defaults(
            x_values,
            y_values,
            update_x=not self._axis_user_override["x"],
            update_y=not self._axis_user_override["y"],
        )

        if self.linear_fit_var.get() and x_values.size >= 2:
            self._draw_linear_fit(self.ax, x_values, y_values)

        self._update_r2_display(self.ax, x_values, y_values)

        if self.legend_var.get():
            self._show_legend()
        else:
            self._legend_loc = None

        if self._label_manager is not None:
            self._label_manager.complete_plot()

        self._apply_axis_formatting()
        self.canvas.draw_idle()

    def _draw_linear_fit(self, ax: Axes, x_values: np.ndarray, y_values: np.ndarray) -> None:
        try:
            slope, intercept = np.polyfit(x_values, y_values, 1)
        except Exception:  # noqa: BLE001
            return
        x_min = float(np.min(x_values))
        x_max = float(np.max(x_values))
        if not np.isfinite(x_min) or not np.isfinite(x_max) or x_min == x_max:
            return
        x_line = np.array([x_min, x_max], dtype=float)
        y_line = slope * x_line + intercept
        self.ax.plot(
            x_line,
            y_line,
            color=self._resolve_linear_fit_color(),
            linestyle="--",
            linewidth=1.2,
            label="Linear fit",
        )

    def _update_r2_display(self, _ax: Axes, x_values: np.ndarray, y_values: np.ndarray) -> None:
        self.r2_value_var.set("")
        if not self.compute_r2_var.get() or x_values.size < 2:
            return

        try:
            slope, intercept = np.polyfit(x_values, y_values, 1)
            y_pred = slope * x_values + intercept
            if not np.all(np.isfinite(y_pred)):
                return
            score = r2_score(y_values, y_pred)
        except Exception:  # noqa: BLE001
            return

        if not np.isfinite(score):
            return

        self.r2_value_var.set(f"{score:.4f}")

    def _compute_partial_residual_arrays(
        self,
        context: EPRSContext,
        x_key: str,
        y_key: str,
        x_values: np.ndarray,
        y_values: np.ndarray,
    ) -> Optional[tuple[np.ndarray, np.ndarray, Optional[str], Optional[str]]]:
        if not self.partial_residuals_var.get():
            return None
        if self.axis_source_var.get() != "model":
            return None
        model_id = self._current_model_id
        if model_id is None:
            return None

        model_vars = [
            var
            for var in self._model_variables.get(model_id, [])
            if var in context.train_df.columns
        ]
        if not model_vars:
            return None

        target_column = context.target_column
        if not target_column or target_column not in context.train_df.columns:
            return None

        df = context.train_df.reset_index(drop=True)
        if df.empty:
            return None

        x_transformed = x_values.astype(float, copy=True)
        y_transformed = y_values.astype(float, copy=True)
        x_label_override: Optional[str] = None
        y_label_override: Optional[str] = None

        model_var_set = set(model_vars)
        x_is_model_var = x_key in model_var_set
        y_is_model_var = y_key in model_var_set
        x_is_target = x_key == target_column
        y_is_target = y_key == target_column

        if not any((x_is_model_var, y_is_model_var, x_is_target, y_is_target)):
            return None

        def compute_residuals(
            response: str, predictors: list[str]
        ) -> Optional[np.ndarray]:
            numeric_cols = [response] + predictors
            try:
                numeric_df = df.loc[:, numeric_cols].apply(
                    pd.to_numeric, errors="coerce"
                )
            except KeyError:
                return None

            valid_mask = numeric_df.notna().all(axis=1)
            residuals = np.full(len(df), np.nan, dtype=float)
            if not valid_mask.any():
                return residuals

            design = np.ones((int(valid_mask.sum()), 1), dtype=float)
            if predictors:
                predictor_values = numeric_df.loc[valid_mask, predictors].to_numpy(
                    dtype=float
                )
                design = np.column_stack([design, predictor_values])

            target_values = numeric_df.loc[valid_mask, response].to_numpy(dtype=float)
            try:
                coeffs, _, _, _ = np.linalg.lstsq(design, target_values, rcond=None)
            except Exception:  # noqa: BLE001
                return residuals

            fitted = design @ coeffs
            valid_indices = np.flatnonzero(valid_mask.to_numpy())
            residuals[valid_indices] = target_values - fitted
            return residuals

        if x_is_model_var:
            predictors = [var for var in model_vars if var != x_key]
            residuals = compute_residuals(x_key, predictors)
            if residuals is not None:
                x_transformed = residuals.astype(float)
                x_label_override = f"Partial residual of {x_key}"

        if y_is_model_var:
            predictors = [var for var in model_vars if var != y_key]
            residuals = compute_residuals(y_key, predictors)
            if residuals is not None:
                y_transformed = residuals.astype(float)
                y_label_override = f"Partial residual of {y_key}"

        if x_is_target:
            exclude = y_key if y_key in model_var_set else None
            predictors = [var for var in model_vars if var != exclude]
            residuals = compute_residuals(target_column, predictors)
            if residuals is not None:
                x_transformed = residuals.astype(float)
                x_label_override = f"Partial residual of {target_column}"

        if y_is_target:
            exclude = x_key if x_key in model_var_set else None
            predictors = [var for var in model_vars if var != exclude]
            residuals = compute_residuals(target_column, predictors)
            if residuals is not None:
                y_transformed = residuals.astype(float)
                y_label_override = f"Partial residual of {target_column}"

        return x_transformed, y_transformed, x_label_override, y_label_override

    def _resolve_axis_data(
        self, context: EPRSContext, x_key: str, y_key: str
    ) -> Optional[tuple[np.ndarray, np.ndarray, np.ndarray, tuple[Optional[str], Optional[str]]]]:
        df = context.train_df.reset_index(drop=True)
        if df.empty:
            return None

        observation_column = context.observation_column
        if observation_column in df.columns:
            observation_series = df[observation_column].reset_index(drop=True)
        else:
            observation_series = pd.Series(range(1, len(df) + 1))

        observation_labels = observation_series.to_numpy(dtype=object)
        observation_numeric = pd.to_numeric(
            observation_series, errors="coerce"
        ).to_numpy(dtype=float)
        fallback_index = np.arange(1, len(df) + 1, dtype=float)
        invalid_mask = ~np.isfinite(observation_numeric)
        if invalid_mask.any():
            observation_numeric = np.where(invalid_mask, fallback_index, observation_numeric)

        label_column = getattr(context, "primary_non_variable_column", None)
        label_values: Optional[np.ndarray] = None
        if label_column and label_column in df.columns:
            label_values = df[label_column].reset_index(drop=True).to_numpy(dtype=object)

        def _extract(key: str) -> Optional[np.ndarray]:
            if key == "__observations__":
                return observation_numeric.astype(float, copy=True)
            if key not in df.columns:
                return None
            try:
                values = pd.to_numeric(df[key], errors="coerce").to_numpy(dtype=float)
            except Exception:  # noqa: BLE001
                return None
            return values

        x_values = _extract(x_key)
        y_values = _extract(y_key)
        if x_values is None or y_values is None:
            return None

        label_overrides: tuple[Optional[str], Optional[str]] = (None, None)
        partial_result = self._compute_partial_residual_arrays(
            context, x_key, y_key, x_values, y_values
        )
        if partial_result is not None:
            x_values, y_values, x_label_override, y_label_override = partial_result
            label_overrides = (x_label_override, y_label_override)

        identities = np.empty(len(df), dtype=object)
        for idx in range(len(df)):
            unique_key = observation_labels[idx]
            if unique_key is None or (
                isinstance(unique_key, float) and not np.isfinite(unique_key)
            ):
                unique_key = idx + 1
            display_label = unique_key
            if isinstance(display_label, float):
                if not np.isfinite(display_label):
                    display_label = idx + 1
                elif float(display_label).is_integer():
                    display_label = int(display_label)
            extras: dict[str, object] = {}
            if label_values is not None and label_column is not None:
                extras[label_column] = label_values[idx]
            identities[idx] = _PointIdentity(
                dataset=None,
                unique_key=unique_key,
                display_label=display_label,
                x_value=float(x_values[idx]) if np.isfinite(x_values[idx]) else None,
                y_value=float(y_values[idx]) if np.isfinite(y_values[idx]) else None,
                extras=extras,
            )

        return (
            x_values.astype(float),
            y_values.astype(float),
            identities,
            label_overrides,
        )

    def _handle_legend_location_change(self) -> None:
        self._legend_loc = self._get_user_legend_location()
        self._update_plot()

    def _show_legend(self) -> None:
        location = self._get_user_legend_location()
        handles, labels = self.ax.get_legend_handles_labels()
        if not handles:
            return
        legend = self.ax.legend(loc=location)
        if legend is not None:
            self._legend_loc = location

    def _get_user_legend_location(self) -> Optional[Union[str, int]]:
        mapping = {label: loc for label, loc in self.LEGEND_LOCATION_CHOICES}
        return mapping.get(self.legend_location_var.get())

    def _legend_value_from_location(self, location: Union[str, int]) -> Optional[str]:
        if isinstance(location, int):
            for label, loc in self.LEGEND_LOCATION_CHOICES:
                if loc == location:
                    return label
            return None
        lookup = {
            "upper right": "1",
            "upper left": "2",
            "lower left": "3",
            "lower right": "4",
        }
        return lookup.get(str(location).lower().strip())

    def _sync_legend_controls(self) -> None:
        state = tk.NORMAL if self.legend_var.get() else tk.DISABLED
        for btn in getattr(self, "legend_loc_buttons", []):
            btn.configure(state=state)

    def _sync_r2_entry_width(self) -> None:
        combo = getattr(self, "linear_fit_color_combo", None)
        entry = getattr(self, "r2_value_entry", None)
        if combo is None or entry is None:
            return

        try:
            combo.update_idletasks()
            entry.update_idletasks()
        except tk.TclError:
            return

        try:
            combo_width = combo.winfo_reqwidth()
            entry_width = entry.winfo_reqwidth()
        except tk.TclError:
            return

        font_name = entry.cget("font")
        try:
            font = tkfont.nametofont(font_name)
        except (tk.TclError, RuntimeError):
            font = tkfont.nametofont("TkDefaultFont")

        char_width = font.measure("0") or 1
        try:
            configured_chars = int(entry.cget("width"))
        except (TypeError, ValueError):
            configured_chars = 0
        if configured_chars <= 0:
            configured_chars = max(int(entry_width / char_width), 1)

        interior_padding = entry_width - (configured_chars * char_width)
        target_chars = max(math.ceil((combo_width - interior_padding) / char_width), configured_chars)
        entry.configure(width=target_chars)

    def _resolve_marker_color(self) -> str:
        selection = self.marker_color_var.get()
        mapping = {label: value for label, value in self.COLOR_CHOICES}
        color = mapping.get(selection)
        return color if color is not None else "#1f77b4"

    def _resolve_marker_style(self) -> str:
        mapping = {label: value for label, value in self.MARKER_CHOICES}
        return mapping.get(self.marker_style_var.get(), "o")

    def _resolve_bar_color(self) -> str:
        selection = self.bar_color_var.get()
        mapping = {label: value for label, value in self.COLOR_CHOICES}
        color = mapping.get(selection)
        return color if color is not None else "#1f77b4"

    def _compute_axis_parameters_for_values(
        self, values: np.ndarray, axis_name: str
    ) -> Optional[AxisParameters]:
        return compute_axis_parameters(values, axis_name, verbose=False)

    def _with_zero_symmetric_limits(self, values: np.ndarray) -> np.ndarray:
        if values.size == 0:
            return values
        finite_values = values[np.isfinite(values)]
        if finite_values.size == 0:
            return values
        min_val = float(np.min(finite_values))
        max_val = float(np.max(finite_values))
        max_abs = max(abs(min_val), abs(max_val))
        if not math.isfinite(max_abs):
            return values

        symmetric_min = -max_abs if abs(min_val) < max_abs else min_val
        symmetric_max = max_abs if abs(max_val) < max_abs else max_val
        symmetric_bounds = np.asarray([symmetric_min, symmetric_max], dtype=float)
        return np.concatenate([values, symmetric_bounds])

    def _set_axis_limits(
        self,
        x_limits: Optional[Union[AxisParameters, tuple[float, float]]] = None,
        y_limits: Optional[Union[AxisParameters, tuple[float, float]]] = None,
    ) -> None:
        def _as_tuple(data: Optional[Union[AxisParameters, tuple[float, float]]]) -> Optional[tuple[float, float]]:
            if isinstance(data, AxisParameters):
                return (data.minimum, data.maximum)
            return data

        x_tuple = _as_tuple(x_limits)
        y_tuple = _as_tuple(y_limits)
        if x_tuple is not None and len(x_tuple) == 2:
            left, right = x_tuple
            if math.isfinite(left) and math.isfinite(right) and left < right:
                self.ax.set_xlim(left, right)
        if y_tuple is not None and len(y_tuple) == 2:
            bottom, top = y_tuple
            if math.isfinite(bottom) and math.isfinite(top) and bottom < top:
                self.ax.set_ylim(bottom, top)

    def _set_axis_field_defaults(
        self,
        x_params: Optional[Union[AxisParameters, tuple[float, float]]],
        y_params: Optional[Union[AxisParameters, tuple[float, float]]],
        *,
        update_x: bool,
        update_y: bool,
    ) -> None:
        if update_x:
            if isinstance(x_params, AxisParameters):
                self.xmin_var.set(f"{x_params.minimum:.6g}")
                self.xmax_var.set(f"{x_params.maximum:.6g}")
            elif isinstance(x_params, tuple) and len(x_params) == 2:
                self.xmin_var.set(f"{x_params[0]:.6g}")
                self.xmax_var.set(f"{x_params[1]:.6g}")
            else:
                self.xmin_var.set("")
                self.xmax_var.set("")
        if update_y:
            if isinstance(y_params, AxisParameters):
                self.ymin_var.set(f"{y_params.minimum:.6g}")
                self.ymax_var.set(f"{y_params.maximum:.6g}")
            elif isinstance(y_params, tuple) and len(y_params) == 2:
                self.ymin_var.set(f"{y_params[0]:.6g}")
                self.ymax_var.set(f"{y_params[1]:.6g}")
            else:
                self.ymin_var.set("")
                self.ymax_var.set("")

    def _apply_axis_defaults(
        self,
        x_values: Optional[Union[np.ndarray, list, tuple]] = None,
        y_values: Optional[Union[np.ndarray, list, tuple]] = None,
        *,
        update_x: bool = True,
        update_y: bool = True,
    ) -> tuple[Optional[tuple[float, float]], Optional[tuple[float, float]]]:
        x_array = self._sanitize_array(x_values)
        y_array = self._sanitize_array(y_values)

        x_params: Optional[AxisParameters] = self._axis_parameters.get("x")
        y_params: Optional[AxisParameters] = self._axis_parameters.get("y")

        if update_x:
            x_params = None
            if x_array.size:
                x_params = self._compute_axis_parameters_for_values(x_array, "X")
            self._axis_parameters["x"] = x_params
        if update_y:
            y_params = None
            if y_array.size:
                y_params = self._compute_axis_parameters_for_values(y_array, "Y")
            self._axis_parameters["y"] = y_params

        x_limits: Optional[tuple[float, float]] = None
        y_limits: Optional[tuple[float, float]] = None

        if update_x and x_params is not None:
            apply_axis_to_plot(self.ax, "x", x_params)
            x_limits = (x_params.minimum, x_params.maximum)
            self._stored_limits["x"] = x_limits
        elif update_x:
            self._stored_limits["x"] = None

        if update_y and y_params is not None:
            apply_axis_to_plot(self.ax, "y", y_params)
            y_limits = (y_params.minimum, y_params.maximum)
            self._stored_limits["y"] = y_limits
        elif update_y:
            self._stored_limits["y"] = None

        self._set_axis_field_defaults(
            x_params,
            y_params,
            update_x=update_x,
            update_y=update_y,
        )

        self.ax.tick_params(axis="both", which="both", direction="out")
        self._enforce_axis_ratio(self.ax)
        return x_limits, y_limits

    def _apply_axis_formatting(self) -> None:
        def _apply_for_axis(
            axis: str,
            params: Optional[AxisParameters],
            min_var: tk.StringVar,
            max_var: tk.StringVar,
            step_var: tk.StringVar,
        ) -> None:
            base_min = params.minimum if params is not None else None
            base_max = params.maximum if params is not None else None

            override_min = self._parse_optional_float(min_var.get())
            override_max = self._parse_optional_float(max_var.get())

            left = base_min if override_min is None else override_min
            right = base_max if override_max is None else override_max

            if (
                left is None
                or right is None
                or not math.isfinite(left)
                or not math.isfinite(right)
                or left >= right
            ):
                return

            if axis == "x":
                self.ax.set_xlim(left, right)
            else:
                self.ax.set_ylim(left, right)

            step_override = self._parse_optional_float(step_var.get())
            step_value: Optional[float] = None
            decimals = 0
            if step_override is not None and step_override > 0:
                step_value = step_override
                if params is not None:
                    decimals = max(decimals, params.decimals)
            elif params is not None and params.step > 0:
                step_value = params.step
                decimals = params.decimals

            if step_value is not None and step_value > 0:
                try:
                    ticks = build_ticks(left, right, step_value)
                except Exception:  # noqa: BLE001
                    ticks = []
                if ticks and len(ticks) <= self.MAX_TICKS:
                    locator = FixedLocator(ticks)
                    formatter = FormatStrFormatter(f"%.{max(decimals, 0)}f")
                    axis_obj = self.ax.xaxis if axis == "x" else self.ax.yaxis
                    axis_obj.set_major_locator(locator)
                    axis_obj.set_major_formatter(formatter)

        _apply_for_axis(
            "x",
            self._axis_parameters.get("x"),
            self.xmin_var,
            self.xmax_var,
            self.x_tick_step_var,
        )
        _apply_for_axis(
            "y",
            self._axis_parameters.get("y"),
            self.ymin_var,
            self.ymax_var,
            self.y_tick_step_var,
        )

        self._stored_limits["x"] = self.ax.get_xlim()
        self._stored_limits["y"] = self.ax.get_ylim()
        self.ax.tick_params(axis="both", which="both", direction="out")
        self._enforce_axis_ratio(self.ax)

    def _resolve_linear_fit_color(self) -> str:
        selection = self.linear_fit_color_var.get()
        mapping = {label: value for label, value in self.COLOR_CHOICES}
        color = mapping.get(selection)
        return color if color is not None else "#000000"

    def _format_point_label(self, identity: _PointIdentity) -> str:
        mode = self.label_mode_var.get()
        if mode == "x" and identity.x_value is not None:
            label = self._current_axis_labels[0] or "X"
            return f"{label}: {identity.x_value:g}"
        if mode == "y" and identity.y_value is not None:
            label = self._current_axis_labels[1] or "Y"
            return f"{label}: {identity.y_value:g}"
        if mode.startswith("non_variable:"):
            column_name = mode.split(":", 1)[1]
            extras = getattr(identity, "extras", {})
            value = extras.get(column_name)
            if value is not None:
                text = str(value).strip()
                if text:
                    return text
        return identity.display_value()

    def _bind_axis_entry(self, entry: ttk.Entry, axis: Optional[str]) -> None:
        entry.bind("<FocusOut>", lambda _e, a=axis: self._handle_axis_entry(a))
        entry.bind("<Return>", lambda _e, a=axis: self._handle_axis_entry(a))

    def _handle_axis_entry(self, axis: Optional[str]) -> None:
        if axis == "x":
            self._axis_user_override["x"] = bool(
                self.xmin_var.get().strip() or self.xmax_var.get().strip()
            )
        elif axis == "y":
            self._axis_user_override["y"] = bool(
                self.ymin_var.get().strip() or self.ymax_var.get().strip()
            )
        self._update_plot()

    def _apply_gridlines(self, ax: Axes) -> None:
        if self.gridline_var.get():
            ax.grid(True, linestyle="--", linewidth=0.5, color="#dddddd")
        else:
            ax.grid(False)

    def _apply_axis_labels(self, xlabel: str, ylabel: str) -> tuple[str, str]:
        applied_x = self.custom_xlabel if self.custom_xlabel else xlabel
        applied_y = self.custom_ylabel if self.custom_ylabel else ylabel
        font_kwargs: dict[str, float] = {}
        if self.axis_label_fontsize is not None:
            font_kwargs["fontsize"] = self.axis_label_fontsize
        self.ax.set_xlabel(applied_x, **font_kwargs)
        self.ax.set_ylabel(applied_y, **font_kwargs)
        return applied_x, applied_y

    def _sanitize_array(self, values: Optional[Union[np.ndarray, list, tuple]]) -> np.ndarray:
        if values is None:
            return np.array([], dtype=float)
        if isinstance(values, np.ndarray):
            arr = values.astype(float, copy=False)
        else:
            arr = np.asarray(values, dtype=float)
        if arr.ndim != 1:
            arr = arr.flatten()
        return arr[np.isfinite(arr)]

    def _parse_optional_float(self, text: str) -> Optional[float]:
        value = text.strip()
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def _resolve_box_aspect(self) -> float:
        selected = self.aspect_ratio_var.get()
        ratio_map = {value: dims for value, dims in self.ASPECT_RATIO_OPTIONS}
        width, height = ratio_map.get(selected, (1, 1))
        if width <= 0 or height <= 0:
            return 1.0
        return float(height) / float(width)

    def _enforce_axis_ratio(self, ax: Optional[Axes]) -> None:
        if ax is None:
            return
        aspect_value = self._resolve_box_aspect()
        try:
            ax.set_box_aspect(aspect_value)
        except Exception:  # noqa: BLE001
            try:
                ax.set_aspect(aspect_value, adjustable="datalim")
            except Exception:  # noqa: BLE001
                try:
                    ax.set_aspect(aspect_value, adjustable="box")
                except Exception:  # noqa: BLE001
                    ax.set_aspect(aspect_value)

    def _handle_canvas_resize(self, _event=None) -> None:
        if self._in_resize:
            return
        if self.ax is None:
            return
        if self._stored_limits["x"] is None and self._stored_limits["y"] is None:
            return
        self._in_resize = True
        try:
            x_limits = self._stored_limits.get("x")
            y_limits = self._stored_limits.get("y")
            if x_limits is not None and len(x_limits) == 2 and x_limits[0] < x_limits[1]:
                self.ax.set_xlim(*x_limits)
            if y_limits is not None and len(y_limits) == 2 and y_limits[0] < y_limits[1]:
                self.ax.set_ylim(*y_limits)
            self._apply_axis_formatting()
            self.canvas.draw_idle()
        finally:
            self._in_resize = False

    def _update_model_option_state(self) -> None:
        has_models = bool(self._model_variables)
        state = "normal" if has_models else "disabled"
        if has_models:
            if self.axis_source_var.get() == "model":
                self.model_entry.configure(state="normal")
            else:
                self.model_entry.configure(state="disabled")
        else:
            self.axis_source_var.set("all")
            self.model_entry.configure(state="disabled")
        self.model_radio.configure(state=state)
        button_state = "normal" if has_models else "disabled"
        self.create_dataset_button.configure(state=button_state)
        if not has_models:
            self.dataset_filter_mode_var.set("current")
            self._close_filter_dialog()
        self._handle_dataset_filter_mode_change()
        self._sync_partial_residual_controls()

    def prepare_for_new_run(self) -> None:
        self._results_df = None
        self._model_variables = {}
        self._current_model_id = None
        self._update_model_option_state()
        self.axis_source_var.set("all")
        self.model_id_var.set("1")
        self.model_range_start_var.set("1")
        self.model_range_end_var.set("")
        self._handle_dataset_filter_mode_change()
        self._reset_axis_state()
        if self._label_manager is not None:
            self._label_manager.reset()
        self._update_axis_choices(options=[])
        self._clear_plot()
        self.label_mode_var.set("observations")
        self._non_variable_label_column = None
        self._refresh_label_mode_options()

    def update_results(self, df: Optional[pd.DataFrame]) -> None:
        if df is None or df.empty:
            self._results_df = None
            self._model_variables = {}
            self._current_model_id = None
            self._update_model_option_state()
            if self.axis_source_var.get() == "model":
                self.axis_source_var.set("all")
                self._update_axis_choices()
                self._update_plot()
            return

        self._results_df = df.copy()
        mapping: dict[int, list[str]] = {}
        normalize = getattr(self.master_app, "_normalize_variables", None)
        for _, row in df.iterrows():
            try:
                model_id = int(row.get("Model"))
            except (TypeError, ValueError):
                continue
            variables_raw = row.get("Variables")
            if callable(normalize):
                variables = list(normalize(variables_raw))
            else:
                if isinstance(variables_raw, str):
                    variables = [v.strip() for v in variables_raw.split(",") if v.strip()]
                else:
                    variables = list(variables_raw or [])
            if variables:
                mapping[model_id] = variables

        self._model_variables = mapping
        self._update_model_option_state()

        context = getattr(self.master_app, "last_context", None)
        if isinstance(context, EPRSContext):
            self._apply_context(context)

        if self.axis_source_var.get() == "model":
            self._refresh_model_variables()
        else:
            self._update_axis_choices()
            self._update_plot()

class VisualizationTab(ttk.Frame):
    PLOT_OPTIONS: tuple[tuple[str, str], ...] = (
        ("Correlation heatmap", "correlation_heatmap"),
        ("Correlation heatmap (with dependent)", "correlation_heatmap_with_target"),
        ("Predicted vs Actual", "exp_vs_pred"),
        ("Predicted vs Actual by LOO", "exp_vs_pred_loo"),
        ("Residuals vs Actual", "exp_vs_resid"),
        ("Residuals vs Actual by LOO", "exp_vs_resid_loo"),
        ("Residuals vs Predictions", "resid_vs_pred"),
        ("Residuals vs Predictions by LOO", "resid_vs_pred_loo"),
        ("Scale-Location", "scale_location"),
        ("Scale-Location by LOO", "scale_location_loo"),
        ("Q-Q residuals", "qq_resid"),
        ("Q-Q residuals by LOO", "qq_resid_loo"),
        ("Residual distribution", "residual_distribution"),
        ("Residual distribution by LOO", "residual_distribution_loo"),
        ("Williams plot", "williams"),
        ("Williams plot by LOO", "williams_loo"),
        ("Cook's distance vs Leverage", "cooks_distance"),
        ("Cook's distance vs Leverage by LOO", "cooks_distance_loo"),
        ("Leverage plot", "hat"),
        ("Predicted vs Leverage", "pred_vs_hat"),
    )

    FILTER_OPTIONS: tuple[tuple[str, str], ...] = (
        ("Training only", "training"),
        ("Training and testing", "both"),
        ("Testing only", "testing"),
    )

    PLOT_TITLES = {
        "correlation_heatmap": "Correlation heatmap",
        "correlation_heatmap_with_target": "Correlation heatmap (with dependent)",
        "exp_vs_pred": "Predicted vs Actual",
        "exp_vs_pred_loo": "Predicted vs Actual by LOO",
        "exp_vs_resid": "Residuals vs Actual",
        "exp_vs_resid_loo": "Residuals vs Actual by LOO",
        "resid_vs_pred": "Residuals vs Predictions",
        "resid_vs_pred_loo": "Residuals vs Predictions by LOO",
        "scale_location": "Scale-Location",
        "scale_location_loo": "Scale-Location by LOO",
        "qq_resid": "Q-Q plot of residuals",
        "qq_resid_loo": "Q-Q plot of residuals by LOO",
        "residual_distribution": "Residual distribution",
        "residual_distribution_loo": "Residual distribution by LOO",
        "williams": "Williams plot",
        "williams_loo": "Williams plot by LOO",
        "cooks_distance": "Cook's distance vs Leverage",
        "cooks_distance_loo": "Cook's distance vs Leverage by LOO",
        "hat": "Leverage plot",
        "pred_vs_hat": "Predicted vs Leverage",
        "y_randomization_r2": "Y-Randomization",
        "y_randomization_loo": "Y-Randomization by LOO",
    }

    COLOR_MAP = {
        "Training": "#1f77b4",
        "Testing": "#ff7f0e",
    }

    COLOR_CHOICES: tuple[tuple[str, Optional[str]], ...] = (
        ("Default", None),
        ("Blue", "#1f77b4"),
        ("Orange", "#ff7f0e"),
        ("Green", "#2ca02c"),
        ("Red", "#d62728"),
        ("Purple", "#9467bd"),
        ("Gray", "#7f7f7f"),
        ("Black", "#000000"),
    )

    LINEAR_FIT_DEFAULT_COLOR = "#000000"
    H_LINE_DEFAULT_COLOR = "#d62728"
    MAX_TICKS: int = 1000

    LEGEND_LOCATION_CHOICES: tuple[tuple[str, Union[str, int]], ...] = (
        ("1", 2),
        ("2", 1),
        ("3", 3),
        ("4", 4),
    )

    LINEAR_FIT_SUPPORTED: frozenset[str] = frozenset({
        "exp_vs_pred",
        "exp_vs_pred_loo",
    })
    IDENTITY_SUPPORTED: frozenset[str] = frozenset({
        "exp_vs_pred",
        "exp_vs_pred_loo",
        "qq_resid",
        "qq_resid_loo",
    })
    H_LINE_SUPPORTED: frozenset[str] = frozenset({
        "williams",
        "williams_loo",
        "hat",
        "pred_vs_hat",
        "cooks_distance",
        "cooks_distance_loo",
    })

    MARKER_CHOICES: tuple[tuple[str, str], ...] = (
        ("Circle", "o"),
        ("Square", "s"),
        ("Triangle", "^"),
        ("Diamond", "D"),
        ("Plus", "P"),
        ("Cross", "X"),
    )

    LABEL_MODE_BASE_OPTIONS: tuple[tuple[str, str], ...] = (
        ("Observations", "observations"),
    )

    BASE_DIAGNOSTIC_COLUMNS: frozenset[str] = frozenset(
        {
            "Observation",
            "Set",
            "Actual",
            "Predicted",
            "Predicted_LOO",
            "Residual",
            "Residual_LOO",
            "Z_value",
            "Leverage",
            "StdPredResid",
            "StdPredResid_LOO",
            "CooksDistance",
            "CooksDistance_LOO",
        }
    )

    def _format_numeric_value(self, value: object) -> str:
        return self.master_app._format_numeric_value(value)

    def _format_hat_threshold(self, value: object) -> str:
        try:
            float_value = float(value)
        except (TypeError, ValueError):
            return self._format_numeric_value(value)
        if not np.isfinite(float_value):
            return self._format_numeric_value(value)
        return f"{float_value:.3f}"

    def _with_zero_symmetric_limits(self, values: np.ndarray) -> np.ndarray:
        if values.size == 0:
            return values

        finite_values = values[np.isfinite(values)]
        if finite_values.size == 0:
            return values

        min_val = float(np.min(finite_values))
        max_val = float(np.max(finite_values))
        max_abs = max(abs(min_val), abs(max_val))
        if not math.isfinite(max_abs):
            return values

        symmetric_min = -max_abs if abs(min_val) < max_abs else min_val
        symmetric_max = max_abs if abs(max_val) < max_abs else max_val
        symmetric_bounds = np.asarray([symmetric_min, symmetric_max], dtype=float)
        return np.concatenate([values, symmetric_bounds])

    def __init__(self, notebook: ttk.Notebook, master_app: "MLRXApp"):
        super().__init__(notebook)
        self.master_app = master_app
        self.results_df: Optional[pd.DataFrame] = None
        self.current_model_id: Optional[int] = None
        self.current_variables: list[str] = []
        self.current_observation_df: Optional[pd.DataFrame] = None
        self.current_correlation_df: Optional[pd.DataFrame] = None
        self.hat_threshold: float = float("nan")
        self.available = False

        self.model_var = tk.StringVar()
        self.model_status_var = tk.StringVar(value="Select a model to inspect.")
        self.dataset_filter = tk.StringVar(value="training")
        self.dataset_choice_var = tk.StringVar(value=self.FILTER_OPTIONS[0][0])
        self.identity_var = tk.BooleanVar(value=True)
        self.legend_var = tk.BooleanVar(value=True)
        self.legend_location_var = tk.StringVar(value="1")
        self.point_size_var = tk.DoubleVar(value=50.0)
        self.xmin_var = tk.StringVar()
        self.xmax_var = tk.StringVar()
        self.ymin_var = tk.StringVar()
        self.ymax_var = tk.StringVar()
        self.x_tick_step_var = tk.StringVar()
        self.y_tick_step_var = tk.StringVar()
        self.marker_color_train_var = tk.StringVar(value="Default")
        self.marker_color_test_var = tk.StringVar(value="Default")
        self.marker_style_train_var = tk.StringVar(value="Circle")
        self.marker_style_test_var = tk.StringVar(value="Circle")
        self.plot_status_var = tk.StringVar(value="Select a model to visualize.")
        self.linear_fit_var = tk.BooleanVar(value=False)
        self.linear_fit_color_var = tk.StringVar(value="Default")
        self.h_line_color_var = tk.StringVar(value="Default")
        self.gridline_var = tk.BooleanVar(value=True)
        self.label_mode_var = tk.StringVar(value="observations")
        self.custom_xlabel: Optional[str] = None
        self.custom_ylabel: Optional[str] = None
        self.axis_label_fontsize: Optional[float] = None
        self._last_default_xlabel: str = ""
        self._last_default_ylabel: str = ""
        self.legend_loc: Optional[Union[str, int]] = None
        self.legend_loc_buttons: list[ttk.Radiobutton] = []
        self._label_mode_options: list[tuple[str, str]] = []
        self.label_mode_label: Optional[ttk.Label] = None
        self.label_mode_combo: Optional[ttk.Combobox] = None
        self.marker_color_train_combo: Optional[ttk.Combobox] = None
        self.marker_color_test_combo: Optional[ttk.Combobox] = None
        self.marker_style_train_combo: Optional[ttk.Combobox] = None
        self.marker_style_test_combo: Optional[ttk.Combobox] = None
        self.marker_size_label: Optional[ttk.Label] = None
        self.marker_size_scale: Optional[ttk.Scale] = None
        self.gridline_check: Optional[ttk.Checkbutton] = None
        self.h_line_color_label: Optional[ttk.Label] = None
        self._extra_label_column: Optional[str] = None
        self._current_axis_labels: tuple[str, str] = ("", "")
        self._axis_default_context: Optional[tuple[Any, ...]] = None
        self._axis_user_override = {"x": False, "y": False}
        self._axis_parameters: dict[str, Optional[AxisParameters]] = {"x": None, "y": None}
        self._identity_limits_snapshot: Optional[AxisParameters] = None
        self._use_identity_snapshot: bool = False
        self._stored_limits: dict[str, Optional[tuple[float, float]]] = {
            "x": None,
            "y": None,
        }
        self._in_resize: bool = False
        self._resize_cid: Optional[int] = None
        self._heatmap_colorbar = None
        self._heatmap_cbar_ax: Optional[Axes] = None
        self._heatmap_label_ax: Optional[Axes] = None
        self._heatmap_active: bool = False
        self._primary_axes_default_bbox: Optional[Bbox] = None

        self.option_controls: list[tk.Widget] = []
        self.entry_controls: list[ttk.Entry] = []
        self.dataset_combo: Optional[ttk.Combobox] = None
        self.linear_fit_check: Optional[ttk.Checkbutton] = None
        self.identity_check: Optional[ttk.Checkbutton] = None
        self.h_line_combo: Optional[ttk.Combobox] = None
        self._identity_prev_value: bool = True
        self._gridline_prev_value: bool = self.gridline_var.get()
        self._gridline_disabled_for_y_random: bool = False
        self._identity_disabled_for_plot: bool = False
        self._label_manager: Optional[_InteractiveLabelManager] = None
        self._current_plot_key: Optional[str] = None
        self._restricted_dataset_previous_choice: Optional[tuple[str, str]] = None

        self._plot_entries: list[tuple[str, str]] = list(self.PLOT_OPTIONS)
        self._y_randomization_results: dict[int, dict[str, YRandomizationResult]] = {}
        self._y_random_dialog: Optional["VisualizationTab._YRandomizationDialog"] = None
        self.y_random_button: Optional[ttk.Button] = None

        self._build_ui()
        self.set_available(False)

    def _build_ui(self):
        selection_frame = ttk.LabelFrame(self, text="Model selection")
        selection_frame.pack(fill="x", padx=10, pady=(10, 0))

        ttk.Label(selection_frame, text="Model:").grid(
            row=0, column=0, sticky="w", padx=5, pady=5
        )
        self.model_combo = ttk.Combobox(
            selection_frame,
            textvariable=self.model_var,
            state="disabled",
            width=12,
        )
        self.model_combo.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        self.model_combo.bind("<<ComboboxSelected>>", self._handle_model_change)

        ttk.Label(
            selection_frame, textvariable=self.model_status_var, foreground="#666666"
        ).grid(row=0, column=2, columnspan=2, sticky="w", padx=5, pady=5)

        ttk.Label(selection_frame, text="Dataset:").grid(
            row=1, column=0, sticky="w", padx=5, pady=(0, 5)
        )
        dataset_labels = [label for label, _value in self.FILTER_OPTIONS]
        self.dataset_combo = ttk.Combobox(
            selection_frame,
            textvariable=self.dataset_choice_var,
            state="disabled",
            values=dataset_labels,
            width=20,
        )
        if dataset_labels:
            self.dataset_combo.set(self.FILTER_OPTIONS[0][0])
            self.dataset_filter.set(self.FILTER_OPTIONS[0][1])
        self.dataset_combo.grid(row=1, column=1, sticky="w", padx=5, pady=(0, 5))
        self.dataset_combo.bind("<<ComboboxSelected>>", self._handle_dataset_change)

        self.y_random_button = ttk.Button(
            selection_frame,
            text="Add Y-Randomization plot",
            command=self._open_y_randomization_dialog,
            state="disabled",
        )
        self.y_random_button.grid(row=1, column=3, sticky="e", padx=5, pady=(0, 5))

        selection_frame.columnconfigure(2, weight=1)
        selection_frame.columnconfigure(3, weight=0)

        plot_frame = ttk.Frame(self)
        plot_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        plot_frame.columnconfigure(0, weight=1)
        plot_frame.columnconfigure(1, weight=0)
        plot_frame.rowconfigure(1, weight=1)

        status_label = ttk.Label(
            plot_frame, textvariable=self.plot_status_var, foreground="#666666"
        )
        status_label.grid(row=0, column=0, columnspan=2, sticky="w", padx=5, pady=0)

        figure_container = ttk.Frame(plot_frame)
        figure_container.grid(row=1, column=0, sticky="nsew", padx=(5, 0), pady=(0, 5))
        self.figure = Figure(figsize=(5, 5))
        self.ax = self.figure.add_subplot(111)
        self._primary_axes = self.ax
        self._heatmap_ax: Optional[Axes] = None
        self._primary_axes_default_bbox = self.ax.get_position().frozen()
        self._enforce_axis_ratio()
        self.canvas = FigureCanvasTkAgg(self.figure, master=figure_container)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)
        try:
            self._resize_cid = self.canvas.mpl_connect("resize_event", self._handle_canvas_resize)
        except Exception:  # noqa: BLE001
            self._resize_cid = None
        self._label_manager = _InteractiveLabelManager(self.canvas, self._format_observation_label)

        controls_frame = ttk.Frame(plot_frame)
        controls_frame.grid(row=1, column=1, sticky="ns", padx=(10, 5), pady=(0, 5))
        controls_frame.columnconfigure(0, weight=1)

        charts_frame = ttk.LabelFrame(controls_frame, text="Charts")
        charts_frame.pack(fill="both", expand=True, pady=(0, 5))
        charts_frame.columnconfigure(0, weight=1)
        charts_frame.rowconfigure(0, weight=1)

        entries = self._plot_entries or list(self.PLOT_OPTIONS)
        max_label_len = max((len(label) for label, _key in entries), default=0)
        listbox_width = max(20, int(max_label_len * 1.2)) if max_label_len else 20
        self.plot_listbox = tk.Listbox(
            charts_frame,
            height=4,
            exportselection=False,
            selectmode="extended",
            width=listbox_width,
        )
        for label, _key in entries:
            self.plot_listbox.insert("end", label)
        if entries:
            self.plot_listbox.selection_set(0)
        self.plot_listbox.grid(row=0, column=0, sticky="nsew", padx=(5, 0), pady=5)
        self.plot_listbox.bind("<<ListboxSelect>>", self._handle_plot_change)

        plot_scroll = ttk.Scrollbar(
            charts_frame, orient="vertical", command=self.plot_listbox.yview
        )
        plot_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 5), pady=5)
        self.plot_listbox.configure(yscrollcommand=plot_scroll.set)

        format_frame = ttk.LabelFrame(controls_frame, text="Plot options")
        format_frame.pack(fill="both", expand=True, pady=(5, 0), ipady=4)

        legend_row = ttk.Frame(format_frame)
        legend_row.pack(fill="x", padx=5, pady=(5, 0))
        legend_btn = ttk.Checkbutton(
            legend_row,
            text="Show legend",
            variable=self.legend_var,
            command=lambda: (self._sync_legend_controls(), self._update_plot()),
        )
        legend_btn.pack(side="left")
        label_mode_frame = ttk.Frame(legend_row)
        label_mode_frame.pack(side="left", padx=(12, 0))
        label_mode_label = ttk.Label(label_mode_frame, text="Label mode:")
        label_mode_label.pack(side="left")
        self.label_mode_label = label_mode_label
        self.label_mode_combo = ttk.Combobox(
            label_mode_frame,
            state="readonly",
            width=10,
        )
        self.label_mode_combo.pack(side="left", padx=(5, 0))
        self.label_mode_combo.bind(
            "<<ComboboxSelected>>",
            lambda _e: self._handle_label_mode_change(self.label_mode_combo.get()),
        )
        self.option_controls.append(legend_btn)

        legend_loc_row = ttk.Frame(format_frame)
        legend_loc_row.pack(fill="x", padx=5, pady=(2, 0))
        ttk.Label(legend_loc_row, text="Legend location:").grid(row=0, column=0, sticky="w")
        self.legend_loc_buttons.clear()
        for idx, (label, _loc) in enumerate(self.LEGEND_LOCATION_CHOICES, start=1):
            btn = ttk.Radiobutton(
                legend_loc_row,
                text=label,
                value=label,
                variable=self.legend_location_var,
                command=self._handle_legend_location_change,
            )
            pad = (5, 0) if idx == 1 else (2, 0)
            btn.grid(row=0, column=idx, sticky="w", padx=pad)
            self.legend_loc_buttons.append(btn)
        legend_loc_row.columnconfigure(len(self.LEGEND_LOCATION_CHOICES) + 1, weight=1)
        self._sync_legend_controls()

        line_option_row = ttk.Frame(format_frame)
        line_option_row.pack(fill="x", padx=5, pady=(10, 2))
        identity_btn = ttk.Checkbutton(
            line_option_row,
            text="Show identity line",
            variable=self.identity_var,
            command=self._update_plot,
        )
        identity_btn.pack(side="left")
        self.identity_check = identity_btn
        h_line_label = ttk.Label(line_option_row, text="h* line color:")
        h_line_label.pack(side="left", padx=(12, 0))
        self.h_line_color_label = h_line_label
        h_line_values = [label for label, _value in self.COLOR_CHOICES]
        h_line_combo = ttk.Combobox(
            line_option_row,
            textvariable=self.h_line_color_var,
            state="readonly",
            values=h_line_values,
            width=7,
        )
        h_line_combo.pack(side="left", padx=(5, 20))
        h_line_combo.bind("<<ComboboxSelected>>", lambda _e: self._update_plot())
        self.h_line_combo = h_line_combo
        self.option_controls.extend([identity_btn, h_line_label, h_line_combo])

        linear_row = ttk.Frame(format_frame)
        linear_row.pack(fill="x", padx=5, pady=(8, 0))
        linear_btn = ttk.Checkbutton(
            linear_row,
            text="Show linear fit",
            variable=self.linear_fit_var,
            command=self._handle_linear_fit_toggle,
        )
        linear_btn.pack(side="left")
        self.linear_fit_check = linear_btn
        linear_color_values = [label for label, _value in self.COLOR_CHOICES]
        self.linear_fit_color_combo = ttk.Combobox(
            linear_row,
            textvariable=self.linear_fit_color_var,
            state="readonly",
            values=linear_color_values,
            width=LINEAR_FIT_COLOR_WIDTH,
        )
        self.linear_fit_color_combo.pack(side="left", padx=(8, 12))
        self.linear_fit_color_combo.bind("<<ComboboxSelected>>", lambda _e: self._update_plot())
        grid_btn = ttk.Checkbutton(
            linear_row,
            text="Show gridline",
            variable=self.gridline_var,
            command=self._update_plot,
        )
        grid_btn.pack(side="left")
        self.gridline_check = grid_btn
        self.option_controls.extend([linear_btn, self.linear_fit_color_combo, grid_btn])
        self._sync_linear_fit_controls()

        marker_size_label = ttk.Label(format_frame, text="Marker size:")
        marker_size_label.pack(anchor="w", padx=5, pady=(10, 0))
        self.marker_size_label = marker_size_label
        size_scale = ttk.Scale(
            format_frame,
            from_=20,
            to=200,
            variable=self.point_size_var,
            command=lambda _value: self._update_plot(),
            orient="horizontal",
        )
        size_scale.pack(fill="x", padx=5, pady=2)
        self.marker_size_scale = size_scale
        self.option_controls.append(size_scale)

        limits_frame = ttk.Frame(format_frame)
        limits_frame.pack(fill="x", padx=5, pady=(10, 0))
        limits_frame.columnconfigure(1, weight=1)
        limits_frame.columnconfigure(3, weight=1)

        x_controls = (
            ("X min", self.xmin_var),
            ("X max", self.xmax_var),
            ("X tick step", self.x_tick_step_var),
        )
        y_controls = (
            ("Y min", self.ymin_var),
            ("Y max", self.ymax_var),
            ("Y tick step", self.y_tick_step_var),
        )

        for row, (label_text, var) in enumerate(x_controls):
            ttk.Label(limits_frame, text=label_text).grid(
                row=row, column=0, sticky="w", padx=(0, 5), pady=(0 if row == 0 else 5, 0)
            )
            entry = ttk.Entry(limits_frame, textvariable=var, width=8)
            entry.grid(row=row, column=1, sticky="w", padx=(0, 15), pady=(0 if row == 0 else 5, 0))
            axis = "x" if label_text in {"X min", "X max"} else None
            self._bind_axis_entry(entry, axis)
            self.entry_controls.append(entry)

        for row, (label_text, var) in enumerate(y_controls):
            ttk.Label(limits_frame, text=label_text).grid(
                row=row, column=2, sticky="w", padx=(0, 5), pady=(0 if row == 0 else 5, 0)
            )
            entry = ttk.Entry(limits_frame, textvariable=var, width=8)
            entry.grid(row=row, column=3, sticky="w", pady=(0 if row == 0 else 5, 0))
            axis = "y" if label_text in {"Y min", "Y max"} else None
            self._bind_axis_entry(entry, axis)
            self.entry_controls.append(entry)

        ttk.Label(format_frame, text="Marker color:").pack(anchor="w", padx=5, pady=(10, 0))
        color_values = [label for label, _value in self.COLOR_CHOICES]
        color_frame = ttk.Frame(format_frame)
        color_frame.pack(anchor="w", padx=5, pady=2)
        ttk.Label(color_frame, text="Training:").grid(row=0, column=0, sticky="w")
        color_combo_train = ttk.Combobox(
            color_frame,
            textvariable=self.marker_color_train_var,
            state="readonly",
            values=color_values,
            width=7,
        )
        color_combo_train.grid(row=0, column=1, sticky="w", padx=(5, 15))
        color_combo_train.bind("<<ComboboxSelected>>", lambda _e: self._update_plot())
        self.marker_color_train_combo = color_combo_train
        ttk.Label(color_frame, text="Testing:").grid(row=0, column=2, sticky="w")
        color_combo_test = ttk.Combobox(
            color_frame,
            textvariable=self.marker_color_test_var,
            state="readonly",
            values=color_values,
            width=7,
        )
        color_combo_test.grid(row=0, column=3, sticky="w", padx=(5, 0))
        color_combo_test.bind("<<ComboboxSelected>>", lambda _e: self._update_plot())
        self.marker_color_test_combo = color_combo_test
        self.option_controls.extend([color_combo_train, color_combo_test])

        ttk.Label(format_frame, text="Marker style:").pack(anchor="w", padx=5, pady=(10, 0))
        marker_values = [label for label, _value in self.MARKER_CHOICES]
        marker_frame = ttk.Frame(format_frame)
        marker_frame.pack(anchor="w", padx=5, pady=(0, 5))
        ttk.Label(marker_frame, text="Training:").grid(row=0, column=0, sticky="w")
        marker_combo_train = ttk.Combobox(
            marker_frame,
            textvariable=self.marker_style_train_var,
            state="readonly",
            values=marker_values,
            width=7,
        )
        marker_combo_train.grid(row=0, column=1, sticky="w", padx=(5, 15))
        marker_combo_train.bind("<<ComboboxSelected>>", lambda _e: self._update_plot())
        self.marker_style_train_combo = marker_combo_train
        ttk.Label(marker_frame, text="Testing:").grid(row=0, column=2, sticky="w", padx=(0, 0))
        marker_combo_test = ttk.Combobox(
            marker_frame,
            textvariable=self.marker_style_test_var,
            state="readonly",
            values=marker_values,
            width=7,
        )
        marker_combo_test.grid(row=0, column=3, sticky="w", padx=(5, 0))
        marker_combo_test.bind("<<ComboboxSelected>>", lambda _e: self._update_plot())
        self.marker_style_test_combo = marker_combo_test
        self.option_controls.extend([marker_combo_train, marker_combo_test])

        axis_button = ttk.Button(
            format_frame,
            text="Set axis labels...",
            command=self._prompt_axis_labels,
        )
        axis_button.pack(fill="x", padx=5, pady=(10, 0))
        self.option_controls.append(axis_button)

        save_button = ttk.Button(
            format_frame,
            text="Save current plot",
            command=self._save_current_plot,
        )
        save_button.pack(fill="x", padx=5, pady=(5, 5))
        self.save_plot_button = save_button
        self.option_controls.append(save_button)

        self._refresh_label_mode_options()
        self._clear_plot()

    def _bind_axis_entry(self, entry: ttk.Entry, axis: Optional[str]) -> None:
        entry.bind("<FocusOut>", lambda _e, a=axis: self._handle_axis_entry(a))
        entry.bind("<Return>", lambda _e, a=axis: self._handle_axis_entry(a))

    def _handle_axis_entry(self, axis: Optional[str]) -> None:
        if axis == "x":
            self._axis_user_override["x"] = bool(
                self.xmin_var.get().strip() or self.xmax_var.get().strip()
            )
        elif axis == "y":
            self._axis_user_override["y"] = bool(
                self.ymin_var.get().strip() or self.ymax_var.get().strip()
            )
        if not (self._axis_user_override["x"] is False and self._axis_user_override["y"] is False):
            self._use_identity_snapshot = False
            self._identity_limits_snapshot = None
        self._update_plot()

    def prepare_for_new_run(self):
        self.results_df = None
        self.current_model_id = None
        self.current_variables = []
        self.current_observation_df = None
        self.current_correlation_df = None
        self.hat_threshold = float("nan")
        if self._label_manager is not None:
            self._label_manager.reset()
        self.model_var.set("")
        self.model_status_var.set("Select a model to inspect.")
        self.plot_status_var.set("Select a model to visualize.")
        self.dataset_filter.set(self.FILTER_OPTIONS[0][1])
        self.dataset_choice_var.set(self.FILTER_OPTIONS[0][0])
        if self.dataset_combo is not None:
            self.dataset_combo.set(self.dataset_choice_var.get())
        self.point_size_var.set(50.0)
        self.xmin_var.set("")
        self.xmax_var.set("")
        self.ymin_var.set("")
        self.ymax_var.set("")
        self.x_tick_step_var.set("")
        self.y_tick_step_var.set("")
        self.marker_color_train_var.set("Default")
        self.marker_color_test_var.set("Default")
        self.marker_style_train_var.set("Circle")
        self.marker_style_test_var.set("Circle")
        self.linear_fit_color_var.set("Default")
        self.h_line_color_var.set("Default")
        self.gridline_var.set(True)
        self.linear_fit_var.set(False)
        self.label_mode_var.set("observations")
        self._extra_label_column = None
        self.custom_xlabel = None
        self.custom_ylabel = None
        self.axis_label_fontsize = None
        self._last_default_xlabel = ""
        self._last_default_ylabel = ""
        self._current_axis_labels = ("", "")
        self._axis_default_context = None
        self._axis_user_override = {"x": False, "y": False}
        self._axis_parameters = {"x": None, "y": None}
        self._identity_limits_snapshot = None
        self._use_identity_snapshot = False
        self._stored_limits = {"x": None, "y": None}
        self._refresh_label_mode_options()
        self._clear_plot()
        self._sync_linear_fit_controls()
        self.set_available(False)

    def _handle_linear_fit_toggle(self):
        self._sync_linear_fit_controls()
        self._update_plot()

    def _sync_linear_fit_controls(self, plot_key: Optional[str] = None):
        if not hasattr(self, "linear_fit_color_combo") or self.linear_fit_color_combo is None:
            return
        if plot_key is None:
            plot_key = self._get_selected_plot_key()

        supported = plot_key in self.LINEAR_FIT_SUPPORTED
        if not supported:
            self.linear_fit_var.set(False)

        enable_combo = bool(self.available and supported and self.linear_fit_var.get())
        combo_state = "readonly" if enable_combo else "disabled"
        self.linear_fit_color_combo.configure(state=combo_state)

        if self.linear_fit_check is not None:
            check_state = tk.NORMAL if (self.available and supported) else tk.DISABLED
            self.linear_fit_check.configure(state=check_state)
            if check_state == tk.DISABLED:
                self.linear_fit_var.set(False)

        identity_supported = plot_key in self.IDENTITY_SUPPORTED
        if self.identity_check is not None:
            if not identity_supported:
                if not self._identity_disabled_for_plot:
                    self._identity_prev_value = self.identity_var.get()
                self.identity_var.set(False)
                self.identity_check.configure(state=tk.DISABLED)
                self._identity_disabled_for_plot = True
                self._identity_limits_snapshot = None
                self._use_identity_snapshot = False
            else:
                state = tk.NORMAL if self.available else tk.DISABLED
                self.identity_check.configure(state=state)
                if self._identity_disabled_for_plot and state != tk.DISABLED:
                    self.identity_var.set(self._identity_prev_value)
                if state != tk.DISABLED:
                    self._identity_prev_value = self.identity_var.get()
                self._identity_disabled_for_plot = False

        h_supported = plot_key in self.H_LINE_SUPPORTED
        if self.h_line_combo is not None:
            if self.available and h_supported:
                combo_state = "readonly"
            else:
                combo_state = "disabled"
            self.h_line_combo.configure(state=combo_state)
            if not h_supported:
                self.h_line_color_var.set("Default")
        if self.h_line_color_label is not None:
            if self.available and h_supported:
                label_state = tk.NORMAL
            else:
                label_state = tk.DISABLED
            self.h_line_color_label.configure(state=label_state)

    def _sync_legend_controls(self):
        is_heatmap = self._is_heatmap_plot_key(self._get_selected_plot_key())
        if is_heatmap:
            state = tk.DISABLED
        else:
            state = tk.NORMAL if (self.available and self.legend_var.get()) else tk.DISABLED
        for btn in self.legend_loc_buttons:
            btn.configure(state=state)

    def _refresh_label_mode_options(self) -> None:
        options = list(self.LABEL_MODE_BASE_OPTIONS)
        extra_label = self._extra_label_column
        if extra_label:
            options.insert(1, (extra_label, f"non_variable:{extra_label}"))

        self._label_mode_options = options

        current_value = self.label_mode_var.get() or "observations"
        valid_values = {value for _, value in options}
        if current_value not in valid_values:
            current_value = "observations"
            self.label_mode_var.set(current_value)

        if self.label_mode_combo is not None:
            labels = [label for label, _ in options]
            self.label_mode_combo.configure(values=labels)
            label_lookup = {value: label for label, value in options}
            display_label = label_lookup.get(current_value)
            if not display_label and labels:
                display_label = labels[0]
                self.label_mode_var.set(options[0][1])
            if display_label:
                self.label_mode_combo.set(display_label)

    def _handle_label_mode_change(self, selection: str) -> None:
        lookup = {label: value for label, value in self._label_mode_options}
        mode = lookup.get(selection, "observations")
        self.label_mode_var.set(mode)
        if self._label_manager is not None:
            self._label_manager.suspend()
        self._update_plot()

    def _detect_extra_label_column(self, df: pd.DataFrame) -> Optional[str]:
        for column in df.columns:
            if column in self.BASE_DIAGNOSTIC_COLUMNS:
                continue
            if column == "Observation" or column == "Set":
                continue
            return column
        return None

    def _handle_legend_location_change(self, _event=None):
        location = self._get_user_legend_location()
        if location is not None:
            self.legend_loc = location
        self._update_plot()

    def _get_user_legend_location(self) -> Optional[Union[str, int]]:
        value = self.legend_location_var.get().strip()
        mapping = {label: loc for label, loc in self.LEGEND_LOCATION_CHOICES}
        return mapping.get(value)

    def _legend_value_from_location(self, location: Union[str, int]) -> Optional[str]:
        if isinstance(location, int):
            for label, loc in self.LEGEND_LOCATION_CHOICES:
                if loc == location:
                    return label
            return None
        normalized = str(location).lower().strip()
        lookup = {
            "upper right": "2",
            "upper left": "1",
            "lower left": "3",
            "lower right": "4",
        }
        return lookup.get(normalized)

    def _get_current_plot_selection(self) -> Optional[str]:
        if not getattr(self, "plot_listbox", None):
            return None
        selection = self.plot_listbox.curselection()
        if not selection:
            return None
        index = selection[-1]
        if 0 <= index < len(self._plot_entries):
            return self._plot_entries[index][1]
        return None

    def _rebuild_plot_listbox(self, preferred_key: Optional[str] = None) -> None:
        if not getattr(self, "plot_listbox", None):
            return
        if not self._plot_entries:
            self._plot_entries = list(self.PLOT_OPTIONS)
        entries = self._plot_entries
        current_key = preferred_key if preferred_key is not None else self._get_current_plot_selection()
        self.plot_listbox.delete(0, "end")
        for label, _key in entries:
            self.plot_listbox.insert("end", label)
        if not entries:
            return
        available_keys = {key for _, key in entries}
        desired_key = current_key if current_key in available_keys else None
        if desired_key is None:
            desired_key = entries[0][1]
        for idx, (_label, key) in enumerate(entries):
            if key == desired_key:
                self.plot_listbox.selection_clear(0, "end")
                self.plot_listbox.selection_set(idx)
                self.plot_listbox.see(idx)
                break
        else:
            self.plot_listbox.selection_clear(0, "end")
            self.plot_listbox.selection_set(0)
            self.plot_listbox.see(0)

    @staticmethod
    def _y_randomization_plot_key(metric_key: str) -> str:
        return "y_randomization_r2" if metric_key == "R2" else "y_randomization_loo"

    @staticmethod
    def _y_randomization_label(metric_key: str) -> str:
        return "Y-Randomization" if metric_key == "R2" else "Y-Randomization by LOO"

    def _update_plot_entries_for_model(self, preferred_key: Optional[str] = None) -> None:
        extras: list[tuple[str, str]] = []
        if self.current_model_id is not None:
            store = self._y_randomization_results.get(self.current_model_id, {})
            for metric_key in ("R2", "R2_loo"):
                result = store.get(metric_key)
                if result and result.chart_added:
                    extras.append((self._y_randomization_label(metric_key), self._y_randomization_plot_key(metric_key)))
        self._plot_entries = extras + list(self.PLOT_OPTIONS)
        self._rebuild_plot_listbox(preferred_key)

    def _store_y_randomization_result(self, result: YRandomizationResult) -> YRandomizationResult:
        store = self._y_randomization_results.setdefault(result.model_id, {})
        existing = store.get(result.metric_key)
        if existing is not None:
            result.chart_added = existing.chart_added
        store[result.metric_key] = result
        if result.chart_added and result.model_id == self.current_model_id:
            self._update_plot_entries_for_model(self._y_randomization_plot_key(result.metric_key))
        elif result.model_id == self.current_model_id:
            self._update_plot_entries_for_model()
        summary_tab = getattr(self.master_app, "summary_tab", None)
        if summary_tab is not None:
            summary_tab.update_y_randomization_result(result)
        return result

    def _get_y_randomization_result(
        self, model_id: int, metric_key: str
    ) -> Optional[YRandomizationResult]:
        return self._y_randomization_results.get(model_id, {}).get(metric_key)

    def _add_y_randomization_chart(self, metric_key: str) -> None:
        if self.current_model_id is None:
            messagebox.showinfo("Y-Randomization", "Select a model before adding the chart.")
            return
        store = self._y_randomization_results.get(self.current_model_id)
        if not store or metric_key not in store:
            messagebox.showwarning(
                "Y-Randomization", "Run the Y-randomization analysis before adding the chart."
            )
            return
        result = store[metric_key]
        result.chart_added = True
        preferred = self._y_randomization_plot_key(metric_key)
        self._update_plot_entries_for_model(preferred)
        self._update_plot()

    def _prepare_design_matrix(self) -> tuple[np.ndarray, np.ndarray, Optional[tuple[float, float]]]:
        context = self.master_app.last_context
        config = self.master_app.last_config
        if context is None or config is None:
            raise ValueError("Training context is not available.")
        if not self.current_variables:
            raise ValueError("Select a model with predictors to run Y-randomization.")
        try:
            idx = [context.col_idx[var] for var in self.current_variables]
        except KeyError as exc:  # pragma: no cover - defensive
            raise ValueError("Model predictors are not available in the dataset.") from exc
        X = context.X_np[:, idx]
        if X.size == 0:
            raise ValueError("Unable to build the design matrix for the selected model.")
        design = np.c_[np.ones((X.shape[0], 1), dtype=float), X.astype(float, copy=False)]
        y = np.asarray(context.y_np, dtype=float)
        if design.shape[0] <= design.shape[1]:
            raise ValueError("Not enough observations to compute Y-randomization.")
        clip = getattr(config, "clip_predictions", None)
        return design, y, clip

    def _compute_metric_for_design(
        self,
        design: np.ndarray,
        y: np.ndarray,
        metric_key: str,
        clip: Optional[tuple[float, float]],
    ) -> float:
        if design.size == 0 or y.size == 0:
            return float("nan")
        try:
            coefficients, *_ = np.linalg.lstsq(design, y, rcond=None)
        except np.linalg.LinAlgError:
            return float("nan")
        preds = design @ coefficients
        if clip is not None:
            lo, hi = clip
            preds = np.clip(preds, lo, hi)
        if metric_key == "R2":
            y_mean = float(np.mean(y))
            ss_tot = float(np.sum((y - y_mean) ** 2))
            if not np.isfinite(ss_tot) or ss_tot <= 0.0:
                return float("nan")
            ss_res = float(np.sum((y - preds) ** 2))
            if not np.isfinite(ss_res):
                return float("nan")
            return float(1.0 - (ss_res / ss_tot))
        if metric_key == "R2_loo":
            loo_value = _compute_loo_r2(design, y, clip)
            if loo_value is None or not np.isfinite(loo_value):
                return float("nan")
            return float(loo_value)
        return float("nan")

    def _open_y_randomization_dialog(self) -> None:
        if not self.available:
            messagebox.showinfo("Y-Randomization", "Select a model before running Y-randomization.")
            return
        if self.current_model_id is None or not self.current_variables:
            messagebox.showinfo("Y-Randomization", "Select a model before running Y-randomization.")
            return
        if self._y_random_dialog is not None and self._y_random_dialog.winfo_exists():
            self._y_random_dialog.lift()
            self._y_random_dialog.focus_set()
            return
        try:
            design, y, clip = self._prepare_design_matrix()
        except ValueError as exc:
            messagebox.showerror("Y-Randomization", str(exc))
            return
        dialog = self._YRandomizationDialog(self, design, y, clip)
        self._y_random_dialog = dialog

    def set_available(self, available: bool):
        self.available = available
        if available:
            self.model_combo.configure(state="readonly")
            self.plot_listbox.configure(state=tk.NORMAL)
            self._update_plot_entries_for_model()
        else:
            self.model_combo.configure(state="disabled")
            self.plot_listbox.configure(state=tk.DISABLED)
            self._axis_default_context = None
            self._axis_user_override = {"x": False, "y": False}
            self._axis_parameters = {"x": None, "y": None}
            self._stored_limits = {"x": None, "y": None}
            self._identity_limits_snapshot = None
            self._use_identity_snapshot = False
            self._plot_entries = list(self.PLOT_OPTIONS)
            self._rebuild_plot_listbox()
        if self.dataset_combo is not None:
            self.dataset_combo.configure(state="readonly" if available else "disabled")
        if self.y_random_button is not None:
            state = "normal" if (available and self.current_model_id is not None) else "disabled"
            self.y_random_button.configure(state=state)
        plot_key = self._get_selected_plot_key()
        self._apply_plot_option_availability(plot_key)
        self._sync_linear_fit_controls(plot_key)
        self._sync_legend_controls()
        if not available:
            self.plot_status_var.set("Select a model to visualize.")

    def _apply_plot_option_availability(self, plot_key: Optional[str] = None) -> None:
        if plot_key is None:
            plot_key = self._get_selected_plot_key()

        is_heatmap = self._is_heatmap_plot_key(plot_key)
        available = self.available

        y_randomization_keys = {
            self._y_randomization_plot_key("R2"),
            self._y_randomization_plot_key("R2_loo"),
        }
        residual_distribution_keys = {
            "residual_distribution",
            "residual_distribution_loo",
        }
        cooks_distance_keys = {"cooks_distance", "cooks_distance_loo"}
        is_y_random_plot = bool(plot_key in y_randomization_keys) if plot_key is not None else False
        is_residual_distribution_plot = (
            bool(plot_key in residual_distribution_keys) if plot_key is not None else False
        )
        restrict_dataset_to_training = (
            bool(plot_key in cooks_distance_keys) if plot_key is not None else False
        )

        general_enabled = bool(available and not is_heatmap)
        widget_state = tk.NORMAL if general_enabled else tk.DISABLED
        entry_state = "normal" if general_enabled else "disabled"

        save_button = getattr(self, "save_plot_button", None)
        for widget in self.option_controls:
            if widget is save_button:
                widget_state_override = tk.NORMAL if available else tk.DISABLED
            else:
                widget_state_override = widget_state
            widget.configure(state=widget_state_override)

        entry_widget_state = entry_state if available else "disabled"
        for entry in self.entry_controls:
            entry.configure(state=entry_widget_state)

        training_label = next(
            (label for label, value in self.FILTER_OPTIONS if value == "training"),
            self.FILTER_OPTIONS[0][0],
        )
        if restrict_dataset_to_training:
            if self._restricted_dataset_previous_choice is None:
                self._restricted_dataset_previous_choice = (
                    self.dataset_choice_var.get(),
                    self.dataset_filter.get(),
                )
            if self.dataset_filter.get() != "training":
                self.dataset_filter.set("training")
            if self.dataset_choice_var.get() != training_label:
                self.dataset_choice_var.set(training_label)
                if self.dataset_combo is not None:
                    self.dataset_combo.set(training_label)
        elif self._restricted_dataset_previous_choice is not None:
            previous_label, previous_value = self._restricted_dataset_previous_choice
            self._restricted_dataset_previous_choice = None
            value_to_label = {value: label for label, value in self.FILTER_OPTIONS}
            restored_value = previous_value if previous_value in value_to_label else "training"
            if restored_value in {"both", "testing"} and not getattr(
                self.master_app, "holdout_ready", False
            ):
                restored_value = "training"
            restored_label = value_to_label.get(restored_value)
            if not restored_label:
                restored_label = previous_label or training_label
            self.dataset_filter.set(restored_value)
            self.dataset_choice_var.set(restored_label)
            if self.dataset_combo is not None:
                self.dataset_combo.set(restored_label)

        if self.dataset_combo is not None:
            if restrict_dataset_to_training or not (available and not is_heatmap):
                dataset_state = "disabled"
            else:
                dataset_state = "readonly"
            self.dataset_combo.configure(state=dataset_state)

        legend_state = tk.NORMAL if (general_enabled and self.legend_var.get()) else tk.DISABLED
        for btn in self.legend_loc_buttons:
            btn.configure(state=legend_state)

        marker_color_enabled = general_enabled and not is_y_random_plot
        marker_color_state = "readonly" if marker_color_enabled else "disabled"
        if self.marker_color_train_combo is not None:
            self.marker_color_train_combo.configure(state=marker_color_state)
        if self.marker_color_test_combo is not None:
            self.marker_color_test_combo.configure(state=marker_color_state)

        marker_style_enabled = general_enabled and not (
            is_y_random_plot or is_residual_distribution_plot
        )
        marker_style_state = "readonly" if marker_style_enabled else "disabled"
        if self.marker_style_train_combo is not None:
            self.marker_style_train_combo.configure(state=marker_style_state)
        if self.marker_style_test_combo is not None:
            self.marker_style_test_combo.configure(state=marker_style_state)

        histogram_selected = (
            self.histogram_var.get() if hasattr(self, "histogram_var") else False
        )
        marker_size_enabled = general_enabled and not (
            is_y_random_plot or is_residual_distribution_plot or histogram_selected
        )
        marker_size_state = tk.NORMAL if marker_size_enabled else tk.DISABLED
        if self.marker_size_scale is not None:
            self.marker_size_scale.configure(state=marker_size_state)
        if self.marker_size_label is not None:
            label_state = tk.NORMAL if marker_size_enabled else tk.DISABLED
            self.marker_size_label.configure(state=label_state)

        if is_y_random_plot:
            if not self._gridline_disabled_for_y_random:
                self._gridline_prev_value = self.gridline_var.get()
            self.gridline_var.set(False)
            self._gridline_disabled_for_y_random = True
        elif self._gridline_disabled_for_y_random:
            self.gridline_var.set(self._gridline_prev_value)
            self._gridline_prev_value = self.gridline_var.get()
            self._gridline_disabled_for_y_random = False

        gridline_state = tk.NORMAL if (general_enabled and not is_y_random_plot) else tk.DISABLED
        if self.gridline_check is not None:
            self.gridline_check.configure(state=gridline_state)

        label_mode_enabled = bool(general_enabled and not is_y_random_plot)
        if self.label_mode_combo is not None:
            combo_state = "readonly" if label_mode_enabled else "disabled"
            self.label_mode_combo.configure(state=combo_state)
        if self.label_mode_label is not None:
            label_state = tk.NORMAL if label_mode_enabled else tk.DISABLED
            self.label_mode_label.configure(state=label_state)

    def update_training_results(self, df: Optional[pd.DataFrame]):
        summary_tab = getattr(self.master_app, "summary_tab", None)
        if df is None or df.empty:
            self.results_df = None
            self.model_combo.configure(values=())
            self.model_var.set("")
            self.set_available(False)
            self._clear_plot()
            self.current_correlation_df = None
            self.current_correlation_with_target_df = None
            self._y_randomization_results.clear()
            self._plot_entries = list(self.PLOT_OPTIONS)
            self._rebuild_plot_listbox()
            if self.y_random_button is not None:
                self.y_random_button.configure(state="disabled")
            if summary_tab is not None:
                summary_tab.sync_y_randomization_results({})
            return

        self.results_df = df.copy()
        models: list[str] = []
        for value in df["Model"].tolist():
            try:
                models.append(str(int(value)))
            except (TypeError, ValueError):
                continue
        model_ids: set[int] = set()
        for value in models:
            try:
                model_ids.add(int(value))
            except (TypeError, ValueError):
                continue
        if model_ids:
            self._y_randomization_results = {
                mid: store for mid, store in self._y_randomization_results.items() if mid in model_ids
            }
        else:
            self._y_randomization_results.clear()
        if summary_tab is not None:
            summary_tab.sync_y_randomization_results(self._y_randomization_results)
        self.model_combo.configure(values=tuple(models))
        if models:
            previous = self.model_var.get()
            if previous and previous in models:
                self.model_combo.set(previous)
            else:
                self.model_combo.set(models[0])
            self.set_available(True)
            self.apply_holdout_default(self.master_app.holdout_ready)
            self._handle_model_change()
        else:
            self.model_var.set("")
            self.set_available(False)
            self._clear_plot()
            self._plot_entries = list(self.PLOT_OPTIONS)
            self._rebuild_plot_listbox()

    def apply_holdout_default(self, ready: bool) -> None:
        value_to_label = {value: label for label, value in self.FILTER_OPTIONS}
        if ready:
            if self.dataset_filter.get() == "training":
                self.dataset_filter.set("both")
                label = value_to_label.get("both")
                if label:
                    self.dataset_choice_var.set(label)
                    if self.dataset_combo is not None:
                        self.dataset_combo.set(label)
                if self.available:
                    self._handle_dataset_change()
        else:
            if self.dataset_filter.get() != "training":
                self.dataset_filter.set("training")
                label = value_to_label.get("training")
                if label:
                    self.dataset_choice_var.set(label)
                    if self.dataset_combo is not None:
                        self.dataset_combo.set(label)
                if self.available:
                    self._handle_dataset_change()

    def _handle_model_change(self, _event=None):
        if not self.available:
            return
        previous_model_id = self.current_model_id
        self.current_model_id = None
        self.current_variables = []
        if self.y_random_button is not None:
            self.y_random_button.configure(state="disabled")
        self._update_plot_entries_for_model()
        model_text = self.model_var.get().strip()
        if not model_text:
            self._clear_plot()
            self.current_correlation_df = None
            self.current_correlation_with_target_df = None
            self.model_status_var.set("Select a model to inspect.")
            return
        try:
            model_id = int(model_text)
        except ValueError:
            self.model_status_var.set("Invalid model identifier.")
            self._clear_plot("Invalid model identifier.")
            self.current_correlation_df = None
            self.current_correlation_with_target_df = None
            return

        if self.results_df is None or self.results_df.empty:
            self.model_status_var.set("Results are not available.")
            self._clear_plot("Results unavailable.")
            self.current_correlation_df = None
            self.current_correlation_with_target_df = None
            return

        subset = self.results_df[self.results_df["Model"] == model_id]
        if subset.empty:
            self.model_status_var.set("Model not found in current results.")
            self._clear_plot("Model not found.")
            self.current_correlation_df = None
            self.current_correlation_with_target_df = None
            return

        record = subset.iloc[0]
        variables = list(self.master_app._normalize_variables(record.get("Variables")))
        if not variables:
            self.model_status_var.set("Selected model does not contain predictors.")
            self._clear_plot("Selected model lacks predictors.")
            self.current_correlation_df = None
            self.current_correlation_with_target_df = None
            return

        diagnostics_df, hat_threshold = self.master_app.get_observation_diagnostics(model_id)

        if diagnostics_df is None or diagnostics_df.empty:
            self.model_status_var.set("Diagnostics unavailable for this model.")
            self._clear_plot("Diagnostics unavailable.")
            self.current_correlation_df = None
            self.current_correlation_with_target_df = None
            return
        if model_id != previous_model_id and self._label_manager is not None:
            self._label_manager.reset()
        self.current_model_id = model_id
        self.current_variables = variables
        if self.y_random_button is not None:
            self.y_random_button.configure(state="normal")
        self.current_observation_df = diagnostics_df.copy()
        self.hat_threshold = hat_threshold
        self._extra_label_column = self._detect_extra_label_column(self.current_observation_df)
        self._refresh_label_mode_options()
        self._update_plot_entries_for_model()

        corr_df = self.master_app.get_model_correlation(model_id)
        self.current_correlation_df = corr_df.copy() if corr_df is not None else None
        corr_with_target = self.master_app.get_model_correlation(model_id, include_target=True)
        self.current_correlation_with_target_df = (
            corr_with_target.copy() if corr_with_target is not None else None
        )

        preview = ", ".join(variables[:5])
        if len(variables) > 5:
            preview += ", ..."
        self.model_status_var.set(f"Predictors ({len(variables)}): {preview}")

        self._update_plot()

    def _handle_plot_change(self, _event=None):
        if not self.available:
            return
        self._update_plot()

    def _handle_dataset_change(self, _event=None):
        mapping = {label: value for label, value in self.FILTER_OPTIONS}
        selection = self.dataset_choice_var.get()
        desired = mapping.get(selection, "training")
        if desired in {"both", "testing"}:
            if not self.master_app.ensure_holdout_data_available():  # noqa: SLF001
                self.dataset_filter.set("training")
                self.dataset_choice_var.set(self.FILTER_OPTIONS[0][0])
                if self.dataset_combo is not None:
                    self.dataset_combo.set(self.FILTER_OPTIONS[0][0])
                self._update_plot()
                return
        self.dataset_filter.set(desired)
        self._update_plot()

    def _clear_plot(self, message: str = "Select a model to visualize."):
        self._clear_heatmap_artifacts()
        self.ax.clear()
        self._enforce_axis_ratio()
        self.ax.set_xticks([])
        self.ax.set_yticks([])
        self.ax.text(
            0.5,
            0.5,
            message,
            ha="center",
            va="center",
            transform=self.ax.transAxes,
            color="#666666",
        )
        self.ax.figure.canvas.draw_idle()
        self.plot_status_var.set(message)
        self.legend_loc = None
        self._axis_parameters = {"x": None, "y": None}
        self._identity_limits_snapshot = None
        self._use_identity_snapshot = False
        self._stored_limits = {"x": None, "y": None}
        self._current_axis_labels = ("", "")
        self._set_axis_field_defaults(
            None,
            None,
            update_x=not self._axis_user_override["x"],
            update_y=not self._axis_user_override["y"],
        )

    def _get_selected_plot_key(self) -> str:
        if not self._plot_entries:
            self._plot_entries = list(self.PLOT_OPTIONS)
        if not self._plot_entries:
            return ""
        selection = self.plot_listbox.curselection()
        if not selection:
            self.plot_listbox.selection_clear(0, "end")
            self.plot_listbox.selection_set(0)
            return self._plot_entries[0][1]
        index = selection[-1]
        index = max(0, min(index, len(self._plot_entries) - 1))
        return self._plot_entries[index][1]

    @staticmethod
    def _is_heatmap_plot_key(plot_key: Optional[str]) -> bool:
        if plot_key is None:
            return False
        return plot_key in {
            "correlation_heatmap",
            "correlation_heatmap_with_target",
        }

    def _get_heatmap_dataframe(self, plot_key: str) -> Optional[pd.DataFrame]:
        if plot_key == "correlation_heatmap":
            return self.current_correlation_df
        if plot_key == "correlation_heatmap_with_target":
            return self.current_correlation_with_target_df
        return None

    @staticmethod
    def _parse_optional_float(value: str) -> Optional[float]:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    def _resolve_marker_style(self, dataset: Optional[str] = None) -> str:
        mapping = {label: marker for label, marker in self.MARKER_CHOICES}
        if dataset == "Testing":
            selection = self.marker_style_test_var.get()
        elif dataset == "Training":
            selection = self.marker_style_train_var.get()
        else:
            if self.dataset_filter.get() == "testing":
                selection = self.marker_style_test_var.get()
            else:
                selection = self.marker_style_train_var.get()
        return mapping.get(selection, "o")

    def _resolve_marker_color(self, dataset: Optional[str] = None) -> Optional[str]:
        mapping = {label: color for label, color in self.COLOR_CHOICES}
        if dataset == "Testing":
            selection = self.marker_color_test_var.get()
        elif dataset == "Training":
            selection = self.marker_color_train_var.get()
        else:
            if self.dataset_filter.get() == "testing":
                selection = self.marker_color_test_var.get()
            else:
                selection = self.marker_color_train_var.get()
        return mapping.get(selection, None)

    def _resolve_linear_fit_color(self) -> str:
        mapping = {label: color for label, color in self.COLOR_CHOICES}
        selection = self.linear_fit_color_var.get()
        color = mapping.get(selection)
        if color is None:
            return self.LINEAR_FIT_DEFAULT_COLOR
        return color

    def _resolve_h_line_color(self) -> str:
        mapping = {label: color for label, color in self.COLOR_CHOICES}
        selection = self.h_line_color_var.get()
        color = mapping.get(selection)
        if color is None:
            return self.H_LINE_DEFAULT_COLOR
        return color

    def _resolve_color_choice(self, label: str, fallback: str) -> str:
        mapping = {choice_label: color for choice_label, color in self.COLOR_CHOICES}
        color = mapping.get(label)
        if not color:
            return fallback
        return color

    def _apply_gridlines(self, ax: Axes) -> None:
        ax.grid(False)
        if self.gridline_var.get():
            ax.grid(True, linestyle="--", alpha=0.3)

    @staticmethod
    def _compute_limits(values: np.ndarray) -> Optional[tuple[float, float]]:
        if values is None:
            return None
        finite = values[np.isfinite(values)]
        if finite.size == 0:
            return None
        min_val = float(finite.min())
        max_val = float(finite.max())
        if np.isclose(min_val, max_val):
            pad = max(abs(min_val) * 0.05, 1.0)
        else:
            pad = 0.05 * (max_val - min_val)
            if pad <= 0:
                pad = max(abs(max_val), abs(min_val), 1.0) * 0.05
        return min_val - pad, max_val + pad

    @staticmethod
    def _sanitize_array(values: Optional[Union[np.ndarray, list, tuple]]) -> np.ndarray:
        if values is None:
            return np.empty(0, dtype=float)
        arr = np.asarray(values, dtype=float).ravel()
        if arr.size == 0:
            return np.empty(0, dtype=float)
        arr = arr[np.isfinite(arr)]
        return arr.astype(float, copy=False)

    @staticmethod
    def _format_axis_value(value: float) -> str:
        if not math.isfinite(value):
            return ""
        if abs(value) >= 1000 or (0 < abs(value) < 0.001):
            text = f"{value:.6g}"
        else:
            text = f"{value:.6f}"
        if "e" not in text and "E" not in text:
            text = text.rstrip("0").rstrip(".")
        if text == "-0":
            return "0"
        return text

    def _set_axis_field_defaults(
        self,
        x_params: Optional[Union[AxisParameters, tuple[float, float]]],
        y_params: Optional[Union[AxisParameters, tuple[float, float]]],
        *,
        update_x: bool,
        update_y: bool,
    ) -> None:
        def _as_limits(data: Optional[Union[AxisParameters, tuple[float, float]]]) -> Optional[tuple[float, float]]:
            if isinstance(data, AxisParameters):
                return (data.minimum, data.maximum)
            if isinstance(data, tuple) and len(data) == 2:
                return data
            return None

        if update_x:
            limits = _as_limits(x_params)
            if limits is None:
                self.xmin_var.set("")
                self.xmax_var.set("")
            else:
                self.xmin_var.set(self._format_axis_value(limits[0]))
                self.xmax_var.set(self._format_axis_value(limits[1]))
        if update_y:
            limits = _as_limits(y_params)
            if limits is None:
                self.ymin_var.set("")
                self.ymax_var.set("")
            else:
                self.ymin_var.set(self._format_axis_value(limits[0]))
                self.ymax_var.set(self._format_axis_value(limits[1]))

    def _enforce_axis_ratio(self, axes: Optional[Axes] = None) -> None:
        target = axes if axes is not None else self.ax
        try:
            target.set_box_aspect(1.0)
        except Exception:  # noqa: BLE001
            try:
                target.set_aspect("equal", adjustable="datalim")
            except Exception:  # noqa: BLE001
                try:
                    target.set_aspect("equal", adjustable="box")
                except Exception:  # noqa: BLE001
                    target.set_aspect("equal")

    def _compute_axis_parameters_for_values(
        self, values: np.ndarray, axis_name: str
    ) -> Optional[AxisParameters]:
        return compute_axis_parameters(values, axis_name, verbose=False)

    def _apply_axis_defaults(
        self,
        x_values: Optional[Union[np.ndarray, list, tuple]] = None,
        y_values: Optional[Union[np.ndarray, list, tuple]] = None,
        *,
        identity: bool = False,
        update_x: bool = True,
        update_y: bool = True,
    ) -> tuple[Optional[tuple[float, float]], Optional[tuple[float, float]]]:
        x_array = self._sanitize_array(x_values)
        y_array = self._sanitize_array(y_values)

        if identity:
            if not (update_x and update_y):
                self._identity_limits_snapshot = None
                self._use_identity_snapshot = False
            combined = (
                np.concatenate([x_array, y_array])
                if x_array.size or y_array.size
                else np.empty(0, dtype=float)
            )
            if combined.size == 0:
                if update_x:
                    self._axis_parameters["x"] = None
                    self._stored_limits["x"] = None
                if update_y:
                    self._axis_parameters["y"] = None
                    self._stored_limits["y"] = None
                self._identity_limits_snapshot = None
                self._use_identity_snapshot = False
                self._set_axis_field_defaults(None, None, update_x=update_x, update_y=update_y)
                return None, None
            params = self._compute_axis_parameters_for_values(combined, "Identity")
            if params is None:
                if update_x:
                    self._axis_parameters["x"] = None
                    self._stored_limits["x"] = None
                if update_y:
                    self._axis_parameters["y"] = None
                    self._stored_limits["y"] = None
                self._identity_limits_snapshot = None
                self._use_identity_snapshot = False
                self._set_axis_field_defaults(None, None, update_x=update_x, update_y=update_y)
                return None, None
            limits = (params.minimum, params.maximum)
            if update_x:
                self._axis_parameters["x"] = params
                apply_axis_to_plot(self.ax, "x", params)
                self._stored_limits["x"] = limits
            if update_y:
                self._axis_parameters["y"] = params
                apply_axis_to_plot(self.ax, "y", params)
                self._stored_limits["y"] = limits
            self._identity_limits_snapshot = params if update_x and update_y else None
            self._use_identity_snapshot = update_x and update_y
            self._set_axis_field_defaults(
                params if update_x else None,
                params if update_y else None,
                update_x=update_x,
                update_y=update_y,
            )
            self.ax.tick_params(axis="both", which="both", direction="out")
            self._enforce_axis_ratio()
            x_result = limits if update_x else self._stored_limits.get("x")
            y_result = limits if update_y else self._stored_limits.get("y")
            return x_result, y_result

        if not (update_x and update_y):
            self._use_identity_snapshot = False

        if (
            update_x
            and update_y
            and self._use_identity_snapshot
            and self._identity_limits_snapshot is not None
        ):
            params = self._identity_limits_snapshot
            limits = (params.minimum, params.maximum)
            self._axis_parameters["x"] = params
            self._axis_parameters["y"] = params
            apply_axis_to_plot(self.ax, "x", params)
            apply_axis_to_plot(self.ax, "y", params)
            self._stored_limits["x"] = limits
            self._stored_limits["y"] = limits
            self._set_axis_field_defaults(params, params, update_x=True, update_y=True)
            self.ax.tick_params(axis="both", which="both", direction="out")
            self._enforce_axis_ratio()
            return limits, limits

        self._use_identity_snapshot = False
        self._identity_limits_snapshot = None

        x_params: Optional[AxisParameters] = self._axis_parameters.get("x")
        y_params: Optional[AxisParameters] = self._axis_parameters.get("y")

        if update_x:
            x_params = None
            if x_array.size:
                x_params = self._compute_axis_parameters_for_values(x_array, "X")
            self._axis_parameters["x"] = x_params
        if update_y:
            y_params = None
            if y_array.size:
                y_params = self._compute_axis_parameters_for_values(y_array, "Y")
            self._axis_parameters["y"] = y_params

        x_limits: Optional[tuple[float, float]] = None
        y_limits: Optional[tuple[float, float]] = None

        if update_x and x_params is not None:
            apply_axis_to_plot(self.ax, "x", x_params)
            x_limits = (x_params.minimum, x_params.maximum)
            self._stored_limits["x"] = x_limits
        elif update_x:
            self._stored_limits["x"] = None

        if update_y and y_params is not None:
            apply_axis_to_plot(self.ax, "y", y_params)
            y_limits = (y_params.minimum, y_params.maximum)
            self._stored_limits["y"] = y_limits
        elif update_y:
            self._stored_limits["y"] = None

        self._set_axis_field_defaults(
            x_params if update_x else None,
            y_params if update_y else None,
            update_x=update_x,
            update_y=update_y,
        )
        self.ax.tick_params(axis="both", which="both", direction="out")
        self._enforce_axis_ratio()
        return x_limits, y_limits

    def _handle_canvas_resize(self, _event=None) -> None:
        plot_key = self._get_selected_plot_key()
        if self._heatmap_active and self._is_heatmap_plot_key(plot_key):
            if self._in_resize:
                return
            if not self.available:
                return
            self._in_resize = True
            try:
                self._update_plot()
            finally:
                self._in_resize = False
            return
        if self._heatmap_active:
            return
        if self._in_resize:
            return
        if self.ax is None:
            return
        if not self.available:
            return
        if self._stored_limits["x"] is None and self._stored_limits["y"] is None:
            return
        self._in_resize = True
        try:
            x_limits = self._stored_limits.get("x")
            y_limits = self._stored_limits.get("y")
            self._set_axis_limits(x_limits, y_limits)
            self._enforce_axis_ratio()
            self._apply_axis_formatting()
            self.ax.figure.canvas.draw_idle()
        finally:
            self._in_resize = False

    def _set_axis_limits(
        self,
        x_limits: Optional[Union[AxisParameters, tuple[float, float]]] = None,
        y_limits: Optional[Union[AxisParameters, tuple[float, float]]] = None,
    ) -> None:
        def _as_tuple(data: Optional[Union[AxisParameters, tuple[float, float]]]) -> Optional[tuple[float, float]]:
            if isinstance(data, AxisParameters):
                return (data.minimum, data.maximum)
            return data if isinstance(data, tuple) and len(data) == 2 else None

        x_tuple = _as_tuple(x_limits)
        y_tuple = _as_tuple(y_limits)
        if x_tuple is not None:
            left, right = x_tuple
            if math.isfinite(left) and math.isfinite(right) and left < right:
                self.ax.set_xlim(left, right)
        if y_tuple is not None:
            bottom, top = y_tuple
            if math.isfinite(bottom) and math.isfinite(top) and bottom < top:
                self.ax.set_ylim(bottom, top)

    def _apply_axis_formatting(self):
        def _apply_for_axis(
            axis: str,
            params: Optional[AxisParameters],
            min_var: tk.StringVar,
            max_var: tk.StringVar,
            step_var: tk.StringVar,
        ) -> None:
            base_min = params.minimum if params is not None else None
            base_max = params.maximum if params is not None else None
            override_min = self._parse_optional_float(min_var.get())
            override_max = self._parse_optional_float(max_var.get())
            left = base_min if override_min is None else override_min
            right = base_max if override_max is None else override_max
            if (
                left is None
                or right is None
                or not math.isfinite(left)
                or not math.isfinite(right)
                or left >= right
            ):
                return

            if axis == "x":
                self.ax.set_xlim(left, right)
            else:
                self.ax.set_ylim(left, right)

            step_override = self._parse_optional_float(step_var.get())
            step_value: Optional[float] = None
            decimals = 0
            if step_override is not None and step_override > 0:
                step_value = step_override
                if params is not None:
                    decimals = max(decimals, params.decimals)
            elif params is not None and params.step > 0:
                step_value = params.step
                decimals = params.decimals

            if step_value is not None and step_value > 0:
                try:
                    ticks = build_ticks(left, right, step_value)
                except Exception:  # noqa: BLE001
                    ticks = []
                if ticks and len(ticks) <= self.MAX_TICKS:
                    locator = FixedLocator(ticks)
                    formatter = FormatStrFormatter(f"%.{max(decimals, 0)}f")
                    axis_obj = self.ax.xaxis if axis == "x" else self.ax.yaxis
                    axis_obj.set_major_locator(locator)
                    axis_obj.set_major_formatter(formatter)

        _apply_for_axis(
            "x",
            self._axis_parameters.get("x"),
            self.xmin_var,
            self.xmax_var,
            self.x_tick_step_var,
        )
        _apply_for_axis(
            "y",
            self._axis_parameters.get("y"),
            self.ymin_var,
            self.ymax_var,
            self.y_tick_step_var,
        )

        self._stored_limits["x"] = self.ax.get_xlim()
        self._stored_limits["y"] = self.ax.get_ylim()
        self.ax.tick_params(axis="both", which="both", direction="out")
        self._enforce_axis_ratio()

    def _show_legend(self) -> None:
        if not self.legend_var.get():
            return
        handles, labels = self.ax.get_legend_handles_labels()
        if not handles:
            return

        seen: dict[str, Any] = {}
        ordered_handles: list[Any] = []
        ordered_labels: list[str] = []
        for handle, label in zip(handles, labels):
            if not label:
                continue
            if label in seen:
                continue
            seen[label] = handle
            ordered_handles.append(handle)
            ordered_labels.append(label)

        if ordered_handles:
            user_loc = self._get_user_legend_location()
            if user_loc is not None:
                loc = user_loc
                self.legend_loc = user_loc
            else:
                loc = self.legend_loc if self.legend_loc is not None else "best"
            legend = self.ax.legend(ordered_handles, ordered_labels, loc=loc)
            inferred_loc = self._infer_legend_location(legend)
            if inferred_loc is not None:
                self.legend_loc = inferred_loc
                legend_value = self._legend_value_from_location(inferred_loc)
                if legend_value is not None:
                    self.legend_location_var.set(legend_value)

    def _infer_legend_location(self, legend) -> Optional[Union[str, int]]:
        if legend is None:
            return None

        try:
            loc = legend.get_loc()
        except AttributeError:
            loc = getattr(legend, "_loc", None)

        if loc not in {None, "best", 0}:
            return loc

        renderer = None
        try:
            renderer = self.ax.figure.canvas.get_renderer()
        except Exception:  # noqa: BLE001
            renderer = None

        if renderer is None:
            try:
                self.ax.figure.canvas.draw()
                renderer = self.ax.figure.canvas.get_renderer()
            except Exception:  # noqa: BLE001
                renderer = None

        if renderer is None:
            return None

        try:
            bbox = legend.get_window_extent(renderer=renderer)
        except Exception:  # noqa: BLE001
            return None

        try:
            axes_bbox = bbox.transformed(self.ax.transAxes.inverted())
        except Exception:  # noqa: BLE001
            return None

        x_center = (axes_bbox.x0 + axes_bbox.x1) / 2.0
        y_center = (axes_bbox.y0 + axes_bbox.y1) / 2.0

        # Map the legend centre to one of Matplotlib's canonical locations.
        if x_center < 1 / 3:
            horiz = "left"
        elif x_center > 2 / 3:
            horiz = "right"
        else:
            horiz = "center"

        if y_center < 1 / 3:
            vert = "lower"
        elif y_center > 2 / 3:
            vert = "upper"
        else:
            vert = "center"

        if vert == "center" and horiz == "center":
            return "center"

        return f"{vert} {horiz}".strip()

    def _apply_axis_labels(self, default_x: str, default_y: str):
        self._last_default_xlabel = default_x
        self._last_default_ylabel = default_y
        xlabel = (
            self._format_axis_label_markup(self.custom_xlabel)
            if self.custom_xlabel
            else default_x
        )
        ylabel = (
            self._format_axis_label_markup(self.custom_ylabel)
            if self.custom_ylabel
            else default_y
        )
        font_kwargs: dict[str, float] = {}
        if self.axis_label_fontsize is not None:
            font_kwargs["fontsize"] = self.axis_label_fontsize
        self.ax.set_xlabel(xlabel, **font_kwargs)
        self.ax.set_ylabel(ylabel, **font_kwargs)
        self._current_axis_labels = (xlabel, ylabel)

    _SUPERSUB_PATTERN = re.compile(
        r"(?<!\\)(?P<op>[\^_])(?:\{(?P<braced>[^{}]+)\}|(?P<single>[^\s\^_{}$]+))"
    )

    @classmethod
    def _format_axis_label_markup(cls, label: str) -> str:
        if not label:
            return label
        if "$" in label:
            return label

        placeholders = {"\\^": "\uFFF0", "\\_": "\uFFF1"}
        for key, value in placeholders.items():
            label = label.replace(key, value)

        def replacer(match: re.Match[str]) -> str:
            op = match.group("op")
            text = match.group("braced") or match.group("single")
            if not text:
                return match.group(0)
            if op == "^":
                return "$^{" + text + "}$"
            return "$_{" + text + "}$"

        converted = cls._SUPERSUB_PATTERN.sub(replacer, label)

        for key, value in placeholders.items():
            converted = converted.replace(value, key[-1])

        return converted

    def _format_observation_label(self, observation: object) -> str:
        if isinstance(observation, _PointIdentity):
            mode = self.label_mode_var.get()
            if mode == "x" and observation.x_value is not None:
                label = self._current_axis_labels[0] or self._last_default_xlabel or "X"
                return f"{label}: {observation.x_value:g}"
            if mode == "y" and observation.y_value is not None:
                label = self._current_axis_labels[1] or self._last_default_ylabel or "Y"
                return f"{label}: {observation.y_value:g}"
            if mode.startswith("non_variable:"):
                column_name = mode.split(":", 1)[1]
                extras = getattr(observation, "extras", {}) or {}
                value = extras.get(column_name)
                if value is not None:
                    text = str(value).strip()
                    if text:
                        return text
            text = observation.display_value().strip()
            return text or "0"
        if observation is None:
            return "0"
        if isinstance(observation, (np.integer, int)):
            return str(int(observation))
        if isinstance(observation, (np.floating, float)):
            numeric = float(observation)
            if not math.isfinite(numeric):
                return "0"
            if numeric.is_integer():
                return str(int(numeric))
            return str(numeric)
        text = str(observation).strip()
        return text or "0"

    def _register_scatter_points(
        self,
        collection: PathCollection,
        x_values: np.ndarray,
        y_values: np.ndarray,
        *,
        dataset_label: Optional[str],
        observation_ids: Optional[np.ndarray] = None,
        index_values: Optional[np.ndarray] = None,
        extra_labels: Optional[np.ndarray] = None,
    ) -> None:
        if self._label_manager is None or self._current_plot_key is None:
            return
        x_array = np.asarray(x_values, dtype=float)
        y_array = np.asarray(y_values, dtype=float)
        count = min(x_array.size, y_array.size)
        if count == 0:
            return
        if x_array.size != count:
            x_array = x_array[:count]
        if y_array.size != count:
            y_array = y_array[:count]

        index_array: np.ndarray
        if index_values is not None:
            index_array = np.asarray(index_values, dtype=object)
            if index_array.size != count:
                index_array = np.arange(count, dtype=int)
        else:
            index_array = np.arange(count, dtype=int)

        observation_array: Optional[np.ndarray]
        if observation_ids is not None:
            temp = np.asarray(observation_ids, dtype=object)
            observation_array = temp if temp.size == count else None
        else:
            observation_array = None

        extra_array: Optional[np.ndarray]
        if extra_labels is not None:
            temp_extra = np.asarray(extra_labels, dtype=object)
            extra_array = temp_extra if temp_extra.size == count else None
        else:
            extra_array = None

        identities = np.empty(count, dtype=object)
        for idx in range(count):
            display_value: int
            if observation_array is not None:
                raw_label = observation_array[idx]
                is_missing = False
                if raw_label is None:
                    is_missing = True
                elif isinstance(raw_label, float) and math.isnan(raw_label):
                    is_missing = True
                if not is_missing:
                    try:
                        display_value = int(float(raw_label))
                    except (TypeError, ValueError):
                        display_value = int(index_array[idx]) + 1
                else:
                    display_value = int(index_array[idx]) + 1
                unique_key = raw_label if not is_missing else index_array[idx]
            else:
                unique_key = index_array[idx]
                display_value = int(index_array[idx]) + 1

            extras: dict[str, object] = {}
            if extra_array is not None and self._extra_label_column is not None:
                extras[self._extra_label_column] = extra_array[idx]

            x_val = float(x_array[idx]) if np.isfinite(x_array[idx]) else None
            y_val = float(y_array[idx]) if np.isfinite(y_array[idx]) else None

            identities[idx] = _PointIdentity(
                dataset=dataset_label,
                unique_key=unique_key,
                display_label=display_value,
                x_value=x_val,
                y_value=y_val,
                extras=extras,
            )

        self._label_manager.register_collection(
            self._current_plot_key,
            collection,
            identities,
            x_array,
            y_array,
        )

    def _prepare_plot_dataframe(self, df: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
        if "Set" not in df.columns:
            return df
        if not any(col.endswith("_LOO") for col in columns):
            return df
        mask = df["Set"] == "Testing"
        if not mask.any():
            return df
        working = df.copy()
        for column in columns:
            if not column.endswith("_LOO"):
                continue
            base = column[:-4]
            if base in working.columns:
                working.loc[mask, column] = working.loc[mask, base]
        return working

    def _filter_dataframe(self) -> pd.DataFrame:
        if self.current_observation_df is None:
            return pd.DataFrame()
        mode = self.dataset_filter.get()
        if mode == "training":
            return self.current_observation_df[self.current_observation_df["Set"] == "Training"].copy()
        if mode == "testing":
            return self.current_observation_df[self.current_observation_df["Set"] == "Testing"].copy()
        return self.current_observation_df.copy()

    def _update_plot(self, *_args):
        plot_key = self._get_selected_plot_key()
        self._current_plot_key = plot_key
        self._apply_plot_option_availability(plot_key)
        self._sync_linear_fit_controls(plot_key)
        self._sync_legend_controls()
        if not self.available:
            self._clear_plot()
            if self._label_manager is not None:
                self._label_manager.suspend()
            self._current_plot_key = None
            return

        is_heatmap = self._is_heatmap_plot_key(plot_key)
        if is_heatmap:
            if self._label_manager is not None:
                self._label_manager.suspend()
            corr_df = self._get_heatmap_dataframe(plot_key)
            if corr_df is None or corr_df.empty:
                self._clear_heatmap_artifacts()
                self._clear_plot("Correlation matrix unavailable.")
                self._current_plot_key = None
                return
        elif self.current_observation_df is None:
            self._clear_plot()
            if self._label_manager is not None:
                self._label_manager.suspend()
            self._current_plot_key = None
            return

        context = (self.current_model_id, plot_key, self.dataset_filter.get())
        if context != self._axis_default_context:
            self._axis_default_context = context
            self._axis_user_override = {"x": False, "y": False}
            self._axis_parameters = {"x": None, "y": None}
            self._identity_limits_snapshot = None
            self._use_identity_snapshot = False
            self._stored_limits = {"x": None, "y": None}

        if is_heatmap:
            df = pd.DataFrame()
        else:
            df = self._filter_dataframe()

            if df.empty:
                self._clear_plot("No data for the selected dataset.")
                return

        plot_method = getattr(self, f"_plot_{plot_key}", None)
        if not plot_method:
            self._clear_plot("Plot not implemented.")
            if self._label_manager is not None:
                self._label_manager.suspend()
            self._current_plot_key = None
            return

        original_ax = self._primary_axes
        if is_heatmap:
            active_ax = self._ensure_heatmap_axis()
        else:
            self._clear_heatmap_artifacts()
            active_ax = original_ax

        self.ax = active_ax
        self.ax.clear()
        if not is_heatmap:
            self._enforce_axis_ratio()
        manager_active = bool(self._label_manager is not None and not is_heatmap)
        if manager_active and self._label_manager is not None:
            self._label_manager.begin_plot(plot_key, active_ax)
        plot_succeeded = False
        try:
            custom_title = plot_method(df)
            title = self.PLOT_TITLES.get(plot_key, "")
            if isinstance(custom_title, str) and custom_title:
                title = custom_title
            self.ax.set_title(title)
            self.plot_status_var.set("")
            if not is_heatmap:
                self._apply_axis_formatting()
            plot_succeeded = True
        except Exception as exc:  # noqa: BLE001
            self._clear_plot(f"Unable to draw plot: {exc}")
            if manager_active and self._label_manager is not None:
                self._label_manager.suspend()
            return
        finally:
            if is_heatmap:
                self.ax = original_ax
                self._primary_axes = original_ax
            else:
                self.ax = original_ax
            if manager_active and self._label_manager is not None:
                if plot_succeeded:
                    self._label_manager.complete_plot()
                else:
                    self._label_manager.suspend()
            self._current_plot_key = None

        self.ax.figure.canvas.draw_idle()

    def _compute_heatmap_bbox(self) -> Bbox:
        if self._primary_axes_default_bbox is None:
            self._primary_axes_default_bbox = self._primary_axes.get_position().frozen()

        base_bbox = self._primary_axes_default_bbox
        base_cx = (base_bbox.x0 + base_bbox.x1) / 2.0
        base_cy = (base_bbox.y0 + base_bbox.y1) / 2.0

        target_width = min(0.82, max(0.45, base_bbox.width * 0.95))
        target_height = min(0.82, max(0.5, base_bbox.height * 0.98))
        center_y = min(0.9, max(0.54, base_cy + 0.04))

        left = max(0.08, 0.5 - target_width / 2.0)
        right = min(0.92, left + target_width)
        if right - left < target_width:
            left = max(0.06, right - target_width)
        bottom = max(0.08, center_y - target_height / 2.0)
        top = min(0.96, bottom + target_height)
        if top - bottom < target_height:
            bottom = max(0.06, top - target_height)

        return Bbox.from_extents(left, bottom, right, top)

    def _ensure_heatmap_axis(self) -> Axes:
        bbox = self._compute_heatmap_bbox()
        if self._heatmap_ax is None:
            self._heatmap_ax = self.figure.add_axes(bbox)
        else:
            self._heatmap_ax.set_position(bbox)

        self._primary_axes.set_visible(False)
        self._heatmap_ax.set_visible(True)
        self._heatmap_active = True
        return self._heatmap_ax

    def _clear_heatmap_colorbar(self) -> None:
        if self._heatmap_colorbar is not None:
            try:
                self._heatmap_colorbar.remove()
            except Exception:  # noqa: BLE001
                pass
            finally:
                self._heatmap_colorbar = None

        if self._heatmap_cbar_ax is not None:
            try:
                self._heatmap_cbar_ax.remove()
            except Exception:  # noqa: BLE001
                try:
                    self.figure.delaxes(self._heatmap_cbar_ax)
                except Exception:  # noqa: BLE001
                    pass
            finally:
                self._heatmap_cbar_ax = None

    def _clear_heatmap_label_axis(self) -> None:
        if self._heatmap_label_ax is not None:
            try:
                self._heatmap_label_ax.remove()
            except Exception:  # noqa: BLE001
                try:
                    self.figure.delaxes(self._heatmap_label_ax)
                except Exception:  # noqa: BLE001
                    pass
            finally:
                self._heatmap_label_ax = None

    def _clear_heatmap_artifacts(self) -> None:
        self._clear_heatmap_colorbar()
        self._clear_heatmap_label_axis()
        if self._heatmap_ax is not None:
            try:
                self._heatmap_ax.remove()
            except Exception:  # noqa: BLE001
                try:
                    self.figure.delaxes(self._heatmap_ax)
                except Exception:  # noqa: BLE001
                    pass
            finally:
                self._heatmap_ax = None
        self._primary_axes.set_visible(True)
        if self._primary_axes_default_bbox is not None:
            self._primary_axes.set_position(self._primary_axes_default_bbox)
        self.ax = self._primary_axes
        self._heatmap_active = False

    def _plot_correlation_heatmap(self, _df: pd.DataFrame):
        return self._render_correlation_heatmap(
            self.current_correlation_df,
            "correlation_heatmap",
        )

    def _plot_correlation_heatmap_with_target(self, _df: pd.DataFrame):
        return self._render_correlation_heatmap(
            self.current_correlation_with_target_df,
            "correlation_heatmap_with_target",
        )

    def _render_correlation_heatmap(
        self,
        corr_df: Optional[pd.DataFrame],
        plot_key: str,
    ) -> str:
        if corr_df is None or corr_df.empty:
            raise ValueError("Correlation matrix unavailable.")

        numeric_df = corr_df.astype(float)
        self._clear_heatmap_colorbar()
        self._clear_heatmap_label_axis()
        ax = self.ax
        initial_bbox = self._compute_heatmap_bbox()
        ax.set_position([initial_bbox.x0, initial_bbox.y0, initial_bbox.width, initial_bbox.height])
        ax.set_facecolor("#ffffff")

        data = numeric_df.to_numpy()
        heatmap = ax.imshow(data, cmap="coolwarm", vmin=-1, vmax=1)

        column_labels = [str(label) for label in numeric_df.columns]
        row_labels = [str(label) for label in numeric_df.index]
        x_positions = np.arange(len(column_labels))
        y_positions = np.arange(len(row_labels))
        ax.set_xticks(x_positions)
        ax.set_yticks(y_positions)
        ax.set_yticklabels(row_labels)
        ax.tick_params(
            axis="x",
            which="major",
            top=False,
            bottom=True,
            labeltop=False,
            labelbottom=True,
            length=5,
            width=0.8,
            pad=4,
        )
        ax.tick_params(
            axis="y",
            left=True,
            right=False,
            labelleft=True,
            labelright=False,
            pad=6,
        )
        ax.set_xlim(-0.5, len(column_labels) - 0.5)
        ax.set_ylim(len(row_labels) - 0.5, -0.5)
        ax.margins(0)
        ax.grid(False)

        if column_labels:
            aspect_ratio = len(row_labels) / len(column_labels)
            if aspect_ratio > 0:
                try:
                    ax.set_box_aspect(aspect_ratio)
                except Exception:  # noqa: BLE001
                    pass
        ax.set_anchor("C")

        ax.set_xlabel(None)
        ax.set_ylabel(None)

        if column_labels:
            ax.set_xticklabels(column_labels)
            y_ticklabels = ax.yaxis.get_ticklabels()
            font_size = y_ticklabels[0].get_fontsize() if y_ticklabels else 10
            for tick_label in ax.get_xticklabels():
                tick_label.set_rotation(90)
                tick_label.set_horizontalalignment("center")
                tick_label.set_verticalalignment("top")
                tick_label.set_fontsize(font_size)
        else:
            ax.set_xticklabels([])

        def _figure_pixel_size(fig) -> tuple[float, float]:
            if getattr(fig, "bbox", None) is not None:
                try:
                    return float(fig.bbox.width) or 1.0, float(fig.bbox.height) or 1.0
                except Exception:  # noqa: BLE001
                    pass
            try:
                return (
                    float(fig.get_figwidth() * fig.get_dpi()),
                    float(fig.get_figheight() * fig.get_dpi()),
                )
            except Exception:  # noqa: BLE001
                return 1.0, 1.0

        adjusted_bbox: Union[Bbox, tuple[float, float, float, float]] = initial_bbox
        renderer = None
        canvas = getattr(self, "canvas", None)
        if canvas is not None:
            try:
                renderer = canvas.get_renderer()
            except Exception:  # noqa: BLE001
                renderer = None
            if renderer is None:
                try:
                    canvas.draw()
                except Exception:  # noqa: BLE001
                    renderer = None
                else:
                    try:
                        renderer = canvas.get_renderer()
                    except Exception:  # noqa: BLE001
                        renderer = None

        if renderer is not None and self.figure.bbox is not None:
            fig_bbox = self.figure.bbox
            fig_width = float(fig_bbox.width) or 1.0
            fig_height = float(fig_bbox.height) or 1.0

            left = initial_bbox.x0
            right = initial_bbox.x1
            bottom = initial_bbox.y0
            top = initial_bbox.y1

            min_width = 0.25
            min_height = 0.25

            xticks = [tick for tick in ax.get_xticklabels() if tick.get_visible() and tick.get_text()]
            if xticks:
                try:
                    extents = [tick.get_window_extent(renderer) for tick in xticks]
                except Exception:  # noqa: BLE001
                    extents = []
                if extents:
                    min_y0 = min(ext.y0 for ext in extents)
                    max_y1 = max(ext.y1 for ext in extents)
                    bottom_overflow = max(0.0, -min_y0) / fig_height
                    top_overflow = max(0.0, max_y1 - fig_height) / fig_height
                    if bottom_overflow > 0.0:
                        desired_bottom = bottom + bottom_overflow + 0.01
                        max_bottom = min(top - min_height, 0.9)
                        bottom = min(desired_bottom, max_bottom)
                    if top_overflow > 0.0:
                        desired_top = top - (top_overflow + 0.01)
                        min_top = bottom + min_height
                        top = max(desired_top, min_top)

            yticks = [tick for tick in ax.get_yticklabels() if tick.get_visible() and tick.get_text()]
            if yticks:
                desired_left = left
                try:
                    extents = [tick.get_window_extent(renderer) for tick in yticks]
                    y_label = ax.yaxis.get_label()
                    if y_label is not None and y_label.get_text():
                        try:
                            label_extent = y_label.get_window_extent(renderer)
                        except Exception:  # noqa: BLE001
                            label_extent = None
                        if label_extent is not None:
                            extents.append(label_extent)
                except Exception:  # noqa: BLE001
                    extents = []
                if extents:
                    min_x0 = min(ext.x0 for ext in extents)
                    max_x1 = max(ext.x1 for ext in extents)
                    left_overflow = max(0.0, -min_x0) / fig_width
                    right_overflow = max(0.0, max_x1 - fig_width) / fig_width
                    if left_overflow > 0.0:
                        desired_left = max(desired_left, left + left_overflow + 0.01)
                        max_left = min(right - min_width, 0.88)
                        left = min(desired_left, max_left)
                    if right_overflow > 0.0:
                        desired_right = right - (right_overflow + 0.01)
                        min_right = left + min_width
                        right = max(desired_right, min_right)

                    widest_label = max(ext.width for ext in extents) / fig_width
                    fig_dpi = float(getattr(self.figure, "dpi", 72.0) or 72.0)

                    padding_points = 0.0
                    try:
                        ytick_objs = list(getattr(ax.yaxis, "get_major_ticks", lambda: [])())
                    except Exception:  # noqa: BLE001
                        ytick_objs = []

                    if ytick_objs:
                        try:
                            padding_points = max(
                                float(getattr(tick, "get_pad", lambda: 0.0)() or 0.0)
                                for tick in ytick_objs
                            )
                        except Exception:  # noqa: BLE001
                            padding_points = 0.0
                    padding_fraction = (padding_points * fig_dpi / 72.0) / fig_width
                    fallback_estimated = 0.0
                    try:
                        font_size = yticks[0].get_fontsize() or 10
                        max_len = max(len(tick.get_text()) for tick in yticks)
                        fallback_estimated = (max_len * max(font_size, 8) * 1.15) / fig_width
                    except Exception:  # noqa: BLE001
                        fallback_estimated = 0.0
                    desired_left = max(desired_left, fallback_estimated + padding_fraction + 0.07)
                    desired_left = max(desired_left, widest_label + padding_fraction + 0.06)
                    if desired_left > left:
                        left = min(desired_left, right - min_width)

            if top - bottom < min_height:
                bottom = max(0.05, top - min_height)
            if right - left < min_width:
                left = max(0.05, right - min_width)

            adjusted_bbox = Bbox.from_extents(left, bottom, right, top)
            ax.set_position(adjusted_bbox)
        elif self.figure is not None:
            fig_width = max(float(self.figure.get_figwidth() * self.figure.get_dpi()), 1.0)
            fig_height = max(float(self.figure.get_figheight() * self.figure.get_dpi()), 1.0)

            left = initial_bbox.x0
            right = initial_bbox.x1
            bottom = initial_bbox.y0
            top = initial_bbox.y1

            min_width = 0.25
            min_height = 0.25

            xticks = [tick for tick in ax.get_xticklabels() if tick.get_visible() and tick.get_text()]
            if xticks:
                font_size = xticks[0].get_fontsize() or 10
                max_len = max(len(tick.get_text()) for tick in xticks)
                estimated_height = (max_len * max(font_size, 8) * 0.6) / fig_height
                desired_bottom = estimated_height + 0.02
                if desired_bottom > bottom:
                    bottom = min(desired_bottom, top - min_height)

            yticks = [tick for tick in ax.get_yticklabels() if tick.get_visible() and tick.get_text()]
            desired_left = left
            if yticks:
                font_size = yticks[0].get_fontsize() or 10
                max_len = max(len(tick.get_text()) for tick in yticks)
                estimated_width = (max_len * max(font_size, 8) * 1.15) / fig_width
                fig_dpi = float(getattr(self.figure, "dpi", 72.0) or 72.0)
                padding_points = 0.0
                try:
                    ytick_objs = list(getattr(ax.yaxis, "get_major_ticks", lambda: [])())
                except Exception:  # noqa: BLE001
                    ytick_objs = []

                if ytick_objs:
                    try:
                        padding_points = max(
                            float(getattr(tick, "get_pad", lambda: 0.0)() or 0.0)
                            for tick in ytick_objs
                        )
                    except Exception:  # noqa: BLE001
                        padding_points = 0.0

                padding_fraction = (padding_points * fig_dpi / 72.0) / fig_width
                desired_left = max(desired_left, estimated_width + padding_fraction + 0.06)

            y_label = ax.yaxis.get_label()
            if y_label is not None and y_label.get_text():
                font_size = y_label.get_fontsize() or 10
                label_estimated_width = (len(y_label.get_text()) * max(font_size, 8) * 1.02) / fig_width
                desired_left = max(desired_left, label_estimated_width + 0.05)

            if desired_left > left:
                left = min(desired_left, right - min_width)

            if top - bottom < min_height:
                bottom = max(0.05, top - min_height)
            if right - left < min_width:
                left = max(0.05, right - min_width)

            adjusted_bbox = Bbox.from_extents(left, bottom, right, top)
            ax.set_position(adjusted_bbox)

        self._heatmap_colorbar = self.figure.colorbar(
            heatmap,
            ax=ax,
            fraction=0.05,
            pad=0.02,
        )
        self._heatmap_cbar_ax = self._heatmap_colorbar.ax
        self._heatmap_cbar_ax.set_facecolor("#ffffff")

        heatmap_bbox = ax.get_position().frozen()
        preferred_pad = min(0.04, max(0.02, heatmap_bbox.width * 0.08))
        preferred_width = min(0.05, max(0.02, heatmap_bbox.width * 0.075))
        total_available = max(0.0, 1.0 - heatmap_bbox.x1)

        tentative_width = total_available - preferred_pad
        if tentative_width <= 0.0:
            colorbar_width = min(preferred_width, max(0.02, total_available))
            pad = max(0.0, total_available - colorbar_width)
        else:
            colorbar_width = min(preferred_width, max(0.02, tentative_width))
            pad = preferred_pad

        left = heatmap_bbox.x1 + pad
        if left + colorbar_width > 1.0:
            overflow = (left + colorbar_width) - 1.0
            left = max(heatmap_bbox.x1, left - overflow)

        self._heatmap_cbar_ax.set_position(
            [left, heatmap_bbox.y0, colorbar_width, heatmap_bbox.height]
        )

        cbar_bbox = self._heatmap_cbar_ax.get_position().frozen()
        combined_left = min(heatmap_bbox.x0, cbar_bbox.x0)
        combined_right = max(heatmap_bbox.x1, cbar_bbox.x1)
        combined_center = (combined_left + combined_right) / 2.0
        target_center = (initial_bbox.x0 + initial_bbox.x1) / 2.0
        shift = target_center - combined_center

        min_x0 = min(heatmap_bbox.x0, cbar_bbox.x0)
        max_x1 = max(heatmap_bbox.x1, cbar_bbox.x1)
        shift = min(max(shift, -min_x0), 1.0 - max_x1)

        if abs(shift) > 1e-6:
            ax.set_position(heatmap_bbox.translated(shift, 0.0))
            self._heatmap_cbar_ax.set_position(cbar_bbox.translated(shift, 0.0))
        
        abs_matrix = np.abs(numeric_df.to_numpy(dtype=float))
        if abs_matrix.size:
            np.fill_diagonal(abs_matrix, np.nan)
            with np.errstate(invalid="ignore"):
                try:
                    max_abs = np.nanmax(abs_matrix)
                except ValueError:
                    max_abs = 0.0
            if np.isnan(max_abs):
                max_abs = 0.0
        else:
            max_abs = 0.0

        base_title = self.PLOT_TITLES.get(plot_key, "Correlation heatmap")
        return fr"{base_title} ($|r|_{{\mathrm{{max}}}}$ = {max_abs:.3f})"

    def _plot_exp_vs_pred(self, df: pd.DataFrame):
        self._scatter_plot(
            df,
            x_column="Actual",
            y_column="Predicted",
            xlabel="Actual values",
            ylabel="Predicted values",
            include_identity=True,
        )

    def _plot_exp_vs_pred_loo(self, df: pd.DataFrame):
        self._scatter_plot(
            df,
            x_column="Actual",
            y_column="Predicted_LOO",
            xlabel="Actual values",
            ylabel="Predicted values by LOO",
            include_identity=True,
        )

    def _plot_exp_vs_resid(self, df: pd.DataFrame):
        self._scatter_plot(
            df,
            x_column="Actual",
            y_column="Residual",
            xlabel="Actual values",
            ylabel="Residuals",
            include_zero_line=True,
            zero_line_axis="y",
        )

    def _plot_exp_vs_resid_loo(self, df: pd.DataFrame):
        self._scatter_plot(
            df,
            x_column="Actual",
            y_column="Residual_LOO",
            xlabel="Actual values",
            ylabel="Residuals by LOO",
            include_zero_line=True,
            zero_line_axis="y",
        )

    def _plot_resid_vs_pred(self, df: pd.DataFrame):
        self._scatter_plot(
            df,
            x_column="Predicted",
            y_column="Residual",
            xlabel="Predicted values",
            ylabel="Residuals",
            include_zero_line=True,
            zero_line_axis="y",
        )

    def _plot_resid_vs_pred_loo(self, df: pd.DataFrame):
        self._scatter_plot(
            df,
            x_column="Predicted_LOO",
            y_column="Residual_LOO",
            xlabel="Predicted values by LOO",
            ylabel="Residuals by LOO",
            include_zero_line=True,
            zero_line_axis="y",
        )

    def _plot_scale_location(self, df: pd.DataFrame):
        working = self._prepare_plot_dataframe(df, ("Predicted", "StdPredResid"))
        if "Predicted" not in working.columns or "StdPredResid" not in working.columns:
            raise ValueError("Scale-Location plot requires predicted values and standardized residuals.")
        working = working.copy()
        std_resid = pd.to_numeric(working["StdPredResid"], errors="coerce")
        with np.errstate(invalid="ignore"):
            working["_ScaleLocation"] = np.sqrt(np.abs(std_resid.astype(float)))
        self._scatter_plot(
            working,
            x_column="Predicted",
            y_column="_ScaleLocation",
            xlabel="Predicted values",
            ylabel=r"$\sqrt{|\mathrm{std.\ residuals}|}$",
            include_zero_line=False,
        )

    def _plot_scale_location_loo(self, df: pd.DataFrame):
        working = self._prepare_plot_dataframe(df, ("Predicted_LOO", "StdPredResid_LOO"))
        if "Predicted_LOO" not in working.columns or "StdPredResid_LOO" not in working.columns:
            raise ValueError("Scale-Location plot requires LOO predicted values and standardized residuals.")
        working = working.copy()
        std_resid = pd.to_numeric(working["StdPredResid_LOO"], errors="coerce")
        with np.errstate(invalid="ignore"):
            working["_ScaleLocation"] = np.sqrt(np.abs(std_resid.astype(float)))
        self._scatter_plot(
            working,
            x_column="Predicted_LOO",
            y_column="_ScaleLocation",
            xlabel="Predicted values by LOO",
            ylabel=r"$\sqrt{|\mathrm{std.\ residuals\ by\ LOO}|}$",
            include_zero_line=False,
        )

    def _plot_qq_resid(self, df: pd.DataFrame):
        self._qq_plot(
            df,
            column="StdPredResid",
            ylabel="Ordered std. residuals",
        )

    def _plot_qq_resid_loo(self, df: pd.DataFrame):
        self._qq_plot(
            df,
            column="StdPredResid_LOO",
            ylabel="Ordered std. residuals by LOO",
        )

    def _residual_distribution_plot(
        self,
        df: pd.DataFrame,
        *,
        column: str,
        xlabel: str,
        base_title: str,
    ) -> str:
        working = self._prepare_plot_dataframe(df, (column,))
        if column not in working.columns:
            raise ValueError("Residual distribution requires standardized residuals.")

        filtered = working.copy()
        filtered[column] = pd.to_numeric(filtered[column], errors="coerce")
        filtered = filtered[np.isfinite(filtered[column].astype(float))]
        if filtered.empty:
            raise ValueError("Insufficient data to draw the selected chart.")

        values = filtered[column].astype(float).to_numpy()
        datasets: list[tuple[Optional[str], np.ndarray]] = []
        if "Set" in filtered.columns:
            for dataset in filtered["Set"].unique():
                subset = filtered[filtered["Set"] == dataset]
                data = subset[column].astype(float).to_numpy()
                if data.size:
                    datasets.append((str(dataset), data))
        if not datasets:
            datasets.append((None, values))

        bins = np.histogram_bin_edges(values, bins="auto")
        if bins.size < 2 or not np.all(np.isfinite(bins)):
            min_val = float(np.min(values))
            max_val = float(np.max(values))
            if np.isclose(min_val, max_val):
                width = max(abs(min_val) * 0.1, 1.0)
                bins = np.linspace(min_val - width, max_val + width, 11)
            else:
                bins = np.linspace(min_val, max_val, min(values.size + 1, 20))

        ax = self.ax
        try:
            ax.set_box_aspect(None)
        except Exception:  # noqa: BLE001
            pass
        self._apply_gridlines(ax)
        self._apply_axis_labels(xlabel, "Density")

        hist_components: list[np.ndarray] = []
        total_groups = len(datasets)
        for dataset, data in datasets:
            color_override = self._resolve_marker_color(dataset)
            base_color = color_override or self.COLOR_MAP.get(dataset or "", "#1f77b4")
            alpha = 0.5 if total_groups > 1 else 0.6
            counts, _, _ = ax.hist(
                data,
                bins=bins,
                density=True,
                alpha=alpha,
                color=base_color,
                edgecolor="#ffffff",
                linewidth=0.6,
                label=dataset,
                histtype="stepfilled",
            )
            hist_components.append(np.asarray(counts, dtype=float))

        mu = float(np.mean(values))
        sigma = float(np.std(values, ddof=1)) if values.size > 1 else 0.0

        normal_curve: Optional[np.ndarray] = None
        if sigma > 0 and np.isfinite(sigma):
            x_min = float(bins[0])
            x_max = float(bins[-1])
            x_grid = np.linspace(x_min, x_max, 200)
            normal_curve = (
                (1.0 / (sigma * math.sqrt(2.0 * math.pi)))
                * np.exp(-0.5 * ((x_grid - mu) / sigma) ** 2)
            )
            ax.plot(
                x_grid,
                normal_curve,
                color="#222222",
                linestyle="--",
                linewidth=1.2,
                label="Normal fuction",
            )

        ax.axvline(0.0, color="black", linestyle=":", linewidth=1)

        y_components: list[np.ndarray] = [comp for comp in hist_components if comp.size]
        if normal_curve is not None:
            y_components.append(normal_curve)
        y_values = np.concatenate(y_components) if y_components else np.empty(0, dtype=float)

        update_x = not self._axis_user_override["x"]
        update_y = not self._axis_user_override["y"]
        self._apply_axis_defaults(
            values,
            y_values,
            update_x=update_x,
            update_y=update_y,
        )

        if self.legend_var.get():
            self._show_legend()

        count = values.size
        def _format_residual_stat(value: float) -> str:
            if not np.isfinite(value):
                return "nan"
            if value == 0.0:
                return "0.00"
            if abs(value) < 1e-3:
                return f"{value:.2e}"
            return f"{value:.2f}"

        mu_text = _format_residual_stat(mu)
        sigma_text = _format_residual_stat(sigma)
        return f"{base_title} (n={count}, µ={mu_text}, s={sigma_text})"

    def _plot_residual_distribution(self, df: pd.DataFrame):
        base_title = self.PLOT_TITLES.get("residual_distribution", "Residual distribution")
        return self._residual_distribution_plot(
            df,
            column="StdPredResid",
            xlabel="Standardized residuals",
            base_title=base_title,
        )

    def _plot_residual_distribution_loo(self, df: pd.DataFrame):
        base_title = self.PLOT_TITLES.get(
            "residual_distribution_loo",
            "Residual distribution by LOO",
        )
        return self._residual_distribution_plot(
            df,
            column="StdPredResid_LOO",
            xlabel="Standardized residuals by LOO",
            base_title=base_title,
        )

    def _plot_williams(self, df: pd.DataFrame):
        self._williams_plot(df, column="StdPredResid")

    def _plot_williams_loo(self, df: pd.DataFrame):
        self._williams_plot(df, column="StdPredResid_LOO")

    def _plot_hat(self, df: pd.DataFrame):
        columns = ["Leverage", "Set"]
        if "Observation" in df.columns:
            columns.insert(0, "Observation")
        if self._extra_label_column and self._extra_label_column in df.columns:
            insert_pos = 1 if "Observation" in columns else 0
            columns.insert(insert_pos, self._extra_label_column)
        values = df[columns].copy()
        values = values[np.isfinite(values["Leverage"].astype(float))]
        if values.empty:
            raise ValueError("Leverage values are unavailable for the selected dataset.")

        values = values.reset_index(drop=True)
        fallback_positions = np.arange(1, len(values) + 1, dtype=float)
        if "Observation" in values.columns:
            numeric_obs = pd.to_numeric(values["Observation"], errors="coerce")
            if numeric_obs.notna().any():
                positions = fallback_positions.copy()
                valid_mask = numeric_obs.notna().to_numpy()
                positions[valid_mask] = numeric_obs[valid_mask].to_numpy(dtype=float)
                values["__position"] = positions
            else:
                values["__position"] = fallback_positions
        else:
            values["__position"] = fallback_positions

        ax = self.ax
        self._enforce_axis_ratio(ax)
        self._apply_gridlines(ax)
        self._apply_axis_labels("Observation", "Leverage")

        x_all: list[np.ndarray] = []
        y_all: list[np.ndarray] = []

        for dataset in values["Set"].unique():
            subset = values[values["Set"] == dataset]
            x = subset["__position"].to_numpy(dtype=float)
            y = subset["Leverage"].astype(float).to_numpy()
            observations = subset["Observation"].to_numpy() if "Observation" in subset.columns else None
            extra_labels = (
                subset[self._extra_label_column].to_numpy()
                if self._extra_label_column and self._extra_label_column in subset.columns
                else None
            )
            color_override = self._resolve_marker_color(dataset)
            marker_style = self._resolve_marker_style(dataset)
            color = color_override or self.COLOR_MAP.get(dataset, "#333333")
            collection = ax.scatter(
                x,
                y,
                label=dataset,
                s=self.point_size_var.get(),
                color=color,
                alpha=0.8,
                marker=marker_style,
            )
            index_values = subset.index.to_numpy()
            self._register_scatter_points(
                collection,
                x,
                y,
                dataset_label=dataset,
                observation_ids=observations,
                index_values=index_values,
                extra_labels=extra_labels,
            )
            x_all.append(x)
            y_all.append(y)

        if np.isfinite(self.hat_threshold):
            hat_label = f"h* ({self._format_hat_threshold(self.hat_threshold)})"
            ax.axhline(
                self.hat_threshold,
                color=self._resolve_h_line_color(),
                linestyle="--",
                linewidth=1,
                label=hat_label,
            )

        if x_all and y_all:
            x_concat = np.concatenate(x_all)
            y_concat = np.concatenate(y_all)

            if not self._axis_user_override["y"]:
                extras: list[float] = [0.0]
                if np.isfinite(self.hat_threshold):
                    extras.append(float(self.hat_threshold))
                if extras:
                    y_limits_values = np.concatenate(
                        [y_concat, np.asarray(extras, dtype=float)]
                    )
                else:
                    y_limits_values = y_concat
            else:
                y_limits_values = y_concat

            self._apply_axis_defaults(
                x_concat,
                y_limits_values,
                update_x=not self._axis_user_override["x"],
                update_y=not self._axis_user_override["y"],
            )

        self._show_legend()

    def _scatter_plot(
        self,
        df: pd.DataFrame,
        *,
        x_column: str,
        y_column: str,
        xlabel: str,
        ylabel: str,
        include_identity: bool = False,
        include_zero_line: bool = False,
        zero_line_axis: str = "y",
    ):
        df = self._prepare_plot_dataframe(df, (x_column, y_column))
        columns = ["Set", x_column, y_column]
        if "Observation" in df.columns:
            columns.insert(0, "Observation")
        if self._extra_label_column and self._extra_label_column in df.columns:
            insert_pos = 1 if "Observation" in columns else 0
            columns.insert(insert_pos, self._extra_label_column)
        filtered = df[columns].copy()
        filtered = filtered[np.isfinite(filtered[x_column].astype(float))]
        filtered = filtered[np.isfinite(filtered[y_column].astype(float))]
        if filtered.empty:
            raise ValueError("Insufficient data to draw the selected chart.")

        ax = self.ax
        self._enforce_axis_ratio(ax)
        self._apply_gridlines(ax)
        self._apply_axis_labels(xlabel, ylabel)

        sizes = float(self.point_size_var.get())
        x_values = filtered[x_column].astype(float).to_numpy()
        y_values = filtered[y_column].astype(float).to_numpy()

        for dataset in filtered["Set"].unique():
            subset = filtered[filtered["Set"] == dataset]
            x = subset[x_column].astype(float).to_numpy()
            y = subset[y_column].astype(float).to_numpy()
            observations = subset["Observation"].to_numpy() if "Observation" in subset.columns else None
            extra_labels = (
                subset[self._extra_label_column].to_numpy()
                if self._extra_label_column and self._extra_label_column in subset.columns
                else None
            )
            color_override = self._resolve_marker_color(dataset)
            marker_style = self._resolve_marker_style(dataset)
            color = color_override or self.COLOR_MAP.get(dataset, "#333333")
            collection = ax.scatter(
                x,
                y,
                label=dataset,
                s=sizes,
                alpha=0.8,
                color=color,
                marker=marker_style,
            )
            index_values = subset.index.to_numpy()
            self._register_scatter_points(
                collection,
                x,
                y,
                dataset_label=dataset,
                observation_ids=observations,
                index_values=index_values,
                extra_labels=extra_labels,
            )

        line_data: Optional[tuple[np.ndarray, np.ndarray]] = None
        if self.linear_fit_var.get() and x_values.size >= 2:
            x_min = float(np.min(x_values))
            x_max = float(np.max(x_values))
            if not np.isclose(x_min, x_max):
                slope, intercept = np.polyfit(x_values, y_values, 1)
                line_x = np.linspace(x_min, x_max, 100)
                line_y = slope * line_x + intercept
                line_color = self._resolve_linear_fit_color()
                ax.plot(line_x, line_y, color=line_color, linewidth=1.2, label="Linear fit")
                line_data = (line_x, line_y)

        axis_x = x_values
        axis_y = y_values
        if line_data is not None:
            axis_x = np.concatenate([axis_x, line_data[0]])
            axis_y = np.concatenate([axis_y, line_data[1]])

        axis_y_for_limits = axis_y
        if include_zero_line and zero_line_axis == "y":
            axis_y_for_limits = self._with_zero_symmetric_limits(axis_y)

        update_x = not self._axis_user_override["x"]
        update_y = not self._axis_user_override["y"]

        if include_identity and self.identity_var.get():
            x_limits, _ = self._apply_axis_defaults(
                axis_x,
                axis_y_for_limits,
                identity=True,
                update_x=update_x,
                update_y=update_y,
            )
            if x_limits is not None:
                start, end = x_limits
                ax.plot(
                    [start, end],
                    [start, end],
                    color="black",
                    linestyle="--",
                    linewidth=1,
                    label="Identity",
                )
        else:
            self._apply_axis_defaults(
                axis_x,
                axis_y_for_limits,
                update_x=update_x,
                update_y=update_y,
            )

        if include_zero_line:
            if zero_line_axis == "x":
                ax.axvline(
                    0.0, color="black", linestyle=":", linewidth=1, label="Zero line"
                )
            else:
                ax.axhline(
                    0.0, color="black", linestyle=":", linewidth=1, label="Zero line"
                )

        if self.legend_var.get():
            self._show_legend()

    def _plot_pred_vs_hat(self, df: pd.DataFrame):
        df = self._prepare_plot_dataframe(df, ("Predicted", "Leverage"))
        columns = ["Set", "Predicted", "Leverage"]
        if "Observation" in df.columns:
            columns.insert(0, "Observation")
        if self._extra_label_column and self._extra_label_column in df.columns:
            insert_pos = 1 if "Observation" in columns else 0
            columns.insert(insert_pos, self._extra_label_column)
        filtered = df[columns].copy()
        filtered = filtered[np.isfinite(filtered["Predicted"].astype(float))]
        filtered = filtered[np.isfinite(filtered["Leverage"].astype(float))]
        if filtered.empty:
            raise ValueError("Insufficient data to draw the selected chart.")

        ax = self.ax
        self._enforce_axis_ratio(ax)
        self._apply_gridlines(ax)
        self._apply_axis_labels("Leverage", "Predicted values")

        sizes = float(self.point_size_var.get())

        x_all: list[np.ndarray] = []
        y_all: list[np.ndarray] = []

        for dataset in filtered["Set"].unique():
            subset = filtered[filtered["Set"] == dataset]
            x = subset["Leverage"].astype(float).to_numpy()
            y = subset["Predicted"].astype(float).to_numpy()
            observations = subset["Observation"].to_numpy() if "Observation" in subset.columns else None
            extra_labels = (
                subset[self._extra_label_column].to_numpy()
                if self._extra_label_column and self._extra_label_column in subset.columns
                else None
            )
            color_override = self._resolve_marker_color(dataset)
            marker_style = self._resolve_marker_style(dataset)
            color = color_override or self.COLOR_MAP.get(dataset, "#333333")
            collection = ax.scatter(
                x,
                y,
                label=dataset,
                s=sizes,
                alpha=0.8,
                color=color,
                marker=marker_style,
            )
            index_values = subset.index.to_numpy()
            self._register_scatter_points(
                collection,
                x,
                y,
                dataset_label=dataset,
                observation_ids=observations,
                index_values=index_values,
                extra_labels=extra_labels,
            )
            x_all.append(x)
            y_all.append(y)

        if np.isfinite(self.hat_threshold):
            hat_label = f"h* ({self._format_hat_threshold(self.hat_threshold)})"
            ax.axvline(
                self.hat_threshold,
                color=self._resolve_h_line_color(),
                linestyle="--",
                linewidth=1,
                label=hat_label,
            )

        if x_all and y_all:
            x_concat = np.concatenate(x_all)
            y_concat = np.concatenate(y_all)

            self._apply_axis_defaults(
                x_concat,
                y_concat,
                update_x=not self._axis_user_override["x"],
                update_y=not self._axis_user_override["y"],
            )

        if self.legend_var.get():
            self._show_legend()

    def _plot_y_randomization_r2(self, df: pd.DataFrame):
        del df
        return self._plot_y_randomization_histogram("R2")

    def _plot_y_randomization_loo(self, df: pd.DataFrame):
        del df
        return self._plot_y_randomization_histogram("R2_loo")

    def _plot_y_randomization_histogram(self, metric_key: str):
        if self.current_model_id is None:
            raise ValueError("Select a model to visualize.")
        result = self._get_y_randomization_result(self.current_model_id, metric_key)
        if result is None:
            raise ValueError("No Y-Randomization results are available for this model.")

        values = np.asarray(result.metrics, dtype=float)
        values = values[np.isfinite(values)]
        if values.size == 0:
            raise ValueError("No valid permutation metrics are available for plotting.")

        bins = max(5, min(30, int(math.sqrt(max(values.size, 1)))))
        bin_color = self._resolve_color_choice(result.bin_color_label, "#1f77b4")
        self._apply_gridlines(self.ax)
        counts, bin_edges, _ = self.ax.hist(values, bins=bins, color=bin_color, edgecolor="#ffffff")

        metric_label = (
            R_SQUARED_SYMBOL if metric_key == "R2" else f"{Q_SQUARED_SYMBOL} (LOO)"
        )
        self._apply_axis_labels(metric_label, "Frequency")

        edge_array = (
            bin_edges.astype(float, copy=False)
            if isinstance(bin_edges, np.ndarray)
            else np.asarray([], dtype=float)
        )
        if edge_array.size:
            finite_mask = np.isfinite(edge_array)
            if finite_mask.any():
                edge_array = edge_array[finite_mask]
            else:
                edge_array = np.asarray([], dtype=float)
        left_edges = edge_array[:-1] if edge_array.size else np.asarray([], dtype=float)
        right_edges = edge_array[1:] if edge_array.size else np.asarray([], dtype=float)

        x_components: list[np.ndarray] = [values]
        if left_edges.size:
            x_components.append(left_edges)
        if right_edges.size:
            x_components.append(right_edges)
        if np.isfinite(result.actual_value):
            x_components.append(np.asarray([float(result.actual_value)], dtype=float))
        x_for_limits = (
            np.concatenate([component for component in x_components if component.size])
            if any(component.size for component in x_components)
            else np.asarray([0.0, 1.0], dtype=float)
        )

        y_components: list[np.ndarray] = []
        if isinstance(counts, np.ndarray) and counts.size:
            y_components.append(counts.astype(float, copy=False))
        y_components.append(np.asarray([0.0], dtype=float))
        y_for_limits = (
            np.concatenate([component for component in y_components if component.size])
            if any(component.size for component in y_components)
            else np.asarray([0.0], dtype=float)
        )

        update_x = not self._axis_user_override["x"]
        update_y = not self._axis_user_override["y"]
        self._apply_axis_defaults(
            x_for_limits,
            y_for_limits,
            update_x=update_x,
            update_y=update_y,
        )
        self.ax.set_ylim(bottom=0.0)

        try:
            self.ax.xaxis.set_major_formatter(FormatStrFormatter("%.3f"))
        except Exception:  # noqa: BLE001 - defensive
            pass

        if np.isfinite(result.actual_value):
            line_color = self._resolve_color_choice(result.actual_line_color_label, "#d62728")
            self.ax.axvline(
                result.actual_value,
                color=line_color,
                linestyle="--",
                linewidth=2,
                label="Actual model",
            )
            if self.legend_var.get():
                self._show_legend()

        p_text = f"{result.p_value:.4f}" if np.isfinite(result.p_value) else "n/a"
        title_prefix = "Y-Randomization" if metric_key == "R2" else "Y-Randomization by LOO"
        self.plot_status_var.set(f"p-value = {p_text}. {result.interpretation}")
        return f"{title_prefix} (p-value = {p_text})"

    class _YRandomizationDialog(tk.Toplevel):
        PERMUTATION_CHOICES: tuple[str, ...] = (
            "1000",
            "5000",
            "10000",
            "Custom",
        )
        DEFAULT_ACTUAL_COLOR_LABEL = "Default"
        DEFAULT_BIN_COLOR_LABEL = "Default"
        PROGRESSBAR_STYLE = "YRandomization.Horizontal.TProgressbar"
        _progressbar_style_configured = False

        def __init__(
            self,
            parent_tab: "VisualizationTab",
            design: np.ndarray,
            y: np.ndarray,
            clip: Optional[tuple[float, float]],
        ) -> None:
            super().__init__(parent_tab)
            self.parent_tab = parent_tab
            self._initial_design = design
            self._initial_y = y
            self._initial_clip = clip
            self._initial_model_id = parent_tab.current_model_id
            self._thread: Optional[threading.Thread] = None
            self._cancel_event = threading.Event()
            self._result: Optional[YRandomizationResult] = None
            self._closing = False

            self.metric_var = tk.StringVar(value="R2")
            self.permutation_var = tk.StringVar(value=self.PERMUTATION_CHOICES[0])
            self.custom_perm_var = tk.StringVar()
            self.actual_color_var = tk.StringVar(value=self.DEFAULT_ACTUAL_COLOR_LABEL)
            self.bin_color_var = tk.StringVar(value=self.DEFAULT_BIN_COLOR_LABEL)
            self.progress_var = tk.StringVar(value="Progress: 0%")
            self.status_var = tk.StringVar(value="Status: Waiting for calculation.")

            self.title("Y-Randomization")
            self._sync_preferences_from_store(force_default=True)
            self._build_ui()
            self._set_running_state(False)
            self._handle_permutation_choice()
            self.bind("<Return>", lambda _e: self._start_computation())
            self.bind("<Escape>", lambda _e: self._handle_close())
            self.transient(parent_tab.winfo_toplevel())
            self.resizable(False, False)
            self.update_idletasks()
            self._center_window()
            self.grab_set()
            self.focus_set()

        def _build_ui(self) -> None:
            body = ttk.Frame(self)
            body.grid(row=0, column=0, sticky="nsew", padx=16, pady=16)
            body.grid_columnconfigure(0, weight=1)
            body.grid_columnconfigure(1, weight=0)

            color_labels = [label for label, _color in self.parent_tab.COLOR_CHOICES]

            permutation_row = ttk.Frame(body)
            permutation_row.grid(row=0, column=0, columnspan=2, sticky="w")
            ttk.Label(permutation_row, text="Number of permutations:").pack(side="left")
            self.permutation_combo = ttk.Combobox(
                permutation_row,
                textvariable=self.permutation_var,
                values=self.PERMUTATION_CHOICES,
                state="readonly",
                width=9,
            )
            self.permutation_combo.pack(side="left", padx=(12, 0))
            self.permutation_combo.bind("<<ComboboxSelected>>", self._handle_permutation_choice)

            custom_row = ttk.Frame(permutation_row)
            custom_row.pack(side="left", padx=(24, 0))
            ttk.Label(custom_row, text="Custom value:").pack(side="left")
            self.custom_perm_entry = ttk.Entry(
                custom_row, textvariable=self.custom_perm_var, width=9, state="disabled"
            )
            self.custom_perm_entry.pack(side="left", padx=(12, 0))

            metric_row = ttk.Frame(body)
            metric_row.grid(row=1, column=0, sticky="w", pady=(12, 0))
            ttk.Label(metric_row, text="Metric:").pack(side="left")
            metric_buttons = ttk.Frame(metric_row)
            metric_buttons.pack(side="left", padx=(12, 0))
            ttk.Radiobutton(
                metric_buttons,
                text=R_SQUARED_SYMBOL,
                value="R2",
                variable=self.metric_var,
                command=self._handle_metric_change,
            ).pack(side="left")
            ttk.Radiobutton(
                metric_buttons,
                text=f"{Q_SQUARED_SYMBOL} (LOO)",
                value="R2_loo",
                variable=self.metric_var,
                command=self._handle_metric_change,
            ).pack(side="left", padx=(16, 0))

            color_column = ttk.Frame(body)
            color_column.grid(row=1, column=1, rowspan=2, sticky="nw", pady=(12, 0))
            color_table = ttk.Frame(color_column)
            color_table.grid(row=0, column=0, sticky="nw")
            color_table.grid_columnconfigure(0, weight=0)
            color_table.grid_columnconfigure(1, weight=1)

            ttk.Label(color_table, text="Actual model line color:").grid(
                row=0, column=0, sticky="e", padx=(0, 8)
            )
            self.actual_color_combo = ttk.Combobox(
                color_table,
                textvariable=self.actual_color_var,
                values=color_labels,
                state="readonly",
                width=7,
            )
            self.actual_color_combo.grid(row=0, column=1, sticky="w")

            ttk.Label(color_table, text="Bin color:").grid(
                row=1, column=0, sticky="e", padx=(0, 8), pady=(12, 0)
            )
            self.bin_color_combo = ttk.Combobox(
                color_table,
                textvariable=self.bin_color_var,
                values=color_labels,
                state="readonly",
                width=7,
            )
            self.bin_color_combo.grid(row=1, column=1, sticky="w", pady=(12, 0))

            button_row = ttk.Frame(body)
            button_row.grid(row=2, column=0, sticky="w", pady=(16, 0))
            self.compute_button = ttk.Button(button_row, text="Compute", command=self._start_computation)
            self.compute_button.grid(row=0, column=0, sticky="w")
            self.cancel_button = ttk.Button(
                button_row, text="Cancel", command=self._cancel_computation, state="disabled"
            )
            self.cancel_button.grid(row=0, column=1, sticky="w", padx=(12, 0))

            progress_row = ttk.Frame(body)
            progress_row.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(16, 0))
            progress_row.columnconfigure(0, weight=1)
            self.progressbar = ttk.Progressbar(progress_row, mode="determinate", maximum=100)
            self._setup_progressbar()
            self.progressbar.grid(row=0, column=0, sticky="ew")
            ttk.Label(progress_row, textvariable=self.progress_var).grid(
                row=0, column=1, sticky="w", padx=(12, 0)
            )

            ttk.Label(body, textvariable=self.status_var, wraplength=360).grid(
                row=4, column=0, columnspan=2, sticky="w", pady=(16, 0)
            )

            action_row = ttk.Frame(body)
            action_row.grid(row=5, column=0, columnspan=2, sticky="e", pady=(12, 0))
            self.add_chart_button = ttk.Button(
                action_row, text="Add chart", command=self._on_add_chart, state="disabled"
            )
            self.add_chart_button.pack(side="left")
            self.export_button = ttk.Button(
                action_row, text="Export data", command=self._on_export, state="disabled"
            )
            self.export_button.pack(side="left", padx=(12, 0))

            self.permutation_combo.set(self.permutation_var.get())
            self.progressbar["value"] = 0

        def _center_window(self) -> None:
            parent = self.parent_tab.winfo_toplevel()
            parent.update_idletasks()
            self.update_idletasks()
            width = self.winfo_width()
            height = self.winfo_height()
            px = parent.winfo_rootx()
            py = parent.winfo_rooty()
            pw = parent.winfo_width()
            ph = parent.winfo_height()
            x = px + max((pw - width) // 2, 0)
            y = py + max((ph - height) // 2, 0)
            self.geometry(f"+{x}+{y}")

        def _handle_metric_change(self) -> None:
            self._sync_preferences_from_store(force_default=True)

        def _sync_preferences_from_store(self, force_default: bool = False) -> None:
            metric_key = self.metric_var.get()
            model_id = self.parent_tab.current_model_id
            result: Optional[YRandomizationResult] = None
            if model_id is not None:
                result = self.parent_tab._get_y_randomization_result(model_id, metric_key)
            if result is not None:
                self.actual_color_var.set(result.actual_line_color_label)
                self.bin_color_var.set(result.bin_color_label)
            elif force_default:
                self.actual_color_var.set(self.DEFAULT_ACTUAL_COLOR_LABEL)
                self.bin_color_var.set(self.DEFAULT_BIN_COLOR_LABEL)

        def _handle_permutation_choice(self, *_event) -> None:
            choice = (self.permutation_var.get() or "").strip().lower()
            if choice == "custom":
                self.custom_perm_entry.configure(state="normal")
                self.custom_perm_entry.focus_set()
            else:
                self.custom_perm_entry.configure(state="disabled")
                if choice:
                    self.custom_perm_var.set("")

        def _resolve_permutations(self) -> int:
            choice = (self.permutation_var.get() or "").strip()
            candidates = []
            if choice and choice.lower() != "custom":
                candidates.append(choice)
            custom = self.custom_perm_var.get().strip()
            if choice.lower() == "custom" or not candidates:
                candidates.append(custom)
            for text in candidates:
                if not text:
                    continue
                try:
                    value = int(text)
                except ValueError:
                    continue
                if value > 0:
                    return value
            raise ValueError("Enter a valid number of permutations.")

        def _start_computation(self) -> None:
            if self._thread and self._thread.is_alive():
                return
            try:
                permutations = self._resolve_permutations()
            except ValueError as exc:
                messagebox.showerror("Y-Randomization", str(exc), parent=self)
                return

            metric_key = self.metric_var.get()
            if metric_key not in {"R2", "R2_loo"}:
                messagebox.showerror("Y-Randomization", "Select a valid metric.", parent=self)
                return

            if (
                self._initial_design is not None
                and self._initial_model_id is not None
                and self.parent_tab.current_model_id == self._initial_model_id
            ):
                design = self._initial_design
                y = self._initial_y
                clip = self._initial_clip
                model_id = self._initial_model_id
                self._initial_design = None
                self._initial_y = None
                self._initial_clip = None
                self._initial_model_id = None
            else:
                model_id = self.parent_tab.current_model_id
                if model_id is None:
                    messagebox.showerror(
                        "Y-Randomization", "Select a model before running Y-randomization.", parent=self
                    )
                    return
                try:
                    design, y, clip = self.parent_tab._prepare_design_matrix()
                except ValueError as exc:
                    messagebox.showerror("Y-Randomization", str(exc), parent=self)
                    return

            actual_metric = self.parent_tab._compute_metric_for_design(design, y, metric_key, clip)
            if not np.isfinite(actual_metric):
                messagebox.showerror(
                    "Y-Randomization",
                    "Unable to compute the selected metric for the current model.",
                    parent=self,
                )
                return

            self._result = None
            self._cancel_event = threading.Event()
            self._set_running_state(True)
            self.progressbar["value"] = 0
            self.progress_var.set("Progress: 0%")
            self.status_var.set("Status: Running computation...")

            self._thread = threading.Thread(
                target=self._run_worker,
                args=(model_id, design, y, clip, permutations, metric_key, actual_metric),
                daemon=True,
            )
            self._thread.start()

        @classmethod
        def _ensure_progressbar_style(cls) -> None:
            if cls._progressbar_style_configured:
                return
            style = ttk.Style()
            base_style = "Horizontal.TProgressbar"
            try:
                trough = style.lookup(base_style, "troughcolor")
                background = style.lookup(base_style, "background")
                options: dict[str, Any] = {}
                if trough:
                    options["troughcolor"] = trough
                if background:
                    options["background"] = background
                if options:
                    style.configure(cls.PROGRESSBAR_STYLE, **options)
                else:
                    style.configure(cls.PROGRESSBAR_STYLE)
            except tk.TclError:
                style.configure(cls.PROGRESSBAR_STYLE)
            cls._progressbar_style_configured = True

        def _setup_progressbar(self) -> None:
            self._ensure_progressbar_style()
            self.progressbar.configure(style=self.PROGRESSBAR_STYLE, value=0)

        def _run_worker(
            self,
            model_id: int,
            design: np.ndarray,
            y: np.ndarray,
            clip: Optional[tuple[float, float]],
            permutations: int,
            metric_key: str,
            actual_metric: float,
        ) -> None:
            rng = np.random.default_rng(42)
            metrics: list[float] = []
            total = int(permutations)
            try:
                for index in range(total):
                    if self._cancel_event.is_set():
                        break
                    permuted = rng.permutation(y)
                    metric_value = self.parent_tab._compute_metric_for_design(
                        design, permuted, metric_key, clip
                    )
                    metrics.append(float(metric_value))
                    self._schedule_progress(index + 1, total)
            except Exception as exc:  # noqa: BLE001 - defensive
                self.after(0, lambda exc=exc: self._handle_failure(exc))
                return
            cancelled = self._cancel_event.is_set()
            processed = len(metrics)
            self.after(
                0,
                lambda: self._handle_completion(
                    model_id, metrics, metric_key, actual_metric, total, processed, cancelled
                ),
            )

        def _schedule_progress(self, done: int, total: int) -> None:
            if total <= 0:
                percent = 0
            else:
                percent = int(min(100, max(0, round((done / total) * 100))))
            self.after(0, lambda percent=percent: self._update_progress(percent))

        def _update_progress(self, percent: int) -> None:
            percent = max(0, min(100, percent))
            self.progressbar["value"] = percent
            self.progress_var.set(f"Progress: {percent}%")

        def _handle_completion(
            self,
            model_id: int,
            metrics: list[float],
            metric_key: str,
            actual_metric: float,
            permutations: int,
            processed: int,
            cancelled: bool,
        ) -> None:
            self._thread = None
            self._set_running_state(False)
            percent = int(min(100, max(0, round((processed / permutations) * 100)))) if permutations else 0
            self._update_progress(percent)
            if cancelled:
                self.status_var.set("Status: Cancelled by user.")
                self._cancel_event.clear()
                if self._closing:
                    self._finalize_close()
                return

            finite_metrics = [value for value in metrics if np.isfinite(value)]
            if finite_metrics:
                exceed = sum(1 for value in finite_metrics if value >= actual_metric)
                p_value = (exceed + 1) / (len(finite_metrics) + 1)
            else:
                p_value = float("nan")

            interpretation = self._interpret_p_value(p_value)
            result = YRandomizationResult(
                model_id=model_id,
                metric_key=metric_key,
                permutations_requested=permutations,
                metrics=list(metrics),
                actual_value=float(actual_metric),
                p_value=float(p_value),
                interpretation=interpretation,
                actual_line_color_label=self.actual_color_var.get(),
                bin_color_label=self.bin_color_var.get(),
                completed_permutations=processed,
                completed=processed == permutations,
            )
            self._result = self.parent_tab._store_y_randomization_result(result)
            p_text = f"{p_value:.4f}" if np.isfinite(p_value) else "n/a"
            self.status_var.set(f"Status: Finished. p-value = {p_text}. Interpretation: {interpretation}")
            self.add_chart_button.configure(state="normal")
            self.export_button.configure(state="normal")
            self._cancel_event.clear()
            if self._closing:
                self._finalize_close()

        @staticmethod
        def _interpret_p_value(p_value: float) -> str:
            if not np.isfinite(p_value):
                return "Unable to compute p-value."
            if p_value <= 0.01:
                return "Very strong evidence against randomness."
            if p_value <= 0.05:
                return "Model is unlikely to be due to chance."
            if p_value <= 0.1:
                return "Weak evidence against randomness."
            return "Model may be explained by random chance."

        def _set_running_state(self, running: bool) -> None:
            if running:
                self.compute_button.configure(state="disabled")
                self.cancel_button.configure(state="normal")
                self.add_chart_button.configure(state="disabled")
                self.export_button.configure(state="disabled")
            else:
                self.compute_button.configure(state="normal")
                self.cancel_button.configure(state="disabled")
                if self._result is None:
                    self.add_chart_button.configure(state="disabled")
                    self.export_button.configure(state="disabled")

        def _cancel_computation(self) -> None:
            if not (self._thread and self._thread.is_alive()):
                return
            self._cancel_event.set()
            self.cancel_button.configure(state="disabled")
            self.status_var.set("Status: Cancelling...")

        def _handle_failure(self, exc: Exception) -> None:
            self._thread = None
            self._set_running_state(False)
            self._cancel_event.clear()
            self.status_var.set("Status: Failed to complete the computation.")
            messagebox.showerror(
                "Y-Randomization", f"An error occurred while computing Y-randomization:\n{exc}", parent=self
            )
            if self._closing:
                self._finalize_close()

        def _on_add_chart(self) -> None:
            if not self._result:
                return
            if self.parent_tab.current_model_id != self._result.model_id:
                self.parent_tab.model_combo.set(str(self._result.model_id))
                self.parent_tab._handle_model_change()
            self.parent_tab._add_y_randomization_chart(self._result.metric_key)

        def _on_export(self) -> None:
            if not self._result:
                return
            filename = f"y_randomization_model_{self._result.model_id}.csv"
            path = filedialog.asksaveasfilename(
                parent=self,
                title="Export Y-Randomization data",
                defaultextension=".csv",
                filetypes=(("CSV files", "*.csv"), ("All files", "*.*")),
                initialfile=filename,
            )
            if not path:
                return
            try:
                with open(path, "w", newline="", encoding="utf-8") as handle:
                    writer = csv.writer(handle, delimiter=";")
                    writer.writerow(["Type", self._result.metric_label])
                    writer.writerow(["Actual model", self._format_float(self._result.actual_value)])
                    for index, value in enumerate(self._result.metrics, start=1):
                        writer.writerow([f"Permutation {index}", self._format_float(value)])
            except Exception as exc:  # noqa: BLE001 - defensive
                messagebox.showerror(
                    "Y-Randomization", f"Unable to export Y-randomization data:\n{exc}", parent=self
                )
                return
            messagebox.showinfo("Y-Randomization", f"Results exported to {path}", parent=self)

        def _handle_close(self) -> None:
            if self._thread and self._thread.is_alive():
                self._closing = True
                self._cancel_computation()
                return
            self._finalize_close()

        def _finalize_close(self) -> None:
            if self.parent_tab._y_random_dialog is self:
                self.parent_tab._y_random_dialog = None
            self.destroy()

        @staticmethod
        def _format_float(value: float) -> str:
            if not np.isfinite(value):
                return ""
            return f"{value:.6f}"

    def _qq_plot(self, df: pd.DataFrame, column: str, ylabel: str = "Ordered residuals"):
        df = self._prepare_plot_dataframe(df, (column,))

        if "Set" not in df.columns:
            working = df.copy()
            working["Set"] = "Training"
        else:
            columns = [column, "Set"]
            if "Observation" in df.columns:
                columns.insert(0, "Observation")
            if self._extra_label_column and self._extra_label_column in df.columns:
                insert_pos = 1 if "Observation" in columns else 0
                columns.insert(insert_pos, self._extra_label_column)
            working = df[columns].copy()

        working[column] = working[column].astype(float)
        working = working[np.isfinite(working[column])]
        if working.empty:
            raise ValueError("Not enough residuals to compute the Q-Q plot.")

        preferred_order = ["Training", "Testing"]
        available_sets = list(working["Set"].unique())
        ordered_sets: list[str] = [item for item in preferred_order if item in available_sets]
        ordered_sets.extend(item for item in available_sets if item not in ordered_sets)

        ax = self.ax
        self._enforce_axis_ratio(ax)
        self._apply_gridlines(ax)
        self._apply_axis_labels("Theoretical quantiles (Z)", ylabel)

        point_size = float(self.point_size_var.get())
        theoretical_values: list[np.ndarray] = []
        residual_values: list[np.ndarray] = []
        plotted = False

        for dataset in ordered_sets:
            subset = working[working["Set"] == dataset]
            residuals = subset[column].to_numpy(dtype=float)
            finite_mask = np.isfinite(residuals)
            if not finite_mask.any():
                continue
            residuals = residuals[finite_mask]
            observations = (
                subset["Observation"].to_numpy()[finite_mask]
                if "Observation" in subset.columns
                else None
            )
            extra_values = (
                subset[self._extra_label_column].to_numpy()[finite_mask]
                if self._extra_label_column and self._extra_label_column in subset.columns
                else None
            )
            index_values = subset.index.to_numpy()[finite_mask]
            if residuals.size < 2:
                continue

            order = np.argsort(residuals)
            residuals_sorted = residuals[order]
            if observations is not None:
                observations = observations[order]
            if extra_values is not None:
                extra_values = extra_values[order]
            index_values = index_values[order]

            count = residuals_sorted.size
            probs = (np.arange(1, count + 1) - 0.5) / count
            theoretical = norm_ppf(probs)

            color_override = self._resolve_marker_color(dataset)
            default_color = self.COLOR_MAP.get(dataset, "#1f77b4")
            color = color_override or default_color
            marker_style = self._resolve_marker_style(dataset)

            collection = ax.scatter(
                theoretical,
                residuals_sorted,
                label=dataset,
                color=color,
                s=point_size,
                alpha=0.8,
                marker=marker_style,
            )

            self._register_scatter_points(
                collection,
                theoretical,
                residuals_sorted,
                dataset_label=dataset,
                observation_ids=observations,
                index_values=index_values,
                extra_labels=extra_values,
            )

            theoretical_values.append(theoretical)
            residual_values.append(residuals_sorted)
            plotted = True

        if not plotted:
            raise ValueError("Not enough residuals to compute the Q-Q plot.")

        theoretical_concat = np.concatenate(theoretical_values)
        residual_concat = np.concatenate(residual_values)
        update_x = not self._axis_user_override["x"]
        update_y = not self._axis_user_override["y"]
        limits, _ = self._apply_axis_defaults(
            theoretical_concat,
            residual_concat,
            identity=True,
            update_x=update_x,
            update_y=update_y,
        )
        if self.identity_var.get() and limits is not None:
            start, end = limits
            ax.plot(
                [start, end],
                [start, end],
                color="black",
                linestyle="--",
                linewidth=1,
                label="Identity",
            )

        if self.legend_var.get():
            self._show_legend()

    def _williams_plot(self, df: pd.DataFrame, column: str):
        df = self._prepare_plot_dataframe(df, (column,))
        columns = ["Leverage", column, "Set"]
        if "Observation" in df.columns:
            columns.insert(0, "Observation")
        if self._extra_label_column and self._extra_label_column in df.columns:
            insert_pos = 1 if "Observation" in columns else 0
            columns.insert(insert_pos, self._extra_label_column)
        values = df[columns].copy()
        values = values[np.isfinite(values["Leverage"].astype(float))]
        values = values[np.isfinite(values[column].astype(float))]
        if values.empty:
            raise ValueError("Insufficient data to draw the Williams plot.")

        ax = self.ax
        self._enforce_axis_ratio(ax)
        self._apply_gridlines(ax)
        ylabel = "Std. residuals (LOO)" if column.endswith("_LOO") else "Std. residuals"
        self._apply_axis_labels("Leverage", ylabel)

        sizes = float(self.point_size_var.get())

        x_all: list[np.ndarray] = []
        y_all: list[np.ndarray] = []

        for dataset in values["Set"].unique():
            subset = values[values["Set"] == dataset]
            x = subset["Leverage"].astype(float).to_numpy()
            y = subset[column].astype(float).to_numpy()
            observations = subset["Observation"].to_numpy() if "Observation" in subset.columns else None
            extra_labels = (
                subset[self._extra_label_column].to_numpy()
                if self._extra_label_column and self._extra_label_column in subset.columns
                else None
            )
            color_override = self._resolve_marker_color(dataset)
            marker_style = self._resolve_marker_style(dataset)
            color = color_override or self.COLOR_MAP.get(dataset, "#333333")
            collection = ax.scatter(
                x,
                y,
                label=dataset,
                s=sizes,
                alpha=0.8,
                color=color,
                marker=marker_style,
            )
            index_values = subset.index.to_numpy()
            self._register_scatter_points(
                collection,
                x,
                y,
                dataset_label=dataset,
                observation_ids=observations,
                index_values=index_values,
                extra_labels=extra_labels,
            )
            x_all.append(x)
            y_all.append(y)

        if np.isfinite(self.hat_threshold):
            hat_label = f"h* ({self._format_hat_threshold(self.hat_threshold)})"
            ax.axvline(
                self.hat_threshold,
                color=self._resolve_h_line_color(),
                linestyle="--",
                linewidth=1,
                label=hat_label,
            )
        ax.axhline(0.0, color="black", linestyle=":", linewidth=1, label="Zero line")
        ax.axhline(3.0, color="gray", linestyle="--", linewidth=1)
        ax.axhline(-3.0, color="gray", linestyle="--", linewidth=1)

        if x_all and y_all:
            x_concat = np.concatenate(x_all)
            y_concat = np.concatenate(y_all)
            update_x = not self._axis_user_override["x"]
            update_y = not self._axis_user_override["y"]

            y_for_limits = y_concat
            if update_y and y_concat.size:
                y_min = float(np.min(y_concat))
                y_max = float(np.max(y_concat))
                extra_limits: list[float] = []
                if y_min >= -3.0:
                    extra_limits.append(-4.0)
                if y_max <= 3.0:
                    extra_limits.append(4.0)
                if extra_limits:
                    y_for_limits = np.concatenate(
                        [y_for_limits, np.asarray(extra_limits, dtype=float)]
                    )
                y_for_limits = self._with_zero_symmetric_limits(y_for_limits)

            self._apply_axis_defaults(
                x_concat,
                y_for_limits,
                update_x=update_x,
                update_y=update_y,
            )

        if self.legend_var.get():
            self._show_legend()

    def _cooks_distance_plot(self, df: pd.DataFrame, column: str, ylabel: str):
        df = self._prepare_plot_dataframe(df, ("Leverage", column))
        columns = ["Leverage", column, "Set"]
        if "Observation" in df.columns:
            columns.insert(0, "Observation")
        if self._extra_label_column and self._extra_label_column in df.columns:
            insert_pos = 1 if "Observation" in columns else 0
            columns.insert(insert_pos, self._extra_label_column)
        values = df[columns].copy()
        values = values[np.isfinite(values["Leverage"].astype(float))]
        values = values[np.isfinite(values[column].astype(float))]
        if values.empty:
            raise ValueError("Cook's distance is unavailable for the selected dataset.")

        ax = self.ax
        self._enforce_axis_ratio(ax)
        self._apply_gridlines(ax)
        self._apply_axis_labels("Leverage", ylabel)

        sizes = float(self.point_size_var.get())
        x_all: list[np.ndarray] = []
        y_all: list[np.ndarray] = []

        for dataset in values["Set"].unique():
            subset = values[values["Set"] == dataset]
            x = subset["Leverage"].astype(float).to_numpy()
            y = subset[column].astype(float).to_numpy()
            observations = subset["Observation"].to_numpy() if "Observation" in subset.columns else None
            extra_labels = (
                subset[self._extra_label_column].to_numpy()
                if self._extra_label_column and self._extra_label_column in subset.columns
                else None
            )
            color_override = self._resolve_marker_color(dataset)
            marker_style = self._resolve_marker_style(dataset)
            color = color_override or self.COLOR_MAP.get(dataset, "#333333")
            collection = ax.scatter(
                x,
                y,
                label=dataset,
                s=sizes,
                alpha=0.8,
                color=color,
                marker=marker_style,
            )
            index_values = subset.index.to_numpy()
            self._register_scatter_points(
                collection,
                x,
                y,
                dataset_label=dataset,
                observation_ids=observations,
                index_values=index_values,
                extra_labels=extra_labels,
            )
            x_all.append(x)
            y_all.append(y)

        cooks_reference = float("nan")
        if len(values) > 0:
            with np.errstate(divide="ignore", invalid="ignore"):
                cooks_reference = float(4.0 / len(values))
            if not np.isfinite(cooks_reference) or cooks_reference <= 0:
                cooks_reference = float("nan")

        line_color = self._resolve_h_line_color()

        if np.isfinite(self.hat_threshold):
            hat_label = f"h* ({self._format_hat_threshold(self.hat_threshold)})"
            ax.axvline(
                self.hat_threshold,
                color=line_color,
                linestyle="--",
                linewidth=1,
                label=hat_label,
            )

        if np.isfinite(cooks_reference):
            if abs(cooks_reference) < 1e-3:
                formatted = f"{cooks_reference:.2e}"
            else:
                formatted = f"{cooks_reference:.3f}"
            ax.axhline(
                cooks_reference,
                color=line_color,
                linestyle=":",
                linewidth=1,
                label=f"4/n ({formatted})",
            )

        if x_all and y_all:
            x_concat = np.concatenate(x_all)
            y_concat = np.concatenate(y_all)

            if not self._axis_user_override["x"]:
                x_extras: list[float] = []
                if np.isfinite(self.hat_threshold):
                    x_extras.append(float(self.hat_threshold))
                if x_extras:
                    x_limits_values = np.concatenate([x_concat, np.asarray(x_extras, dtype=float)])
                else:
                    x_limits_values = x_concat
            else:
                x_limits_values = x_concat

            if not self._axis_user_override["y"]:
                y_extras: list[float] = [0.0]
                if np.isfinite(cooks_reference):
                    y_extras.append(float(cooks_reference))
                if y_extras:
                    y_limits_values = np.concatenate([y_concat, np.asarray(y_extras, dtype=float)])
                else:
                    y_limits_values = y_concat
            else:
                y_limits_values = y_concat

            self._apply_axis_defaults(
                x_limits_values,
                y_limits_values,
                update_x=not self._axis_user_override["x"],
                update_y=not self._axis_user_override["y"],
            )

        if self.legend_var.get():
            self._show_legend()

    def _plot_cooks_distance(self, df: pd.DataFrame):
        self._cooks_distance_plot(df, "CooksDistance", "Cook's distance")

    def _plot_cooks_distance_loo(self, df: pd.DataFrame):
        self._cooks_distance_plot(df, "CooksDistance_LOO", "Cook's distance by LOO")

    def _prompt_axis_labels(self):
        if not self.available:
            return
        default_x = self.custom_xlabel if self.custom_xlabel else self._last_default_xlabel
        default_y = self.custom_ylabel if self.custom_ylabel else self._last_default_ylabel
        dialog = _AxisLabelDialog(
            self,
            title="Axis labels",
            initial_x=default_x,
            initial_y=default_y,
            initial_font_size=self.axis_label_fontsize,
        )
        if dialog.result is None:
            return
        x_label, y_label, font_size = dialog.result
        self.custom_xlabel = x_label or None
        self.custom_ylabel = y_label or None
        self.axis_label_fontsize = font_size
        self._update_plot()

    def _save_current_plot(self):
        if not self.available:
            messagebox.showinfo("Save plot", "Select a model before saving the plot.")
            return
        path = filedialog.asksaveasfilename(
            title="Save plot",
            defaultextension=".png",
            filetypes=(
                ("PNG", "*.png"),
                ("TIFF", "*.tiff"),
                ("PDF", "*.pdf"),
                ("SVG", "*.svg"),
                ("All files", "*.*"),
            ),
        )
        if not path:
            return
        try:
            current_dpi = float(self.figure.dpi)
            target_dpi = current_dpi if current_dpi >= 1000 else 1000
            self.figure.savefig(path, bbox_inches="tight", dpi=target_dpi)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("Save plot", f"Could not save the plot: {exc}")
        else:
            messagebox.showinfo("Save plot", f"Plot saved to {path}")


def main(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for both the GUI and CLI modes."""

    args = list(sys.argv[1:] if argv is None else argv)
    if args == ["--version"]:
        print(f"MLR-X version {VERSION}")
        return

    if not args:
        _start_background_imports()
        app = MLRXApp()
        app.mainloop()
        return

    if len(args) > 1:
        print("Usage: MLRX.py [config.conf]")
        sys.exit(1)

    config_path = args[0]
    try:
        run_cli(config_path)
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        if message.startswith("Configuration not allowed:"):
            print(message, file=sys.stderr)
        else:
            print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
