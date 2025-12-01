#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compare performance across three directories of CSV results and plot 4 line graphs.
Now saves both PNG and PDF outputs.
Usage:
  python compare_dirs_geomean_plots_3dirs.py /path/to/dirA /path/to/dirB /path/to/dirC \
      --labelA "Baseline" --labelB "NewImpl" --labelC "Experimental" \
      --out geomean_comparison.png \
      --summary_csv geomean_summary.csv
"""
import argparse
import glob
import os
import re
from typing import Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
FILE_REGEX = re.compile(r".*outB_(?:ver|update)(\d{3})\.csv$")
def find_version_from_path(path: str) -> Optional[int]:
    m = FILE_REGEX.match(path)
    return int(m.group(1)) if m else None
def list_files_for_dir(d: str) -> Dict[int, str]:
    patt1 = os.path.join(d, "basic_AGGLOMERATIVE_OURS_DESCENDING_outB_ver*.csv")
    patt2 = os.path.join(d, "basic_AGGLOMERATIVE_OURS_DESCENDING_outB_update*.csv")
    candidates = glob.glob(patt1) + glob.glob(patt2)
    files = {}
    for p in candidates:
        v = find_version_from_path(p)
        if v is not None:
            # Keep original behavior: if duplicate, last seen or existing 'update' wins
            if v not in files or "update" in os.path.basename(files[v]).lower():
                files[v] = p
    return files
def safe_geomean(values: np.ndarray) -> float:
    vals = np.array(values, dtype=float)
    vals = vals[np.isfinite(vals)]
    vals = vals[vals > 0]
    if len(vals) == 0:
        return float("nan")
    return float(np.exp(np.mean(np.log(vals))))
def parse_csv_geomean_qet(path: str, q_ids: List[str]) -> float:
    try:
        df = pd.read_csv(path)
    except Exception:
        return float("nan")
    cols_lower = {c.lower().strip(): c for c in df.columns}
    q_col = cols_lower.get("querynumber", cols_lower.get("query", None))
    qet_col = cols_lower.get("queryexecutiontime", None)
    if q_col is None or qet_col is None:
        for c in df.columns:
            lc = c.strip().lower()
            if q_col is None and lc.startswith("query"):
                q_col = c
            if qet_col is None and lc.startswith("queryexecution"):
                qet_col = c
        if q_col is None or qet_col is None:
            return float("nan")
    df[q_col] = df[q_col].astype(str).str.strip()
    df = df[df[q_col].isin(q_ids)]
    if df.empty:
        return float("nan")
    qet = pd.to_numeric(df[qet_col], errors="coerce").to_numpy()
    return safe_geomean(qet)
def build_query_groups() -> List[Tuple[str, List[str]]]:
    g1 = ("Q1 only", [f"Q1"])
    g2 = ("Q2–Q30", [f"Q{i}" for i in range(2, 31)])
    g3 = ("Q31–Q70", [f"Q{i}" for i in range(31, 71)])
    g4 = ("Q1–Q70", [f"Q{i}" for i in range(1, 71)])
    # return [g1, g2, g3, g4]
    return [g4]
def compute_series_for_dir(d: str, versions: List[int], query_groups: List[Tuple[str, List[str]]]) -> Dict[str, List[float]]:
    files = list_files_for_dir(d)
    series = {name: [] for name, _ in query_groups}
    for v in versions:
        path = files.get(v)
        for name, qids in query_groups:
            if path is None or not os.path.exists(path):
                series[name].append(float("nan"))
            else:
                gm = parse_csv_geomean_qet(path, qids)
                series[name].append(gm)
    return series

def main():
    parser = argparse.ArgumentParser(description="Compare three directories of CSV results.")
    parser.add_argument("dirA", type=str, help="First directory")
    parser.add_argument("dirB", type=str, help="Second directory")
    parser.add_argument("dirC", type=str, help="Third directory")
    parser.add_argument("--labelA", type=str, default="Dir A", help="Legend label for dirA")
    parser.add_argument("--labelB", type=str, default="Dir B", help="Legend label for dirB")
    parser.add_argument("--labelC", type=str, default="Dir C", help="Legend label for dirC")
    parser.add_argument("--out", type=str, default="comparison_geomean.png", help="Output PNG filename")
    parser.add_argument("--summary_csv", type=str, default="comparison_geomean_summary.csv", help="Output CSV summary")
    parser.add_argument("--versions", type=str, default="1-100", help="Version range")
    args = parser.parse_args()

    # --- 폰트 크기 설정 ---
    FONT_LABEL = 19   # X축, Y축 라벨 크기
    FONT_TICK = 20    # 눈금(Ticks) 숫자 크기
    FONT_LEGEND = 17  # 범례(Legend) 폰트 크기
    # -------------------

    if "-" in args.versions:
        a, b = args.versions.split("-", 1)
        versions = list(range(int(a), int(b) + 1))
    else:
        versions = [int(x) for x in args.versions.split(",")]

    query_groups = build_query_groups()

    A = compute_series_for_dir(args.dirA, versions, query_groups)
    B = compute_series_for_dir(args.dirB, versions, query_groups)
    C = compute_series_for_dir(args.dirC, versions, query_groups)

    rows = []
    for i, v in enumerate(versions):
        row = {"version": v}
        for name, _ in query_groups:
            row[f"{args.labelA} - {name}"] = A[name][i]
            row[f"{args.labelB} - {name}"] = B[name][i]
            row[f"{args.labelC} - {name}"] = C[name][i]
        rows.append(row)
    summary_df = pd.DataFrame(rows)
    summary_df.to_csv(args.summary_csv, index=False)

    n = len(query_groups)
    fig, axes = plt.subplots(n, 1, figsize=(6, 4.2), sharex=True)
    if n == 1:
        axes = [axes]

    for ax, (name, _) in zip(axes, query_groups):
        ax.plot(versions, A[name], label=args.labelA, linewidth=2)
        ax.plot(versions, B[name], label=args.labelB, linewidth=2, linestyle="--")
        ax.plot(versions, C[name], label=args.labelC, linewidth=2, linestyle=":")
        
        # Y Label 폰트 크기 설정
        ax.set_ylabel("Geomean Query\nExecution Time (ms)", fontsize=FONT_LABEL)
        
        # Ticks (눈금) 폰트 크기 설정 (X, Y 모두 적용)
        ax.tick_params(axis='both', which='major', labelsize=FONT_TICK)
        
        ax.yaxis.set_major_formatter(StrMethodFormatter("{x:.2f}"))
        ax.grid(True, linestyle=":", alpha=0.6)
        
        # Legend 폰트 크기 설정
        ax.legend(loc="best", fontsize=FONT_LEGEND)

    # X Label 폰트 크기 설정
    axes[-1].set_xlabel("Version", fontsize=FONT_LABEL)
    axes[-1].tick_params(axis='x', pad=0)
    
    
    plt.tight_layout()
    plt.savefig(args.out, dpi=200)
    pdf_out = os.path.splitext(args.out)[0] + ".pdf"
    plt.savefig(pdf_out, dpi=800)
    
    print(f"Saved plot PNG: {args.out}")
    print(f"Saved plot PDF: {pdf_out}")
    print(f"Saved summary CSV: {args.summary_csv}")

if __name__ == "__main__":
    main()