import os
import re
import json
import io
import sys
import hashlib
import signal
import warnings
import threading
import time
from datetime import datetime
from typing import Dict, Optional, Tuple, List

import pandas as pd
import streamlit as st
import openpyxl
from openpyxl.utils.cell import range_boundaries

# Suppress openpyxl conditional formatting warning (read-only for our purposes)
warnings.filterwarnings(
    "ignore",
    message="Conditional Formatting extension is not supported and will be removed",
    category=UserWarning,
    module="openpyxl",
)

# -----------------------------
# Config
# -----------------------------
APP_VERSION = "v3.0.0"
JOBNO_RE = re.compile(r"^\d{4}-\d{3}$")
MIN_JOB_YEAR = 2024  # show 2024+ only

DATA_DIR = os.path.join(os.path.dirname(__file__), ".data")
os.makedirs(DATA_DIR, exist_ok=True)

# Job Summary column names you confirmed exist in tblSummary
JS_COL_GROSS_PROFIT = "Gross Profit-JTD"  # AK
JS_COL_GP_PCT = "GP%-JTD"                 # AL
JS_COL_CASH_FLOW = "Cash Flow"            # AQ
JS_COL_TOTAL_CASH_FLOW = "Total"          # AR

# -----------------------------
# Helpers
# -----------------------------
def _file_sha256(file_bytes: bytes) -> str:
    h = hashlib.sha256()
    h.update(file_bytes)
    return h.hexdigest()


def _table_to_df(wb: "openpyxl.Workbook", sheet: str, table_name: str) -> pd.DataFrame:
    ws = wb[sheet]
    if table_name not in ws.tables:
        raise KeyError(
            f"Table '{table_name}' not found on sheet '{sheet}'. Found tables: {list(ws.tables.keys())}"
        )
    tbl = ws.tables[table_name]
    min_col, min_row, max_col, max_row = range_boundaries(tbl.ref)

    rows = []
    for row in ws.iter_rows(
        min_row=min_row,
        max_row=max_row,
        min_col=min_col,
        max_col=max_col,
        values_only=True,
    ):
        rows.append(list(row))

    header = rows[0]
    data = rows[1:]
    return pd.DataFrame(data, columns=header)


def _clean_jobno(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.where(s.str.match(JOBNO_RE), other=pd.NA)
    return s


def _job_year(job_no: str) -> Optional[int]:
    try:
        return int(str(job_no)[:4])
    except Exception:
        return None


def _to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.date


def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _money_str(x) -> str:
    try:
        if pd.isna(x) or x is None:
            return ""
        return f"${float(x):,.2f}"
    except Exception:
        return ""


def _to_float(x) -> float:
    """Best-effort conversion to float for values that may be strings/NaN."""
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return 0.0
        return float(x)
    except Exception:
        try:
            return float(str(x).replace(",", "").replace("$", "").strip())
        except Exception:
            return 0.0


def _fmt_date_mmddyyyy(d) -> str:
    if pd.isna(d) or d is None:
        return ""
    try:
        return pd.to_datetime(d).strftime("%m-%d-%Y")
    except Exception:
        return str(d)


def _df_dates_to_str(df: pd.DataFrame, cols) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].apply(_fmt_date_mmddyyyy)
    return out


def _load_previous_job_summary_snapshot() -> Optional[pd.DataFrame]:
    snap_path = os.path.join(DATA_DIR, "job_summary_last_upload.pkl")
    if not os.path.exists(snap_path):
        return None
    try:
        return pd.read_pickle(snap_path)
    except Exception:
        return None


def _load_previous_job_summary_meta() -> Optional[dict]:
    meta_path = os.path.join(DATA_DIR, "job_summary_last_upload_meta.json")
    if not os.path.exists(meta_path):
        return None
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _save_job_summary_snapshot(job_summary_df: pd.DataFrame, file_hash: str, file_name: str) -> None:
    meta = {
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "file_hash": file_hash,
        "file_name": file_name,
        "rows": int(len(job_summary_df)),
    }
    snap_path = os.path.join(DATA_DIR, "job_summary_last_upload.pkl")
    meta_path = os.path.join(DATA_DIR, "job_summary_last_upload_meta.json")
    job_summary_df.to_pickle(snap_path)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)


def _normalize_pct(x: float) -> float:
    """If value looks like 25 (percent), convert to 0.25. If already 0.25, leave."""
    try:
        if x is None or pd.isna(x):
            return 0.0
        x = float(x)
        if abs(x) > 1.5:
            return x / 100.0
        return x
    except Exception:
        return 0.0


def _pct_str(p: Optional[float]) -> str:
    if p is None or pd.isna(p):
        return ""
    try:
        return f"{float(p) * 100:,.1f}%"
    except Exception:
        return ""


def _round0(v) -> Optional[float]:
    if v is None or pd.isna(v):
        return None
    try:
        return float(round(float(v), 0))
    except Exception:
        return None


# -----------------------------
# Load / Normalize tables
# -----------------------------
@st.cache_data(show_spinner=False)
def load_raw_tables(file_bytes: bytes) -> Dict[str, pd.DataFrame]:
    wb = openpyxl.load_workbook(filename=io.BytesIO(file_bytes), data_only=True)
    raw = {}
    raw["job_summary"] = _table_to_df(wb, "Job Summary", "tblSummary")
    raw["job"] = _table_to_df(wb, "1-Job List", "tblCustJob")
    raw["cust"] = _table_to_df(wb, "2-Customer Invoices & Pymts", "tblCustTran")
    raw["po"] = _table_to_df(wb, "3-Vendor Purchase Orders", "tblPO")
    raw["ap"] = _table_to_df(wb, "4-Vendor Bills", "tblAPBills")
    raw["dov"] = _table_to_df(wb, "5-Dovico Time & Expense", "tblDovico")
    # optional archive
    try:
        raw["dovA"] = _table_to_df(wb, "6-Dovico Archive 2-13-25", "tblDovicoArchive")
    except Exception:
        raw["dovA"] = pd.DataFrame()
    return raw


@st.cache_data(show_spinner=False)
def load_tables(file_bytes: bytes) -> Dict[str, pd.DataFrame]:
    raw = load_raw_tables(file_bytes)
    return normalize_tables(raw)


def normalize_tables(raw: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}

    # Job Summary
    js = raw["job_summary"].copy()
    if "Job No" not in js.columns:
        raise KeyError("Job Summary table must include 'Job No' column.")
    js["Job No"] = _clean_jobno(js["Job No"])
    js = js[js["Job No"].notna()].copy()

    numeric_candidates = [
        "Estimate","Invoice","Paid","Open AR","Reimb","Sales Tax",
        "Vendor PO","Vendor Bills","Payments","Open AP",
        "Unbilled","Unbilled PO",
        JS_COL_GROSS_PROFIT, JS_COL_GP_PCT, JS_COL_CASH_FLOW, JS_COL_TOTAL_CASH_FLOW
    ]
    for c in numeric_candidates:
        if c in js.columns:
            js[c] = _to_num(js[c]).fillna(0.0)

    out["job_summary"] = js

    # Job List
    job = raw["job"].copy()
    job["JobNo"] = _clean_jobno(job["JobNo"])
    job = job[job["JobNo"].notna()].copy()
    job["Job Type"] = job["Job Type"].astype(str).str.strip()
    out["job"] = job

    # Customer transactions
    cust = raw["cust"].copy()
    cust["Job No"] = _clean_jobno(cust["Job No"])
    cust = cust[cust["Job No"].notna()].copy()
    cust["Type"] = cust["Type"].astype(str).str.strip()
    cust["Date"] = _to_date(cust["Date"])
    cust["Amount"] = _to_num(cust["Amount"]).fillna(0.0)
    out["cust"] = cust

    # Purchase Orders
    po = raw["po"].copy()
    po["Job No"] = _clean_jobno(po["Job No"])
    po = po[po["Job No"].notna()].copy()
    po["PO Date"] = _to_date(po["PO Date"])
    po["PO Amount"] = _to_num(po["PO Amount"]).fillna(0.0)
    if "Open Bal" in po.columns:
        po["Open Bal"] = _to_num(po["Open Bal"]).fillna(0.0)
    po["PO Num"] = po["PO Num"].astype(str).str.strip()
    po["Vendor Source Name"] = po["Vendor Source Name"].astype(str).str.strip()
    out["po"] = po

    # Vendor Bills / AP
    ap = raw["ap"].copy()
    ap["Job No"] = _clean_jobno(ap["Job No"])
    ap = ap[ap["Job No"].notna()].copy()
    ap["Type"] = ap["Type"].astype(str).str.strip()
    ap["Bill Date"] = _to_date(ap["Bill Date"])
    ap["Inv Amount"] = _to_num(ap["Inv Amount"]).fillna(0.0)
    ap["Open Balance"] = _to_num(ap["Open Balance"]).fillna(0.0)
    ap["Invoice#"] = ap["Invoice#"].astype(str).str.strip()
    ap["Vendor Source Name"] = ap["Vendor Source Name"].astype(str).str.strip()

    # CC charges/credits are settled immediately (override open balance to 0)
    cc_mask = ap["Type"].astype(str).str.strip().str.lower().isin(
        ["credit card charge", "credit card credit"]
    )
    ap.loc[cc_mask, "Open Balance"] = 0.0
    ap["Type Short"] = ap["Type"].replace({"Credit Card Charge": "CC", "Credit Card Credit": "CC"})
    out["ap"] = ap

    # Dovico
    dov = raw["dov"].copy()
    dov["Job Number"] = _clean_jobno(dov["Job Number"])
    dov = dov[dov["Job Number"].notna()].copy()
    for c in ["Cost-B", "Cost-A", "Expenses-B", "Expenses-A", "Labor", "Expense"]:
        if c in dov.columns:
            dov[c] = _to_num(dov[c]).fillna(0.0)
    out["dov"] = dov

    return out


def get_capital_jobs(job_df: pd.DataFrame) -> pd.Series:
    cap = (
        job_df.loc[job_df["Job Type"].str.lower().eq("capital project"), "JobNo"]
        .dropna()
        .unique()
    )
    cap = [j for j in cap if (_job_year(j) is not None and _job_year(j) >= MIN_JOB_YEAR)]
    return pd.Series(sorted(set(cap)))


# -----------------------------
# Domain logic
# -----------------------------
def job_summary_metrics_only(job_no: str, js: pd.DataFrame) -> Dict[str, float]:
    r = js[js["Job No"] == job_no].head(1)

    js_est = js_inv = js_paid = js_open_ar = js_vpo = js_vbills = js_vpay = js_open_ap = js_reimb = js_tax = 0.0
    js_unbilled_cust = js_unbilled_vendor = 0.0

    js_gp = 0.0
    js_gp_pct = 0.0
    js_cf = 0.0
    js_total_cf = 0.0

    if not r.empty:
        js_est = _to_float(r.get("Estimate", pd.Series([0.0])).iloc[0])
        js_inv = _to_float(r.get("Invoice", pd.Series([0.0])).iloc[0])
        js_paid = _to_float(r.get("Paid", pd.Series([0.0])).iloc[0])
        js_open_ar = _to_float(r.get("Open AR", pd.Series([max(js_inv - js_paid, 0.0)])).iloc[0])
        js_vpo = _to_float(r.get("Vendor PO", pd.Series([0.0])).iloc[0])
        js_vbills = _to_float(r.get("Vendor Bills", pd.Series([0.0])).iloc[0])
        js_vpay = _to_float(r.get("Payments", pd.Series([0.0])).iloc[0])
        js_open_ap = _to_float(r.get("Open AP", pd.Series([0.0])).iloc[0])
        js_reimb = _to_float(r.get("Reimb", pd.Series([0.0])).iloc[0])
        js_tax = _to_float(r.get("Sales Tax", pd.Series([0.0])).iloc[0])

        # Customer unbilled column is exactly "Unbilled"
        if "Unbilled" in r.columns:
            js_unbilled_cust = _to_float(r.get("Unbilled", pd.Series([0.0])).iloc[0])
        else:
            js_unbilled_cust = js_est - js_inv - js_reimb - js_tax

        # Vendor unbilled column is titled "Unbilled PO"
        if "Unbilled PO" in r.columns:
            js_unbilled_vendor = _to_float(r.get("Unbilled PO", pd.Series([0.0])).iloc[0])
        else:
            js_unbilled_vendor = max(js_vpo - js_vbills, 0.0)

        # Profitability + Cashflow columns in Job Summary
        if JS_COL_GROSS_PROFIT in r.columns:
            js_gp = _to_float(r.get(JS_COL_GROSS_PROFIT, pd.Series([0.0])).iloc[0])
        if JS_COL_GP_PCT in r.columns:
            js_gp_pct = _normalize_pct(_to_float(r.get(JS_COL_GP_PCT, pd.Series([0.0])).iloc[0]))
        if JS_COL_CASH_FLOW in r.columns:
            js_cf = _to_float(r.get(JS_COL_CASH_FLOW, pd.Series([0.0])).iloc[0])
        if JS_COL_TOTAL_CASH_FLOW in r.columns:
            js_total_cf = _to_float(r.get(JS_COL_TOTAL_CASH_FLOW, pd.Series([0.0])).iloc[0])

    # PM-facing: no negatives in summary for these
    js_open_ar = max(js_open_ar, 0.0)
    js_open_ap = max(js_open_ap, 0.0)
    js_unbilled_cust = max(js_unbilled_cust, 0.0)
    js_unbilled_vendor = max(js_unbilled_vendor, 0.0)

    return {
        "Total Estimate / PO": js_est,
        "Total Invoices Sent": js_inv,
        "Total Payments Received": js_paid,
        "Open AR": js_open_ar,
        "Unbilled PO": js_unbilled_cust,
        "Reimbursable Expenses": js_reimb,
        "Sales Tax": js_tax,
        "Total Vendor POs": js_vpo,
        "Total Vendor Bills Received": js_vbills,
        "Total Payments Made to Vendors": js_vpay,
        "Open AP": js_open_ap,
        "Unbilled Vendor POs": js_unbilled_vendor,

        # extra (Job Summary reference values)
        "Gross Profit-JTD": js_gp,
        "GP%-JTD": js_gp_pct,
        "Cash Flow": js_cf,
        "Total Cash Flow": js_total_cf,
    }

def _identify_holdback_checks(ap_df: pd.DataFrame) -> pd.DataFrame:
    """
    Detects likely vendor holdbacks / chargebacks entered as Check transactions.
    Rule:
      - Type == "Check"
      - Inv Amount > 0
      - Open Balance == Inv Amount (unpaid)
    """
    ap = ap_df.copy()

    ap["Inv Amount"] = pd.to_numeric(ap["Inv Amount"], errors="coerce").fillna(0.0)
    ap["Open Balance"] = pd.to_numeric(ap["Open Balance"], errors="coerce").fillna(0.0)

    is_check = ap["Type"].astype(str).str.strip().str.lower().eq("check")

    mask = (
        is_check
        & (ap["Inv Amount"] > 0)
        & ((ap["Inv Amount"] - ap["Open Balance"]).abs() < 0.01)
    )

    return ap[mask].copy()


def vendor_rollup(job_no: str, po_df: pd.DataFrame, ap_df: pd.DataFrame) -> pd.DataFrame:
    """
    Canonical vendor rollup (PM-facing):
    - Payments: derived from Vendor Bills tab as (Inv Amount - Open Balance).
      Credits (negative Inv Amount) reduce Payment_Total (correct).
    - Open AP: sum of Open Balance (credits may reduce this).
    - Open PO (commitment remaining): max(PO Issued - Bills_Received_PositiveOnly, 0).
      IMPORTANT: Vendor credits should NOT increase Open PO, so they are excluded from
      the "bills received" amount used for Open PO calculation.
    - Special-case: "holdback" style Check rows (Inv Amount == Open Balance) can be excluded
      from rollup totals via _identify_holdback_checks().
    """
    po_j = po_df[po_df["Job No"] == job_no].copy()
    ap_j = ap_df[ap_df["Job No"] == job_no].copy()

    # Identify holdback-style check entries
    holdbacks = _identify_holdback_checks(ap_j)

    # Exclude them from financial rollup calculations
    if holdbacks is not None and not holdbacks.empty:
        ap_j = ap_j.drop(holdbacks.index)

    # --- PO issued total: only positive PO Amount lines
    po_j["PO Amount"] = pd.to_numeric(po_j["PO Amount"], errors="coerce").fillna(0.0)
    issued = po_j[po_j["PO Amount"] > 0].copy()

    po_grp = (
        issued.groupby(["Vendor Source Name"], dropna=False)
        .agg(PO_Issued_Total=("PO Amount", "sum"))
        .reset_index()
    )

    # --- Bills / AP
    ap_j["Inv Amount"] = pd.to_numeric(ap_j["Inv Amount"], errors="coerce").fillna(0.0)
    ap_j["Open Balance"] = pd.to_numeric(ap_j["Open Balance"], errors="coerce").fillna(0.0)

    # Paid Amount includes credits (credits reduce totals) — keep this behavior
    ap_j["Paid Amount"] = ap_j["Inv Amount"] - ap_j["Open Balance"]

    # Bills_Received_PositiveOnly is used ONLY for Open PO (commitment remaining)
    # Exclude credits (Type == "Credit") and any negative Inv Amount rows.
    type_is_credit = ap_j["Type"].astype(str).str.strip().str.lower().eq("credit")
    bills_for_commitment = ap_j[~type_is_credit & (ap_j["Inv Amount"] > 0)].copy()

    bills_commit_grp = (
        bills_for_commitment.groupby(["Vendor Source Name"], dropna=False)
        .agg(Bill_Total_For_OpenPO=("Inv Amount", "sum"))
        .reset_index()
    )

    # Normal totals (these can include credits)
    bills_grp = (
        ap_j.groupby(["Vendor Source Name"], dropna=False)
        .agg(
            Bill_Count=("Inv Amount", "size"),
            Bill_Total=("Inv Amount", "sum"),      # includes credits
            Bill_Open=("Open Balance", "sum"),     # credits may reduce
            Payment_Total=("Paid Amount", "sum"),  # includes credits
        )
        .reset_index()
    )

    out = po_grp.merge(bills_grp, on="Vendor Source Name", how="outer").fillna(0.0)
    out = out.merge(bills_commit_grp, on="Vendor Source Name", how="left").fillna(
        {"Bill_Total_For_OpenPO": 0.0}
    )

    # Open PO should NOT increase due to credits
    out["Open PO"] = (out["PO_Issued_Total"] - out["Bill_Total_For_OpenPO"]).clip(lower=0.0)

    out = out.sort_values(["Open PO", "Bill_Open"], ascending=[False, False]).reset_index(drop=True)
    return out

def _dovico_actuals(job_no: str, dov_df: pd.DataFrame) -> Tuple[float, float]:
    """Return (labor_actual, expenses_actual) from Dovico tab for a job."""
    j = dov_df[dov_df["Job Number"] == job_no].copy()
    if j.empty:
        return 0.0, 0.0
    labor_actual = float(pd.to_numeric(j.get("Cost-A", 0.0), errors="coerce").fillna(0.0).sum())
    expenses_actual = float(pd.to_numeric(j.get("Expenses-A", 0.0), errors="coerce").fillna(0.0).sum())
    return labor_actual, expenses_actual


def project_summary_metrics(
    job_no: str,
    cust_df: pd.DataFrame,
    po_df: pd.DataFrame,
    ap_df: pd.DataFrame,
    js: pd.DataFrame,
    dov_df: pd.DataFrame,
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float]]:
    """
    Returns:
      - tool_metrics (project summary metrics computed by tool)
      - js_metrics   (job summary reference values for discrepancy)
      - extra_tool_metrics (profitability + cashflows computed by tool)
    """
    js_metrics = job_summary_metrics_only(job_no, js)

    jcust = cust_df[cust_df["Job No"] == job_no].copy()
    jcust["Type"] = jcust["Type"].astype(str).str.strip()
    jcust["Amount"] = pd.to_numeric(jcust["Amount"], errors="coerce").fillna(0.0)

    est = float(jcust[jcust["Type"].eq("Estimate")]["Amount"].sum())
    inv = float(jcust[jcust["Type"].isin(["Invoice", "Credit Memo"])]["Amount"].sum())
    paid = float(jcust[jcust["Type"].eq("Payment")]["Amount"].sum())
    open_ar = max(inv - paid, 0.0)

    roll = vendor_rollup(job_no, po_df, ap_df)
    total_vpo = float(roll["PO_Issued_Total"].sum()) if not roll.empty else 0.0
    total_vbills = float(roll["Bill_Total"].sum()) if not roll.empty else 0.0
    total_vpay = float(roll["Payment_Total"].sum()) if not roll.empty else 0.0
    open_ap = max(float(roll["Bill_Open"].sum()) if not roll.empty else 0.0, 0.0)

    # Per requirement: Unbilled Vendor POs in Project Summary must match Vendor Rollup "Open PO" sum
    unbilled_vendor = float(roll["Open PO"].sum()) if not roll.empty else 0.0

    reimb = js_metrics.get("Reimbursable Expenses", 0.0)
    tax = js_metrics.get("Sales Tax", 0.0)

    # Customer Unbilled PO should reflect: (Estimate + reimb + tax) - invoices, but never negative
    # because reimb/tax often are billable on top of PO.
    unbilled_cust = max((est + reimb + tax) - inv, 0.0)

    tool_metrics = {
        "Total Estimate / PO": est,
        "Total Invoices Sent": inv,
        "Total Payments Received": paid,
        "Open AR": open_ar,
        "Unbilled PO": unbilled_cust,
        "Reimbursable Expenses": reimb,
        "Sales Tax": tax,
        "Total Vendor POs": total_vpo,
        "Total Vendor Bills Received": total_vbills,
        "Total Payments Made to Vendors": total_vpay,
        "Open AP": open_ap,
        "Unbilled Vendor POs": unbilled_vendor,
    }

    # ---- Extra metrics (neither customer nor vendor) ----
    labor_actual, expenses_actual = _dovico_actuals(job_no, dov_df)

    gp_dollars = inv - total_vbills - labor_actual - expenses_actual

    denom = inv - reimb - tax
    gp_pct = None
    if denom != 0:
        gp_pct = gp_dollars / denom  # requirement: blank if 0 or negative

    current_cash_flow = open_ar - open_ap
    total_cash_flow = (open_ar + unbilled_cust) - (open_ap + unbilled_vendor)

    extra_tool_metrics = {
        "Gross Profit-JTD": gp_dollars,
        "GP%-JTD": gp_pct if gp_pct is not None else pd.NA,
        "Cash Flow": current_cash_flow,
        "Total Cash Flow": total_cash_flow,
    }

    return tool_metrics, js_metrics, extra_tool_metrics


def build_discrepancies(
    tool_metrics: Dict[str, float],
    js_metrics: Dict[str, float],
    extra_tool_metrics: Optional[Dict[str, float]] = None,
    threshold: float = 1.0,
) -> pd.DataFrame:
    """
    Discrepancy rules:
      - $ metrics: ignore abs(delta) < $1
      - % metrics: ignore abs(delta) < 1.0 percentage point
    """

    # Merge extra metrics (profitability/cash flow/etc.) into the tool metrics
    merged_tool = dict(tool_metrics or {})
    if extra_tool_metrics:
        merged_tool.update(extra_tool_metrics)

    # Add any % metrics you want treated with the +/-1% rule
    PCT_METRICS = {"GP%-JTD"}

    def _pct_points(v: float) -> float:
        """
        Convert v to percentage points.
        - If stored as fraction (0.207), convert to 20.7
        - If stored as percent points already (20.7), leave as-is
        """
        try:
            v = float(v)
        except Exception:
            return 0.0
        return v * 100.0 if abs(v) <= 2.0 else v

    rows: List[Dict[str, float]] = []
    for k, tool_val in merged_tool.items():
        if k not in js_metrics:
            continue

        js_val = js_metrics.get(k, 0.0)
        delta = tool_val - js_val

        if k in PCT_METRICS:
            tool_pp = _pct_points(tool_val)
            js_pp = _pct_points(js_val)
            delta_pp = tool_pp - js_pp

            if abs(delta_pp) >= 1.0:
                rows.append({"Metric": k, "Tool": tool_val, "Job Summary": js_val, "Delta": delta})
        else:
            if abs(delta) >= threshold:
                rows.append({"Metric": k, "Tool": tool_val, "Job Summary": js_val, "Delta": delta})

    return pd.DataFrame(rows)

def build_project_summary_table(
    tool_metrics: Dict[str, float],
    prev_metrics: Optional[Dict[str, float]],
) -> pd.DataFrame:
    side_map = {
        "Total Estimate / PO": "Customer",
        "Total Invoices Sent": "Customer",
        "Total Payments Received": "Customer",
        "Open AR": "Customer",
        "Unbilled PO": "Customer",
        "Reimbursable Expenses": "Customer",
        "Sales Tax": "Customer",
        "Total Vendor POs": "Vendor",
        "Total Vendor Bills Received": "Vendor",
        "Total Payments Made to Vendors": "Vendor",
        "Open AP": "Vendor",
        "Unbilled Vendor POs": "Vendor",
    }

    rows = []
    for k in tool_metrics.keys():
        cur_raw = tool_metrics.get(k, 0.0)
        prev_raw = None if prev_metrics is None else prev_metrics.get(k, None)

        # No negatives in Current/Previous summary columns (PM-facing) for these metrics
        if k in ["Open AR", "Open AP", "Unbilled PO", "Unbilled Vendor POs"]:
            cur_raw = max(cur_raw, 0.0)
            if prev_raw is not None:
                prev_raw = max(float(prev_raw), 0.0)

        cur = _round0(cur_raw)
        prev = _round0(prev_raw)
        delta = None if prev is None else _round0(cur - prev)

        rows.append(
            {
                "Side": side_map.get(k, ""),
                "Metric": k,
                "Current": cur,
                "Previous Upload": prev,
                "Change since last upload": delta,
            }
        )

    df = pd.DataFrame(rows)
    d = pd.to_numeric(df["Change since last upload"], errors="coerce")
    df.loc[d.abs() < 1.0, "Change since last upload"] = pd.NA
    return df


def build_extra_metrics_table(
    extra_tool_metrics: Dict[str, float],
    prev_js_metrics: Optional[Dict[str, float]],
) -> pd.DataFrame:
    """
    Builds a small table for:
      - Gross Profit-JTD (dollars)
      - GP%-JTD (percent)
      - Cash Flow
      - Total Cash Flow
    Using same rounding rules as Project Summary.
    """
    def get_prev(k):
        if prev_js_metrics is None:
            return None
        return prev_js_metrics.get(k, None)

    rows = []

    # Gross Profit-JTD
    cur_gp = extra_tool_metrics.get("Gross Profit-JTD", 0.0)
    prev_gp = get_prev("Gross Profit-JTD")
    rows.append({
        "Metric": "Job To Date Profitability",
        "Current": _round0(cur_gp),
        "Previous Upload": _round0(prev_gp),
        "Change since last upload": None if prev_gp is None else _round0(_round0(cur_gp) - _round0(prev_gp)),
        "Format": "money",
        "Tooltip": ""
    })

    # GP%-JTD (blank if 0 or negative per your rule is already enforced in calc)
    cur_pct = extra_tool_metrics.get("GP%-JTD", pd.NA)
    prev_pct = get_prev("GP%-JTD")
    # normalize prev percent
    prev_pct = _normalize_pct(prev_pct) if prev_pct is not None else None

    cur_pct_r = None if pd.isna(cur_pct) else float(cur_pct)
    prev_pct_r = None if prev_pct is None else float(prev_pct)

    rows.append({
        "Metric": "Job To Date Profitability %",
        "Current": cur_pct_r,
        "Previous Upload": prev_pct_r,
        "Change since last upload": None if prev_pct_r is None or cur_pct_r is None else (cur_pct_r - prev_pct_r),
        "Format": "pct",
        "Tooltip": ""
    })

    # Cash Flow (with tooltip)
    cur_cf = extra_tool_metrics.get("Cash Flow", 0.0)
    prev_cf = get_prev("Cash Flow")
    rows.append({
        "Metric": "Current Cash Flow",
        "Current": _round0(cur_cf),
        "Previous Upload": _round0(prev_cf),
        "Change since last upload": None if prev_cf is None else _round0(_round0(cur_cf) - _round0(prev_cf)),
        "Format": "money",
        "Tooltip": "Current Cash Flow = Open AR − Open AP"
    })

    # Total Cash Flow (with tooltip)
    cur_tcf = extra_tool_metrics.get("Total Cash Flow", 0.0)
    prev_tcf = get_prev("Total Cash Flow")
    rows.append({
        "Metric": "Total Cash Flow",
        "Current": _round0(cur_tcf),
        "Previous Upload": _round0(prev_tcf),
        "Change since last upload": None if prev_tcf is None else _round0(_round0(cur_tcf) - _round0(prev_tcf)),
        "Format": "money",
        "Tooltip": "Total Cash Flow = (Open AR + Unbilled Customer PO) − (Open AP + Unbilled Vendor PO)"
    })

    return pd.DataFrame(rows)


def vendor_bills_detail(job_no: str, ap_df: pd.DataFrame, vendor: Optional[str] = None) -> pd.DataFrame:
    j = ap_df[ap_df["Job No"] == job_no].copy()
    if vendor is not None:
        j = j[j["Vendor Source Name"].astype(str) == str(vendor)].copy()

    j["Paid Amount"] = j["Inv Amount"] - j["Open Balance"]
    cols = ["Bill Date", "Vendor Source Name", "Invoice#", "Memo", "Type Short", "Inv Amount", "Open Balance", "Paid Amount", "Tran#"]
    keep = [c for c in cols if c in j.columns]
    out = j[keep].rename(columns={"Type Short": "Type"})
    out = out.sort_values(["Bill Date", "Invoice#"], ascending=[False, True])
    return out


def vendor_pos_detail(job_no: str, po_df: pd.DataFrame, vendor: Optional[str] = None) -> pd.DataFrame:
    j = po_df[po_df["Job No"] == job_no].copy()
    if vendor is not None:
        j = j[j["Vendor Source Name"].astype(str) == str(vendor)].copy()

    j["PO Amount"] = pd.to_numeric(j["PO Amount"], errors="coerce").fillna(0.0)
    issued = j[j["PO Amount"] > 0].copy()
    if issued.empty:
        return issued

    grp = (
        issued.groupby(["PO Num", "Vendor Source Name"], dropna=False)
        .agg(**{"PO Date": ("PO Date", "min"), "PO Issued": ("PO Amount", "sum")})
        .reset_index()
    )
    grp = grp[["PO Date", "Vendor Source Name", "PO Num", "PO Issued"]]
    grp = grp.sort_values(["PO Date", "PO Num"], ascending=[False, True])
    return grp


def vendor_payments_detail_from_bills(job_no: str, ap_df: pd.DataFrame, vendor: Optional[str] = None) -> pd.DataFrame:
    j = ap_df[ap_df["Job No"] == job_no].copy()
    if vendor is not None:
        j = j[j["Vendor Source Name"].astype(str) == str(vendor)].copy()

    j["Paid Amount"] = j["Inv Amount"] - j["Open Balance"]
    cc_mask = j["Type"].astype(str).str.strip().str.lower().isin(["credit card charge", "credit card credit"])
    j = j[~cc_mask].copy()
    j = j[j["Paid Amount"].abs() > 0.005].copy()

    out = j[["Bill Date", "Vendor Source Name", "Invoice#", "Memo", "Paid Amount"]].copy()
    out = out.sort_values(["Bill Date", "Invoice#"], ascending=[False, True])
    return out


def query_ar_detail(job_no: str, cust_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    j = cust_df[cust_df["Job No"] == job_no].copy()
    invoices = j[j["Type"].isin(["Invoice", "Credit Memo"])].copy()
    payments = j[j["Type"].isin(["Payment"])].copy()
    estimates = j[j["Type"].isin(["Estimate"])].copy()

    inv_tbl = invoices[["Date", "Num", "Tran#", "Type", "Memo", "Amount"]].sort_values(["Date", "Num"], ascending=[False, True])
    pay_tbl = payments[["Date", "Num", "Tran#", "Type", "Memo", "Amount"]].sort_values(["Date", "Num"], ascending=[False, True])
    est_tbl = estimates[["Date", "Num", "Tran#", "Type", "Memo", "Amount"]].sort_values(["Date", "Num"], ascending=[False, True])
    return inv_tbl, pay_tbl, est_tbl


def query_dovico(job_no: str, dov_df: pd.DataFrame) -> pd.DataFrame:
    j = dov_df[dov_df["Job Number"] == job_no].copy()
    if j.empty:
        return j

    keep = [c for c in ["Dovico Task", "Cost-B", "Cost-A", "Expenses-B", "Expenses-A", "Labor", "Expense"] if c in j.columns]
    if "Dovico Task" not in keep:
        out = j[keep].sum(numeric_only=True).to_frame().T
        out.insert(0, "Dovico Task", "Total Project")
        return out

    out = (
        j[keep]
        .groupby(["Dovico Task"], dropna=False)
        .sum(numeric_only=True)
        .reset_index()
    )

    out = out.rename(
        columns={
            "Cost-B": "Labor Budget",
            "Cost-A": "Labor Actual",
            "Expenses-B": "Expenses Budget",
            "Expenses-A": "Expenses Actual",
            "Labor": "Unsubmitted/Unapproved Labor",
            "Expense": "Unsubmitted/Unapproved Expenses",
        }
    )
    out["Total Budget"] = out["Labor Budget"] + out["Expenses Budget"]
    out["Total Actual"] = out["Labor Actual"] + out["Expenses Actual"]

    total = out.drop(columns=["Dovico Task"]).sum(numeric_only=True).to_dict()
    total["Dovico Task"] = "Total Project"
    out = pd.concat([out, pd.DataFrame([total])], ignore_index=True)
    return out


def _style_total_project_row(df: pd.DataFrame) -> "pd.io.formats.style.Styler":
    if df is None or df.empty or "Dovico Task" not in df.columns:
        return df.style
    def row_style(row):
        if str(row.get("Dovico Task", "")) == "Total Project":
            return ["font-weight: 700; background-color: #f2f2f2;"] * len(row)
        return [""] * len(row)
    return df.style.apply(row_style, axis=1)


# -----------------------------
# Auto-shutdown (stop server when browser tab closes)
# -----------------------------
HEARTBEAT_FILE = os.path.join(DATA_DIR, "heartbeat.txt")
SHUTDOWN_TIMEOUT = 3600  # 1 hour of no heartbeat before shutdown


def _write_heartbeat():
    """Write current timestamp to heartbeat file."""
    try:
        with open(HEARTBEAT_FILE, "w") as f:
            f.write(str(time.time()))
    except Exception:
        pass


def _shutdown_watchdog():
    """Background thread: if no heartbeat for SHUTDOWN_TIMEOUT seconds, kill the server."""
    # Wait an initial period before starting to check (let the app fully load)
    time.sleep(60)
    while True:
        time.sleep(30)
        try:
            if os.path.exists(HEARTBEAT_FILE):
                with open(HEARTBEAT_FILE, "r") as f:
                    last_beat = float(f.read().strip())
                if time.time() - last_beat > SHUTDOWN_TIMEOUT:
                    try:
                        os.remove(HEARTBEAT_FILE)
                    except Exception:
                        pass
                    os._exit(0)
        except Exception:
            pass


# Start the watchdog thread (only once)
if "watchdog_started" not in st.session_state:
    st.session_state["watchdog_started"] = True
    t = threading.Thread(target=_shutdown_watchdog, daemon=True)
    t.start()

# Write heartbeat on every page load/rerun
_write_heartbeat()


# -----------------------------
# Admin / Workbook persistence
# -----------------------------
ADMIN_PASSWORD = "e2s-1221"
WORKBOOK_PATH = os.path.join(DATA_DIR, "current_workbook.xlsx")
PREV_SNAP_PATH = os.path.join(DATA_DIR, "previous_calculated_metrics.pkl")
PREV_META_PATH = os.path.join(DATA_DIR, "previous_upload_meta.json")
CURRENT_METRICS_PATH = os.path.join(DATA_DIR, "current_calculated_metrics.pkl")
CURRENT_META_PATH = os.path.join(DATA_DIR, "job_summary_last_upload_meta.json")


def _compute_all_job_metrics(tables: dict) -> dict:
    """Compute tool-calculated metrics for every capital job. Returns {job_no: {metric: value}}."""
    job_df = tables["job"]
    cap_jobs = set(get_capital_jobs(job_df))
    all_metrics = {}

    for job_no in cap_jobs:
        try:
            tool_metrics, js_metrics, extra_tool_metrics = project_summary_metrics(
                job_no, tables["cust"], tables["po"], tables["ap"],
                tables["job_summary"], tables["dov"]
            )
            # Merge tool_metrics and extra_tool_metrics into one dict
            combined = dict(tool_metrics)
            combined.update(extra_tool_metrics)
            all_metrics[job_no] = combined
        except Exception:
            pass

    return all_metrics


def _save_current_workbook(file_bytes: bytes, file_name: str, tables: dict):
    """Admin: save the uploaded workbook and rotate previous snapshot."""
    # If there's already a current workbook, rotate current metrics to previous
    if os.path.exists(CURRENT_METRICS_PATH):
        try:
            import shutil
            shutil.copy2(CURRENT_METRICS_PATH, PREV_SNAP_PATH)
        except Exception:
            pass

    if os.path.exists(CURRENT_META_PATH):
        try:
            import shutil
            shutil.copy2(CURRENT_META_PATH, PREV_META_PATH)
        except Exception:
            pass

    # Save new workbook
    with open(WORKBOOK_PATH, "wb") as f:
        f.write(file_bytes)

    # Compute and save tool-calculated metrics for all jobs
    all_metrics = _compute_all_job_metrics(tables)
    pd.to_pickle(all_metrics, CURRENT_METRICS_PATH)

    # Save metadata
    file_hash = _file_sha256(file_bytes)
    meta = {
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "file_hash": file_hash,
        "file_name": file_name,
        "rows": int(len(tables["job_summary"])),
    }
    with open(CURRENT_META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)


def _load_current_workbook():
    """Load the saved workbook from disk. Returns (file_bytes, file_name) or (None, None)."""
    if not os.path.exists(WORKBOOK_PATH):
        return None, None
    meta = None
    if os.path.exists(CURRENT_META_PATH):
        try:
            with open(CURRENT_META_PATH, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            pass
    file_name = meta.get("file_name", "workbook.xlsx") if meta else "workbook.xlsx"
    with open(WORKBOOK_PATH, "rb") as f:
        return f.read(), file_name


def _load_prev_snapshot():
    """Load the previous upload's calculated metrics and meta."""
    prev_metrics = None
    prev_meta = None
    if os.path.exists(PREV_SNAP_PATH):
        try:
            prev_metrics = pd.read_pickle(PREV_SNAP_PATH)
        except Exception:
            pass
    if os.path.exists(PREV_META_PATH):
        try:
            with open(PREV_META_PATH, "r", encoding="utf-8") as f:
                prev_meta = json.load(f)
        except Exception:
            pass
    return prev_metrics, prev_meta


# -----------------------------
# UI
# -----------------------------
st.set_page_config(page_title="PM Snapshot", layout="wide")

# Hide Streamlit default UI elements but keep sidebar accessible
st.markdown("""
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    input[type="password"] {
        -webkit-text-security: disc;
    }
    </style>
    <script>
    document.addEventListener('DOMContentLoaded', function() {
        document.querySelectorAll('input[type="password"]').forEach(function(el) {
            el.setAttribute('autocomplete', 'new-password');
        });
    });
    </script>
""", unsafe_allow_html=True)

st.markdown(
    f"""
    <div style="margin-bottom:5px;">
        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 290 80" width="290" height="80">
            <text x="0" y="48" font-family="'Segoe UI', 'Helvetica Neue', sans-serif" font-size="48" font-weight="700" fill="#2C3E50" letter-spacing="-1">PM</text>
            <text x="82" y="48" font-family="'Segoe UI', 'Helvetica Neue', sans-serif" font-size="48" font-weight="300" fill="#3A9AD9" letter-spacing="-0.5">Snapshot</text>
            <text x="3" y="72" font-family="'Segoe UI', 'Helvetica Neue', sans-serif" font-size="12" font-weight="500" fill="#7F8C8D" textLength="260" lengthAdjust="spacing">PROJECT FINANCIALS</text>
        </svg>
        <div style="margin-top:8px; font-size:0.8em; color:#AAAAAA; letter-spacing:0.5px;">
            {APP_VERSION} &nbsp;&bull;&nbsp; March 5, 2026
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# Initialize session state
if "is_admin" not in st.session_state:
    st.session_state["is_admin"] = False

# ---- Sidebar ----
with st.sidebar:
    # Admin login section
    st.header("Admin")
    if st.session_state["is_admin"]:
        st.success("Logged in as Admin")
        if st.button("Logout"):
            st.session_state["is_admin"] = False
            st.rerun()

        st.divider()
        st.subheader("Upload Workbook")
        uploaded = st.file_uploader("Upload Excel workbook (.xlsx)", type=["xlsx"])

        if uploaded:
            upload_bytes = uploaded.getvalue()
            upload_hash = _file_sha256(upload_bytes)

            # Only process if it's a new file
            if st.session_state.get("last_upload_hash") != upload_hash:
                with st.spinner("Processing workbook..."):
                    try:
                        upload_tables = load_tables(upload_bytes)
                        _save_current_workbook(upload_bytes, uploaded.name, upload_tables)
                        st.session_state["last_upload_hash"] = upload_hash
                        st.session_state["active_hash"] = None  # force reload
                        st.success(f"Workbook saved: {uploaded.name}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error processing workbook: {e}")
    else:
        with st.form("admin_login", clear_on_submit=True):
            pw = st.text_input("Password", type="password", placeholder="Enter admin password", autocomplete="off")
            submitted = st.form_submit_button("Login")
            if submitted:
                if pw == ADMIN_PASSWORD:
                    st.session_state["is_admin"] = True
                    st.rerun()
                else:
                    st.error("Incorrect password.")

    st.divider()

    # Workbook info (always visible)
    st.subheader("Workbook Info")

# ---- Load workbook (from disk, not upload) ----
file_bytes, file_name = _load_current_workbook()

if file_bytes is None:
    with st.sidebar:
        st.write("**Current workbook:** (none loaded)")
        st.write("**Previous workbook:** (none)")
    st.info("No workbook has been loaded yet. An administrator needs to upload a workbook to get started.")
    st.stop()

file_hash = _file_sha256(file_bytes)

# Load previous snapshot
prev_calc_metrics, prev_meta_data = _load_prev_snapshot()

# Freeze previous only when workbook changes
if st.session_state.get("active_hash") != file_hash:
    st.session_state["prev_calc_frozen"] = prev_calc_metrics
    st.session_state["prev_meta_frozen"] = prev_meta_data
    st.session_state["active_hash"] = file_hash

prev_all_metrics = st.session_state.get("prev_calc_frozen")  # dict of {job_no: {metric: value}}
prev_meta = st.session_state.get("prev_meta_frozen") or {}
prev_file_name = prev_meta.get("file_name")

# Sidebar workbook info
with st.sidebar:
    st.write("**Current workbook:**")
    st.code(file_name, language=None)
    if prev_file_name:
        st.write("**Previous workbook:**")
        st.code(prev_file_name, language=None)
    else:
        st.write("**Previous workbook:** (none)")

tables = load_tables(file_bytes)

# Select project (capital + year >= 2024)
job_df = tables["job"]
capital_jobs = set(get_capital_jobs(job_df))
scoped = job_df[job_df["JobNo"].isin(capital_jobs)].copy()
scoped["Project Name"] = scoped["Project Name"].astype(str)
scoped["Label"] = scoped["JobNo"].astype(str) + " — " + scoped["Project Name"]

labels = scoped[["JobNo", "Label"]].drop_duplicates().sort_values("JobNo")
label_to_job = dict(zip(labels["Label"], labels["JobNo"]))

sel = st.selectbox("Select Project (Job Number — Project Name)", options=labels["Label"].tolist())
job_no = label_to_job.get(sel)

jr = scoped[scoped["JobNo"] == job_no].head(1)
if not jr.empty:
    cust_name = str(jr.get("Customer Name", pd.Series([""])).iloc[0])
    proj_name = str(jr.get("Project Name", pd.Series([""])).iloc[0])
    st.markdown(f"**{job_no} — {proj_name}**  \nCustomer: `{cust_name}`")

st.divider()

tool_metrics, js_metrics, extra_tool_metrics = project_summary_metrics(
    job_no, tables["cust"], tables["po"], tables["ap"], tables["job_summary"], tables["dov"]
)

prev_metrics = None
if prev_all_metrics is not None and job_no in prev_all_metrics:
    prev_metrics = prev_all_metrics[job_no]

ps = build_project_summary_table(tool_metrics, prev_metrics)

ps_disp = ps.copy()

# format money columns
for col in ["Current", "Previous Upload", "Change since last upload"]:
    if col in ps_disp.columns:
        ps_disp[col] = ps_disp[col].apply(_money_str)

# reorder columns for better readability
ps_disp = ps_disp[
    ["Side", "Metric", "Previous Upload", "Current", "Change since last upload"]
]

st.subheader("Project Summary")
st.dataframe(ps_disp, use_container_width=True, hide_index=True)

# ---- Extra metrics mini table (Profitability + Cash Flow) ----
extras = build_extra_metrics_table(extra_tool_metrics, prev_metrics)
st.markdown("### Profitability & Cash Flow")

# Display formatting (match Project Summary behavior)
extras_disp = extras.copy()

# Format Current/Previous/Change
for i, row in extras_disp.iterrows():
    fmt = row.get("Format", "money")
    for col in ["Current", "Previous Upload", "Change since last upload"]:
        val = extras_disp.at[i, col]
        if fmt == "pct":
            extras_disp.at[i, col] = _pct_str(val)
        else:
            extras_disp.at[i, col] = _money_str(val)

# Keep only display columns and match column order used elsewhere
extras_disp = extras_disp[["Metric", "Previous Upload", "Current", "Change since last upload"]]

st.dataframe(extras_disp, use_container_width=True, hide_index=True)

# Clean, non-intrusive definitions (keeps UI consistent + avoids HTML tables)
st.caption(
    "Cash Flow definitions: "
    "Current Cash Flow = Open AR − Open AP.  "
    "Total Cash Flow = (Open AR + Unbilled Customer PO) − (Open AP + Unbilled Vendor PO)."
)# ---- Discrepancies ----
st.subheader("Discrepancies")
disc = build_discrepancies(tool_metrics, js_metrics, extra_tool_metrics=extra_tool_metrics, threshold=1.0)

if disc.empty:
    st.info("No discrepancies greater than $1 compared to the Job Summary tab.")
else:
    disc_disp = disc.copy()

    def format_disc_row(row):
        if row["Metric"] == "GP%-JTD":
            return pd.Series({
                "Metric": row["Metric"],
                "Tool": _pct_str(row["Tool"]),
                "Job Summary": _pct_str(row["Job Summary"]),
                "Delta": _pct_str(row["Delta"]),
            })
        return pd.Series({
            "Metric": row["Metric"],
            "Tool": _money_str(row["Tool"]),
            "Job Summary": _money_str(row["Job Summary"]),
            "Delta": _money_str(row["Delta"]),
        })

    disc_disp = disc_disp.apply(format_disc_row, axis=1)
    st.dataframe(disc_disp, use_container_width=True, hide_index=True)

roll = vendor_rollup(job_no, tables["po"], tables["ap"])

st.divider()

tab_ar, tab_roll, tab_dov, tab_notes = st.tabs(
    [
        "Customer Invoices & Payments (AR)",
        "Vendor Rollup Drilldown",
        "Dovico Time & Expense",
        "Notes / Known Limitations",
    ]
)

with tab_ar:
    inv_tbl, pay_tbl, est_tbl = query_ar_detail(job_no, tables["cust"])
    for df in (inv_tbl, pay_tbl, est_tbl):
        if "Amount" in df.columns:
            df["Amount"] = df["Amount"].apply(_money_str)

    st.subheader("Estimates / POs")
    st.dataframe(_df_dates_to_str(est_tbl, ["Date"]), use_container_width=True, hide_index=True)

    st.subheader("Invoices / Credit Memos")
    st.dataframe(_df_dates_to_str(inv_tbl, ["Date"]), use_container_width=True, hide_index=True)

    st.subheader("Payments")
    st.dataframe(_df_dates_to_str(pay_tbl, ["Date"]), use_container_width=True, hide_index=True)

with tab_roll:
    st.subheader("Vendor Rollup")

    # Rename + reorder columns as requested:
    # Vendor Name, Total PO Issued, Qty Bills Received, Total Bills Received, Total Payments Made, Open AP, Unbilled PO
    roll_view = roll.copy()

    # Ensure order exists even if some cols missing (shouldn't, but safe)
    desired_cols = [
        ("Vendor Source Name", "Vendor Name"),
        ("PO_Issued_Total", "Total PO Issued"),
        ("Bill_Count", "Qty Bills Received"),
        ("Bill_Total", "Total Bills Received"),
        ("Payment_Total", "Total Payments Made"),
        ("Bill_Open", "Open AP"),
        ("Open PO", "Unbilled PO"),
    ]
    present = [c for c, _ in desired_cols if c in roll_view.columns]
    roll_view = roll_view[present].copy()

    rename_map = {c: new for c, new in desired_cols if c in roll_view.columns}
    roll_view = roll_view.rename(columns=rename_map)

    # Format money columns for display
    for c in ["Total PO Issued", "Total Bills Received", "Total Payments Made", "Open AP", "Unbilled PO"]:
        if c in roll_view.columns:
            roll_view[c] = roll_view[c].apply(_money_str)

    st.dataframe(roll_view, use_container_width=True, hide_index=True)

    st.caption(
        "Unbilled PO = remaining commitment (PO issued minus billed), clipped at 0. "
        "Total Payments Made is derived from Vendor Bills (Inv Amount - Open Balance). "
        "CC charges/credits are counted as paid but excluded from payment line items below."
    )

    st.divider()
    st.subheader("Vendor Drilldown")
    vend_opts = sorted(
        [v for v in roll["Vendor Source Name"].astype(str).unique().tolist() if v not in ["0", "nan", "None"]]
    )
    vend = st.selectbox("Select vendor", options=["(All Vendors)"] + vend_opts)
    vend_sel = None if vend == "(All Vendors)" else vend

    st.markdown("### Vendor POs (issued)")
    pos = vendor_pos_detail(job_no, tables["po"], vendor=vend_sel)
    if pos is None or pos.empty:
        st.caption("No issued vendor POs found for this selection.")
    else:
        pos_disp = pos.copy()
        if "PO Issued" in pos_disp.columns:
            pos_disp["PO Issued"] = pos_disp["PO Issued"].apply(_money_str)
        pos_disp = _df_dates_to_str(pos_disp, ["PO Date"])
        st.dataframe(pos_disp, use_container_width=True, hide_index=True)

    st.markdown("### Vendor Bills")
    bills = vendor_bills_detail(job_no, tables["ap"], vendor=vend_sel)
    bills_disp = bills.copy()
    for c in ["Inv Amount", "Open Balance", "Paid Amount"]:
        if c in bills_disp.columns:
            bills_disp[c] = bills_disp[c].apply(_money_str)
    bills_disp = _df_dates_to_str(bills_disp, ["Bill Date"])
    st.dataframe(bills_disp, use_container_width=True, hide_index=True)

# ---- Flag holdback-style entries (not counted in Open AP) ----
holdbacks = _identify_holdback_checks(tables["ap"])
holdbacks = holdbacks[holdbacks["Job No"] == job_no]

if not holdbacks.empty:
    st.warning("Possible vendor holdback / chargeback entries detected (excluded from Open AP):")

    hb = holdbacks[["Vendor Source Name", "Invoice#", "Memo", "Inv Amount"]].copy()
    hb["Inv Amount"] = hb["Inv Amount"].apply(_money_str)

    st.dataframe(hb, use_container_width=True, hide_index=True)

    st.markdown("### Vendor Payments (derived from bills; CC excluded)")
    payd = vendor_payments_detail_from_bills(job_no, tables["ap"], vendor=vend_sel)
    payd_disp = payd.copy()
    if "Paid Amount" in payd_disp.columns:
        payd_disp["Paid Amount"] = payd_disp["Paid Amount"].apply(_money_str)
    payd_disp = _df_dates_to_str(payd_disp, ["Bill Date"])
    st.dataframe(payd_disp, use_container_width=True, hide_index=True)

with tab_dov:
    st.subheader("Dovico Budget vs Actual (grouped by task)")
    dov_tbl = query_dovico(job_no, tables["dov"])
    if dov_tbl.empty:
        st.info("No Dovico rows found for this job.")
    else:
        dov_disp = dov_tbl.copy()
        for c in [
            "Labor Budget",
            "Labor Actual",
            "Expenses Budget",
            "Expenses Actual",
            "Total Budget",
            "Total Actual",
            "Unsubmitted/Unapproved Labor",
            "Unsubmitted/Unapproved Expenses",
        ]:
            if c in dov_disp.columns:
                dov_disp[c] = dov_disp[c].apply(_money_str)

        # Bold + light gray Total Project row (stable)
        st.dataframe(_style_total_project_row(dov_disp), use_container_width=True, hide_index=True)

with tab_notes:
    st.markdown(
        """
**Change since last upload:**
- “Previous Upload” comes from the prior workbook’s Job Summary snapshot stored in `.data/`.
- “Change since last upload” = rounded(Current) − rounded(Previous).
- Open AR / Open AP / Unbilled metrics are clipped at 0 for PM-facing display.

**Cash Flow definitions (hover tooltips on the metrics):**
- Current Cash Flow = Open AR − Open AP  
- Total Cash Flow = (Open AR + Unbilled Customer PO) − (Open AP + Unbilled Vendor PO)
        """
    )