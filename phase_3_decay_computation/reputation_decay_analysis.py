"""
decay_analysis.py
─────────────────
Analysis functions for final_decay_results.csv.
Each function returns a DataFrame report and optionally saves it to CSV.

Usage:
    from decay_analysis import run_all_reports
    run_all_reports("final_decay_results.csv")

Or run individual functions:
    from decay_analysis import tenant_abuse_report
    df = pd.read_csv("final_decay_results.csv")
    report = tenant_abuse_report(df)
"""

import pandas as pd
import numpy as np

OUTPUT_PREFIX = "report_"   # all saved CSVs are prefixed with this


# ── Helpers ────────────────────────────────────────────────────────────────────

def load(path: str = "final_decay_results.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    df["lease_start"]        = pd.to_datetime(df["lease_start"])
    df["first_abuse_report"] = pd.to_datetime(df["first_abuse_report"])
    return df


def _save(df: pd.DataFrame, name: str, save: bool):
    if save:
        fname = f"{OUTPUT_PREFIX}{name}.csv"
        df.to_csv(fname, index=False)
        print(f"  Saved → {fname}")
    return df


def _print_header(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ── 1. Decay time summary ──────────────────────────────────────────────────────

def decay_summary(df: pd.DataFrame, save: bool = True) -> pd.DataFrame:
    """
    Overall distribution of how quickly abuse appears after a lease starts.
    Includes percentile breakdown and threshold counts (24h / 72h / 1w).
    """
    _print_header("1. Decay Time Summary")

    stats = df["decay_hours"].describe(percentiles=[.10, .25, .50, .75, .90, .95])
    report = stats.rename("value").reset_index().rename(columns={"index": "metric"})

    # Add threshold rows
    thresholds = [(24, "within_24h_%"), (72, "within_72h_%"), (168, "within_1week_%")]
    rows = []
    for hrs, label in thresholds:
        pct = round((df["decay_hours"] <= hrs).mean() * 100, 1)
        rows.append({"metric": label, "value": pct})

    report = pd.concat([report, pd.DataFrame(rows)], ignore_index=True)
    report["value"] = report["value"].round(2)

    print(report.to_string(index=False))
    return _save(report, "decay_summary", save)


# ── 2. Tenant abuse ranking ────────────────────────────────────────────────────

def tenant_abuse_report(df: pd.DataFrame, top_n: int = 20,
                        save: bool = True) -> pd.DataFrame:
    """
    Ranks tenant ASes by how many prefixes showed abuse, average decay speed,
    and total dirty IPs. High count + fast decay = highest risk tenant.
    """
    _print_header("2. Tenant Abuse Ranking")

    report = (
        df.groupby("tenant_as")
        .agg(
            abusive_prefixes   = ("prefix",       "count"),
            avg_decay_hours    = ("decay_hours",   "mean"),
            median_decay_hours = ("decay_hours",   "median"),
            fastest_decay_hrs  = ("decay_hours",   "min"),
            avg_abuse_score    = ("abuse_score",   "mean"),
            total_dirty_ips    = ("ips_in_prefix", "sum"),
            avg_churn_ratio    = ("churn_ratio",   "mean"),
        )
        .round(2)
        .sort_values("abusive_prefixes", ascending=False)
        .head(top_n)
        .reset_index()
    )

    print(report.to_string(index=False))
    return _save(report, "tenant_abuse", save)


# ── 3. Landlord exposure report ────────────────────────────────────────────────

def landlord_exposure_report(df: pd.DataFrame, top_n: int = 20,
                              save: bool = True) -> pd.DataFrame:
    """
    Ranks original landlord ASes by how many of their prefixes were abused
    after being leased out, and how many distinct tenants were involved.
    Repeated exposure to different tenants suggests poor vetting.
    """
    _print_header("3. Landlord Exposure Report")

    report = (
        df.groupby("original_landlord")
        .agg(
            abused_prefixes  = ("prefix",      "count"),
            unique_tenants   = ("tenant_as",   "nunique"),
            avg_decay_hours  = ("decay_hours", "mean"),
            min_decay_hours  = ("decay_hours", "min"),
            avg_abuse_score  = ("abuse_score", "mean"),
        )
        .round(2)
        .sort_values("abused_prefixes", ascending=False)
        .head(top_n)
        .reset_index()
    )

    # Flag landlords with many tenants (potentially indiscriminate leasing)
    report["multi_tenant_flag"] = report["unique_tenants"] > 1

    print(report.to_string(index=False))
    return _save(report, "landlord_exposure", save)


# ── 4. Churn vs decay correlation ─────────────────────────────────────────────

def churn_decay_correlation(df: pd.DataFrame, save: bool = True) -> pd.DataFrame:
    """
    Computes correlation of BGP churn/instability features against decay_hours.
    Negative correlation = that feature predicts FASTER abuse appearance.
    Also splits into high/low churn buckets for a direct comparison.
    """
    _print_header("4. Churn vs Decay Correlation")

    features = [
        "churn_ratio", "num_transitions", "num_unique_ases",
        "min_duration_hrs", "avg_duration_hrs", "ips_in_prefix",
        "pingpong_count",
    ]
    available = [c for c in features if c in df.columns]
    corr = (
        df[available + ["decay_hours"]]
        .corr()["decay_hours"]
        .drop("decay_hours")
        .rename("pearson_r")
        .reset_index()
        .rename(columns={"index": "feature"})
        .sort_values("pearson_r")
    )
    corr["pearson_r"] = corr["pearson_r"].round(4)
    corr["direction"] = corr["pearson_r"].apply(
        lambda r: "faster abuse ↑" if r < -0.05 else
                  ("slower abuse ↑" if r > 0.05 else "no signal")
    )

    print(corr.to_string(index=False))

    # Bucket comparison
    median_churn = df["churn_ratio"].median()
    bucket = df.copy()
    bucket["churn_bucket"] = np.where(
        bucket["churn_ratio"] >= median_churn, "high_churn", "low_churn"
    )
    bucket_summary = (
        bucket.groupby("churn_bucket")["decay_hours"]
        .agg(count="count", mean="mean", median="median", pct_under_24h=lambda x: (x <= 24).mean() * 100)
        .round(2)
        .reset_index()
    )
    print(f"\n  High vs Low Churn bucket comparison (median churn={median_churn:.3f}):")
    print(bucket_summary.to_string(index=False))

    return _save(corr, "churn_decay_corr", save)


# ── 5. Repeat offender prefix report ──────────────────────────────────────────

def prefix_risk_report(df: pd.DataFrame, save: bool = True) -> pd.DataFrame:
    """
    Ranks individual prefixes by abuse score and decay speed.
    A fast decay + high score prefix is a reliable abuse pipeline.
    """
    _print_header("5. Prefix Risk Report")

    report = df[[
        "prefix", "tenant_as", "original_landlord",
        "decay_hours", "decay_days", "abuse_score",
        "ips_in_prefix", "total_reports",
        "churn_ratio", "num_transitions",
    ]].copy().sort_values(["abuse_score", "decay_hours"], ascending=[False, True])

    # Composite risk score: high abuse score + fast decay + many dirty IPs
    report["risk_score"] = (
        report["abuse_score"] * 0.5
        + (1 / (report["decay_hours"].clip(lower=1))) * 1000 * 0.3
        + report["ips_in_prefix"] * 0.2
    ).round(2)

    report = report.sort_values("risk_score", ascending=False).reset_index(drop=True)
    print(report.head(20).to_string(index=False))
    return _save(report, "prefix_risk", save)


# ── 6. Decay speed buckets ────────────────────────────────────────────────────

def decay_bucket_report(df: pd.DataFrame, save: bool = True) -> pd.DataFrame:
    """
    Groups leases into speed buckets and profiles each bucket's BGP behaviour.
    Helps answer: do the fastest-abused prefixes look different before abuse?
    """
    _print_header("6. Decay Speed Bucket Profile")

    bins   = [0, 6, 24, 72, 168, float("inf")]
    labels = ["<6h", "6–24h", "24–72h", "72h–1w", ">1w"]
    df = df.copy()
    df["decay_bucket"] = pd.cut(df["decay_hours"], bins=bins, labels=labels)

    report = (
        df.groupby("decay_bucket", observed=True)
        .agg(
            count            = ("prefix",          "count"),
            avg_churn        = ("churn_ratio",      "mean"),
            avg_transitions  = ("num_transitions",  "mean"),
            avg_unique_ases  = ("num_unique_ases",  "mean"),
            avg_abuse_score  = ("abuse_score",      "mean"),
            avg_dirty_ips    = ("ips_in_prefix",    "mean"),
        )
        .round(3)
        .reset_index()
    )

    report["pct_of_total"] = (report["count"] / report["count"].sum() * 100).round(1)
    print(report.to_string(index=False))
    return _save(report, "decay_buckets", save)


# ── Run all ────────────────────────────────────────────────────────────────────

def run_all_reports(path: str = "final_decay_results.csv", save: bool = True):
    """
    Loads results and runs all six reports in sequence.
    Set save=False to skip writing CSVs.
    """
    print(f"\nLoading {path}...")
    df = load(path)
    print(f"  {len(df):,} rows loaded.\n")

    decay_summary(df, save=save)
    tenant_abuse_report(df, save=save)
    landlord_exposure_report(df, save=save)
    churn_decay_correlation(df, save=save)
    prefix_risk_report(df, save=save)
    decay_bucket_report(df, save=save)

    print(f"\n{'─' * 60}")
    print(f"  All reports complete.")
    print(f"{'─' * 60}\n")


if __name__ == "__main__":
    run_all_reports()