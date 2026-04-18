"""
bgp_lease_analysis.py
─────────────────────
Analyzes BGP lease duration distributions, intermediary churn,
and transition frequency — no external API calls required.

Usage:
    python bgp_lease_analysis.py
"""

import pandas as pd
import numpy as np

BGP_FILE = "lease_start_events.csv"


# ── Load & collapse ────────────────────────────────────────────────────────────

def load_and_collapse(path: str = BGP_FILE) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads raw events and returns two DataFrames:
      - transitions : one row per (prefix, tenant) hold period with duration
      - prefixes    : one row per prefix with aggregate churn stats
    """
    df = pd.read_csv(path)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["prefix", "timestamp"], kind="mergesort").reset_index(drop=True)
    print(f"Loaded {len(df):,} raw events across {df['prefix'].nunique():,} prefixes.\n")

    transition_rows = []
    prefix_rows     = []

    for prefix, group in df.groupby("prefix"):
        group             = group.reset_index(drop=True)
        original_landlord = group.iloc[0]["landlord_as"]

        # Collapse consecutive duplicate tenants
        collapsed = []
        for _, row in group.iterrows():
            if not collapsed or collapsed[-1]["tenant_as"] != row["tenant_as"]:
                collapsed.append(row.to_dict())

        if len(collapsed) < 2:
            continue

        # Compute duration for each hold period
        holds = []
        for i in range(len(collapsed) - 1):
            duration_sec = (
                pd.to_datetime(collapsed[i + 1]["timestamp"]) -
                pd.to_datetime(collapsed[i]["timestamp"])
            ).total_seconds()

            tenant = collapsed[i]["tenant_as"]
            is_landlord = (tenant == original_landlord)

            holds.append({
                "prefix":            prefix,
                "original_landlord": original_landlord,
                "tenant_as":         tenant,
                "is_landlord_hold":  is_landlord,
                "hold_start":        pd.to_datetime(collapsed[i]["timestamp"]),
                "hold_end":          pd.to_datetime(collapsed[i + 1]["timestamp"]),
                "duration_sec":      duration_sec,
                "duration_min":      round(duration_sec / 60, 4),
                "duration_hrs":      round(duration_sec / 3600, 6),
            })
            transition_rows.append(holds[-1])

        # Per-prefix summary
        tenant_holds    = [h for h in holds if not h["is_landlord_hold"]]
        all_durations   = [h["duration_sec"] for h in holds]
        tenant_durations= [h["duration_sec"] for h in tenant_holds]

        tenants_seen  = set()
        pingpong      = 0
        for h in holds:
            if h["tenant_as"] in tenants_seen:
                pingpong += 1
            tenants_seen.add(h["tenant_as"])

        prefix_rows.append({
            "prefix":                  prefix,
            "original_landlord":       original_landlord,
            "final_tenant":            collapsed[-1]["tenant_as"],
            "num_transitions":         len(holds),
            "num_unique_tenants":      len({h["tenant_as"] for h in tenant_holds}),
            "pingpong_count":          pingpong,
            "churn_ratio":             round(pingpong / len(holds), 4) if holds else 0,
            # all hold durations (including landlord reclaims)
            "min_hold_sec":            min(all_durations) if all_durations else None,
            "max_hold_sec":            max(all_durations) if all_durations else None,
            "avg_hold_sec":            round(np.mean(all_durations), 2) if all_durations else None,
            "median_hold_sec":         round(np.median(all_durations), 2) if all_durations else None,
            # tenant-only hold durations
            "tenant_min_hold_sec":     min(tenant_durations) if tenant_durations else None,
            "tenant_avg_hold_sec":     round(np.mean(tenant_durations), 2) if tenant_durations else None,
            "tenant_median_hold_sec":  round(np.median(tenant_durations), 2) if tenant_durations else None,
            "returned_to_landlord":    1 if collapsed[-1]["tenant_as"] == original_landlord else 0,
        })

    transitions = pd.DataFrame(transition_rows)
    prefixes    = pd.DataFrame(prefix_rows)
    print(f"Collapsed into {len(transitions):,} hold periods across "
          f"{len(prefixes):,} prefixes.\n")
    return transitions, prefixes


# ── 1. Lease duration distribution ────────────────────────────────────────────

def lease_duration_distribution(transitions: pd.DataFrame) -> pd.DataFrame:
    """
    Distribution of tenant hold durations (excludes landlord reclaim periods).
    Shows percentiles and bucket breakdown across time scales.
    """
    print("─" * 60)
    print("1. Lease Duration Distribution (tenant holds only)")
    print("─" * 60)

    tenant = transitions[~transitions["is_landlord_hold"]]["duration_sec"]

    # Percentile table
    pcts = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    pct_df = pd.DataFrame({
        "percentile": [f"p{p}" for p in pcts],
        "seconds":    [round(np.percentile(tenant, p), 1) for p in pcts],
        "minutes":    [round(np.percentile(tenant, p) / 60, 2) for p in pcts],
        "hours":      [round(np.percentile(tenant, p) / 3600, 4) for p in pcts],
    })
    print("\nPercentile breakdown:")
    print(pct_df.to_string(index=False))

    # Bucket counts
    buckets = [
        ("< 1 min",      0,       60),
        ("1–5 min",      60,      300),
        ("5–30 min",     300,     1800),
        ("30 min–2 hr",  1800,    7200),
        ("2–24 hr",      7200,    86400),
        ("> 24 hr",      86400,   float("inf")),
    ]
    bucket_rows = []
    for label, lo, hi in buckets:
        count = ((tenant >= lo) & (tenant < hi)).sum()
        bucket_rows.append({
            "bucket":      label,
            "count":       count,
            "pct_of_total": round(count / len(tenant) * 100, 1),
        })
    bucket_df = pd.DataFrame(bucket_rows)
    print("\nBucket breakdown:")
    print(bucket_df.to_string(index=False))

    bucket_df.to_csv("report_lease_duration_buckets.csv", index=False)
    pct_df.to_csv("report_lease_duration_percentiles.csv", index=False)
    print("\n  Saved → report_lease_duration_buckets.csv")
    print("  Saved → report_lease_duration_percentiles.csv")
    return bucket_df


# ── 2. Churn frequency per prefix ─────────────────────────────────────────────

def churn_frequency_report(prefixes: pd.DataFrame) -> pd.DataFrame:
    """
    How often do prefixes churn? Distribution of transition counts
    and churn ratio (fraction of transitions that are ping-pong repeats).
    """
    print("\n" + "─" * 60)
    print("2. Churn Frequency per Prefix")
    print("─" * 60)

    # Transition count distribution
    trans_desc = prefixes["num_transitions"].describe(
        percentiles=[.25, .5, .75, .9, .95, .99]
    ).round(2)
    print("\nTransitions per prefix:")
    print(trans_desc.to_string())

    # Churn ratio distribution
    churn_desc = prefixes["churn_ratio"].describe(
        percentiles=[.25, .5, .75, .9, .95, .99]
    ).round(4)
    print("\nChurn ratio per prefix (pingpongs / total transitions):")
    print(churn_desc.to_string())

    # Bucket by transition count
    t_buckets = [(1, 1), (2, 2), (3, 5), (6, 10), (11, 50), (51, 999999)]
    t_labels  = ["1", "2", "3–5", "6–10", "11–50", "51+"]
    rows = []
    for label, (lo, hi) in zip(t_labels, t_buckets):
        mask  = (prefixes["num_transitions"] >= lo) & (prefixes["num_transitions"] <= hi)
        sub   = prefixes[mask]
        rows.append({
            "transitions":      label,
            "num_prefixes":     len(sub),
            "pct_prefixes":     round(len(sub) / len(prefixes) * 100, 1),
            "avg_churn_ratio":  round(sub["churn_ratio"].mean(), 4) if len(sub) else 0,
            "pct_pingpong":     round((sub["pingpong_count"] > 0).mean() * 100, 1) if len(sub) else 0,
        })
    freq_df = pd.DataFrame(rows)
    print("\nPrefix count by number of transitions:")
    print(freq_df.to_string(index=False))

    freq_df.to_csv("report_churn_frequency.csv", index=False)
    print("\n  Saved → report_churn_frequency.csv")
    return freq_df


# ── 3. Intermediary hold time (ping-pong cadence) ─────────────────────────────

def intermediary_hold_report(transitions: pd.DataFrame, prefixes: pd.DataFrame) -> pd.DataFrame:
    """
    For prefixes with ping-pong churn, how long does each intermediary
    hold the prefix before it flips again? This captures the cadence
    of rapid reuse — key signal for suspicious BGP behaviour.
    """
    print("\n" + "─" * 60)
    print("3. Intermediary Hold Time (ping-pong prefixes only)")
    print("─" * 60)

    # Only prefixes that ping-pong at least once
    pp_prefixes = prefixes[prefixes["pingpong_count"] > 0]["prefix"]
    pp_trans    = transitions[transitions["prefix"].isin(pp_prefixes) &
                              ~transitions["is_landlord_hold"]]

    print(f"\n  {len(pp_prefixes):,} ping-pong prefixes → "
          f"{len(pp_trans):,} intermediary hold periods")

    if pp_trans.empty:
        print("  No ping-pong transitions found.")
        return pd.DataFrame()

    pcts = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    pct_df = pd.DataFrame({
        "percentile": [f"p{p}" for p in pcts],
        "seconds":    [round(np.percentile(pp_trans["duration_sec"], p), 1) for p in pcts],
        "minutes":    [round(np.percentile(pp_trans["duration_sec"], p) / 60, 2) for p in pcts],
    })
    print("\nIntermediary hold duration percentiles:")
    print(pct_df.to_string(index=False))

    # Most churned prefixes
    top_churn = (
        prefixes[prefixes["pingpong_count"] > 0]
        .sort_values("pingpong_count", ascending=False)
        .head(20)[["prefix", "original_landlord", "num_transitions",
                   "pingpong_count", "churn_ratio",
                   "tenant_min_hold_sec", "tenant_avg_hold_sec"]]
        .reset_index(drop=True)
    )
    print("\nTop 20 most churned prefixes:")
    print(top_churn.to_string(index=False))

    top_churn.to_csv("report_intermediary_holds.csv", index=False)
    pct_df.to_csv("report_intermediary_hold_percentiles.csv", index=False)
    print("\n  Saved → report_intermediary_holds.csv")
    print("  Saved → report_intermediary_hold_percentiles.csv")
    return top_churn


# ── 4. Tenant behaviour profile ────────────────────────────────────────────────

def tenant_behaviour_report(transitions: pd.DataFrame, prefixes: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates behaviour per tenant AS — how many prefixes they hold,
    typical hold duration, and how often they appear in churn chains.
    """
    print("\n" + "─" * 60)
    print("4. Tenant Behaviour Profile")
    print("─" * 60)

    tenant_trans = transitions[~transitions["is_landlord_hold"]]

    report = (
        tenant_trans.groupby("tenant_as")
        .agg(
            total_holds       = ("prefix",       "count"),
            unique_prefixes   = ("prefix",       "nunique"),
            avg_hold_sec      = ("duration_sec", "mean"),
            median_hold_sec   = ("duration_sec", "median"),
            min_hold_sec      = ("duration_sec", "min"),
            max_hold_sec      = ("duration_sec", "max"),
        )
        .round(2)
        .reset_index()
        .sort_values("total_holds", ascending=False)
    )

    # Flag tenants who appear in ping-pong chains
    pp_tenants = set(
        transitions[transitions["prefix"].isin(
            prefixes[prefixes["pingpong_count"] > 0]["prefix"]
        ) & ~transitions["is_landlord_hold"]]["tenant_as"]
    )
    report["in_pingpong_chain"] = report["tenant_as"].isin(pp_tenants)

    # Add hold duration bucket
    report["typical_hold"] = pd.cut(
        report["median_hold_sec"],
        bins  = [0, 60, 300, 1800, 7200, 86400, float("inf")],
        labels= ["<1min", "1-5min", "5-30min", "30min-2hr", "2-24hr", ">24hr"],
    )

    print(f"\nTop 30 tenants by total holds:")
    print(report.head(30).to_string(index=False))

    report.to_csv("report_tenant_behaviour.csv", index=False)
    print("\n  Saved → report_tenant_behaviour.csv")
    return report


# ── 5. Lease time by landlord ─────────────────────────────────────────────────

def landlord_lease_profile(prefixes: pd.DataFrame) -> pd.DataFrame:
    """
    Per landlord AS: how many prefixes they lease out, typical
    churn levels, and whether prefixes tend to return to them.
    """
    print("\n" + "─" * 60)
    print("5. Landlord Lease Profile")
    print("─" * 60)

    report = (
        prefixes.groupby("original_landlord")
        .agg(
            prefixes_leased    = ("prefix",               "count"),
            unique_tenants     = ("num_unique_tenants",   "sum"),
            avg_transitions    = ("num_transitions",      "mean"),
            avg_churn_ratio    = ("churn_ratio",          "mean"),
            pct_returned       = ("returned_to_landlord", "mean"),
            avg_min_hold_sec   = ("tenant_min_hold_sec",  "mean"),
            avg_median_hold_sec= ("tenant_median_hold_sec","mean"),
        )
        .round(4)
        .reset_index()
        .sort_values("prefixes_leased", ascending=False)
    )

    report["pct_returned"] = (report["pct_returned"] * 100).round(1)

    print(f"\nTop 30 landlords by prefixes leased:")
    print(report.head(30).to_string(index=False))

    report.to_csv("report_landlord_profile.csv", index=False)
    print("\n  Saved → report_landlord_profile.csv")
    return report


# ── Run all ────────────────────────────────────────────────────────────────────

def run_all(path: str = BGP_FILE):
    transitions, prefixes = load_and_collapse(path)

    lease_duration_distribution(transitions)
    churn_frequency_report(prefixes)
    intermediary_hold_report(transitions, prefixes)
    tenant_behaviour_report(transitions, prefixes)
    landlord_lease_profile(prefixes)

    print("\n" + "─" * 60)
    print("  All reports complete.")
    print("─" * 60)

    return transitions, prefixes


if __name__ == "__main__":
    run_all()