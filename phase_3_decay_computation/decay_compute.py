# import requests
# import pandas as pd
# import time
# import ipaddress
# import json
# import os
# from datetime import datetime

# # ── Configuration ──────────────────────────────────────────────────────────────
# API_KEY           = "271f925c50406ddcb560f6a9f1ec5055beababebfbf7fd686f44dd06b199c0a5f7c8b139568a5d2c"
# BGP_FILE          = "lease_start_events.csv"
# OUTPUT_FILE       = "final_decay_results.csv"
# PROGRESS_FILE     = "progress.json"
# IP_CACHE_FILE     = "ip_cache.json"

# DAILY_IP_LIMIT    = 950

# # Threshold in hours — transitions shorter than this are treated as noise.
# # Based on sample data showing sub-minute flips, 1hr filters too aggressively.
# # 5 minutes (5/60) captures real rapid-reuse patterns without drowning in
# # BGP convergence noise. Tune this after reviewing threshold_scan output.
# MIN_STABLE_HOURS  = 0 / 60   # 0 minutes


# # ── Threshold scanner — run this first to pick MIN_STABLE_HOURS ───────────────
# def run_threshold_scan(input_file: str = BGP_FILE):
#     """
#     Prints a table showing how many prefixes and suspect ASNs survive
#     at each candidate threshold. Use this to pick MIN_STABLE_HOURS.
#     Does not call any API — pure BGP data analysis.
#     """
#     print(f"Loading {input_file} for threshold scan...")
#     df = pd.read_csv(input_file)
#     df["timestamp"] = pd.to_datetime(df["timestamp"])
#     df = df.sort_values(["prefix", "timestamp"], kind="mergesort").reset_index(drop=True)
#     print(f"  {len(df):,} raw events across {df['prefix'].nunique():,} prefixes.\n")

#     thresholds = [0, 1/60, 5/60, 15/60, 0.5, 1, 6, 12, 24]
#     labels     = ["0s", "1min", "5min", "15min", "30min", "1hr", "6hr", "12hr", "24hr"]

#     print(f"{'Threshold':<10} {'Surviving Prefixes':>20} {'Unique Suspect ASNs':>22}")
#     print("─" * 55)

#     for threshold, label in zip(thresholds, labels):
#         surviving            = 0
#         potential_bad_actors = set()

#         for prefix, group in df.groupby("prefix"):
#             group             = group.reset_index(drop=True)
#             original_landlord = group.iloc[0]["landlord_as"]

#             collapsed = []
#             for _, row in group.iterrows():
#                 if not collapsed or collapsed[-1]["tenant_as"] != row["tenant_as"]:
#                     collapsed.append(row.to_dict())

#             if len(collapsed) < 2:
#                 continue

#             qualified_transitions = 0
#             for i in range(len(collapsed) - 1):
#                 duration_hrs = (
#                     pd.to_datetime(collapsed[i + 1]["timestamp"]) -
#                     pd.to_datetime(collapsed[i]["timestamp"])
#                 ).total_seconds() / 3600

#                 tenant = collapsed[i]["tenant_as"]
#                 if duration_hrs >= threshold and tenant != original_landlord:
#                     potential_bad_actors.add(tenant)
#                     qualified_transitions += 1

#             if qualified_transitions > 0:
#                 surviving += 1

#         print(f"{label:<10} {surviving:>20,} {len(potential_bad_actors):>22,}")

#     print(f"\n  Current MIN_STABLE_HOURS = {MIN_STABLE_HOURS:.4f}hr  "
#           f"({MIN_STABLE_HOURS * 60:.1f} minutes)")


# # ── IP Cache ───────────────────────────────────────────────────────────────────
# def load_ip_cache() -> dict:
#     if os.path.exists(IP_CACHE_FILE):
#         with open(IP_CACHE_FILE) as f:
#             return json.load(f)
#     return {}

# def save_ip_cache(cache: dict):
#     with open(IP_CACHE_FILE, "w") as f:
#         json.dump(cache, f, indent=2, default=str)

# def cache_result(cache: dict, ip: str, result):
#     cache[ip] = result
#     save_ip_cache(cache)


# # ── Progress tracking ──────────────────────────────────────────────────────────
# def load_progress() -> dict:
#     if os.path.exists(PROGRESS_FILE):
#         with open(PROGRESS_FILE) as f:
#             return json.load(f)
#     return {
#         "checked_prefixes":  [],
#         "dirty_prefixes":    {},
#         "ip_calls_today":    0,
#         "last_reset_date":   str(datetime.utcnow().date()),
#     }

# def save_progress(progress: dict):
#     with open(PROGRESS_FILE, "w") as f:
#         json.dump(progress, f, indent=2, default=str)

# def reset_daily_counters_if_needed(progress: dict) -> dict:
#     today = str(datetime.utcnow().date())
#     if progress["last_reset_date"] != today:
#         print(f"  New day ({today}) — resetting daily API counters.")
#         progress["ip_calls_today"]  = 0
#         progress["last_reset_date"] = today
#     return progress


# # ── BGP stabilization ──────────────────────────────────────────────────────────
# def stabilize_bgp_data(input_file: str) -> pd.DataFrame:
#     """
#     Collapses raw BGP events into stable lease records.
#     Only transitions that held for at least MIN_STABLE_HOURS are kept.
#     Ping-pong flips (rapid back-and-forth) contribute to churn_ratio
#     but short ones are filtered out before reaching the API phase.
#     """
#     print(f"Loading BGP events from {input_file}...")
#     df = pd.read_csv(input_file)
#     df["timestamp"] = pd.to_datetime(df["timestamp"])
#     # Stable sort preserves original CSV order for same-second events
#     df = df.sort_values(["prefix", "timestamp"], kind="mergesort").reset_index(drop=True)
#     print(f"  Loaded {len(df):,} raw events.")

#     stable_leases = []
#     for prefix, group in df.groupby("prefix"):
#         group             = group.reset_index(drop=True)
#         original_landlord = group.iloc[0]["landlord_as"]

#         # Collapse consecutive duplicate tenant rows into single entries
#         collapsed = []
#         for _, row in group.iterrows():
#             if not collapsed or collapsed[-1]["tenant_as"] != row["tenant_as"]:
#                 collapsed.append(row.to_dict())

#         if len(collapsed) < 2:
#             continue

#         # Keep transitions that held at least MIN_STABLE_HOURS
#         # Include transitions back to landlord — we care about any tenant hold
#         genuine = []
#         for i in range(len(collapsed) - 1):
#             duration_hrs = (
#                 pd.to_datetime(collapsed[i + 1]["timestamp"]) -
#                 pd.to_datetime(collapsed[i]["timestamp"])
#             ).total_seconds() / 3600

#             if duration_hrs >= MIN_STABLE_HOURS:
#                 genuine.append({**collapsed[i], "duration_hrs": round(duration_hrs, 2)})

#         # Always include the final state (duration unknown — still holding or end of data)
#         genuine.append({**collapsed[-1], "duration_hrs": None})

#         # Need at least one real qualified transition + the trailing state
#         if len(genuine) < 2:
#             continue

#         as_sequence    = [e["tenant_as"] for e in genuine]
#         seen_ases      = set()
#         pingpong_count = 0
#         for a in as_sequence:
#             if a in seen_ases:
#                 pingpong_count += 1
#             seen_ases.add(a)

#         final_tenant        = genuine[-1]["tenant_as"]
#         completed_durations = [
#             e["duration_hrs"] for e in genuine if e["duration_hrs"] is not None
#         ]

#         stable_leases.append({
#             "prefix":               prefix,
#             "original_landlord":    original_landlord,
#             "landlord_as":          genuine[0]["landlord_as"],
#             "tenant_as":            final_tenant,
#             "timestamp":            genuine[0]["timestamp"],
#             "num_transitions":      len(genuine),
#             "num_unique_ases":      len(set(as_sequence)),
#             "pingpong_count":       pingpong_count,
#             "returned_to_landlord": 1 if final_tenant == original_landlord else 0,
#             "avg_duration_hrs":     round(sum(completed_durations) / len(completed_durations), 2)
#                                     if completed_durations else None,
#             "min_duration_hrs":     round(min(completed_durations), 2)
#                                     if completed_durations else None,
#             "churn_ratio":          round(pingpong_count / len(genuine), 3)
#                                     if genuine else 0,
#         })

#     stable_df = pd.DataFrame(stable_leases)
#     print(f"  Stabilization: {len(df):,} raw → {len(stable_df):,} stable leases "
#           f"(threshold={MIN_STABLE_HOURS * 60:.1f} min).")
#     return stable_df


# # ── IP enumeration ─────────────────────────────────────────────────────────────
# def enumerate_ips_from_prefix(prefix: str, max_ips: int = 20) -> list:
#     """
#     Samples IPs evenly across a CIDR prefix.
#     Skips network and broadcast addresses.
#     Caps at max_ips to protect daily budget.
#     """
#     try:
#         network = ipaddress.ip_network(prefix, strict=False)
#         hosts   = list(network.hosts())
#         if not hosts:
#             return []
#         step    = max(1, len(hosts) // max_ips)
#         sampled = [str(hosts[i]) for i in range(0, len(hosts), step)][:max_ips]
#         return sampled
#     except ValueError:
#         return []


# # ── check single IP (cache-aware) ─────────────────────────────────────────────
# def check_ip(ip_address: str, cache: dict, progress: dict):
#     """
#     Returns (result_dict_or_None, api_was_called).
#     Cache hit → no API call. Daily limit hit → no API call.
#     """
#     if ip_address in cache:
#         cached = cache[ip_address]
#         if cached is not None:
#             cached["first_reported"] = pd.to_datetime(cached["first_reported"])
#             cached["last_reported"]  = pd.to_datetime(cached["last_reported"])
#         return cached, False

#     if progress["ip_calls_today"] >= DAILY_IP_LIMIT:
#         return None, False

#     url     = "https://api.abuseipdb.com/api/v2/check"
#     params  = {"ipAddress": ip_address, "maxAgeInDays": "90", "verbose": "true"}
#     headers = {"Accept": "application/json", "Key": API_KEY}
#     try:
#         response = requests.get(url, headers=headers, params=params, timeout=10)
#         progress["ip_calls_today"] += 1

#         if response.status_code == 429:
#             print("  Rate limit hit (check-IP) — waiting 60s...")
#             time.sleep(60)
#             return None, True

#         if response.status_code != 200:
#             print(f"  check error {response.status_code} for {ip_address}")
#             return None, True

#         data = response.json()["data"]
#         if data["totalReports"] == 0:
#             cache_result(cache, ip_address, None)
#             return None, True

#         reports = data.get("reports", [])
#         if reports:
#             timestamps     = [pd.to_datetime(r["reportedAt"]).replace(tzinfo=None)
#                               for r in reports]
#             first_reported = min(timestamps)
#             last_reported  = max(timestamps)
#         else:
#             first_reported = pd.to_datetime(data["lastReportedAt"]).replace(tzinfo=None)
#             last_reported  = first_reported

#         result = {
#             "ip":             ip_address,
#             "first_reported": first_reported,
#             "last_reported":  last_reported,
#             "total_reports":  data["totalReports"],
#             "abuse_score":    data["abuseConfidenceScore"],
#             "queried_at":     str(datetime.utcnow().date()),
#         }
#         cache_result(cache, ip_address, result)
#         return result, True

#     except Exception as e:
#         print(f"  Error checking {ip_address}: {e}")
#         return None, True


# # ── Ad-hoc IP check ────────────────────────────────────────────────────────────
# def check_adhoc_ips(ip_list: list, cache: dict, progress: dict):
#     print(f"\n── Ad-hoc IP check: {len(ip_list)} IPs requested ──")
#     print(f"   Budget remaining today: "
#           f"{DAILY_IP_LIMIT - progress['ip_calls_today']} calls\n")

#     adhoc_results = []
#     for ip in ip_list:
#         result, api_called = check_ip(ip, cache, progress)
#         source = "api" if api_called else "cache"
#         if api_called:
#             time.sleep(0.4)

#         if progress["ip_calls_today"] >= DAILY_IP_LIMIT and ip not in cache:
#             print(f"  Daily limit reached — stopping at {ip}.")
#             break

#         adhoc_results.append({
#             "ip":             ip,
#             "source":         source,
#             "abuse_score":    result["abuse_score"]    if result else 0,
#             "total_reports":  result["total_reports"]  if result else 0,
#             "first_reported": str(result["first_reported"]) if result else "clean",
#         })
#         status = f"score={result['abuse_score']}" if result else "clean"
#         print(f"  {ip:<18} [{source}]  {status}")

#     save_progress(progress)
#     if adhoc_results:
#         df = pd.DataFrame(adhoc_results)
#         df.to_csv("adhoc_results.csv", index=False)
#         print(f"\n  Saved {len(adhoc_results)} results to adhoc_results.csv")
#     return adhoc_results


# # ── Main decay pipeline ────────────────────────────────────────────────────────
# def process_decay():
#     df_stable = stabilize_bgp_data(BGP_FILE)
#     if df_stable.empty:
#         print("No stable leases found. Exiting.")
#         return

#     df_stable = df_stable.sort_values(
#         "churn_ratio", ascending=False
#     ).reset_index(drop=True)

#     progress = load_progress()
#     progress = reset_daily_counters_if_needed(progress)
#     cache    = load_ip_cache()

#     print(f"\nDataset          : {len(df_stable):,} stable leases")
#     print(f"IPs cached       : {len(cache):,}")
#     print(f"Today — IP calls : {progress['ip_calls_today']}\n")

#     print("── Checking IPs directly from prefixes ──")
#     results = []

#     for _, row in df_stable.iterrows():
#         prefix = row["prefix"]

#         if progress["ip_calls_today"] >= DAILY_IP_LIMIT:
#             print("  Daily IP limit reached. Resume tomorrow.")
#             break

#         ips_to_check = enumerate_ips_from_prefix(prefix, max_ips=20)
#         if not ips_to_check:
#             continue

#         lease_start_ts = pd.to_datetime(row["timestamp"]).replace(tzinfo=None)
#         hits           = []

#         print(f"  {prefix:<20} → checking {len(ips_to_check)} IPs")

#         for ip in ips_to_check:
#             if progress["ip_calls_today"] >= DAILY_IP_LIMIT and ip not in cache:
#                 print("  Daily IP limit reached mid-prefix. Saving and stopping.")
#                 _save_results(results)
#                 save_progress(progress)
#                 return

#             result, api_called = check_ip(ip, cache, progress)
#             if api_called:
#                 time.sleep(0.4)
#                 save_progress(progress)

#             if result:
#                 hits.append(result)

#         if not hits:
#             continue

#         best        = min(hits, key=lambda h: h["first_reported"])
#         decay_delta = best["first_reported"] - lease_start_ts
#         decay_hours = decay_delta.total_seconds() / 3600

#         if decay_hours >= -1:
#             results.append({
#                 "prefix":             prefix,
#                 "original_landlord":  row["original_landlord"],
#                 "tenant_as":          row["tenant_as"],
#                 "lease_start":        lease_start_ts,
#                 "abusive_ip":         best["ip"],
#                 "ips_in_prefix":      len(ips_to_check),
#                 "first_abuse_report": best["first_reported"],
#                 "decay_hours":        round(max(0, decay_hours), 2),
#                 "decay_days":         round(max(0, decay_hours) / 24, 2),
#                 "total_reports":      best["total_reports"],
#                 "abuse_score":        best["abuse_score"],
#                 "num_transitions":    row["num_transitions"],
#                 "num_unique_ases":    row["num_unique_ases"],
#                 "pingpong_count":     row["pingpong_count"],
#                 "churn_ratio":        row["churn_ratio"],
#                 "avg_duration_hrs":   row["avg_duration_hrs"],
#                 "min_duration_hrs":   row["min_duration_hrs"],
#             })

#     _save_results(results)
#     save_progress(progress)

#     remaining = DAILY_IP_LIMIT - progress["ip_calls_today"]
#     if remaining > 0:
#         print(f"\n── {remaining} API calls remaining today ──")
#         extra_ips = [
#             # "1.2.3.4",
#         ]
#         if extra_ips:
#             check_adhoc_ips(extra_ips, cache, progress)
#         else:
#             print("  Add IPs to extra_ips in process_decay() to use remaining budget.")


# def _save_results(results: list):
#     if not results:
#         print("\nNo decay results yet.")
#         return
#     final_df = pd.DataFrame(results).sort_values("decay_hours")
#     final_df.to_csv(OUTPUT_FILE, index=False)
#     print(f"\n── Results ───────────────────────────────────────")
#     print(f"  Matches     : {len(results):,}")
#     print(f"  Avg decay   : {round(final_df['decay_hours'].mean(), 2)} hours")
#     print(f"  Median decay: {round(final_df['decay_hours'].median(), 2)} hours")
#     print(f"  Fastest     : {final_df['decay_hours'].min()} hours")
#     corr = final_df[["decay_hours", "churn_ratio", "num_transitions",
#                       "min_duration_hrs"]].corr()["decay_hours"].drop("decay_hours")
#     print(f"\nCorrelation with decay_hours:\n{corr.to_string()}")
#     print(f"\nSaved to {OUTPUT_FILE}")


# if __name__ == "__main__":
#     # Step 1: scan thresholds to pick MIN_STABLE_HOURS (no API calls)
#     # run_threshold_scan()

#     # Step 2: run the full pipeline with chosen threshold
#     process_decay()
"""
lease_decay_analysis.py
───────────────────────
Assumes you already have a clean lease dataset CSV with columns:
    prefix, original_landlord, tenant_as, lease_start, lease_end (optional)

Two analyses:
  1. Lease frequency  — how often leases happen, per prefix / tenant / landlord
  2. Reputation decay — uses AbuseIPDB to measure how quickly abuse appears
                        after a lease starts (same cache/quota logic as before)

Usage:
    python lease_decay_analysis.py
"""

import requests
import pandas as pd
import numpy as np
import ipaddress
import json
import os
import time
from datetime import datetime

# ── Configuration ──────────────────────────────────────────────────────────────
API_KEY        = "YOUR_KEY_HERE"
LEASE_FILE     = "leases.csv"           # your pre-extracted lease dataset
OUTPUT_FILE    = "decay_results.csv"
PROGRESS_FILE  = "progress.json"
IP_CACHE_FILE  = "ip_cache.json"

DAILY_IP_LIMIT = 950
MAX_IPS_PER_PREFIX = 20


# ══════════════════════════════════════════════════════════════════════════════
# PART 1 — LEASE FREQUENCY
# ══════════════════════════════════════════════════════════════════════════════

def load_leases(path: str = LEASE_FILE) -> pd.DataFrame:
    """
    Loads the pre-extracted lease CSV.
    Expected columns: prefix, original_landlord, tenant_as, lease_start
    Optional column:  lease_end  (if present, duration is computed)
    """
    df = pd.read_csv(path)
    df["lease_start"] = pd.to_datetime(df["lease_start"])
    if "lease_end" in df.columns:
        df["lease_end"]      = pd.to_datetime(df["lease_end"])
        df["duration_hrs"]   = (df["lease_end"] - df["lease_start"]).dt.total_seconds() / 3600
        df["duration_days"]  = df["duration_hrs"] / 24
    df = df.sort_values("lease_start").reset_index(drop=True)
    print(f"Loaded {len(df):,} leases from {path}")
    print(f"  Date range : {df['lease_start'].min()} → {df['lease_start'].max()}")
    print(f"  Prefixes   : {df['prefix'].nunique():,}")
    print(f"  Tenants    : {df['tenant_as'].nunique():,}")
    print(f"  Landlords  : {df['original_landlord'].nunique():,}\n")
    return df


def lease_frequency_report(df: pd.DataFrame) -> dict:
    """
    Computes lease frequency across three dimensions:
      - per prefix   : how often each prefix gets re-leased
      - per tenant   : how active each tenant AS is
      - per landlord : how many leases each landlord originates
    Also computes inter-lease gap (time between consecutive leases on same prefix).
    Returns a dict of DataFrames, all saved to CSV.
    """
    print("─" * 60)
    print("PART 1 — Lease Frequency")
    print("─" * 60)

    reports = {}

    # ── 1a. Per-prefix lease frequency ────────────────────────────────────────
    prefix_freq = (
        df.groupby("prefix")
        .agg(
            lease_count       = ("tenant_as",    "count"),
            unique_tenants    = ("tenant_as",    "nunique"),
            unique_landlords  = ("original_landlord", "nunique"),
            first_lease       = ("lease_start",  "min"),
            last_lease        = ("lease_start",  "max"),
        )
        .reset_index()
    )
    # Span in days between first and last lease on this prefix
    prefix_freq["span_days"] = (
        (prefix_freq["last_lease"] - prefix_freq["first_lease"])
        .dt.total_seconds() / 86400
    ).round(2)
    # Leases per day (activity rate)
    prefix_freq["leases_per_day"] = (
        prefix_freq["lease_count"] / prefix_freq["span_days"].clip(lower=1)
    ).round(4)
    prefix_freq = prefix_freq.sort_values("lease_count", ascending=False).reset_index(drop=True)

    print("\n[1a] Per-prefix lease frequency (top 20):")
    print(prefix_freq.head(20).to_string(index=False))
    prefix_freq.to_csv("report_freq_by_prefix.csv", index=False)
    reports["prefix"] = prefix_freq

    # ── 1b. Per-tenant lease frequency ────────────────────────────────────────
    tenant_freq = (
        df.groupby("tenant_as")
        .agg(
            lease_count      = ("prefix",           "count"),
            unique_prefixes  = ("prefix",           "nunique"),
            unique_landlords = ("original_landlord","nunique"),
            first_lease      = ("lease_start",      "min"),
            last_lease       = ("lease_start",      "max"),
        )
        .reset_index()
    )
    tenant_freq["span_days"] = (
        (tenant_freq["last_lease"] - tenant_freq["first_lease"])
        .dt.total_seconds() / 86400
    ).round(2)
    tenant_freq["leases_per_day"] = (
        tenant_freq["lease_count"] / tenant_freq["span_days"].clip(lower=1)
    ).round(4)
    tenant_freq = tenant_freq.sort_values("lease_count", ascending=False).reset_index(drop=True)

    print("\n[1b] Per-tenant lease frequency (top 20):")
    print(tenant_freq.head(20).to_string(index=False))
    tenant_freq.to_csv("report_freq_by_tenant.csv", index=False)
    reports["tenant"] = tenant_freq

    # ── 1c. Per-landlord lease frequency ──────────────────────────────────────
    landlord_freq = (
        df.groupby("original_landlord")
        .agg(
            lease_count     = ("prefix",    "count"),
            unique_prefixes = ("prefix",    "nunique"),
            unique_tenants  = ("tenant_as", "nunique"),
            first_lease     = ("lease_start","min"),
            last_lease      = ("lease_start","max"),
        )
        .reset_index()
    )
    landlord_freq["span_days"] = (
        (landlord_freq["last_lease"] - landlord_freq["first_lease"])
        .dt.total_seconds() / 86400
    ).round(2)
    landlord_freq["leases_per_day"] = (
        landlord_freq["lease_count"] / landlord_freq["span_days"].clip(lower=1)
    ).round(4)
    landlord_freq = landlord_freq.sort_values("lease_count", ascending=False).reset_index(drop=True)

    print("\n[1c] Per-landlord lease frequency (top 20):")
    print(landlord_freq.head(20).to_string(index=False))
    landlord_freq.to_csv("report_freq_by_landlord.csv", index=False)
    reports["landlord"] = landlord_freq

    # ── 1d. Inter-lease gap per prefix ────────────────────────────────────────
    # How long between consecutive leases on the same prefix?
    gap_rows = []
    for prefix, group in df.groupby("prefix"):
        group = group.sort_values("lease_start").reset_index(drop=True)
        for i in range(1, len(group)):
            gap_sec = (
                group.iloc[i]["lease_start"] - group.iloc[i - 1]["lease_start"]
            ).total_seconds()
            gap_rows.append({
                "prefix":     prefix,
                "gap_sec":    gap_sec,
                "gap_min":    round(gap_sec / 60, 3),
                "gap_hrs":    round(gap_sec / 3600, 4),
                "from_tenant": group.iloc[i - 1]["tenant_as"],
                "to_tenant":   group.iloc[i]["tenant_as"],
            })

    if gap_rows:
        gaps = pd.DataFrame(gap_rows)
        pcts = [5, 10, 25, 50, 75, 90, 95, 99]
        gap_pct = pd.DataFrame({
            "percentile": [f"p{p}" for p in pcts],
            "gap_sec":    [round(np.percentile(gaps["gap_sec"], p), 1) for p in pcts],
            "gap_min":    [round(np.percentile(gaps["gap_sec"], p) / 60, 2) for p in pcts],
            "gap_hrs":    [round(np.percentile(gaps["gap_sec"], p) / 3600, 4) for p in pcts],
        })
        print("\n[1d] Inter-lease gap distribution (same prefix, consecutive leases):")
        print(gap_pct.to_string(index=False))
        gap_pct.to_csv("report_interlease_gaps.csv", index=False)
        reports["gaps"] = gaps

    print("\n  Saved → report_freq_by_prefix.csv, report_freq_by_tenant.csv,")
    print("           report_freq_by_landlord.csv, report_interlease_gaps.csv")
    return reports


# ══════════════════════════════════════════════════════════════════════════════
# PART 2 — REPUTATION DECAY
# ══════════════════════════════════════════════════════════════════════════════

# ── Cache ──────────────────────────────────────────────────────────────────────
def load_ip_cache() -> dict:
    if os.path.exists(IP_CACHE_FILE):
        with open(IP_CACHE_FILE) as f:
            return json.load(f)
    return {}

def save_ip_cache(cache: dict):
    with open(IP_CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2, default=str)

def cache_result(cache: dict, ip: str, result):
    cache[ip] = result
    save_ip_cache(cache)


# ── Progress ───────────────────────────────────────────────────────────────────
def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {
        "checked_prefixes": [],
        "ip_calls_today":   0,
        "last_reset_date":  str(datetime.utcnow().date()),
    }

def save_progress(p: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(p, f, indent=2, default=str)

def reset_daily_counters(p: dict) -> dict:
    today = str(datetime.utcnow().date())
    if p["last_reset_date"] != today:
        print(f"  New day ({today}) — resetting daily counters.")
        p["ip_calls_today"]  = 0
        p["last_reset_date"] = today
    return p


# ── IP helpers ─────────────────────────────────────────────────────────────────
def enumerate_ips(prefix: str, max_ips: int = MAX_IPS_PER_PREFIX) -> list:
    try:
        network = ipaddress.ip_network(prefix, strict=False)
        hosts   = list(network.hosts())
        if not hosts:
            return []
        step = max(1, len(hosts) // max_ips)
        return [str(hosts[i]) for i in range(0, len(hosts), step)][:max_ips]
    except ValueError:
        return []


def check_ip(ip: str, cache: dict, progress: dict):
    """Returns (result_or_None, api_was_called)."""
    if ip in cache:
        cached = cache[ip]
        if cached is not None:
            cached["first_reported"] = pd.to_datetime(cached["first_reported"])
            cached["last_reported"]  = pd.to_datetime(cached["last_reported"])
        return cached, False

    if progress["ip_calls_today"] >= DAILY_IP_LIMIT:
        return None, False

    url     = "https://api.abuseipdb.com/api/v2/check"
    params  = {"ipAddress": ip, "maxAgeInDays": "90", "verbose": "true"}
    headers = {"Accept": "application/json", "Key": API_KEY}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        progress["ip_calls_today"] += 1

        if r.status_code == 429:
            print("  Rate limit — waiting 60s...")
            time.sleep(60)
            return None, True
        if r.status_code != 200:
            print(f"  Error {r.status_code} for {ip}")
            return None, True

        data = r.json()["data"]
        if data["totalReports"] == 0:
            cache_result(cache, ip, None)
            return None, True

        reports = data.get("reports", [])
        if reports:
            ts             = [pd.to_datetime(rep["reportedAt"]).replace(tzinfo=None) for rep in reports]
            first_reported = min(ts)
            last_reported  = max(ts)
        else:
            first_reported = pd.to_datetime(data["lastReportedAt"]).replace(tzinfo=None)
            last_reported  = first_reported

        result = {
            "ip":             ip,
            "first_reported": first_reported,
            "last_reported":  last_reported,
            "total_reports":  data["totalReports"],
            "abuse_score":    data["abuseConfidenceScore"],
            "queried_at":     str(datetime.utcnow().date()),
        }
        cache_result(cache, ip, result)
        return result, True

    except Exception as e:
        print(f"  Exception for {ip}: {e}")
        return None, True


# ── Decay pipeline ─────────────────────────────────────────────────────────────
def reputation_decay_report(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each lease, samples IPs from the prefix and checks AbuseIPDB.
    Computes decay_hours = time between lease_start and first abuse report.
    Resumes safely across daily quota limits using progress.json + ip_cache.json.
    """
    print("\n" + "─" * 60)
    print("PART 2 — Reputation Decay")
    print("─" * 60)

    progress = load_progress()
    progress = reset_daily_counters(progress)
    cache    = load_ip_cache()

    print(f"\n  Leases to process : {len(df):,}")
    print(f"  Already checked   : {len(progress['checked_prefixes']):,} prefixes")
    print(f"  IPs cached        : {len(cache):,}")
    print(f"  IP calls today    : {progress['ip_calls_today']}\n")

    # Deduplicate to one row per (prefix, lease_start) — each is a unique lease event
    leases = df.drop_duplicates(subset=["prefix", "lease_start"]).sort_values("lease_start")

    results = []

    for _, row in leases.iterrows():
        prefix     = row["prefix"]
        lease_key  = f"{prefix}@{row['lease_start']}"

        if lease_key in progress["checked_prefixes"]:
            continue

        if progress["ip_calls_today"] >= DAILY_IP_LIMIT:
            print("  Daily IP limit reached. Resume tomorrow.")
            break

        ips = enumerate_ips(prefix)
        if not ips:
            progress["checked_prefixes"].append(lease_key)
            save_progress(progress)
            continue

        lease_start_ts = pd.to_datetime(row["lease_start"]).replace(tzinfo=None)
        hits = []

        print(f"  {prefix:<22} {str(lease_start_ts)[:16]}  → {len(ips)} IPs")

        for ip in ips:
            if progress["ip_calls_today"] >= DAILY_IP_LIMIT and ip not in cache:
                print("  Quota hit mid-prefix — saving checkpoint.")
                _save_decay_results(results)
                save_progress(progress)
                return pd.DataFrame(results)

            result, api_called = check_ip(ip, cache, progress)
            if api_called:
                time.sleep(0.4)
                save_progress(progress)

            if result:
                hits.append(result)

        progress["checked_prefixes"].append(lease_key)
        save_progress(progress)

        if not hits:
            continue

        # Pick the IP with the earliest abuse report as the decay signal
        best        = min(hits, key=lambda h: h["first_reported"])
        decay_secs  = (best["first_reported"] - lease_start_ts).total_seconds()
        decay_hours = decay_secs / 3600

        # Allow up to 1hr before lease_start (clock skew / reporting lag)
        if decay_hours < -1:
            continue

        results.append({
            "prefix":             prefix,
            "original_landlord":  row["original_landlord"],
            "tenant_as":          row["tenant_as"],
            "lease_start":        lease_start_ts,
            "abusive_ip":         best["ip"],
            "first_abuse_report": best["first_reported"],
            "decay_hours":        round(max(0, decay_hours), 2),
            "decay_days":         round(max(0, decay_hours) / 24, 2),
            "abuse_score":        best["abuse_score"],
            "total_reports":      best["total_reports"],
            "ips_sampled":        len(ips),
            "ips_with_abuse":     len(hits),
        })

    _save_decay_results(results)
    save_progress(progress)
    return pd.DataFrame(results) if results else pd.DataFrame()


def _save_decay_results(results: list):
    if not results:
        print("\n  No decay results yet.")
        return
    out = pd.DataFrame(results).sort_values("decay_hours")
    out.to_csv(OUTPUT_FILE, index=False)

    print(f"\n── Decay Summary ─────────────────────────────────────")
    print(f"  Matches        : {len(out):,}")
    print(f"  Avg decay      : {out['decay_hours'].mean():.2f} hrs")
    print(f"  Median decay   : {out['decay_hours'].median():.2f} hrs")
    print(f"  Fastest decay  : {out['decay_hours'].min():.2f} hrs")

    # Threshold breakdown
    for hrs, label in [(1, "1hr"), (6, "6hr"), (24, "24hr"), (72, "72hr")]:
        pct = (out["decay_hours"] <= hrs).mean() * 100
        print(f"  Within {label:<6}: {pct:.1f}%")

    print(f"\n  Saved → {OUTPUT_FILE}")


# ── Entry point ────────────────────────────────────────────────────────────────

def run(lease_file: str = LEASE_FILE):
    df = load_leases(lease_file)

    # Part 1 — no API calls
    lease_frequency_report(df)

    # Part 2 — uses AbuseIPDB quota
    reputation_decay_report(df)


if __name__ == "__main__":
    run()