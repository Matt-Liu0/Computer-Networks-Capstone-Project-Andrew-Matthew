import requests
import pandas as pd
import time
import ipaddress
import json
import os
from datetime import datetime

# ── Configuration ──────────────────────────────────────────────────────────────
API_KEY           = "271f925c50406ddcb560f6a9f1ec5055beababebfbf7fd686f44dd06b199c0a5f7c8b139568a5d2c"
BGP_FILE          = "lease_start_events.csv"
OUTPUT_FILE       = "final_decay_results.csv"
PROGRESS_FILE     = "progress.json"
IP_CACHE_FILE     = "ip_cache.json"

DAILY_IP_LIMIT    = 950

# Threshold in hours — transitions shorter than this are treated as noise.
# Based on sample data showing sub-minute flips, 1hr filters too aggressively.
# 5 minutes (5/60) captures real rapid-reuse patterns without drowning in
# BGP convergence noise. Tune this after reviewing threshold_scan output.
MIN_STABLE_HOURS  = 0 / 60   # 0 minutes


# ── Threshold scanner — run this first to pick MIN_STABLE_HOURS ───────────────
def run_threshold_scan(input_file: str = BGP_FILE):
    """
    Prints a table showing how many prefixes and suspect ASNs survive
    at each candidate threshold. Use this to pick MIN_STABLE_HOURS.
    Does not call any API — pure BGP data analysis.
    """
    print(f"Loading {input_file} for threshold scan...")
    df = pd.read_csv(input_file)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["prefix", "timestamp"], kind="mergesort").reset_index(drop=True)
    print(f"  {len(df):,} raw events across {df['prefix'].nunique():,} prefixes.\n")

    thresholds = [0, 1/60, 5/60, 15/60, 0.5, 1, 6, 12, 24]
    labels     = ["0s", "1min", "5min", "15min", "30min", "1hr", "6hr", "12hr", "24hr"]

    print(f"{'Threshold':<10} {'Surviving Prefixes':>20} {'Unique Suspect ASNs':>22}")
    print("─" * 55)

    for threshold, label in zip(thresholds, labels):
        surviving            = 0
        potential_bad_actors = set()

        for prefix, group in df.groupby("prefix"):
            group             = group.reset_index(drop=True)
            original_landlord = group.iloc[0]["landlord_as"]

            collapsed = []
            for _, row in group.iterrows():
                if not collapsed or collapsed[-1]["tenant_as"] != row["tenant_as"]:
                    collapsed.append(row.to_dict())

            if len(collapsed) < 2:
                continue

            qualified_transitions = 0
            for i in range(len(collapsed) - 1):
                duration_hrs = (
                    pd.to_datetime(collapsed[i + 1]["timestamp"]) -
                    pd.to_datetime(collapsed[i]["timestamp"])
                ).total_seconds() / 3600

                tenant = collapsed[i]["tenant_as"]
                if duration_hrs >= threshold and tenant != original_landlord:
                    potential_bad_actors.add(tenant)
                    qualified_transitions += 1

            if qualified_transitions > 0:
                surviving += 1

        print(f"{label:<10} {surviving:>20,} {len(potential_bad_actors):>22,}")

    print(f"\n  Current MIN_STABLE_HOURS = {MIN_STABLE_HOURS:.4f}hr  "
          f"({MIN_STABLE_HOURS * 60:.1f} minutes)")


# ── IP Cache ───────────────────────────────────────────────────────────────────
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


# ── Progress tracking ──────────────────────────────────────────────────────────
def load_progress() -> dict:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {
        "checked_prefixes":  [],
        "dirty_prefixes":    {},
        "ip_calls_today":    0,
        "last_reset_date":   str(datetime.utcnow().date()),
    }

def save_progress(progress: dict):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2, default=str)

def reset_daily_counters_if_needed(progress: dict) -> dict:
    today = str(datetime.utcnow().date())
    if progress["last_reset_date"] != today:
        print(f"  New day ({today}) — resetting daily API counters.")
        progress["ip_calls_today"]  = 0
        progress["last_reset_date"] = today
    return progress


# ── BGP stabilization ──────────────────────────────────────────────────────────
def stabilize_bgp_data(input_file: str) -> pd.DataFrame:
    """
    Collapses raw BGP events into stable lease records.
    Only transitions that held for at least MIN_STABLE_HOURS are kept.
    Ping-pong flips (rapid back-and-forth) contribute to churn_ratio
    but short ones are filtered out before reaching the API phase.
    """
    print(f"Loading BGP events from {input_file}...")
    df = pd.read_csv(input_file)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # Stable sort preserves original CSV order for same-second events
    df = df.sort_values(["prefix", "timestamp"], kind="mergesort").reset_index(drop=True)
    print(f"  Loaded {len(df):,} raw events.")

    stable_leases = []
    for prefix, group in df.groupby("prefix"):
        group             = group.reset_index(drop=True)
        original_landlord = group.iloc[0]["landlord_as"]

        # Collapse consecutive duplicate tenant rows into single entries
        collapsed = []
        for _, row in group.iterrows():
            if not collapsed or collapsed[-1]["tenant_as"] != row["tenant_as"]:
                collapsed.append(row.to_dict())

        if len(collapsed) < 2:
            continue

        # Keep transitions that held at least MIN_STABLE_HOURS
        # Include transitions back to landlord — we care about any tenant hold
        genuine = []
        for i in range(len(collapsed) - 1):
            duration_hrs = (
                pd.to_datetime(collapsed[i + 1]["timestamp"]) -
                pd.to_datetime(collapsed[i]["timestamp"])
            ).total_seconds() / 3600

            if duration_hrs >= MIN_STABLE_HOURS:
                genuine.append({**collapsed[i], "duration_hrs": round(duration_hrs, 2)})

        # Always include the final state (duration unknown — still holding or end of data)
        genuine.append({**collapsed[-1], "duration_hrs": None})

        # Need at least one real qualified transition + the trailing state
        if len(genuine) < 2:
            continue

        as_sequence    = [e["tenant_as"] for e in genuine]
        seen_ases      = set()
        pingpong_count = 0
        for a in as_sequence:
            if a in seen_ases:
                pingpong_count += 1
            seen_ases.add(a)

        final_tenant        = genuine[-1]["tenant_as"]
        completed_durations = [
            e["duration_hrs"] for e in genuine if e["duration_hrs"] is not None
        ]

        stable_leases.append({
            "prefix":               prefix,
            "original_landlord":    original_landlord,
            "landlord_as":          genuine[0]["landlord_as"],
            "tenant_as":            final_tenant,
            "timestamp":            genuine[0]["timestamp"],
            "num_transitions":      len(genuine),
            "num_unique_ases":      len(set(as_sequence)),
            "pingpong_count":       pingpong_count,
            "returned_to_landlord": 1 if final_tenant == original_landlord else 0,
            "avg_duration_hrs":     round(sum(completed_durations) / len(completed_durations), 2)
                                    if completed_durations else None,
            "min_duration_hrs":     round(min(completed_durations), 2)
                                    if completed_durations else None,
            "churn_ratio":          round(pingpong_count / len(genuine), 3)
                                    if genuine else 0,
        })

    stable_df = pd.DataFrame(stable_leases)
    print(f"  Stabilization: {len(df):,} raw → {len(stable_df):,} stable leases "
          f"(threshold={MIN_STABLE_HOURS * 60:.1f} min).")
    return stable_df


# ── IP enumeration ─────────────────────────────────────────────────────────────
def enumerate_ips_from_prefix(prefix: str, max_ips: int = 20) -> list:
    """
    Samples IPs evenly across a CIDR prefix.
    Skips network and broadcast addresses.
    Caps at max_ips to protect daily budget.
    """
    try:
        network = ipaddress.ip_network(prefix, strict=False)
        hosts   = list(network.hosts())
        if not hosts:
            return []
        step    = max(1, len(hosts) // max_ips)
        sampled = [str(hosts[i]) for i in range(0, len(hosts), step)][:max_ips]
        return sampled
    except ValueError:
        return []


# ── check single IP (cache-aware) ─────────────────────────────────────────────
def check_ip(ip_address: str, cache: dict, progress: dict):
    """
    Returns (result_dict_or_None, api_was_called).
    Cache hit → no API call. Daily limit hit → no API call.
    """
    if ip_address in cache:
        cached = cache[ip_address]
        if cached is not None:
            cached["first_reported"] = pd.to_datetime(cached["first_reported"])
            cached["last_reported"]  = pd.to_datetime(cached["last_reported"])
        return cached, False

    if progress["ip_calls_today"] >= DAILY_IP_LIMIT:
        return None, False

    url     = "https://api.abuseipdb.com/api/v2/check"
    params  = {"ipAddress": ip_address, "maxAgeInDays": "90", "verbose": "true"}
    headers = {"Accept": "application/json", "Key": API_KEY}
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        progress["ip_calls_today"] += 1

        if response.status_code == 429:
            print("  Rate limit hit (check-IP) — waiting 60s...")
            time.sleep(60)
            return None, True

        if response.status_code != 200:
            print(f"  check error {response.status_code} for {ip_address}")
            return None, True

        data = response.json()["data"]
        if data["totalReports"] == 0:
            cache_result(cache, ip_address, None)
            return None, True

        reports = data.get("reports", [])
        if reports:
            timestamps     = [pd.to_datetime(r["reportedAt"]).replace(tzinfo=None)
                              for r in reports]
            first_reported = min(timestamps)
            last_reported  = max(timestamps)
        else:
            first_reported = pd.to_datetime(data["lastReportedAt"]).replace(tzinfo=None)
            last_reported  = first_reported

        result = {
            "ip":             ip_address,
            "first_reported": first_reported,
            "last_reported":  last_reported,
            "total_reports":  data["totalReports"],
            "abuse_score":    data["abuseConfidenceScore"],
            "queried_at":     str(datetime.utcnow().date()),
        }
        cache_result(cache, ip_address, result)
        return result, True

    except Exception as e:
        print(f"  Error checking {ip_address}: {e}")
        return None, True


# ── Ad-hoc IP check ────────────────────────────────────────────────────────────
def check_adhoc_ips(ip_list: list, cache: dict, progress: dict):
    print(f"\n── Ad-hoc IP check: {len(ip_list)} IPs requested ──")
    print(f"   Budget remaining today: "
          f"{DAILY_IP_LIMIT - progress['ip_calls_today']} calls\n")

    adhoc_results = []
    for ip in ip_list:
        result, api_called = check_ip(ip, cache, progress)
        source = "api" if api_called else "cache"
        if api_called:
            time.sleep(0.4)

        if progress["ip_calls_today"] >= DAILY_IP_LIMIT and ip not in cache:
            print(f"  Daily limit reached — stopping at {ip}.")
            break

        adhoc_results.append({
            "ip":             ip,
            "source":         source,
            "abuse_score":    result["abuse_score"]    if result else 0,
            "total_reports":  result["total_reports"]  if result else 0,
            "first_reported": str(result["first_reported"]) if result else "clean",
        })
        status = f"score={result['abuse_score']}" if result else "clean"
        print(f"  {ip:<18} [{source}]  {status}")

    save_progress(progress)
    if adhoc_results:
        df = pd.DataFrame(adhoc_results)
        df.to_csv("adhoc_results.csv", index=False)
        print(f"\n  Saved {len(adhoc_results)} results to adhoc_results.csv")
    return adhoc_results


# ── Main decay pipeline ────────────────────────────────────────────────────────
def process_decay():
    df_stable = stabilize_bgp_data(BGP_FILE)
    if df_stable.empty:
        print("No stable leases found. Exiting.")
        return

    df_stable = df_stable.sort_values(
        "churn_ratio", ascending=False
    ).reset_index(drop=True)

    progress = load_progress()
    progress = reset_daily_counters_if_needed(progress)
    cache    = load_ip_cache()

    print(f"\nDataset          : {len(df_stable):,} stable leases")
    print(f"IPs cached       : {len(cache):,}")
    print(f"Today — IP calls : {progress['ip_calls_today']}\n")

    print("── Checking IPs directly from prefixes ──")
    results = []

    for _, row in df_stable.iterrows():
        prefix = row["prefix"]

        if progress["ip_calls_today"] >= DAILY_IP_LIMIT:
            print("  Daily IP limit reached. Resume tomorrow.")
            break

        ips_to_check = enumerate_ips_from_prefix(prefix, max_ips=20)
        if not ips_to_check:
            continue

        lease_start_ts = pd.to_datetime(row["timestamp"]).replace(tzinfo=None)
        hits           = []

        print(f"  {prefix:<20} → checking {len(ips_to_check)} IPs")

        for ip in ips_to_check:
            if progress["ip_calls_today"] >= DAILY_IP_LIMIT and ip not in cache:
                print("  Daily IP limit reached mid-prefix. Saving and stopping.")
                _save_results(results)
                save_progress(progress)
                return

            result, api_called = check_ip(ip, cache, progress)
            if api_called:
                time.sleep(0.4)
                save_progress(progress)

            if result:
                hits.append(result)

        if not hits:
            continue

        best        = min(hits, key=lambda h: h["first_reported"])
        decay_delta = best["first_reported"] - lease_start_ts
        decay_hours = decay_delta.total_seconds() / 3600

        if decay_hours >= -1:
            results.append({
                "prefix":             prefix,
                "original_landlord":  row["original_landlord"],
                "tenant_as":          row["tenant_as"],
                "lease_start":        lease_start_ts,
                "abusive_ip":         best["ip"],
                "ips_in_prefix":      len(ips_to_check),
                "first_abuse_report": best["first_reported"],
                "decay_hours":        round(max(0, decay_hours), 2),
                "decay_days":         round(max(0, decay_hours) / 24, 2),
                "total_reports":      best["total_reports"],
                "abuse_score":        best["abuse_score"],
                "num_transitions":    row["num_transitions"],
                "num_unique_ases":    row["num_unique_ases"],
                "pingpong_count":     row["pingpong_count"],
                "churn_ratio":        row["churn_ratio"],
                "avg_duration_hrs":   row["avg_duration_hrs"],
                "min_duration_hrs":   row["min_duration_hrs"],
            })

    _save_results(results)
    save_progress(progress)

    remaining = DAILY_IP_LIMIT - progress["ip_calls_today"]
    if remaining > 0:
        print(f"\n── {remaining} API calls remaining today ──")
        extra_ips = [
            # "1.2.3.4",
        ]
        if extra_ips:
            check_adhoc_ips(extra_ips, cache, progress)
        else:
            print("  Add IPs to extra_ips in process_decay() to use remaining budget.")


def _save_results(results: list):
    if not results:
        print("\nNo decay results yet.")
        return
    final_df = pd.DataFrame(results).sort_values("decay_hours")
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n── Results ───────────────────────────────────────")
    print(f"  Matches     : {len(results):,}")
    print(f"  Avg decay   : {round(final_df['decay_hours'].mean(), 2)} hours")
    print(f"  Median decay: {round(final_df['decay_hours'].median(), 2)} hours")
    print(f"  Fastest     : {final_df['decay_hours'].min()} hours")
    corr = final_df[["decay_hours", "churn_ratio", "num_transitions",
                      "min_duration_hrs"]].corr()["decay_hours"].drop("decay_hours")
    print(f"\nCorrelation with decay_hours:\n{corr.to_string()}")
    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    # Step 1: scan thresholds to pick MIN_STABLE_HOURS (no API calls)
    # run_threshold_scan()

    # Step 2: run the full pipeline with chosen threshold
    process_decay()