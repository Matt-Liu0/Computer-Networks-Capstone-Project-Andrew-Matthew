import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import ipaddress

# ── Configuration ──────────────────────────────────────────────────────────────
API_KEY     = "271f925c50406ddcb560f6a9f1ec5055beababebfbf7fd686f44dd06b199c0a5f7c8b139568a5d2c"   # replace after revoking the exposed one
BGP_FILE    = "lease_start_events.csv"
OUTPUT_FILE = "final_decay_results.csv"
MAX_CHECKS  = 950

# Minimum time a prefix must stay with a new AS to count as a real lease
# Changes shorter than this are treated as BGP flapping/noise
MIN_STABLE_HOURS = 1.0

def stabilize_bgp_data(input_file):
    """
    Detects genuine lease transitions by filtering out ping-pong noise.

    A real lease requires:
    1. The prefix stayed with the new AS for at least MIN_STABLE_HOURS
    2. The prefix did not immediately return to the previous AS (flapping)
    3. Consecutive duplicate AS announcements are collapsed

    Returns a DataFrame of stable lease events with churn metrics per prefix.
    """
    print(f"Loading BGP events from {input_file}...")
    df = pd.read_csv(input_file)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["prefix", "timestamp"]).reset_index(drop=True)
    print(f"  Loaded {len(df):,} raw transition events.")

    stable_leases = []

    for prefix, group in df.groupby("prefix"):
        group = group.reset_index(drop=True)
        original_landlord = group.iloc[0]["landlord_as"]

        # ── Step 1: Collapse consecutive duplicate AS announcements ───────────
        # e.g. A→A→A→B becomes A→B
        collapsed = []
        for _, row in group.iterrows():
            if not collapsed or collapsed[-1]["tenant_as"] != row["tenant_as"]:
                collapsed.append(row.to_dict())

        if len(collapsed) < 2:
            continue

        # ── Step 2: Filter out short-lived transitions (flapping/ping-pong) ──
        # A transition only counts if the prefix stayed for MIN_STABLE_HOURS
        genuine = []
        for i in range(len(collapsed) - 1):
            current  = collapsed[i]
            next_hop = collapsed[i + 1]

            duration_hrs = (
                pd.to_datetime(next_hop["timestamp"]) -
                pd.to_datetime(current["timestamp"])
            ).total_seconds() / 3600

            if duration_hrs >= MIN_STABLE_HOURS:
                genuine.append({**current, "duration_hrs": round(duration_hrs, 2)})

        # Always include the last event (duration unknown — ongoing or end of window)
        genuine.append({**collapsed[-1], "duration_hrs": None})

        if not genuine:
            continue

        # ── Step 3: Count ping-pong behavior ──────────────────────────────────
        # How many times did the prefix bounce back to a previous AS?
        as_sequence  = [e["tenant_as"] for e in genuine]
        seen_ases    = set()
        pingpong_count = 0
        for a in as_sequence:
            if a in seen_ases:
                pingpong_count += 1
            seen_ases.add(a)

        # ── Step 4: Only keep prefixes that genuinely left the landlord ───────
        final_tenant = genuine[-1]["tenant_as"]
        if final_tenant == original_landlord and len(genuine) == 1:
            continue  # pure noise — went back to landlord immediately

        # ── Step 5: Compute churn metrics ─────────────────────────────────────
        completed_durations = [
            e["duration_hrs"] for e in genuine
            if e["duration_hrs"] is not None
        ]

        stable_leases.append({
            "prefix":            prefix,
            "original_landlord": original_landlord,
            "landlord_as":       genuine[0]["landlord_as"],
            "tenant_as":         final_tenant,
            "timestamp":         genuine[0]["timestamp"],   # first genuine lease start

            # Churn metrics — key features for your predictive model
            "num_transitions":   len(genuine),
            "num_unique_ases":   len(set(as_sequence)),
            "pingpong_count":    pingpong_count,            # bounced back to previous AS
            "returned_to_landlord": 1 if final_tenant == original_landlord else 0,
            "avg_duration_hrs":  round(sum(completed_durations) / len(completed_durations), 2)
                                 if completed_durations else None,
            "min_duration_hrs":  round(min(completed_durations), 2)
                                 if completed_durations else None,

            # High churn ratio = lots of short/bouncy leases = suspicious
            "churn_ratio":       round(pingpong_count / len(genuine), 3)
                                 if genuine else 0,
        })

    stable_df = pd.DataFrame(stable_leases)
    print(f"  Stabilization complete: {len(df):,} raw events → {len(stable_df):,} stable leases.")
    print(f"  Ping-pong events filtered: {len(df) - len(stable_df):,}")
    return stable_df


def get_first_abuse_timestamp(ip_address):
    """Queries AbuseIPDB and extracts the EARLIEST abuse report."""
    url = "https://api.abuseipdb.com/api/v2/check"
    params = {
        "ipAddress":    ip_address,
        "maxAgeInDays": "90",
        "verbose":      "true"
    }
    headers = {
        "Accept": "application/json",
        "Key":    API_KEY
    }
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)

        if response.status_code == 429:
            print("  Rate limit hit — waiting 60 seconds...")
            time.sleep(60)
            return None

        if response.status_code != 200:
            print(f"  API error {response.status_code} for {ip_address}")
            return None

        data = response.json()["data"]
        if data["totalReports"] == 0:
            return None

        reports = data.get("reports", [])
        if reports:
            timestamps = [
                pd.to_datetime(r["reportedAt"]).replace(tzinfo=None)
                for r in reports
            ]
            first_reported = min(timestamps)
            last_reported  = max(timestamps)
        else:
            first_reported = pd.to_datetime(
                data["lastReportedAt"]
            ).replace(tzinfo=None)
            last_reported = first_reported

        return {
            "first_reported": first_reported,
            "last_reported":  last_reported,
            "total_reports":  data["totalReports"],
            "abuse_score":    data["abuseConfidenceScore"],
        }

    except Exception as e:
        print(f"  Error checking {ip_address}: {e}")
        return None


def get_sample_ip(prefix, count=3):
    try:
        network = ipaddress.ip_network(prefix, strict=False)
        hosts = list(network.hosts())
        if len(hosts) <= count:
            return [str(h) for h in hosts]
        # Return a spread: start, middle, and end of the range
        return [str(hosts[0]), str(hosts[len(hosts)//2]), str(hosts[-1])]
    except:
        return []


def process_decay():
    # ── Step 1: Stabilize BGP data ─────────────────────────────────────────────
    df_stable = stabilize_bgp_data(BGP_FILE)

    if df_stable.empty:
        print("No stable leases found. Exiting.")
        return

    if len(df_stable) > MAX_CHECKS:
        print(f"  Limiting to {MAX_CHECKS} prefixes to respect API quota.")
        # Prioritize high-churn prefixes — most likely to be malicious
        df_stable = df_stable.sort_values(
            "churn_ratio", ascending=False
        ).head(MAX_CHECKS)

    results  = []
    checked  = 0
    found    = 0
    skipped  = 0

    print(f"\nQuerying AbuseIPDB for {len(df_stable):,} stable leases...")

    # ── Step 2: Query AbuseIPDB ────────────────────────────────────────────────
    for _, row in df_stable.iterrows():
        prefix    = row["prefix"]
        sample_ip = get_sample_ip(prefix)

        if not sample_ip:
            skipped += 1
            continue

        checked += 1
        if checked % 50 == 0:
            print(f"  Checked: {checked:,} | Abuse found: {found:,} | Skipped: {skipped:,}")

        abuse = get_first_abuse_timestamp(sample_ip)

        if abuse:
            lease_start_ts = pd.to_datetime(row["timestamp"]).replace(tzinfo=None)
            decay_delta    = abuse["first_reported"] - lease_start_ts
            decay_hours    = decay_delta.total_seconds() / 3600

            # Allow small negative window for clock skew
            if decay_hours >= -1:
                results.append({
                    "prefix":              prefix,
                    "original_landlord":   row["original_landlord"],
                    "tenant_as":           row["tenant_as"],
                    "lease_start":         lease_start_ts,
                    "first_abuse_report":  abuse["first_reported"],
                    "decay_hours":         round(max(0, decay_hours), 2),
                    "decay_days":          round(max(0, decay_hours) / 24, 2),
                    "total_reports":       abuse["total_reports"],
                    "abuse_score":         abuse["abuse_score"],

                    # Churn features — for correlation with decay time
                    "num_transitions":     row["num_transitions"],
                    "num_unique_ases":     row["num_unique_ases"],
                    "pingpong_count":      row["pingpong_count"],
                    "churn_ratio":         row["churn_ratio"],
                    "avg_duration_hrs":    row["avg_duration_hrs"],
                    "min_duration_hrs":    row["min_duration_hrs"],
                })
                found += 1

        time.sleep(0.4)

    # ── Step 3: Save and summarize ─────────────────────────────────────────────
    if results:
        final_df = pd.DataFrame(results)
        final_df = final_df.sort_values("decay_hours")
        final_df.to_csv(OUTPUT_FILE, index=False)

        print(f"\nResults:")
        print(f"  Prefixes checked       : {checked:,}")
        print(f"  Abuse matches found    : {found:,}")
        print(f"  Avg decay time         : {round(final_df['decay_hours'].mean(), 2)} hours")
        print(f"  Median decay time      : {round(final_df['decay_hours'].median(), 2)} hours")
        print(f"  Fastest decay          : {final_df['decay_hours'].min()} hours")

        # Quick correlation check between churn and decay
        corr = final_df[["decay_hours", "churn_ratio", "num_transitions",
                          "min_duration_hrs"]].corr()["decay_hours"].drop("decay_hours")
        print(f"\nCorrelation with decay_hours:")
        print(corr.to_string())
        print(f"\nSaved to {OUTPUT_FILE}")
    else:
        print("\nNo abuse matches found for stable leases in this window.")


if __name__ == "__main__":
    process_decay()