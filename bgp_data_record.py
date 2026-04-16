import pybgpstream
import csv
import re
from datetime import datetime

# ── Configuration ──────────────────────────────────────────────────────────────
START_TIME  = "2026-04-01 00:00:00"
END_TIME    = "2026-04-02 23:59:59"
COLLECTORS  = ["route-views2"]
OUTPUT_FILE = "lease_start_events.csv"

def detect_lease_transitions():
    # record_type="all" gives RIB entries (elem.type="R") first,
    # then updates (elem.type="A"/"W") — BGPStream handles the ordering
    stream = pybgpstream.BGPStream(
        from_time=START_TIME,
        until_time=END_TIME,
        collectors=COLLECTORS,
        record_type="all",
    )

    # Baseline map: prefix → current known origin AS
    # Built from RIB entries first, then kept up to date via updates
    routing_baseline = {}

    rec_count   = 0
    skip_count  = 0
    event_count = 0

    print("Starting stream — RIB entries will build the baseline first...")
    print(f"Writing lease events to {OUTPUT_FILE}\n")

    fieldnames = ["timestamp", "prefix", "landlord_as", "tenant_as", "event_type"]

    with open(OUTPUT_FILE, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for rec in stream.records():
            # Skip records that failed to download or parse
            if rec.status != "valid":
                skip_count += 1
                continue

            rec_count += 1
            if rec_count % 1000 == 0:
                print(f"  Records: {rec_count:,} | Skipped: {skip_count:,} | "
                      f"Baseline size: {len(routing_baseline):,} | Events: {event_count:,}")

            for elem in rec:
                prefix = elem.fields.get("prefix", "")

                # IPv4 only
                if not prefix or ":" in prefix:
                    continue

                # Skip prefixes longer than /24
                try:
                    if int(prefix.split("/")[-1]) > 24:
                        continue
                except:
                    continue

                as_path = elem.fields.get("as-path", "")
                if not as_path:
                    continue

                # Clean origin AS — strip AS sets like {1,2,3}
                tokens = as_path.strip().split()
                current_as = re.sub(r"[{}]", "", tokens[-1]).split(",")[0]
                if not current_as:
                    continue

                # ── Phase 1: RIB entry → build baseline ───────────────────
                if elem.type == "R":
                    # Only set baseline if not already seen — first RIB wins
                    if prefix not in routing_baseline:
                        routing_baseline[prefix] = current_as

                # ── Phase 2: Update announcement → detect changes ──────────
                elif elem.type == "A":
                    if prefix in routing_baseline:
                        old_as = routing_baseline[prefix]

                        if old_as != current_as:
                            ts = datetime.utcfromtimestamp(elem.time).strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )
                            writer.writerow({
                                "timestamp":  ts,
                                "prefix":     prefix,
                                "landlord_as": old_as,      # original owner
                                "tenant_as":   current_as,  # new announcer
                                "event_type": "LEASE_OR_HIJACK",
                            })
                            event_count += 1

                            # Update baseline so we track subsequent transitions
                            # e.g. tenant → new tenant, or tenant → back to landlord
                            routing_baseline[prefix] = current_as
                    else:
                        # Prefix not in RIB — first time we see it, add to baseline
                        routing_baseline[prefix] = current_as

    print(f"\nStream finished.")
    print(f"  Records processed : {rec_count:,}")
    print(f"  Records skipped   : {skip_count:,}")
    print(f"  Baseline size     : {len(routing_baseline):,} prefixes")
    print(f"  Lease events      : {event_count:,}")
    print(f"\nDone. Events saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    detect_lease_transitions()