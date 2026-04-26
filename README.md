# Computer Networks Capstone Project: IP Leasing Dynamics & Reputation Decay
**Authors:** Andrew & Matthew  
**Topic:** Characterizing IP Lease Behaviors via BGP Churn and Blocklist Correlation

---

## 1. Project Overview
This project investigates the operational lifecycle of leased IP addresses and their subsequent security implications. IP leasing has become a cornerstone of modern network infrastructure, yet it introduces significant challenges for network reputation and security auditing.

### Research Objectives
* **Phase 1: Paper Reproduction:** Using the `RIPE.ipynb` notebook, we reproduce the core findings of ***"Sublet Your Subnet: Inferring IP Leasing in the Wild"***. This establishes the baseline for distinguishing between internal network moves and third-party leasing.
* **Phase 2: Modern Event Extraction:** We use `bgp_data_record.py` to capture high-resolution, real-time BGP transitions. This allows us to move from static "snapshots" to a dynamic view of lease starts and ends.
* **Phase 3: Lease & Decay Characterization:** We extend the research by investigating lease durations and the "reputation decay" period—the time lag between a lease event and blocklist updates.

---

## 2. Phase 1: Foundational Inference (`RIPE.ipynb`)

This phase replicates the methodology from Section 5.1 of the *Sublet Your Subnet* paper. The notebook merges administrative WHOIS data with BGP snapshots to categorize IP blocks.

**Key Functional Steps:**
1. **WHOIS Parsing:** Extracts `inetnum`, `organisation`, and `aut-num` objects to build an address allocation tree.
2. **AS Mapping:** Uses CAIDA datasets to map Autonomous Systems to their parent organizations.
3. **BGP Origin Comparison:** Searches for exact-matching BGP origins for "leaf" nodes.
4. **Inference Logic:** - **ISP Customer:** If a business relationship (Provider-to-Customer) exists between the IP holder and the BGP announcer.
   - **Leased:** If the BGP announcer is an unrelated third party (the "Bold Orange Rectangle" in the paper's methodology).

---

## 3. Phase 2: High-Resolution Extraction (`bgp_data_record.py`)

While Phase 1 identifies *who* is leasing, Phase 2 identifies *when* lease transitions occur by monitoring a live BGP update stream for origin AS changes on observed prefixes.

### A. Data Source
BGP data is collected via `pybgpstream` from the **route-views2** RouteViews collector. A single collector is used as a scoping decision; events that did not propagate to this vantage point are not captured. Only IPv4 prefixes up to /24 are retained — longer prefixes are filtered out as they are typically not globally routable and are not present in the RIPE inference outputs.

### B. Baseline Construction
The stream is opened with `record_type="all"`, which delivers RIB (Routing Information Base) snapshot entries (`elem.type="R"`) before live update messages (`elem.type="A"`). RIB entries are used to build a `routing_baseline` dictionary mapping each prefix to its current known origin AS. The first RIB entry seen for a prefix wins; subsequent duplicates are ignored.

### C. Transition Detection
For each BGP UPDATE announcement (`elem.type="A"`), the script compares the new origin AS against the stored baseline. If they differ, a `LEASE_OR_HIJACK` event is written to the output CSV and the baseline is updated to the new AS. This means every subsequent transition — including a return to the original AS — is also captured.

> **Known limitation:** The `landlord_as` field in the output CSV is the RIB-baseline AS at the
> start of the observation window, **not** the RIPE-registered holder. The RIPE cross-referencing
> that confirms genuine leases is performed in Phase 3 by `lease_time_analysis.py`.

### D. Output
`lease_start_events.csv` contains one row per origin transition with columns:
`timestamp`, `prefix`, `landlord_as`, `tenant_as`, `event_type`.
The `timestamp` is the BGP UPDATE announcement time as recorded by the route collector, **not** the time the route converged globally across the internet. BGP convergence typically takes 90 seconds or more; events recorded within the same second or within the first minute after a prior event should be interpreted as convergence-burst artifacts rather than genuine tenant transitions (see Section 4A for handling).

---

## 4. Phase 3: Extension & Computation

### A. Lease Time Analysis (`lease_time_analysis.py`)

This script processes `lease_start_events.csv` to characterise lease market behaviour.

**RIPE Validation Filter:**
Raw BGP events are first cross-referenced against the Phase 1 RIPE inference outputs (`c1inference` and `c2inferences`). Only prefixes confirmed as inferred leases by both the registry and BGP dimensions are retained. In our dataset, 109 of 3,825 raw prefixes (2.8%) survived this filter; the remainder could not be confirmed as genuine leases and are dropped before any duration or churn analysis.

**Hold Period Construction:**
Consecutive duplicate origin AS entries for the same prefix are collapsed first (back-to-back announcements by the same AS count as one hold). Each hold period duration is then computed as:

`duration_sec = next_event_timestamp − current_event_timestamp`

A hold period where the current AS matches the RIB-baseline AS (`landlord_as`) is flagged `is_landlord_hold = True` and excluded from tenant duration statistics, though retained for structural analysis. Open holds at the end of the observation window are right-censored and excluded.

> **Note on zero-duration holds:** Two BGP events for the same prefix arriving within the same
> second produce `duration_sec = 0`. These are real data points reflecting BGP convergence
> bursts rather than genuine tenant transitions, and are the primary driver of the p25 = 0 s
> result in Section 2.1. They are included in the reported distributions but should be
> interpreted as a lower bound on measurement resolution, not as actual hold durations.

**Ping-Pong Detection:**
A transition is flagged as ping-pong if the incoming `tenant_as` has previously held that prefix within the observation window. The churn ratio is:

`churn_ratio = pingpong_count / num_transitions`

Note: the landlord AS returning to its own prefix mid-sequence is **not** counted as a ping-pong event; only non-landlord tenant repeats are included.

**Outputs per run:**
- `report_lease_duration_buckets.csv` / `report_lease_duration_percentiles.csv` — full distribution
- `report_lease_duration_buckets_low_churn.csv` / `report_lease_duration_percentiles_low_churn.csv` — prefixes with ≤ 4 transitions only
- `report_churn_frequency.csv` — per-prefix transition counts and churn ratios
- `report_intermediary_holds.csv` — hold durations for ping-pong prefixes
- `report_tenant_behaviour.csv` — per-tenant AS aggregated statistics
- `report_landlord_profile.csv` — per-landlord AS lease volume and churn metrics

---

## 5. Execution Sequence
1. **Run `RIPE.ipynb`**: Perform the administrative/BGP inference and validate the "Sublet Your Subnet" results.
2. **Run `bgp_data_record.py`**: Extract the high-resolution transition events.
3. **Run `lease_time_analysis.py`**: Generate duration and frequency statistics.
4. **Run `decay_compute.py`**: Calculate the reputation lag using AbuseIPDB correlations.