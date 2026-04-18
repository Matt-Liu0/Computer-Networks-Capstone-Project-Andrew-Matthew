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

While Phase 1 identifies *who* is leasing, Phase 2 identifies *exactly when* these leases occur. This script monitors the BGP stream for origin transitions.

### A. Temporal Scopes
* **Lease Duration Investigation (3-Day Window):** Conducted from **April 1 to April 3, 2026**. This "idealistic" short window provides a high-resolution snapshot of market frequency and initial lease churn.
* **Abuse & Reputation Analysis (30-Day Window):** A longer window is used to provide enough time for malicious activity to be detected and updated on global blocklists.

### B. Execution
1. **Dependency:** `pip install pybgpstream`
2. **Setup:** Configure `START_TIME` and `END_TIME` within the script.
3. **Run:** `python bgp_data_record.py`
4. **Output:** Generates `lease_start_events.csv`, capturing the exact moment an IP block moves from a Landlord to a Tenant.

---

## 4. Phase 3: Extension & Computation

The final phase uses the data from the previous steps to compute our specific research extensions.

### A. Lease Time Analysis (`lease_time_analysis.py`)
Processes the extracted BGP events to characterize market behavior:
* **Frequency Distribution:** Identifies "IP flipping" by tracking how often specific blocks change origin.
* **Duration Distribution:** A histogram revealing the "long tail" of holding periods (from days to years).
* **Survival Analysis:** Uses **Kaplan-Meier Curves** to show the probability of a lease remaining static over time.

### B. Reputation Decay Compute (`decay_compute.py`) (Theoretical using more computation power)
Correlates the inferred lease events with **AbuseIPDB** data:
* **Decay Delta:** Measures the time between the BGP lease timestamp and the blocklist update.
* **Insight:** Quantifies how long an IP remains "toxic" after a tenant change, identifying potential false positives for new lessees.

---

## 5. Execution Sequence
1. **Run `RIPE.ipynb`**: Perform the administrative/BGP inference and validate the "Sublet Your Subnet" results.
2. **Run `bgp_data_record.py`**: Extract the high-resolution transition events.
3. **Run `lease_time_analysis.py`**: Generate duration and frequency statistics.
4. **Run `decay_compute.py`**: Calculate the reputation lag using AbuseIPDB correlations.