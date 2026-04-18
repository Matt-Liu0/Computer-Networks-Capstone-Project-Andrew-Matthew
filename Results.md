## Extension Results: IP Leasing & BGP Churn Analysis

We analyzed the distribution of time between leasing events using BGP churn data. The results are categorized into how often these events occur and how long the leases actually last.

---

### 1. Frequency Analysis (The "How Often")
This section tracks the velocity of the IP market and identifies patterns in broker activity.

* **Arrival Rate (Time-Series):** A line graph tracking new leases or transfers per month/quarter. 
    * *Insight:* Identifies "boom" periods and market volatility.
* **Update Frequency per Asset (Histogram):** Measures how many times a single IP block (`inetnum`) has been updated. 
    * *Insight:* High-frequency spikes often signal "flipping" behavior by brokers.
* **Activity Heatmap (Grid):** A visual breakdown of broker activity over time.
    * *Insight:* Reveals seasonal trends or specific periods of high broker engagement.

---

### 2. Duration Analysis (The "How Long")
Duration analysis measures the **holding period**—the time an assignment remains static before being transferred or updated.

* **Lease Duration Distribution (Histogram):** A count of leases categorized by timeframes (e.g., 0–6 months, 1–3 years). 
    * *Insight:* Typically shows a "long tail" where the majority of leases are short-term.
* **Holding Periods by Broker (Box Plots):** Compares the lifecycle of leases across different brokers.
    * *Insight:* Distinguishes brokers specializing in temporary "short-term" leases versus long-term infrastructure transfers.
* **Survival Analysis (Kaplan-Meier Curve):** The statistical "gold standard" for this data, showing the probability that a lease remains unchanged over a specific timeline.



> **Note:** The "Survival" curve is particularly useful for predicting the churn rate of the current IP pool based on historical decay.