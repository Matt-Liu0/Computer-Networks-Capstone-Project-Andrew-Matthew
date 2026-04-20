import pandas as pd

BGP_FILE = "lease_start_events.csv"
df = pd.read_csv(BGP_FILE)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values(["prefix", "timestamp"], kind="mergesort").reset_index(drop=True)

for threshold in [0, 1/60, 5/60, 15/60, 0.5, 1, 6]:
    surviving           = 0
    potential_bad_actors = set()

    for prefix, group in df.groupby("prefix"):
        group             = group.reset_index(drop=True)
        original_landlord = group.iloc[0]["landlord_as"]

        # Collapse consecutive duplicate tenant rows
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

        # Count prefix as surviving only if at least one transition qualified
        if qualified_transitions > 0:
            surviving += 1

    print(f"Threshold {threshold:>2}hr | "
          f"Surviving Prefixes: {surviving:>5,} | "
          f"Unique Suspect ASNs: {len(potential_bad_actors)}")