# Generates the warehouse_orders.csv dataset used in this project.
# Usage: python generate_orders.py warehouse_orders.csv
import numpy as np, pandas as pd, sys
rng = np.random.default_rng(650)
N = 3000
dates = pd.to_datetime("2025-01-01") + pd.to_timedelta(rng.integers(0, 365, N), unit="D")
wh = rng.choice(["SDF1", "SDF2", "CVG1", "IND1"], N, p=[.35, .25, .22, .18])
carrier = rng.choice(["UPS", "FedEx", "USPS", "XPO", "Estes"], N, p=[.32, .28, .18, .12, .10])
region = rng.choice(["Midwest", "Southeast", "Northeast", "Southwest", "West"], N, p=[.3, .25, .2, .13, .12])
dist_base = {"Midwest": 250, "Southeast": 450, "Northeast": 650, "Southwest": 900, "West": 1700}
distance = np.array([max(15, rng.normal(dist_base[r], dist_base[r] * .3)) for r in region]).round(0)
ltl = np.isin(carrier, ["XPO", "Estes"])
order_lines = np.where(ltl, rng.integers(8, 40, N), rng.integers(1, 12, N))
units = (order_lines * rng.uniform(1, 6, N)).round().astype(int) + 1
weight = np.where(ltl, rng.uniform(300, 2500, N), units * rng.uniform(0.4, 3.5, N)).round(1)
service = np.where(rng.random(N) < .2, "Expedited", "Standard")
service[ltl] = "Freight"
promised = np.where(service == "Expedited", rng.integers(1, 3, N), np.where(service == "Freight", rng.integers(3, 7, N), rng.integers(2, 6, N)))
pick_minutes = (order_lines * rng.uniform(1.5, 3.5, N) + rng.normal(5, 2, N)).clip(2).round(1)
dow = dates.day_name().str[:3]
month = dates.month
peak = np.isin(month, [11, 12]).astype(int)
inv_short = (rng.random(N) < .07).astype(int)

# lateness score: higher means more likely to be late
carrier_eff = {"UPS": -.2, "FedEx": 0.0, "USPS": .45, "XPO": .3, "Estes": .55}
wh_eff = {"SDF1": -.15, "SDF2": .1, "CVG1": .0, "IND1": .35}
z = (-2.1 + np.vectorize(carrier_eff.get)(carrier) + np.vectorize(wh_eff.get)(wh)
     + 0.0011 * distance - 0.35 * promised + 0.012 * pick_minutes
     + 1.0 * peak + 1.6 * inv_short + 0.5 * (dow == "Fri") + 0.35 * (service == "Expedited")
     + 1.3 * ((distance / promised) > 300)          # promise too aggressive for the lane
     + 0.8 * peak * (wh == "IND1")                  # IND1 struggles in peak season
     + 0.7 * (carrier == "Estes") * (distance > 900) # LTL long-haul delays
     + rng.normal(0, .35, N))
z += 0.2  # shift base rate
p = 1 / (1 + np.exp(-z))
is_late = (rng.random(N) < p).astype(int)

df = pd.DataFrame({
    "order_id": [f"ORD{100000 + i}" for i in range(N)],
    "order_date": dates.strftime("%Y-%m-%d"),
    "order_dow": dow,
    "warehouse_id": wh, "carrier": carrier, "service_level": service, "dest_region": region,
    "distance_miles": distance.astype(int), "order_lines": order_lines, "total_units": units,
    "weight_lbs": weight, "pick_minutes": pick_minutes, "promised_days": promised,
    "peak_season": peak, "inventory_short": inv_short, "is_late": is_late,
}).sort_values("order_date").reset_index(drop=True)
df.to_csv(sys.argv[1], index=False)
print(f"wrote {len(df)} rows, late rate {df.is_late.mean():.1%}")
