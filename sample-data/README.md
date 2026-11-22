# Dataset

[`warehouse_orders.csv`](warehouse_orders.csv) has 3,000 outbound warehouse orders from 2025 and is the file NiFi downloads. I generated it with [`generate_orders.py`](generate_orders.py) (`python generate_orders.py warehouse_orders.csv`), so it doesn't contain any real customer or company data.

| Column | Type | Description |
|---|---|---|
| order_id | string | Order number |
| order_date | date | Date the order was released (yyyy-MM-dd) |
| order_dow | string | Day of the week |
| warehouse_id | string | SDF1, SDF2, CVG1 or IND1 |
| carrier | string | UPS, FedEx, USPS, XPO or Estes |
| service_level | string | Standard, Expedited or Freight |
| dest_region | string | Midwest, Southeast, Northeast, Southwest or West |
| distance_miles | int | Shipping distance |
| order_lines | int | Number of SKU lines |
| total_units | int | Units picked |
| weight_lbs | double | Shipment weight |
| pick_minutes | double | Pick and pack time |
| promised_days | int | Promised transit days |
| peak_season | int | 1 if November or December |
| inventory_short | int | 1 if any line was short at pick |
| is_late | int | 1 if delivered after the promised date (label) |
