-- Creates the warehouse_orders table and loads the file NiFi wrote to HDFS.
-- Run with: hive -f create_tables.sql

SET hive.cli.print.header=true;

-- drop first so the script can be re-run
DROP TABLE IF EXISTS warehouse_orders;

CREATE TABLE warehouse_orders (
    order_id         STRING  COMMENT 'Unique order identifier, e.g. ORD100123',
    order_date       DATE    COMMENT 'Date the order was released to the warehouse (yyyy-MM-dd)',
    order_dow        STRING  COMMENT 'Day of week the order was released (Mon..Sun)',
    warehouse_id     STRING  COMMENT 'Shipping facility code (SDF1, SDF2, CVG1, IND1)',
    carrier          STRING  COMMENT 'Outbound carrier (UPS, FedEx, USPS, XPO, Estes)',
    service_level    STRING  COMMENT 'Standard, Expedited (parcel) or Freight (LTL)',
    dest_region      STRING  COMMENT 'Destination region of the customer',
    distance_miles   INT     COMMENT 'Shipping distance from warehouse to customer',
    order_lines      INT     COMMENT 'Number of distinct SKU lines on the order',
    total_units      INT     COMMENT 'Total units picked across all lines',
    weight_lbs       DOUBLE  COMMENT 'Total shipment weight in pounds',
    pick_minutes     DOUBLE  COMMENT 'Minutes spent picking and packing the order',
    promised_days    INT     COMMENT 'Transit days promised to the customer',
    peak_season      INT     COMMENT '1 if ordered in November or December, else 0',
    inventory_short  INT     COMMENT '1 if any line was short at time of pick, else 0',
    is_late          INT     COMMENT 'Label: 1 = delivered after the promised date, 0 = on time'
)
COMMENT 'Outbound warehouse orders with on-time / late delivery outcome'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- note: this moves the file out of /project/raw into the Hive warehouse folder
LOAD DATA INPATH '/project/raw/warehouse_orders.csv' INTO TABLE warehouse_orders;

-- should return 3000
SELECT COUNT(*) AS row_count FROM warehouse_orders;
