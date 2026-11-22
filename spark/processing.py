"""
processing.py
Loads warehouse_orders from Hive, cleans it, adds features and builds the
MLlib feature stages used by analysis.py.

Can also be run by itself to check the data:
    spark-submit --master yarn --deploy-mode client processing.py
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

HIVE_TABLE = "warehouse_orders"
LABEL_COL = "is_late"

# Text columns that need to be turned into numbers before modeling.
CATEGORICAL_COLS = ["warehouse_id", "carrier", "service_level", "dest_region", "order_dow"]

# Numeric columns read straight from Hive.
NUMERIC_COLS = [
    "distance_miles", "order_lines", "total_units", "weight_lbs",
    "pick_minutes", "promised_days", "peak_season", "inventory_short",
]

# Columns derived in add_features().
ENGINEERED_COLS = ["order_month", "units_per_line", "miles_per_promised_day"]


def load_orders(spark: SparkSession, table: str = HIVE_TABLE) -> DataFrame:
    """Read the order table from Hive and drop anything unusable for modeling."""
    df = spark.sql(f"""
        SELECT order_id, order_date, order_dow, warehouse_id, carrier,
               service_level, dest_region, distance_miles, order_lines,
               total_units, weight_lbs, pick_minutes, promised_days,
               peak_season, inventory_short, is_late
        FROM {table}
    """)
    # Safety net: older Spark versions ignore Hive's skip.header.line.count,
    # which would surface the CSV header as a data row.
    df = df.filter(F.col("order_id") != "order_id")
    # Rows with a missing label or feature cannot be used for training.
    return df.na.drop(subset=[LABEL_COL] + NUMERIC_COLS + CATEGORICAL_COLS + ["order_date"])


def add_features(df: DataFrame) -> DataFrame:
    """Add a few business-driven features on top of the raw columns."""
    return (
        df.withColumn("order_month", F.month("order_date").cast("double"))
          # bigger, bulkier orders take longer to pick
          .withColumn("units_per_line", F.col("total_units") / F.col("order_lines"))
          # how aggressive the delivery promise is relative to distance
          .withColumn("miles_per_promised_day", F.col("distance_miles") / F.col("promised_days"))
          .withColumn("label", F.col(LABEL_COL).cast("double"))
    )


def add_class_weights(df: DataFrame) -> DataFrame:
    """Weight each row inversely to its class frequency.

    Late orders are the minority class (about one in three), so without
    weights the models lean toward predicting 'on time' and miss late orders.
    """
    late_ratio = df.filter(F.col("label") == 1.0).count() / df.count()
    return df.withColumn(
        "class_weight",
        F.when(F.col("label") == 1.0, 0.5 / late_ratio).otherwise(0.5 / (1 - late_ratio)),
    )


def build_feature_stages():
    """Return the MLlib stages that turn raw columns into a 'features' vector.

    StringIndexer -> OneHotEncoder for each categorical column, then
    VectorAssembler to combine them with the numeric columns.
    handleInvalid='keep' means an unseen category in the test split gets its
    own bucket instead of failing the job.
    """
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL_COLS
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in CATEGORICAL_COLS],
        outputCols=[f"{c}_vec" for c in CATEGORICAL_COLS],
    )
    assembler = VectorAssembler(
        inputCols=[f"{c}_vec" for c in CATEGORICAL_COLS] + NUMERIC_COLS + ENGINEERED_COLS,
        outputCol="features",
        handleInvalid="skip",
    )
    return indexers + [encoder, assembler]


def profile(df: DataFrame) -> None:
    """Print a quick profile of the modeling data (row count and label balance)."""
    total = df.count()
    print(f"\nRows available for modeling: {total}")
    (df.groupBy(LABEL_COL).count()
       .withColumn("pct", F.round(F.col("count") / total * 100, 1))
       .orderBy(LABEL_COL)
       .show())


def main():
    spark = (SparkSession.builder
             .appName("WarehouseOrders_Profile")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    df = add_features(load_orders(spark))
    profile(df)
    df.select(["order_id", "carrier", "warehouse_id"] + ENGINEERED_COLS + [LABEL_COL]).show(5)
    spark.stop()


if __name__ == "__main__":
    main()
