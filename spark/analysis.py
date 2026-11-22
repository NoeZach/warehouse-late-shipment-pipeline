"""
analysis.py
Reads warehouse_orders from Hive, trains Logistic Regression and Random Forest
models to predict late orders, evaluates them, writes the metrics to HBase
and reads them back.

Run from the spark folder:
    spark-submit --master yarn --deploy-mode client --name WarehouseLateShipment_ML --py-files processing.py analysis.py
"""

import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

from processing import load_orders, add_features, add_class_weights, build_feature_stages, profile, HIVE_TABLE

HBASE_HOST = "master"                    # Docker service name of the HBase Thrift server
HBASE_TABLE = "shipment_model_metrics"   # created beforehand in the HBase shell
SEED = 42


def evaluate(predictions):
    """Compute classification metrics for one model's test-set predictions."""
    multi = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    binary = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    return {
        "accuracy": multi.evaluate(predictions, {multi.metricName: "accuracy"}),
        "f1": multi.evaluate(predictions, {multi.metricName: "f1"}),
        "weighted_precision": multi.evaluate(predictions, {multi.metricName: "weightedPrecision"}),
        "weighted_recall": multi.evaluate(predictions, {multi.metricName: "weightedRecall"}),
        # precision / recall for the class we care about: late orders (label 1)
        "late_precision": multi.evaluate(predictions, {multi.metricName: "precisionByLabel", multi.metricLabel: 1.0}),
        "late_recall": multi.evaluate(predictions, {multi.metricName: "recallByLabel", multi.metricLabel: 1.0}),
        "auc_roc": binary.evaluate(predictions, {binary.metricName: "areaUnderROC"}),
    }


def feature_names(df, col="features"):
    """Recover readable feature names from the VectorAssembler metadata."""
    attrs = df.schema[col].metadata["ml_attr"]["attrs"]
    names = {}
    for group in attrs.values():
        for a in group:
            names[a["idx"]] = a["name"]
    return [names[i] for i in sorted(names)]


def write_to_hbase_partition(partition):
    """Runs on the executors: open one Thrift connection per partition and put each cell."""
    import happybase  # imported here so it resolves on the worker nodes
    connection = happybase.Connection(HBASE_HOST)
    connection.open()
    table = connection.table(HBASE_TABLE)
    for row_key, column, value in partition:
        table.put(row_key, {column: value})
    connection.close()


def read_back_from_hbase(row_keys):
    """Runs on the driver: fetch the rows just written to prove they persisted."""
    import happybase
    connection = happybase.Connection(HBASE_HOST)
    connection.open()
    table = connection.table(HBASE_TABLE)
    print("\n================ METRICS READ BACK FROM HBASE ================")
    for key in row_keys:
        row = table.row(key)
        print(f"row: {key}")
        for col in sorted(row):
            print(f"   {col.decode():<28} = {row[col].decode()}")
    connection.close()


def main():
    start = time.time()
    spark = (SparkSession.builder
             .appName("WarehouseLateShipment_ML")
             .enableHiveSupport()
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    run_ts = datetime.now().strftime("%Y%m%d%H%M%S")

    # ---- Step 1-2: load from Hive and prepare features --------------------
    print(f"\nReading Hive table '{HIVE_TABLE}'")
    df = add_class_weights(add_features(load_orders(spark))).cache()
    profile(df)

    train, test = df.randomSplit([0.7, 0.3], seed=SEED)
    n_train, n_test = train.count(), test.count()
    print(f"Train rows: {n_train}   Test rows: {n_test}")

    # ---- Step 3: define and train the two models --------------------------
    models = {
        "logistic_regression": LogisticRegression(
            featuresCol="features", labelCol="label", weightCol="class_weight",
            maxIter=100, regParam=0.01),
        "random_forest": RandomForestClassifier(
            featuresCol="features", labelCol="label", weightCol="class_weight",
            numTrees=100, maxDepth=8,
            featureSubsetStrategy="sqrt", seed=SEED),
    }

    results, fitted = {}, {}
    for name, estimator in models.items():
        print(f"\n---------------- Training {name} ----------------")
        t0 = time.time()
        pipeline = Pipeline(stages=build_feature_stages() + [estimator])
        model = pipeline.fit(train)
        preds = model.transform(test).cache()
        metrics = evaluate(preds)
        metrics["train_seconds"] = round(time.time() - t0, 2)
        results[name], fitted[name] = metrics, (model, preds)
        print(f"{name} trained in {metrics['train_seconds']} s")
        print("Confusion matrix (rows = actual, columns = predicted):")
        preds.groupBy("label").pivot("prediction", [0.0, 1.0]).count().na.fill(0).orderBy("label").show()

    # ---- Step 4: evaluation summary ----------------------------------------
    print("\n==================== MODEL EVALUATION (test set) ====================")
    keys = ["accuracy", "f1", "weighted_precision", "weighted_recall",
            "late_precision", "late_recall", "auc_roc"]
    print(f"{'metric':<22}{'logistic_regression':>22}{'random_forest':>18}")
    for k in keys:
        print(f"{k:<22}{results['logistic_regression'][k]:>22.4f}{results['random_forest'][k]:>18.4f}")

    rf_model, rf_preds = fitted["random_forest"]
    importances = rf_model.stages[-1].featureImportances.toArray()
    names = feature_names(rf_preds)
    top = sorted(zip(names, importances), key=lambda x: -x[1])[:5]
    print("\nTop 5 Random Forest feature importances:")
    for n, v in top:
        print(f"   {n:<34}{v:.4f}")

    # ---- Step 5: write metrics to HBase ------------------------------------
    rows, row_keys = [], []
    for name, m in results.items():
        key = f"{name}_{run_ts}"
        row_keys.append(key)
        for k in keys:
            rows.append((key, f"metrics:{k}", f"{m[k]:.4f}"))
        rows += [
            (key, "run_info:algorithm", name),
            (key, "run_info:hive_table", HIVE_TABLE),
            (key, "run_info:train_rows", str(n_train)),
            (key, "run_info:test_rows", str(n_test)),
            (key, "run_info:train_seconds", str(m["train_seconds"])),
            (key, "run_info:run_timestamp", run_ts),
        ]
    rows.append((f"random_forest_{run_ts}", "run_info:top_features",
                 ", ".join(f"{n}={v:.3f}" for n, v in top[:3])))

    print(f"\nWriting {len(rows)} cells ({len(row_keys)} rows) to HBase table '{HBASE_TABLE}'")
    spark.sparkContext.parallelize(rows, 2).foreachPartition(write_to_hbase_partition)

    # ---- Step 6: retrieve the stored metrics -------------------------------
    read_back_from_hbase(row_keys)

    print(f"\nPipeline finished in {time.time() - start:.1f} s")
    spark.stop()


if __name__ == "__main__":
    main()
