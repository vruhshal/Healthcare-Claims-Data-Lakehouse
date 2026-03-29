"""
bronze_to_silver.py
PySpark job: reads raw FHIR claims from bronze layer,
cleans, deduplicates, masks PHI, and writes to silver Delta Lake.

Run:
  spark-submit \
    --packages io.delta:delta-core_2.12:2.4.0 \
    bronze_to_silver.py \
    --input s3://bucket/bronze/claims/ \
    --output s3://bucket/silver/claims/
"""

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def create_spark():
    return (
        SparkSession.builder
        .appName("HealthcareClaimsBronzeToSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def load_bronze(spark, bronze_path: str):
    log.info(f"Loading bronze data from {bronze_path}")
    df = spark.read.option("multiline", "true").json(bronze_path)
    log.info(f"Bronze row count: {df.count()}")
    return df


def clean_and_transform(df):
    """
    Silver layer transformations:
    1. Cast and parse types
    2. Deduplicate on claim_id
    3. Mask PHI fields
    4. Validate critical columns
    5. Standardise codes
    """
    log.info("Applying silver transformations...")

    df = (
        df
        # ── Type casting ───────────────────────────────────────────
        .withColumn("service_date",       F.to_date("service_date", "yyyy-MM-dd"))
        .withColumn("billed_amount",      F.col("billed_amount").cast(DoubleType()))
        .withColumn("approved_amount",    F.col("approved_amount").cast(DoubleType()))
        .withColumn("ingested_at",        F.to_timestamp("_ingested_at"))

        # ── PHI masking (HIPAA compliance) ─────────────────────────
        # In production, real patient names/DOB come from source;
        # here we redact them before they reach silver.
        .withColumn("patient_name",       F.lit("REDACTED"))
        .withColumn("patient_dob",        F.lit("REDACTED"))

        # ── Code standardisation ───────────────────────────────────
        .withColumn("diagnosis_code",     F.trim(F.upper(F.col("diagnosis_code"))))
        .withColumn("procedure_code",     F.trim(F.upper(F.col("procedure_code"))))
        .withColumn("claim_status",       F.upper(F.col("status")))

        # ── Derived columns ────────────────────────────────────────
        .withColumn("patient_responsibility",
                    F.col("billed_amount") - F.coalesce(F.col("approved_amount"), F.lit(0.0)))
        .withColumn("is_denied",
                    F.when(F.col("status") == "DENIED", 1).otherwise(0))
        .withColumn("service_year",       F.year("service_date"))
        .withColumn("service_month",      F.month("service_date"))

        # ── Filter out clearly bad rows ────────────────────────────
        .filter(F.col("id").isNotNull())
        .filter(F.col("billed_amount") > 0)
        .filter(F.col("service_date").isNotNull())
        .filter(F.col("claim_status").isin(
            "APPROVED", "DENIED", "PENDING", "ADJUSTED"
        ))

        # ── Select final columns ───────────────────────────────────
        .select(
            F.col("id").alias("claim_id"),
            F.col("patient_id"),
            "service_date", "service_year", "service_month",
            "provider_name", "provider_npi",
            "diagnosis_code", "diagnosis_description",
            "procedure_code", "procedure_description",
            "claim_status", "is_denied",
            "billed_amount", "approved_amount", "patient_responsibility",
            "insurance_plan",
            "patient_name", "patient_dob",   # redacted
            "ingested_at",
        )
    )

    # ── Deduplicate: keep latest record per claim_id ───────────────────────
    dedup_window = (
        F.row_number()
        .over(
            __import__("pyspark.sql.window", fromlist=["Window"])
            .Window.partitionBy("claim_id")
            .orderBy(F.col("ingested_at").desc())
        )
    )
    df = df.withColumn("_rn", dedup_window).filter(F.col("_rn") == 1).drop("_rn")

    log.info(f"Silver row count after clean: {df.count()}")
    return df


def validate(df):
    """Run data quality assertions before writing."""
    log.info("Running DQ checks...")

    null_claims = df.filter(F.col("claim_id").isNull()).count()
    null_dates   = df.filter(F.col("service_date").isNull()).count()
    neg_billed   = df.filter(F.col("billed_amount") <= 0).count()

    checks = {
        "null_claim_ids":    (null_claims, 0),
        "null_service_dates":(null_dates,  0),
        "negative_amounts":  (neg_billed,  0),
    }

    failed = []
    for name, (actual, expected) in checks.items():
        if actual != expected:
            failed.append(f"{name}: found {actual}, expected {expected}")

    if failed:
        raise ValueError(f"DQ checks failed:\n" + "\n".join(failed))

    total = df.count()
    denied_pct = round(df.filter(F.col("is_denied")==1).count() / total * 100, 2)
    log.info(f"DQ passed — total={total}, denial_rate={denied_pct}%")


def write_silver(df, silver_path: str, spark):
    """
    Write to silver using Delta Lake MERGE.
    Handles late-arriving updates to existing claims.
    """
    log.info(f"Writing silver data to {silver_path}...")

    if DeltaTable.isDeltaTable(spark, silver_path):
        log.info("Delta table exists — running MERGE for upserts...")
        silver_table = DeltaTable.forPath(spark, silver_path)
        (
            silver_table.alias("existing")
            .merge(
                df.alias("updates"),
                "existing.claim_id = updates.claim_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        log.info("No existing table — doing initial write...")
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("service_year", "service_month")
            .save(silver_path)
        )

    log.info("Silver write complete.")


def main(bronze_path: str, silver_path: str):
    spark = create_spark()
    df_bronze = load_bronze(spark, bronze_path)
    df_silver = clean_and_transform(df_bronze)
    validate(df_silver)
    write_silver(df_silver, silver_path, spark)
    spark.stop()
    log.info("bronze_to_silver job done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    main(args.input, args.output)
