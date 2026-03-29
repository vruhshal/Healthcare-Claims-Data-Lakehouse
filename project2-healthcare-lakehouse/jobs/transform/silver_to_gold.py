"""
silver_to_gold.py
PySpark job: aggregates silver claims into gold-layer business metrics.
This is what analysts query — pre-materialised, fast, no PHI.

Run:
  spark-submit \
    --packages io.delta:delta-core_2.12:2.4.0 \
    silver_to_gold.py \
    --input s3://bucket/silver/claims/ \
    --output s3://bucket/gold/
"""

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def create_spark():
    return (
        SparkSession.builder
        .appName("HealthcareClaimsSilverToGold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def build_gold_claims_summary(df):
    """
    Gold table 1: Claims summary by diagnosis, provider, month.
    Used for: cost analysis, utilisation reports, payer dashboards.
    """
    return (
        df.groupBy(
            F.date_trunc("month", "service_date").alias("service_month"),
            "diagnosis_code",
            "diagnosis_description",
            "provider_name",
            "insurance_plan",
            "claim_status",
        )
        .agg(
            F.count("claim_id").alias("claim_count"),
            F.countDistinct("patient_id").alias("unique_patients"),
            F.sum("billed_amount").alias("total_billed"),
            F.sum("approved_amount").alias("total_approved"),
            F.avg("approved_amount").alias("avg_approved"),
            F.sum("patient_responsibility").alias("total_patient_responsibility"),
            F.sum("is_denied").alias("denied_count"),
            F.round(
                F.sum("is_denied") / F.count("claim_id") * 100, 2
            ).alias("denial_rate_pct"),
        )
    )


def build_gold_provider_scorecard(df):
    """
    Gold table 2: Provider-level scorecard.
    Used for: network quality, contracting, audit.
    """
    return (
        df.groupBy("provider_name", "provider_npi")
        .agg(
            F.count("claim_id").alias("total_claims"),
            F.countDistinct("patient_id").alias("total_patients"),
            F.sum("billed_amount").alias("total_billed"),
            F.avg("billed_amount").alias("avg_claim_amount"),
            F.sum("is_denied").alias("total_denials"),
            F.round(
                F.sum("is_denied") / F.count("claim_id") * 100, 2
            ).alias("denial_rate_pct"),
            F.countDistinct("diagnosis_code").alias("unique_diagnoses_treated"),
        )
        .orderBy(F.col("total_claims").desc())
    )


def build_gold_diagnosis_trends(df):
    """
    Gold table 3: Diagnosis code trends over time.
    Used for: population health, disease management, forecasting.
    """
    return (
        df.groupBy(
            F.year("service_date").alias("year"),
            F.month("service_date").alias("month"),
            "diagnosis_code",
            "diagnosis_description",
        )
        .agg(
            F.count("claim_id").alias("claim_count"),
            F.countDistinct("patient_id").alias("patient_count"),
            F.sum("billed_amount").alias("total_cost"),
            F.avg("billed_amount").alias("avg_cost_per_claim"),
        )
        .orderBy("year", "month", F.col("claim_count").desc())
    )


def write_gold(df, path: str, name: str):
    log.info(f"Writing {name} to {path}...")
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    log.info(f"{name} written — {df.count()} rows")


def main(silver_path: str, gold_base: str):
    spark = create_spark()

    log.info(f"Loading silver from {silver_path}")
    df = spark.read.format("delta").load(silver_path)
    log.info(f"Silver rows: {df.count()}")

    # Build and write all three gold tables
    gold_summary   = build_gold_claims_summary(df)
    gold_providers = build_gold_provider_scorecard(df)
    gold_trends    = build_gold_diagnosis_trends(df)

    write_gold(gold_summary,   f"{gold_base}/claims_summary",    "claims_summary")
    write_gold(gold_providers, f"{gold_base}/provider_scorecard","provider_scorecard")
    write_gold(gold_trends,    f"{gold_base}/diagnosis_trends",  "diagnosis_trends")

    spark.stop()
    log.info("silver_to_gold complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  required=True, help="Silver Delta path")
    parser.add_argument("--output", required=True, help="Gold base path")
    args = parser.parse_args()
    main(args.input, args.output)
