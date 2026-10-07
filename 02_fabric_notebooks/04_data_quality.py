# 04 data quality - run checks, fail the run if a critical check fails
# %%
from pyspark.sql import functions as F
from datetime import datetime, timezone

TABLES = ("abfss://<WORKSPACE_NAME>@onelake.dfs.fabric.microsoft.com/"
          "<LAKEHOUSE_NAME>.Lakehouse/Tables")   # TODO: your lakehouse
RUN_AT = datetime.now(timezone.utc)
results = []          # (check_name, table, status, observed, threshold, severity)

def load(name): return spark.read.format("delta").load(f"{TABLES}/{name}")

def run_check(name, table, observed, threshold, severity="critical", invert=False):
    """Record one check. Pass = observed <= threshold (or >= when invert=True,
    e.g. freshness/min-count checks). Returns True when the check passes."""
    passed = (observed >= threshold) if invert else (observed <= threshold)
    results.append((name, table, "PASS" if passed else "FAIL",
                    float(observed), float(threshold), severity))
    print(f"{'PASS' if passed else 'FAIL'}  {name:<46} observed={observed} threshold={threshold}")
    return passed

def count_where(table, condition):
    return load(table).filter(condition).count()

# %%
bronze_prod  = load("bronze_production_runs")
silver_prod  = load("silver_production_runs")
bronze_lines = load("bronze_sales_order_lines")
silver_lines = load("silver_sales_order_lines")
silver_pos   = load("silver_retail_pos")

_max_ingested = bronze_prod.agg(F.max("_ingested_at")).first()[0]
if _max_ingested.tzinfo is None:  # Spark returns offset-naive timestamps
    _max_ingested = _max_ingested.replace(tzinfo=timezone.utc)
freshness_h = (RUN_AT - _max_ingested).total_seconds() / 3600
run_check("DQ-04 freshness: bronze_production_runs < 26h", "bronze_production_runs",
          round(freshness_h, 2), 26, invert=False)

quarantine_n = load("silver_quarantine").filter(F.col("run_id").isNotNull()).count() if \
    __import__("delta").tables.DeltaTable.isDeltaTable(spark, f"{TABLES}/silver_quarantine") else 0
dup_removed = bronze_prod.count() - bronze_prod.dropDuplicates(["run_id"]).count()
recon_gap = abs(bronze_prod.count() - (silver_prod.count() + quarantine_n + dup_removed))
run_check("reconciliation: bronze = silver + quarantine + duplicates removed",
          "silver_production_runs", recon_gap, 0)

dup_runs = (silver_prod.groupBy("run_id").count()
                       .filter("count > 1").count())
run_check("DQ-01 duplicates: production_runs.run_id unique in Silver",
          "silver_production_runs", dup_runs, 0)

orphan_products = silver_prod.join(load("dim_product").filter("is_current"),
                                   "product_id", "left_anti").count()
dim_employee_df = load("dim_employee")
orphan_operators = silver_prod.join(
    dim_employee_df,
    silver_prod["operator_id"] == dim_employee_df["employee_id"],
    "left_anti").count()
run_check("DQ-03 referential integrity: production_runs -> DimProduct/DimEmployee",
          "silver_production_runs", orphan_products + orphan_operators, 0)

neg = (count_where("silver_production_runs", "good_qty < 0 OR scrap_qty < 0")
       + silver_pos.filter("sold_qty < 0").count())
run_check("DQ-05 business rule: non-negative production/POS quantities",
          "silver_production_runs", neg, 0)

null_rate = silver_prod.filter("scrap_qty IS NULL").count() / max(silver_prod.count(), 1)
run_check("DQ-02 null threshold: scrap_qty nulls <= 0.1% in Silver",
          "silver_production_runs", round(null_rate, 5), 0.001)

unparsed_pos_dates = silver_pos.filter("sale_date IS NULL").count()
unmapped_categories = load("silver_product_master").filter("category IS NULL").count()
run_check("DQ-06/DQ-07 conformance: retail dates parsed, Excel categories mapped",
          "silver_retail_pos", unparsed_pos_dates + unmapped_categories, 0)

if __import__("delta").tables.DeltaTable.isDeltaTable(spark, f"{TABLES}/FactProduction"):
    grain_dupes = (load("FactProduction").groupBy("run_id").count()
                                         .filter("count > 1").count())
    run_check("gold grain: FactProduction 1 row per run_id", "FactProduction",
              grain_dupes, 0)
else:
    print("SKIP  gold grain: FactProduction not built yet (validated after 03)")

# %%
dq = spark.createDataFrame(
    [(n, t, s, o, th, sev, RUN_AT) for (n, t, s, o, th, sev) in results],
    ["check_name", "table_name", "status", "observed", "threshold", "severity", "run_at"])
(dq.write.format("delta").mode("append").save(f"{TABLES}/dq_results"))
display(dq.orderBy("status", "check_name"))

critical_failures = [r for r in results if r[2] == "FAIL" and r[5] == "critical"]
if critical_failures:
    bad = silver_prod.join(load("dim_product").filter("is_current"),
                           "product_id", "left_anti")
    if bad.count() > 0:
        (bad.withColumn("_quarantine_reason", F.lit("dq: referential integrity failure"))
            .withColumn("_quarantined_at", F.current_timestamp())
            .write.format("delta").mode("append").save(f"{TABLES}/silver_quarantine"))
    raise RuntimeError("CRITICAL DQ FAILURES: "
                       + "; ".join(f"{r[0]} (observed={r[3]}, threshold={r[4]})"
                                   for r in critical_failures))
print(f"DQ complete: {sum(1 for r in results if r[2] == 'PASS')}/{len(results)} checks passed")
