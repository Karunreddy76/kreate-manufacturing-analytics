# 01 bronze ingest - load raw files into lakehouse tables, no cleaning here
# %%
# parameters (Fabric parameter cell)
batch_date   = "2026-09-30"          # pipeline passes the business date being loaded
batch_id     = None                  # pipeline may pass its own run id; else generated below
fail_on_empty_file = True            # an empty extract is a FAILED run, not a successful no-op

# fabric OneLake locations
ONELAKE = ("abfss://<WORKSPACE_NAME>@onelake.dfs.fabric.microsoft.com/"
           "<LAKEHOUSE_NAME>.Lakehouse")
FILES = f"{ONELAKE}/Files"          # landing zone written by Copy activities
TABLES = f"{ONELAKE}/Tables"        # Lakehouse Delta tables (Bronze/Silver/Gold)

if batch_id is None:
    from datetime import datetime, timezone
    batch_id = f"{batch_date.replace('-', '')}-{datetime.now(timezone.utc):%H%M%S}"
print(f"batch_id={batch_id}  batch_date={batch_date}")

# %%
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime, timezone

RUN_STARTED_AT = datetime.now(timezone.utc)
STATUS = "RUNNING"

def now_utc():
    return datetime.now(timezone.utc)

# pipeline_run_log: created once, appended every run
LOG_SCHEMA = StructType([
    StructField("batch_id", StringType()), StructField("notebook", StringType()),
    StructField("batch_date", StringType()), StructField("status", StringType()),
    StructField("table_name", StringType()), StructField("rows_loaded", StringType()),
    StructField("message", StringType()), StructField("started_at", TimestampType()),
    StructField("finished_at", TimestampType()),
])
LOG_PATH = f"{TABLES}/pipeline_run_log"

def log_run(status, table_name="", rows_loaded="", message=""):
    row = [(batch_id, "01_bronze_ingest", batch_date, status, table_name,
            str(rows_loaded), message[:2000], RUN_STARTED_AT, now_utc())]
    (spark.createDataFrame(row, LOG_SCHEMA)
          .write.format("delta").mode("append").save(LOG_PATH))

log_run("RUNNING", message="Bronze ingest started")

# %%
SOURCES = [
# sQL Server ERP extracts (landed by Copy activity as CSV)
    dict(table="bronze_plants",              system="sql_server_erp", kind="csv",  path=f"{FILES}/sql_server/plants.csv"),
    dict(table="bronze_products",            system="sql_server_erp", kind="csv",  path=f"{FILES}/sql_server/products.csv"),
    dict(table="bronze_product_price_history", system="sql_server_erp", kind="csv", path=f"{FILES}/sql_server/product_price_history.csv"),
    dict(table="bronze_machines",            system="sql_server_erp", kind="csv",  path=f"{FILES}/sql_server/machines.csv"),
    dict(table="bronze_work_orders",         system="sql_server_erp", kind="csv",  path=f"{FILES}/sql_server/work_orders.csv"),
    dict(table="bronze_production_runs",     system="sql_server_erp", kind="csv",  path=f"{FILES}/sql_server/production_runs.csv"),
    dict(table="bronze_quality_inspections", system="sql_server_erp", kind="csv",  path=f"{FILES}/sql_server/quality_inspections.csv"),
    dict(table="bronze_sales_orders",        system="sql_server_erp", kind="csv",  path=f"{FILES}/sql_server/sales_orders.csv"),
    dict(table="bronze_sales_order_lines",   system="sql_server_erp", kind="csv",  path=f"{FILES}/sql_server/sales_order_lines.csv"),
    dict(table="bronze_shipments",           system="sql_server_erp", kind="csv",  path=f"{FILES}/sql_server/shipments.csv"),
    dict(table="bronze_inventory_daily",     system="sql_server_erp", kind="csv",  path=f"{FILES}/sql_server/inventory_daily.csv"),
    dict(table="bronze_hr_employees",   system="excel_hr",       kind="xlsx", path=f"{FILES}/excel/hr_employees.xlsx",
         data_address="'Employees'!A3:H10000"),
    dict(table="bronze_plant_targets",  system="excel_finance",  kind="xlsx", path=f"{FILES}/excel/plant_targets.xlsx",
         data_address="'Plant Targets FY25-26'!A3:E10000"),
    dict(table="bronze_product_master", system="excel_business", kind="xlsx", path=f"{FILES}/excel/product_master.xlsx",
         data_address="'Products'!A1:E10000"),
# retail POS feed: 18 monthly files, one Bronze table
    dict(table="bronze_retail_pos",     system="retail_feed",    kind="csv",  path=f"{FILES}/retail_feed/retail_pos_*.csv"),
]

# %%
def read_extract(src):
    """Read one landed extract. Bronze reads EVERYTHING as string on purpose:
    type-casting dirty source values is a Silver decision (02), so a malformed
    value can never crash ingestion or get silently coerced here."""
    if src["kind"] == "csv":
        return (spark.read.option("header", True).option("inferSchema", False)
                      .csv(src["path"]))
    if src["kind"] == "xlsx":
        return (spark.read.format("excel").option("header", True)
                      .option("inferSchema", False)
                      .option("dataAddress", src["data_address"])
                      .load(src["path"]))
    raise ValueError(f"Unknown extract kind: {src['kind']}")

def add_audit_columns(df, src):
    df = (df.withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_source_system", F.lit(src["system"]))
            .withColumn("_source_file", F.lit(src["path"].split("/")[-1]))
            .withColumn("_ingested_at", F.current_timestamp()))
    if "*" in src["path"]:
        df = df.withColumn("_source_file",
                           F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))
    return df

# %%
loaded, failures = {}, []
for src in SOURCES:
    try:
        df = add_audit_columns(read_extract(src), src)
        n = df.count()
        if fail_on_empty_file and n == 0:
            raise RuntimeError(f"Empty extract: {src['path']}")
        writer = df.write.format("delta").mode("append")
        if src["kind"] == "xlsx":
            writer = writer.option("delta.columnMapping.mode", "name")
        writer.save(f"{TABLES}/{src['table']}")
        loaded[src["table"]] = n
        log_run("SUCCESS", src["table"], n, f"Loaded from {src['path']}")
        print(f"OK   {src['table']:<28} {n:>8,} rows")
    except Exception as e:                       # log, remember, keep triaging
        failures.append((src["table"], str(e)))
        log_run("FAILED", src["table"], "", str(e))
        print(f"FAIL {src['table']:<28} {e}")

if failures:
    raise RuntimeError(f"Bronze ingest failed for: {[t for t, _ in failures]}")

log_run("SUCCESS", message=f"Bronze ingest complete: {len(loaded)} tables, "
                           f"{sum(loaded.values()):,} rows")
print(f"\nBronze complete: {len(loaded)} tables, {sum(loaded.values()):,} rows, batch {batch_id}")

# %%
display(spark.read.format("delta").load(LOG_PATH)
        .filter(F.col("batch_id") == batch_id)
        .orderBy("finished_at"))
