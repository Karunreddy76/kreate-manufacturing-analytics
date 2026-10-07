# 03 gold model - build fact and dim tables for power bi
# %%
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, timezone

TABLES = ("abfss://<WORKSPACE_NAME>@onelake.dfs.fabric.microsoft.com/"
          "<LAKEHOUSE_NAME>.Lakehouse/Tables")   # TODO: your lakehouse

def silver(name): return spark.read.format("delta").load(f"{TABLES}/{name}")
def dim(name):    return spark.read.format("delta").load(f"{TABLES}/{name}")

# incremental watermark: last Silver load this notebook has consumed
WM_PATH = f"{TABLES}/gold_watermark"
try:
    last_wm = spark.read.format("delta").load(WM_PATH).first()["watermark"]
except Exception:
    last_wm = None                     # first run: full load
print(f"incremental watermark: {last_wm}")

# %%
def upsert(df, path, keys):
    """Delta MERGE upsert on natural keys. Idempotent: re-running the same
    batch updates in place instead of duplicating facts (safe retries)."""
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").mode("overwrite").save(path); return
    tgt = DeltaTable.forPath(spark, path)
    cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    (tgt.alias("t").merge(df.alias("s"), cond)
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

def incremental(df):
    """On re-runs, only rows ingested after the watermark are re-processed."""
    return df if last_wm is None else df.filter(F.col("_ingested_at") > F.lit(last_wm))

def product_key_at(df, date_col):
    """Resolve the SCD2 `product_key` (and `unit_cost`) effective at `date_col`.

    One row in, one row out: dim_product has exactly one effective version per
    (product_id, date), so this join cannot fan out — asserted on row count.
    This is what makes FactQuality.cost and historical margin reproducible:
    facts carry the product *version* that was live on the fact date, not
    today's price."""
    before = df.count()
    keys = dim("dim_product").select(
        F.col("product_id").alias("_pk_product_id"), "product_key", "unit_cost",
        "effective_from", "effective_to")
    joined = df.join(keys, [
        df["product_id"] == keys["_pk_product_id"],
        df[date_col] >= keys["effective_from"],
        keys["effective_to"].isNull() | (df[date_col] <= keys["effective_to"])],
        "left").drop("_pk_product_id", "effective_from", "effective_to")
    assert joined.count() == before, \
        f"product SCD2 join changed row count {before} -> {joined.count()}"
    return joined

# %%
dim_date = (spark.range(0, 546)  # 2025-01-01 .. 2026-06-30 (fact range)
    .withColumn("date", F.date_add(F.lit("2025-01-01"), F.col("id").cast("int")))
    .select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("date").alias("Date"),
            F.year("date").alias("Year"), F.quarter("date").alias("Quarter"),
            F.month("date").alias("Month"),
            F.date_format("date", "MMMM").alias("MonthName"),
            F.date_format("date", "yyyy-MM").alias("YearMonth"),
            (((F.dayofweek("date") + 5) % 7) + 1).alias("DayOfWeek"),
            F.dayofweek("date").isin(1, 7).alias("IsWeekend")))
dim_date.write.format("delta").mode("overwrite").save(f"{TABLES}/DimDate")

for silver_name, gold_name in [("dim_plant", "DimPlant"), ("dim_machine", "DimMachine"),
                               ("dim_employee", "DimEmployee"), ("dim_retailer", "DimRetailer")]:
    dim(silver_name).write.format("delta").mode("overwrite").save(f"{TABLES}/{gold_name}")
dim("dim_product").write.format("delta").mode("overwrite").save(f"{TABLES}/DimProduct")
print("Gold dimensions published")

# %%
plant_keys   = dim("dim_plant").select("plant_id", "plant_key")
machine_keys = dim("dim_machine").select("machine_id", "machine_key")
employee_keys = dim("dim_employee").select(
    F.col("employee_id").alias("operator_id"), "employee_key")
retailer_keys = dim("dim_retailer").select("retailer_id", "retailer_key")

# factProduction: 1 row per production run
fact_production = (product_key_at(incremental(silver("silver_production_runs")), "run_date")
    .join(plant_keys, "plant_id", "left")
    .join(machine_keys, "machine_id", "left")
    .join(employee_keys, "operator_id", "left")
    .select("run_id", "work_order_id",
            F.date_format("run_date", "yyyyMMdd").cast("int").alias("date_key"),
            "plant_key", "machine_key", "product_key", "employee_key",
            "shift", "planned_qty", "good_qty", "scrap_qty",
            "downtime_minutes", "resin_used_kg", "cycle_time_sec"))
assert fact_production.count() == fact_production.select("run_id").distinct().count()
upsert(fact_production, f"{TABLES}/FactProduction", ["run_id"])

# factQuality: 1 row per quality inspection
fact_quality = (product_key_at(silver("silver_quality_inspections"), "inspection_date")
    .join(plant_keys, "plant_id", "left")
    .select("inspection_id", "run_id",
            F.date_format("inspection_date", "yyyyMMdd").cast("int").alias("date_key"),
            "plant_key", "product_key", "defect_type", "inspected_qty",
            "defect_qty",
            F.round(F.col("defect_qty") * F.col("unit_cost"), 2).alias("cost"),
            "result"))
assert fact_quality.count() == fact_quality.select("inspection_id").distinct().count()
upsert(fact_quality, f"{TABLES}/FactQuality", ["inspection_id"])

fulfilling_plant_keys = dim("dim_plant").select(
    F.col("plant_id").alias("fulfilling_plant_id"), "plant_key")
fact_sales = (product_key_at(silver("silver_sales_order_lines_enriched"), "order_date")
    .join(retailer_keys, "retailer_id", "left")
    .join(fulfilling_plant_keys, "fulfilling_plant_id", "left")
    .select("sales_order_line_id", "sales_order_id",
            F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
            "retailer_key", "product_key", "plant_key",
            "order_date", "promised_date", "ship_date", "delivery_date",
            "ordered_qty", "shipped_qty", "unit_price", "shipment_count",
            F.round(F.col("shipped_qty") * F.col("unit_price"), 2)
                .alias("line_revenue")))
assert fact_sales.count() == fact_sales.select("sales_order_line_id").distinct().count()
upsert(fact_sales, f"{TABLES}/FactSalesShipment", ["sales_order_line_id"])

# factInventoryDaily: 1 row per plant x resin x date (snapshot fact)
fact_inventory = (silver("silver_inventory_daily")
    .join(plant_keys, "plant_id", "left")
    .select(F.date_format("inventory_date", "yyyyMMdd").cast("int").alias("date_key"),
            "inventory_date", "plant_key", "plant_id", "resin_type",
            "on_hand_kg", "consumed_kg", "received_kg", "safety_stock_kg"))
assert fact_inventory.count() == fact_inventory.select(
    "inventory_date", "plant_id", "resin_type").distinct().count()
upsert(fact_inventory, f"{TABLES}/FactInventoryDaily",
       ["inventory_date", "plant_id", "resin_type"])

# factRetailPOS: 1 row per retailer x product x date
pos_grouped = (silver("silver_retail_pos")
    .groupBy("retailer_id", "product_id", "sale_date")
    .agg(F.sum("sold_qty").alias("sold_qty"),
         F.sum("returns_qty").alias("returns_qty"),
         F.max("promo_flag").alias("promo_flag")))
fact_pos = (product_key_at(pos_grouped, "sale_date")
    .join(retailer_keys, "retailer_id", "left")
    .select(F.date_format("sale_date", "yyyyMMdd").cast("int").alias("date_key"),
            "sale_date", "retailer_key", "product_key",
            "sold_qty", "returns_qty", "promo_flag"))
upsert(fact_pos, f"{TABLES}/FactRetailPOS", ["sale_date", "retailer_key", "product_key"])
print("Gold facts upserted")

# %%
# advance the watermark only AFTER every fact succeeded
(spark.createDataFrame([(datetime.now(timezone.utc),)], ["watermark"])
      .write.format("delta").mode("overwrite").save(WM_PATH))
print("watermark advanced; Gold model complete for this batch")

