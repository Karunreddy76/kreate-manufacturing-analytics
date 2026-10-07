# 02 silver transform - clean bronze tables, bad rows go to quarantine
# %%
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable   # ships with the Fabric Spark runtime

batch_id = None        # parameter: normally passed from the pipeline
TABLES = ("abfss://<WORKSPACE_NAME>@onelake.dfs.fabric.microsoft.com/"
          "<LAKEHOUSE_NAME>.Lakehouse/Tables")   # TODO: your lakehouse

def bronze(name):
    df = spark.read.format("delta").load(f"{TABLES}/bronze_{name}")
    return df if batch_id is None else df.filter(F.col("_batch_id") == batch_id)

def write_delta(df, name, mode="overwrite"):
    df.write.format("delta").mode(mode).save(f"{TABLES}/{name}")

# %%
def std_code(col):   # ' p01 ' / 'P01' / 'p01' all become 'P01'
    return F.upper(F.trim(col))

def std_name(col):   # collapse internal whitespace, title-case display names
    return F.initcap(F.regexp_replace(F.trim(col), r"\s+", " "))

def parse_mixed_date(col):
    """Sources disagree on date format. Try each known pattern; unparseable
    values become null HERE so the null checks below can quarantine them."""
    return F.coalesce(
        F.to_date(col, "yyyy-MM-dd"), F.to_date(col, "MM/dd/yyyy"),
        F.to_date(col, "dd-MMM-yy"),  F.to_date(col, "dd-MMM-yyyy"),
        F.to_date(col, "yyyy/MM/dd"))

def dedupe(df, natural_keys, order_col="_ingested_at"):
    """Keep exactly one row per natural key — the most recently ingested copy.
    Source systems re-send corrected rows; Bronze keeps them all, Silver picks."""
    w = Window.partitionBy(*natural_keys).orderBy(F.col(order_col).desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1).drop("_rn"))

_quarantine_rows = []
def quarantine(df, reason):
    """Collect rejected rows with the rule that rejected them. Written once at
    the end so quarantine is a single auditable table, not scattered deletes."""
    _quarantine_rows.append(
        df.withColumn("_quarantine_reason", F.lit(reason))
          .withColumn("_quarantined_at", F.current_timestamp()))

def split_valid(df, rule, reason):
    """Returns rows passing `rule`; sends the rest to quarantine."""
    quarantine(df.filter(~rule), reason)
    return df.filter(rule)

# %%
# dimPlant / DimMachine / DimRetailer: clean, dedupe, surrogate key
dim_plant = dedupe(
    bronze("plants").select(
        std_code(F.col("plant_id")).alias("plant_id"),
        std_name(F.col("plant_name")).alias("plant_name"),
        std_name(F.col("city")).alias("city"),
        std_code(F.col("state")).alias("state"),
        F.trim(F.col("timezone")).alias("timezone"),
        F.col("opened_year").cast("int").alias("opened_year"),
        F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
        F.col("_ingested_at")), ["plant_id"])
dim_plant = (dim_plant.withColumn("plant_key", F.md5(F.col("plant_id")))
             .select("plant_key", "plant_id", "plant_name", "city", "state",
                     "timezone", "opened_year"))
plant_keys = dim_plant.select("plant_id", "plant_key")

dim_machine = dedupe(
    bronze("machines").select(
        std_code(F.col("machine_id")).alias("machine_id"),
        std_code(F.col("plant_id")).alias("plant_id"),
        F.trim(F.col("machine_name")).alias("machine_name"),   # keep 'IMM-22' as-is
        F.col("tonnage").cast("int").alias("tonnage"),
        std_name(F.col("manufacturer")).alias("manufacturer"),
        F.col("install_year").cast("int").alias("install_year"),
        F.trim(F.col("status")).alias("status"),
        F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
        F.col("_ingested_at")), ["machine_id"])
dim_machine = (dim_machine.withColumn("machine_key", F.md5(F.col("machine_id")))
               .join(plant_keys, "plant_id", "left")
               .select("machine_key", "machine_id", "machine_name", "plant_key",
                       "plant_id", "tonnage", "manufacturer", "install_year",
                       "status"))

retailer_names = {"R01": "Walmart", "R02": "Target", "R03": "Lowe's",
                  "R04": "Home Depot", "R05": "Costco"}
retailer_ids = dedupe(
    bronze("sales_orders").select(std_code(F.col("retailer_id")).alias("retailer_id"),
                                  F.col("_ingested_at"))
    .unionByName(bronze("retail_pos").select(
        std_code(F.col("retailer_id")).alias("retailer_id"),
        F.col("_ingested_at"))), ["retailer_id"])
retailer_name_map = F.create_map(
    [F.lit(x) for pair in retailer_names.items() for x in pair])
dim_retailer = (retailer_ids
    .withColumn("retailer_name",
                F.coalesce(retailer_name_map.getItem(F.col("retailer_id")),
                           F.col("retailer_id")))
    .withColumn("retailer_key", F.md5(F.col("retailer_id")))
    .select("retailer_key", "retailer_id", "retailer_name"))

for df, name in [(dim_plant, "dim_plant"), (dim_machine, "dim_machine"),
                 (dim_retailer, "dim_retailer")]:
    write_delta(df, name)
print("conformed dims written: dim_plant, dim_machine, dim_retailer")

# %%
TARGET = f"{TABLES}/dim_product"
prod_lookup = dedupe(
    bronze("products").select(
        std_code(F.col("product_id")).alias("product_id"),
        F.trim(F.col("sku")).alias("sku"),
        std_name(F.col("product_name")).alias("product_name"),
        F.trim(F.col("category")).alias("category"),      # clean in the ERP
        std_code(F.col("resin_type")).alias("resin_type"),
        F.col("unit_weight_kg").cast("decimal(6,2)").alias("unit_weight_kg"),
        F.col("_ingested_at")), ["product_id"])

price_hist = dedupe(
    bronze("product_price_history").select(
        std_code(F.col("product_id")).alias("product_id"),
        F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
        F.col("standard_cost").cast("decimal(10,2)").alias("unit_cost"),
        parse_mixed_date(F.col("effective_from")).alias("effective_from"),
        parse_mixed_date(F.col("effective_to")).alias("effective_to"),
        (F.col("is_current").cast("int").cast("boolean")).alias("is_current"),
        F.col("_ingested_at")), ["product_id", "effective_from"])

incoming = (price_hist.join(prod_lookup.drop("_ingested_at"), "product_id", "left")
    .withColumn("product_key", F.md5(F.concat_ws(
        "|", "product_id", F.date_format("effective_from", "yyyy-MM-dd"))))
    .select("product_key", "product_id", "sku", "product_name", "category",
            "resin_type", "unit_weight_kg", "unit_price", "unit_cost",
            "effective_from", "effective_to", "is_current"))

if not DeltaTable.isDeltaTable(spark, TARGET):          # first load
    incoming.write.format("delta").mode("overwrite").save(TARGET)
else:
    tgt = DeltaTable.forPath(spark, TARGET)
    changed = incoming.filter("is_current").alias("s").join(
        tgt.toDF().filter("is_current").alias("t"), "product_id").filter(
        "s.unit_price <> t.unit_price OR s.unit_cost <> t.unit_cost "
        "OR s.product_name <> t.product_name OR s.category <> t.category"
        ).select("s.product_id")
    (tgt.alias("t").merge(changed.alias("s"),
                          "t.product_id = s.product_id AND t.is_current")
        .whenMatchedUpdate(set={"is_current": "false",
                                "effective_to": "current_date()"})
        .execute())
    tgt = DeltaTable.forPath(spark, TARGET)
    new_rows = incoming.join(tgt.toDF(), ["product_key"], "left_anti")
    (tgt.alias("t").merge(new_rows.alias("s"), "1 = 0")   # pure-insert merge
        .whenNotMatchedInsert(values={
            "product_key": "s.product_key", "product_id": "s.product_id",
            "sku": "s.sku", "product_name": "s.product_name",
            "category": "s.category", "resin_type": "s.resin_type",
            "unit_weight_kg": "s.unit_weight_kg", "unit_price": "s.unit_price",
            "unit_cost": "s.unit_cost", "effective_from": "s.effective_from",
            "effective_to": "s.effective_to", "is_current": "s.is_current"})
        .execute())
print("dim_product: SCD Type 2 merge complete")

# %%
emp_raw = bronze("hr_employees")
emp = dedupe(emp_raw.select(
        std_code(F.col("employee_id")).alias("employee_id"),
        std_name(F.col("first_name")).alias("first_name"),
        std_name(F.col("last_name")).alias("last_name"),
        std_name(F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
                 ).alias("full_name"),
        std_code(F.col("plant_id")).alias("plant_id"),
        F.col("shift").cast("int").alias("shift"),
        std_name(F.col("role")).alias("role"),
        parse_mixed_date(F.col("hire_date")).alias("hire_date"),
        std_name(F.col("employment_status")).alias("employment_status"),
        F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
        F.col("_ingested_at")), ["employee_id"])

emp = split_valid(emp, F.col("hire_date").isNotNull(),
                  "employee: unparseable or missing hire_date")
emp = split_valid(emp, F.col("plant_id").isin([r.plant_id for r in dim_plant.collect()]),
                  "employee: plant_id not in DimPlant")
emp = (emp.withColumn("employee_key", F.md5(F.col("employee_id")))
          .join(plant_keys, "plant_id", "left")
          .select("employee_key", "employee_id", "full_name", "first_name",
                  "last_name", "plant_key", "plant_id", "shift", "role",
                  "hire_date", "employment_status"))
write_delta(emp, "dim_employee")
print(f"dim_employee rows: {emp.count()}")

# %%
pm_raw = bronze("product_master")
category_map = (prod_lookup.select(
    F.lower(F.trim(F.col("category"))).alias("_cat_key"),
    F.col("category").alias("category")).distinct())
pm = (pm_raw.select(
        std_code(F.col("Product ID")).alias("product_id"),
        std_name(F.col("Product Name")).alias("product_name"),
        F.trim(F.col("Category")).alias("category_as_received"),
        std_code(F.col("Resin")).alias("resin_type"),
        F.col("List Price").cast("decimal(10,2)").alias("list_price"),
        F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
        F.col("_ingested_at"))
      .join(category_map,
            F.lower(F.col("category_as_received")) == F.col("_cat_key"), "left")
      .drop("_cat_key"))
pm = split_valid(pm, F.col("category").isNotNull(),
                 "DQ-07/D3: product_master category did not map to a conformed ERP category")
d3_changed = pm.filter(F.col("category_as_received") != F.col("category")).count()
write_delta(pm.select("product_id", "product_name", "category",
                      "category_as_received", "resin_type", "list_price"),
            "silver_product_master")
print(f"silver_product_master: {d3_changed} categories conformed (D3)")

targets = bronze("plant_targets").select(
    F.trim(F.col("Plant")).alias("plant"),
    F.trim(F.col("Month")).alias("month"),
    F.regexp_replace(F.col("Target Good Qty"), ",", "").cast("int")
        .alias("target_good_qty"),
    F.col("Target Scrap %").cast("double").alias("target_scrap_pct"),
    F.col("Target OTIF %").cast("double").alias("target_otif_pct"))
write_delta(targets, "silver_plant_targets")
print("silver_plant_targets written (Finance text quantities parsed to int)")

# %%
dim_product_current = spark.read.format("delta").load(TARGET).filter("is_current")
plant_ids    = [r.plant_id for r in dim_plant.collect()]
product_ids  = [r.product_id for r in dim_product_current.collect()]
employee_ids = [r.employee_id for r in emp.collect()]

prod = dedupe(bronze("production_runs").select(
    F.trim(F.col("run_id")).alias("run_id"),
    std_code(F.col("work_order_id")).alias("work_order_id"),
    std_code(F.col("machine_id")).alias("machine_id"),
    std_code(F.col("plant_id")).alias("plant_id"),
    std_code(F.col("product_id")).alias("product_id"),
    std_code(F.col("operator_id")).alias("operator_id"),
    parse_mixed_date(F.col("run_date")).alias("run_date"),
    F.col("shift").cast("int").alias("shift"),
    F.col("planned_qty").cast("double").cast("int").alias("planned_qty"),
    F.col("good_qty").cast("double").cast("int").alias("good_qty"),
    F.col("scrap_qty").cast("double").alias("scrap_qty"),   # cast to int AFTER the D2 null check
    F.col("downtime_minutes").cast("double").cast("int").alias("downtime_minutes"),
    F.col("resin_used_kg").cast("double").alias("resin_used_kg"),
    F.col("cycle_time_sec").cast("double").alias("cycle_time_sec"),
    F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
    F.col("_ingested_at")), ["run_id"])                     # D1: dedupe on run_id
prod = split_valid(prod, F.col("scrap_qty").isNotNull(),
                   "DQ-02/D2: null scrap_qty")               # D2 -> quarantine
prod = split_valid(prod, (F.col("good_qty") >= 0) & (F.col("scrap_qty") >= 0),
                   "DQ-05/D6: negative quantity")            # D6 -> quarantine
prod = split_valid(prod, F.col("operator_id").isin(employee_ids),
                   "DQ-03/D4: orphan operator_id not in HR employees")
prod = split_valid(prod, F.col("product_id").isin(product_ids),
                   "DQ-03: orphan product_id not in DimProduct")
prod = split_valid(prod, F.col("plant_id").isin(plant_ids),
                   "production_run: plant_id not in DimPlant (orphan FK)")
prod = split_valid(prod, F.col("run_date").isNotNull(),
                   "DQ-06: unparseable run_date")
prod = prod.withColumn("scrap_qty", F.col("scrap_qty").cast("int"))
write_delta(prod, "silver_production_runs")

work_orders = dedupe(bronze("work_orders").select(
    std_code(F.col("work_order_id")).alias("work_order_id"),
    std_code(F.col("product_id")).alias("product_id"),
    std_code(F.col("plant_id")).alias("plant_id"),
    std_code(F.col("machine_id")).alias("machine_id"),
    F.col("planned_qty").cast("double").cast("int").alias("planned_qty"),
    parse_mixed_date(F.col("start_date")).alias("start_date"),
    parse_mixed_date(F.col("due_date")).alias("due_date"),
    F.trim(F.col("status")).alias("status"),
    parse_mixed_date(F.col("created_date")).alias("created_date"),
    F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
    F.col("_ingested_at")), ["work_order_id"])
write_delta(work_orders, "silver_work_orders")

qi = dedupe(bronze("quality_inspections").select(
    F.trim(F.col("inspection_id")).alias("inspection_id"),
    F.trim(F.col("run_id")).alias("run_id"),
    std_code(F.col("product_id")).alias("product_id"),
    std_code(F.col("plant_id")).alias("plant_id"),
    parse_mixed_date(F.col("inspection_date")).alias("inspection_date"),
    F.col("inspected_qty").cast("double").cast("int").alias("inspected_qty"),
    F.col("defect_qty").cast("double").cast("int").alias("defect_qty"),
    std_name(F.col("defect_type")).alias("defect_type"),
    std_code(F.col("inspector_id")).alias("inspector_id"),
    std_name(F.col("result")).alias("result"),
    F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
    F.col("_ingested_at")), ["inspection_id"])
qi = split_valid(qi, F.col("product_id").isin(product_ids),
                 "quality_inspection: product_id not in DimProduct (orphan FK)")
qi = split_valid(qi, F.col("defect_qty") >= 0,
                 "quality_inspection: negative defect_qty")
write_delta(qi, "silver_quality_inspections")

orders = dedupe(bronze("sales_orders").select(
    F.trim(F.col("sales_order_id")).alias("sales_order_id"),
    std_code(F.col("retailer_id")).alias("retailer_id"),
    parse_mixed_date(F.col("order_date")).alias("order_date"),
    parse_mixed_date(F.col("promised_date")).alias("promised_date"),
    std_code(F.col("fulfilling_plant_id")).alias("fulfilling_plant_id"),
    F.trim(F.col("status")).alias("status"),
    F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
    F.col("_ingested_at")), ["sales_order_id"])
write_delta(orders, "silver_sales_orders")

lines = dedupe(bronze("sales_order_lines").select(
    F.trim(F.col("sales_order_line_id")).alias("sales_order_line_id"),
    F.trim(F.col("sales_order_id")).alias("sales_order_id"),
    std_code(F.col("product_id")).alias("product_id"),
    F.col("ordered_qty").cast("double").cast("int").alias("ordered_qty"),
    F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
    F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
    F.col("_ingested_at")), ["sales_order_line_id"])
lines = split_valid(lines, F.col("ordered_qty") > 0,
                    "sales_order_line: ordered_qty <= 0")
lines = split_valid(lines, F.col("product_id").isin(product_ids),
                    "sales_order_line: product_id not in DimProduct (orphan FK)")
write_delta(lines, "silver_sales_order_lines")

ship = dedupe(bronze("shipments").select(
    F.trim(F.col("shipment_id")).alias("shipment_id"),
    F.trim(F.col("sales_order_line_id")).alias("sales_order_line_id"),
    F.trim(F.col("sales_order_id")).alias("sales_order_id"),
    std_code(F.col("plant_id")).alias("plant_id"),
    parse_mixed_date(F.col("ship_date")).alias("ship_date"),
    parse_mixed_date(F.col("delivery_date")).alias("delivery_date"),
    parse_mixed_date(F.col("promised_date")).alias("promised_date"),
    F.col("shipped_qty").cast("double").cast("int").alias("shipped_qty"),
    std_name(F.col("carrier")).alias("carrier"),
    parse_mixed_date(F.col("record_created_date")).alias("record_created_date"),
    F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
    F.col("_ingested_at")), ["shipment_id"])
ship = split_valid(ship, F.col("shipped_qty") >= 0, "shipment: negative shipped_qty")
write_delta(ship, "silver_shipments")

inv = dedupe(bronze("inventory_daily").select(
    parse_mixed_date(F.col("inventory_date")).alias("inventory_date"),
    std_code(F.col("plant_id")).alias("plant_id"),
    std_code(F.col("resin_type")).alias("resin_type"),
    F.col("on_hand_kg").cast("double").alias("on_hand_kg"),
    F.col("consumed_kg").cast("double").alias("consumed_kg"),
    F.col("received_kg").cast("double").alias("received_kg"),
    F.col("safety_stock_kg").cast("double").alias("safety_stock_kg"),
    F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
    F.col("_ingested_at")), ["inventory_date", "plant_id", "resin_type"])
write_delta(inv, "silver_inventory_daily")

pos = bronze("retail_pos").select(
    std_code(F.col("retailer_id")).alias("retailer_id"),
    std_code(F.col("product_id")).alias("product_id"),
    parse_mixed_date(F.col("sale_date")).alias("sale_date"),   # D7: mixed formats
    F.col("sold_qty").cast("double").alias("sold_qty"),
    F.col("returns_qty").cast("double").cast("int").alias("returns_qty"),
    F.col("promo_flag").cast("double").cast("int").alias("promo_flag"),
    F.col("_batch_id"), F.col("_source_system"), F.col("_source_file"),
    F.col("_ingested_at"))
pos = split_valid(pos, F.col("sale_date").isNotNull(),
                  "DQ-06/D7: unparseable sale_date")
pos = split_valid(pos, F.col("sold_qty") >= 0,
                  "DQ-05/D6: negative sold_qty")
pos = pos.withColumn("sold_qty", F.col("sold_qty").cast("int"))
write_delta(pos, "silver_retail_pos")
print("silver transactional tables written")

# %%
ship_by_line = (ship.groupBy("sales_order_line_id", "sales_order_id")
                    .agg(F.sum("shipped_qty").alias("shipped_qty"),
                         F.max("ship_date").alias("ship_date"),
                         F.max("delivery_date").alias("delivery_date"),
                         F.first("plant_id").alias("shipment_plant_id"),
                         F.count("shipment_id").alias("shipment_count")))

lines_with_header = lines.join(
    orders.select("sales_order_id", "retailer_id", "order_date",
                  "promised_date", "fulfilling_plant_id"),
    "sales_order_id", "left")

lines_before = lines_with_header.count()
lines_enriched = lines_with_header.join(
    ship_by_line, ["sales_order_line_id", "sales_order_id"], "left")
lines_after = lines_enriched.count()

assert lines_after == lines_before, \
    f"FAN-OUT: join changed row count {lines_before} -> {lines_after}"
print(f"anti-fan-out check passed: {lines_before} order lines in, {lines_after} out")
write_delta(lines_enriched, "silver_sales_order_lines_enriched")

# %%
if _quarantine_rows:
    q = _quarantine_rows[0]
    for extra in _quarantine_rows[1:]:
        q = q.unionByName(extra, allowMissingColumns=True)
    write_delta(q, "silver_quarantine", mode="append")
    n_q = q.count()
    print(q.groupBy("_quarantine_reason").count().toPandas().to_string(index=False))
    raise RuntimeError(f"{n_q} rows quarantined in Silver — see silver_quarantine")
print("Silver complete: no quarantined rows")
