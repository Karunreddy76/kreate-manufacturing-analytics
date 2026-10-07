# Kreate Manufacturing Analytics

Fabric project I built to learn Microsoft Fabric end to end.

Plastic injection molding data - 5 plants, 30 machines, 40 products.
Data comes from 3 places: ERP tables (production, quality, sales,
inventory), Excel files (employees, plant targets, product list) and
monthly retail POS files from 5 retailers.

## What I did

1. `01_bronze_ingest` - load all files into the lakehouse as-is.
   15 tables, 110,174 rows. Added batch id and source file columns.
2. `02_silver_transform` - clean the data. Fix text, dates, types,
   remove duplicate runs. Bad rows go to a quarantine table with a
   reason instead of getting deleted. 372 rows quarantined.
3. `04_data_quality` - 8 checks before gold is built. If a check
   fails, the run stops.
4. `03_gold_model` - build the star schema. 5 fact tables and
   6 dimension tables. FactProduction has 17,349 rows
   (17,890 came in - 177 duplicates - 364 quarantined).
5. Power BI - 11 tables, 16 relationships, 55 measures,
   6 report pages.

## Questions the report answers

1. Which plant, machine and shift has the most scrap?
   Bloomington plant 5.69%, machine IMM-22, night shift worst.
2. Are we delivering on time and in full?
   OTIF 85.33% when we ship, 74.74% when customer receives.
   Costco lowest at 70.83%.
3. Where do defects come from and what do they cost?
   Flash + Short Shot are about half of all defects. $490,485 total.
4. Which plant will run out of resin first?
   Georgetown, 5.82 days of supply.
5. Are we making more than we sell?
   Yes, 2.76x of retail sales. Product mix problem.

I also added 4 SQL views on the gold tables
(vw_PlantProductionSummary, vw_RetailerOTIF,
vw_DailyProductionTrend, vw_DefectPareto) so the main
calculations are written once.

## Files

```
01_data_foundation/source_data/   the data files (csv + excel)
02_fabric_notebooks/              4 notebooks, .py and .ipynb
03_powerbi/                       power bi project + dax measures
```

Data is made up (no real company data), 18 months,
Jan 2025 to Jun 2026. Built in a Fabric trial workspace,
Power BI model is Import mode through the lakehouse SQL endpoint.
