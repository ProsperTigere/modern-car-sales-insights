-- Databricks notebook source
SELECT * FROM `workspace`.`dealership_insights`.`car_sales_data`
LIMIT 20;

-- COMMAND ----------

DESCRIBE TABLE `workspace`.`dealership_insights`.`car_sales_data`;

-- COMMAND ----------

SELECT * FROM `workspace`.`dealership_insights`.`car_sales_data`
LIMIT 20;

-- COMMAND ----------

SELECT
  SUM(CASE WHEN split(car_sales_row, ';')[0] IS NULL THEN 1 ELSE 0 END) AS null_year,
  SUM(CASE WHEN split(car_sales_row, ';')[1] IS NULL THEN 1 ELSE 0 END) AS null_make,
  SUM(CASE WHEN split(car_sales_row, ';')[2] IS NULL THEN 1 ELSE 0 END) AS null_model,
  SUM(CASE WHEN split(car_sales_row, ';')[8] IS NULL THEN 1 ELSE 0 END) AS null_condition,
  SUM(CASE WHEN split(car_sales_row, ';')[9] IS NULL THEN 1 ELSE 0 END) AS null_odometer,
  SUM(CASE WHEN split(car_sales_row, ';')[13] IS NULL THEN 1 ELSE 0 END) AS null_mmr,
  SUM(CASE WHEN split(car_sales_row, ';')[14] IS NULL THEN 1 ELSE 0 END) AS null_sellingprice
FROM `workspace`.`dealership_insights`.`car_sales_data`

-- COMMAND ----------



-- COMMAND ----------

-- Remove rows with NULL or empty critical fields and cast columns to appropriate types
CREATE OR REPLACE TABLE `workspace`.`dealership_insights`.`car_sales_data_cleaned` AS
SELECT
  CAST(split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[0] AS INT) AS year,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[1] AS make,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[2] AS model,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[3] AS trim,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[4] AS body,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[5] AS transmission,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[6] AS vin,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[7] AS state,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[8] AS condition,
  CAST(split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[9] AS DOUBLE) AS odometer,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[10] AS color,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[11] AS interior,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[12] AS seller,
  CAST(split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[13] AS DOUBLE) AS mmr,
  CAST(split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[14] AS DOUBLE) AS sellingprice,
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[15] AS saledate
FROM `workspace`.`dealership_insights`.`car_sales_data`
WHERE
  split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[0] IS NOT NULL AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[0] != ''
  AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[1] IS NOT NULL AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[1] != ''
  AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[2] IS NOT NULL AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[2] != ''
  AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[8] IS NOT NULL AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[8] != ''
  AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[9] IS NOT NULL AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[9] != ''
  AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[13] IS NOT NULL AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[13] != ''
  AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[14] IS NOT NULL AND split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[14] != '';

-- COMMAND ----------

-- Fill NULLs in critical numeric columns with default values (e.g., 0 for odometer, mmr, sellingprice)
CREATE OR REPLACE TABLE `workspace`.`dealership_insights`.`car_sales_data_filled` AS
SELECT
  COALESCE(year, 0) AS year,
  COALESCE(make, 'Unknown') AS make,
  COALESCE(model, 'Unknown') AS model,
  COALESCE(trim, 'Unknown') AS trim,
  COALESCE(body, 'Unknown') AS body,
  COALESCE(transmission, 'Unknown') AS transmission,
  COALESCE(vin, 'Unknown') AS vin,
  COALESCE(state, 'Unknown') AS state,
  COALESCE(condition, 'Unknown') AS condition,
  COALESCE(odometer, 0) AS odometer,
  COALESCE(color, 'Unknown') AS color,
  COALESCE(interior, 'Unknown') AS interior,
  COALESCE(seller, 'Unknown') AS seller,
  COALESCE(mmr, 0) AS mmr,
  COALESCE(sellingprice, 0) AS sellingprice,
  COALESCE(saledate, 'Unknown') AS saledate
FROM `workspace`.`dealership_insights`.`car_sales_data`;

-- COMMAND ----------

-- Remove rows with invalid years (e.g., year < 1980 or year > 2025) or odometer values (e.g., odometer < 0 or odometer > 500000)
CREATE OR REPLACE TABLE `workspace`.`dealership_insights`.`car_sales_data_cleaned_valid` AS
SELECT *
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
WHERE year BETWEEN 1980 AND 2025
  AND odometer >= 0 AND odometer <= 500000;

-- COMMAND ----------

-- Check again for nulls
SELECT COUNT(*) FROM `workspace`.`dealership_insights`.`car_sales_data` WHERE split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[0] IS NULL;

-- Optional: Count distinct VINs now
SELECT COUNT(DISTINCT split(`year;make;model;trim;body;transmission;vin;state;condition;odometer;color;interior;seller;mmr;sellingprice;saledate`, ';')[6]) FROM `workspace`.`dealership_insights`.`car_sales_data`;

-- COMMAND ----------

DESCRIBE TABLE `workspace`.`dealership_insights`.`car_sales_data_cleaned`;

-- COMMAND ----------

SELECT * FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned` LIMIT 20;

-- COMMAND ----------



-- COMMAND ----------

SELECT COUNT(*) AS total_rows
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`;

-- COMMAND ----------

SELECT
  SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) AS null_year,
  SUM(CASE WHEN make IS NULL THEN 1 ELSE 0 END) AS null_make,
  SUM(CASE WHEN model IS NULL THEN 1 ELSE 0 END) AS null_model,
  SUM(CASE WHEN odometer IS NULL THEN 1 ELSE 0 END) AS null_odometer,
  SUM(CASE WHEN mmr IS NULL THEN 1 ELSE 0 END) AS null_mmr,
  SUM(CASE WHEN sellingprice IS NULL THEN 1 ELSE 0 END) AS null_sellingprice
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`;

-- COMMAND ----------

DESCRIBE TABLE SUMMARY `workspace`.`dealership_insights`.`car_sales_data_cleaned`;

-- COMMAND ----------

-- Average selling price by make (Shows premium brands vs budget brands)
SELECT
  make,
  COUNT(*) AS total_sold,
  ROUND(AVG(sellingprice), 2) AS avg_selling_price
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
GROUP BY make
ORDER BY avg_selling_price DESC;

-- COMMAND ----------

-- Top 10 most sold car makes
SELECT
  make,
  COUNT(*) AS units_sold
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
GROUP BY make
ORDER BY units_sold DESC
LIMIT 10;

-- COMMAND ----------

-- Mileage and Depreciation
-- Correlation between odometer and selling price
SELECT
  corr(odometer, sellingprice) AS correlation_mileage_price
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`;

-- COMMAND ----------

-- Average price by mileage buckets
SELECT
  CASE 
    WHEN odometer < 50000 THEN '0-50k'
    WHEN odometer < 100000 THEN '50k-100k'
    WHEN odometer < 150000 THEN '100k-150k'
    ELSE '150k+'
  END AS mileage_bucket,
  COUNT(*) AS total_cars,
  ROUND(AVG(sellingprice), 2) AS avg_price
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
GROUP BY
  CASE 
    WHEN odometer < 50000 THEN '0-50k'
    WHEN odometer < 100000 THEN '50k-100k'
    WHEN odometer < 150000 THEN '100k-150k'
    ELSE '150k+'
  END
ORDER BY mileage_bucket;

-- COMMAND ----------

-- Condition vs Price
SELECT
  condition,
  COUNT(*) AS total_units,
  ROUND(AVG(sellingprice), 2) AS avg_price
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
GROUP BY condition
ORDER BY condition;

-- COMMAND ----------

-- Profit Margin Calculation
-- Calculate average profit margin (sellingprice - mmr) by make and model for models with more than 50 sales
SELECT
  make,
  model,
  ROUND(AVG(sellingprice - mmr), 2) AS avg_profit_margin,
  COUNT(*) AS units_sold
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
GROUP BY make, model
HAVING COUNT(*) > 50
ORDER BY avg_profit_margin DESC;

-- COMMAND ----------

----Table: Price Summary by Make
CREATE OR REPLACE TABLE `workspace`.`dealership_insights`.`gold_price_by_make` AS
SELECT
  make,
  COUNT(*) AS units_sold,
  ROUND(AVG(sellingprice), 2) AS avg_selling_price,
  ROUND(AVG(mmr), 2) AS avg_mmr,
  ROUND(AVG(sellingprice - mmr), 2) AS avg_profit_margin
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
GROUP BY make
ORDER BY avg_selling_price DESC;

-- COMMAND ----------

----Table: Mileage Buckets
CREATE OR REPLACE TABLE `workspace`.`dealership_insights`.`gold_mileage_buckets` AS
SELECT
  CASE 
    WHEN odometer < 50000 THEN '0-50k'
    WHEN odometer < 100000 THEN '50k-100k'
    WHEN odometer < 150000 THEN '100k-150k'
    ELSE '150k+'
  END AS mileage_bucket,
  COUNT(*) AS total_cars,
  ROUND(AVG(sellingprice), 2) AS avg_price,
  ROUND(AVG(mmr), 2) AS avg_mmr,
  ROUND(AVG(sellingprice - mmr), 2) AS avg_profit
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
GROUP BY
  CASE 
    WHEN odometer < 50000 THEN '0-50k'
    WHEN odometer < 100000 THEN '50k-100k'
    WHEN odometer < 150000 THEN '100k-150k'
    ELSE '150k+'
  END
ORDER BY mileage_bucket;

-- COMMAND ----------

-----Table: Condition Score Insights
CREATE OR REPLACE TABLE `workspace`.`dealership_insights`.`gold_condition_analysis` AS
SELECT
  condition,
  COUNT(*) AS total_units,
  ROUND(AVG(sellingprice), 2) AS avg_price,
  ROUND(AVG(mmr), 2) AS avg_mmr,
  ROUND(AVG(sellingprice - mmr), 2) AS avg_profit_margin
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
GROUP BY condition
ORDER BY condition;

-- COMMAND ----------

---Table: Top Profit Models
CREATE OR REPLACE TABLE `workspace`.`dealership_insights`.`gold_top_profit_models` AS
SELECT
  make,
  model,
  COUNT(*) AS units_sold,
  ROUND(AVG(sellingprice), 2) AS avg_price,
  ROUND(AVG(mmr), 2) AS avg_mmr,
  ROUND(AVG(sellingprice - mmr), 2) AS avg_profit
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
GROUP BY make, model
HAVING COUNT(*) > 50
ORDER BY avg_profit DESC;

-- COMMAND ----------

-- Export price by make
COPY INTO 'dbfs:/FileStore/gold_price_by_make.csv'
FROM (
  SELECT * FROM `workspace`.`dealership_insights`.`gold_price_by_make`
)
FILEFORMAT = CSV
HEADER = TRUE;

-- COMMAND ----------

----gold_price_by_make.csv
SELECT *
FROM `workspace`.`dealership_insights`.`gold_price_by_make`
ORDER BY avg_selling_price DESC;

-- COMMAND ----------

----gold_mileage_buckets.csv
SELECT *
FROM `workspace`.`dealership_insights`.`gold_mileage_buckets`
ORDER BY mileage_bucket;

-- COMMAND ----------

--- condition analysis
SELECT *
FROM `workspace`.`dealership_insights`.`gold_condition_analysis`
ORDER BY condition;

-- COMMAND ----------

---Top profit models (limit to 50 so download is fast)
SELECT
  make,
  model,
  units_sold,
  avg_price,
  avg_mmr,
  avg_profit
FROM `workspace`.`dealership_insights`.`gold_top_profit_models`
ORDER BY avg_profit DESC
LIMIT 50;

-- COMMAND ----------

CREATE OR REPLACE TABLE `workspace`.`dealership_insights`.`gold_monthly_price_trend` AS
WITH parsed AS (
  SELECT
    sellingprice,
    COALESCE(
      try_to_timestamp(saledate, 'yyyy-MM-dd'),
      try_to_timestamp(saledate, 'yyyy/MM/dd'),
      try_to_timestamp(saledate, 'MM/dd/yyyy'),
      try_to_timestamp(saledate)
    ) AS sale_ts
  FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
)
SELECT
  date_format(sale_ts, 'yyyy-MM') AS year_month,
  ROUND(AVG(sellingprice), 2) AS avg_selling_price,
  COUNT(*) AS units_sold
FROM parsed
WHERE sale_ts IS NOT NULL
  AND sellingprice IS NOT NULL
  AND sellingprice > 0
GROUP BY date_format(sale_ts, 'yyyy-MM')
ORDER BY year_month;

-- COMMAND ----------

SELECT odometer, sellingprice
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
WHERE sellingprice > 0 AND odometer >= 0
ORDER BY RAND()
LIMIT 10000

-- COMMAND ----------

SELECT DISTINCT saledate
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
LIMIT 20;

-- COMMAND ----------

SELECT
  to_timestamp(saledate, 'EEE MMM dd yyyy HH:mm:ss') AS sale_ts
FROM `workspace`.`dealership_insights`.`car_sales_data_cleaned`
LIMIT 20;