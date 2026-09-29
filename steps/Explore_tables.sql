/*
Snowpark lab verification checklist

Run the read-only queries by section after completing the matching lab step.
The optional CALL and EXECUTE TASK statements are commented out because they
write tables and consume stream changes. Run those individually only when you
intend to process the current stream contents.
*/

-- ---------------------------------------------------------------------------
-- Step 1: Setup Snowflake objects
-- Confirms the lab database, schemas, warehouse, and external stage exist.
-- These SHOW commands are read-only.
-- ---------------------------------------------------------------------------
SHOW SCHEMAS IN DATABASE HOL_DB;
SHOW WAREHOUSES LIKE 'HOL_WH';
SHOW STAGES IN SCHEMA HOL_DB.EXTERNAL;

-- ---------------------------------------------------------------------------
-- Step 2: Load raw POS data
-- Counts verify that the raw tables were populated by 02_load_raw.py.
-- ---------------------------------------------------------------------------
SELECT 'COUNTRY' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM HOL_DB.RAW_POS.COUNTRY
UNION ALL
SELECT 'FRANCHISE', COUNT(*) FROM HOL_DB.RAW_POS.FRANCHISE
UNION ALL
SELECT 'LOCATION', COUNT(*) FROM HOL_DB.RAW_POS.LOCATION
UNION ALL
SELECT 'MENU', COUNT(*) FROM HOL_DB.RAW_POS.MENU
UNION ALL
SELECT 'TRUCK', COUNT(*) FROM HOL_DB.RAW_POS.TRUCK
UNION ALL
SELECT 'ORDER_HEADER', COUNT(*) FROM HOL_DB.RAW_POS.ORDER_HEADER
UNION ALL
SELECT 'ORDER_DETAIL', COUNT(*) FROM HOL_DB.RAW_POS.ORDER_DETAIL
UNION ALL
SELECT 'CUSTOMER_LOYALTY', COUNT(*) FROM HOL_DB.RAW_CUSTOMER.CUSTOMER_LOYALTY
ORDER BY TABLE_NAME;

SELECT TOP 20 * FROM HOL_DB.RAW_POS.ORDER_HEADER ORDER BY ORDER_TS DESC;

-- ---------------------------------------------------------------------------
-- Step 3: Access Weather Source shared data
-- Confirms the marketplace database is available to the current role.
-- ---------------------------------------------------------------------------
SELECT TOP 10 POSTAL_CODE, COUNTRY, CITY_NAME
FROM FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.POSTAL_CODES;

SELECT TOP 10 DATE_VALID_STD, CITY_NAME, COUNTRY, AVG_TEMPERATURE_AIR_2M_F
FROM FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.HISTORY_DAY;

-- ---------------------------------------------------------------------------
-- Step 4: Create the POS flattened view and its stream
-- The view check verifies the joined rows. Reading a stream does not consume it;
-- DML that uses the stream is what advances its offset.
-- ---------------------------------------------------------------------------
SELECT TOP 20 * FROM HOL_DB.HARMONIZED.POS_FLATTENED_V;
SHOW STREAMS LIKE 'POS_FLATTENED_V_STREAM' IN SCHEMA HOL_DB.HARMONIZED;

SELECT METADATA$ACTION, METADATA$ISUPDATE, ORDER_ID, ORDER_DETAIL_ID
FROM HOL_DB.HARMONIZED.POS_FLATTENED_V_STREAM
LIMIT 20;

-- ---------------------------------------------------------------------------
-- Step 5: Deploy and verify the Fahrenheit-to-Celsius UDF
-- 35 F should return approximately 1.6667 C with the SciPy conversion.
-- ---------------------------------------------------------------------------
SHOW USER FUNCTIONS LIKE 'FAHRENHEIT_TO_CELSIUS_UDF' IN SCHEMA HOL_DB.ANALYTICS;
SELECT HOL_DB.ANALYTICS.FAHRENHEIT_TO_CELSIUS_UDF(35::FLOAT) AS TEMP_C;

-- ---------------------------------------------------------------------------
-- Step 6: Verify the orders stored procedure and target table
-- These queries inspect the object and current results without processing data.
-- ---------------------------------------------------------------------------
SHOW PROCEDURES LIKE 'ORDERS_UPDATE_SP' IN SCHEMA HOL_DB.HARMONIZED;
SELECT COUNT(*) AS ORDER_ROWS, MAX(ORDER_TS) AS LATEST_ORDER_TS
FROM HOL_DB.HARMONIZED.ORDERS;
SELECT TOP 20 * FROM HOL_DB.HARMONIZED.ORDERS ORDER BY ORDER_TS DESC;
SHOW STREAMS LIKE 'ORDERS_STREAM' IN SCHEMA HOL_DB.HARMONIZED;

-- State-changing: runs Step 6 and consumes POS_FLATTENED_V_STREAM changes.
-- CALL HOL_DB.HARMONIZED.ORDERS_UPDATE_SP();

-- ---------------------------------------------------------------------------
-- Step 7: Verify the daily city metrics procedure and output
-- ---------------------------------------------------------------------------
SHOW PROCEDURES LIKE 'DAILY_CITY_METRICS_UPDATE_SP' IN SCHEMA HOL_DB.ANALYTICS;
SELECT COUNT(*) AS METRIC_ROWS, MAX(DATE) AS LATEST_METRIC_DATE
FROM HOL_DB.ANALYTICS.DAILY_CITY_METRICS;
SELECT TOP 20 *
FROM HOL_DB.ANALYTICS.DAILY_CITY_METRICS
ORDER BY DATE DESC, CITY_NAME;

-- State-changing: runs Step 7 and consumes ORDERS_STREAM changes.
-- CALL HOL_DB.ANALYTICS.DAILY_CITY_METRICS_UPDATE_SP();

-- ---------------------------------------------------------------------------
-- Step 8: Verify task definitions and recent task runs
-- ---------------------------------------------------------------------------
SHOW TASKS IN SCHEMA HOL_DB.HARMONIZED;

SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_CODE, ERROR_MESSAGE
FROM TABLE(HOL_DB.INFORMATION_SCHEMA.TASK_HISTORY(
	SCHEDULED_TIME_RANGE_START => DATEADD('DAY', -1, CURRENT_TIMESTAMP()),
	RESULT_LIMIT => 100
))
WHERE NAME IN ('ORDERS_UPDATE_TASK', 'DAILY_CITY_METRICS_UPDATE_TASK')
ORDER BY SCHEDULED_TIME DESC;

-- State-changing: manually starts the Step 8 task chain.
-- EXECUTE TASK HOL_DB.HARMONIZED.ORDERS_UPDATE_TASK;

-- ---------------------------------------------------------------------------
-- Step 9: Verify the incremental 2022 raw-data load
-- Step 9 loads 2022 files, then tasks process the resulting stream changes.
-- ---------------------------------------------------------------------------
SELECT YEAR(ORDER_TS) AS ORDER_YEAR, COUNT(*) AS ORDER_COUNT
FROM HOL_DB.RAW_POS.ORDER_HEADER
GROUP BY YEAR(ORDER_TS)
ORDER BY ORDER_YEAR;

-- ---------------------------------------------------------------------------
-- Step 10: Verify the CI/CD-deployed UDF
-- The function metadata and result should match the Step 5 checks above.
-- ---------------------------------------------------------------------------
SHOW USER FUNCTIONS LIKE 'FAHRENHEIT_TO_CELSIUS_UDF' IN SCHEMA HOL_DB.ANALYTICS;
