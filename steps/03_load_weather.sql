/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       03_load_weather.sql
Author:       Jeremiah Hansen
Last Updated: 1/9/2023
-----------------------------------------------------------------------------*/

-- SNOWFLAKE ADVANTAGE: Data sharing/marketplace (instead of ETL)
-- SNOWFLAKE ADVANTAGE: Visual Studio Code Snowflake native extension (PrPr, Git integration)


USE ROLE HOL_ROLE;
USE WAREHOUSE HOL_WH;


-- ----------------------------------------------------------------------------
-- Step #1: Connect to weather data in Marketplace
-- ----------------------------------------------------------------------------

/*---
But what about data that needs constant updating - like the WEATHER data? We would
need to build a pipeline process to constantly update that data to keep it fresh.

Perhaps a better way to get this external data would be to source it from a trusted
data supplier. Let them manage the data, keeping it accurate and up to date.

Enter the Snowflake Data Cloud...

Weather Source is a leading provider of global weather and climate data and their
OnPoint Product Suite provides businesses with the necessary weather and climate data
to quickly generate meaningful and actionable insights for a wide range of use cases
across industries. Let's connect to the "Weather Source LLC: frostbyte" feed from
Weather Source in the Snowflake Data Marketplace by following these steps:

    -> Snowsight Home Button
         -> Marketplace
             -> Search: "Weather Source LLC: frostbyte" (and click on tile in results)
                 -> Click the blue "Get" button
                     -> Under "Options", adjust the Database name to read "FROSTBYTE_WEATHERSOURCE" (all capital letters)
                        -> Grant to "HOL_ROLE"
    
That's it... we don't have to do anything from here to keep this data updated.
The provider will do that for us and data sharing means we are always seeing
whatever they they have published.


-- You can also do it via code if you know the account/share details...
SET WEATHERSOURCE_ACCT_NAME = '*** PUT ACCOUNT NAME HERE AS PART OF DEMO SETUP ***';
SET WEATHERSOURCE_SHARE_NAME = '*** PUT ACCOUNT SHARE HERE AS PART OF DEMO SETUP ***';
SET WEATHERSOURCE_SHARE = $WEATHERSOURCE_ACCT_NAME || '.' || $WEATHERSOURCE_SHARE_NAME;

CREATE OR REPLACE DATABASE FROSTBYTE_WEATHERSOURCE
  FROM SHARE IDENTIFIER($WEATHERSOURCE_SHARE);

GRANT IMPORTED PRIVILEGES ON DATABASE FROSTBYTE_WEATHERSOURCE TO ROLE HOL_ROLE;
---*/


-- Let's look at the data - same 3-part naming convention as any other table
SELECT * FROM FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.POSTAL_CODES
WHERE COUNTRY ='US' AND CITY_NAME LIKE '%H%'
LIMIT 100;


-- What is the temperature in London during May?
-- We would like to look at the temperatures in May of last year to determine when to rotate seasonal menu items.
SELECT
    postal_code,
    country,
    date_valid_std,
    min_temperature_air_2m_f,
    avg_temperature_air_2m_f,
    max_temperature_air_2m_f
FROM
    FROSTBYTE_WEATHERSOURCE.onpoint_id.history_day
WHERE
    postal_code = 'SW1A 0AA' AND
    country = 'GB' AND
    date_valid_std BETWEEN DATE_FROM_PARTS(YEAR(CURRENT_DATE)-1,5,1) AND DATE_FROM_PARTS(YEAR(CURRENT_DATE)-1,5,31)
ORDER BY
    date_valid_std
;

-- What will the temperature in Tokyo be next Saturday?
-- Our food truck is catering an outdoor party in Tokyo next Saturday? Can you tell me the forecasted temperatures so we can determine our menu items?
SELECT
    postal_code,
    country,
    date_valid_std,
    min_temperature_air_2m_f,
    avg_temperature_air_2m_f,
    max_temperature_air_2m_f
FROM
(
    SELECT
        postal_code,
        country,
        date_valid_std,
        min_temperature_air_2m_f,
        avg_temperature_air_2m_f,
        max_temperature_air_2m_f,
        DATEADD(DAY,2,CURRENT_DATE()) AS skip_date,
        DATEADD(DAY,6 - DAYOFWEEKISO(skip_date),skip_date) AS next_saturday
    FROM
        FROSTBYTE_WEATHERSOURCE.onpoint_id.forecast_day
    WHERE 1=1 AND
        -- postal_code = '102-0082' AND
        country = 'US'
)
WHERE
    date_valid_std = next_saturday
;
