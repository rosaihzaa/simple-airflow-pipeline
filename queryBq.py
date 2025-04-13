import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'houseELT'))


def queryBigquery():
    query = '''
        CREATE OR REPLACE TABLE `bank-marketing-project-446413.houses.house_airflow` AS
        WITH cleaned_houses AS (
        SELECT DISTINCT
            Name,
            INITCAP(Country) AS Country,
            Link,
            Address,
            COALESCE(SAFE_CAST(CASE WHEN LOWER(Price) IN ('price on demand', 'contact for price', 'n/a') THEN NULL ELSE REGEXP_REPLACE(Price, r'[\$,]', '') END AS FLOAT64), (SELECT APPROX_QUANTILES(SAFE_CAST(REGEXP_REPLACE(Price, r'[\$,]', '') AS FLOAT64), 100)[OFFSET(50)] FROM `bank-marketing-project-446413.houses.house_airflow` WHERE SAFE_CAST(REGEXP_REPLACE(Price, r'[\$,]', '') AS FLOAT64) IS NOT NULL LIMIT 1)) AS Price,
            COALESCE(CAST(`Rooms` AS INT64), (SELECT APPROX_QUANTILES(CAST(`Rooms` AS INT64), 100)[OFFSET(50)] FROM `bank-marketing-project-446413.houses.house_airflow` LIMIT 1))  AS Rooms,
            COALESCE(CAST(`Bedrooms` AS INT64), (SELECT APPROX_QUANTILES(CAST(`Bedrooms` AS INT64), 100)[OFFSET(50)] FROM `bank-marketing-project-446413.houses.house_airflow` LIMIT 1))  AS Bedrooms,
            COALESCE(CAST(`Bathrooms` AS INT64), 0) + COALESCE(CAST(`Toilet rooms` AS INT64), 0) + COALESCE(CAST(`Shower rooms` AS INT64), 0) AS Total_Bathrooms,
            COALESCE(CAST(`Floors` AS INT64), (SELECT APPROX_QUANTILES(CAST(`Floors` AS INT64), 100)[OFFSET(50)] FROM `bank-marketing-project-446413.houses.house_airflow` LIMIT 1))  AS Floors,
            COALESCE(CAST(REGEXP_EXTRACT(`Living area`, r'\((\d+)\s*m²\)') AS FLOAT64), (SELECT APPROX_QUANTILES(CAST(REGEXP_EXTRACT(`Living area`, r'\((\d+)\s*m²\)') AS FLOAT64), 100)[OFFSET(50)] FROM `bank-marketing-project-446413.houses.house_airflow` LIMIT 1)) AS Living_area_m2,
            COALESCE(CAST(REGEXP_EXTRACT(`Land`, r'\((\d+)\s*m²\)') AS FLOAT64), (SELECT APPROX_QUANTILES(CAST(REGEXP_EXTRACT(`Land`, r'\((\d+)\s*m²\)') AS FLOAT64), 100)[OFFSET(50)] FROM `bank-marketing-project-446413.houses.house_airflow` LIMIT 1)) AS Land_m2,
            COALESCE(CAST(REGEXP_EXTRACT(`Total`, r'\((\d+)\s*m²\)') AS FLOAT64), (SELECT APPROX_QUANTILES(CAST(REGEXP_EXTRACT(`Total`, r'\((\d+)\s*m²\)') AS FLOAT64), 100)[OFFSET(50)] FROM `bank-marketing-project-446413.houses.house_airflow` LIMIT 1)) AS Total_m2,
            COALESCE(CAST(REGEXP_EXTRACT(`Garden`, r'\((\d+)\s*m²\)') AS FLOAT64), (SELECT APPROX_QUANTILES(CAST(REGEXP_EXTRACT(`Garden`, r'\((\d+)\s*m²\)') AS FLOAT64), 100)[OFFSET(50)] FROM `bank-marketing-project-446413.houses.house_airflow` LIMIT 1)) AS Garden_m2,
            COALESCE(CAST(`Parking lots inside` AS INT64), 0) + COALESCE(CAST(`Parking lots outside` AS INT64), 0) AS Parking_lots,
            COALESCE(SAFE_CAST(ARRAY_LENGTH(SPLIT(Heating, ',')) AS INT64), 0) AS Heating,
            COALESCE(CAST(`Construction year` AS INT64), (SELECT SAFE_CAST(APPROX_QUANTILES(CAST(`Construction year` AS FLOAT64), 100)[OFFSET(50)] AS INT64) FROM `bank-marketing-project-446413.houses.house_airflow` LIMIT 1))  AS Construction_year,
            COALESCE(CAST(`Renovation year` AS INT64), (SELECT SAFE_CAST(APPROX_QUANTILES(CAST(`Renovation year` AS FLOAT64), 100)[OFFSET(50)] AS INT64) FROM `bank-marketing-project-446413.houses.house_airflow` LIMIT 1))  AS Renovation_year,
            COALESCE(SAFE_CAST(ARRAY_LENGTH(SPLIT(REPLACE(REPLACE(Aminities, '[', ''), ']', ''))) AS INT64), (SELECT APPROX_QUANTILES(CAST(ARRAY_LENGTH(SPLIT(REPLACE(REPLACE(Aminities, '[', ''), ']', ''))) AS INT64), 100)[OFFSET(50)] FROM `bank-marketing-project-446413.houses.house_airflow` WHERE Aminities IS NOT NULL LIMIT 1)) AS Amenities_count,
            COALESCE(`Condition`, 'Other') AS Condition,
            COALESCE(`Hot water`, 'Unknown') AS Hot_water,
            COALESCE((SELECT STRING_AGG(v, ', ')
                        FROM UNNEST(SPLIT(REPLACE(REPLACE(View, '[', ''), ']', ''))) AS v
                        WHERE REGEXP_CONTAINS(v, r'(view|panoramic)')), 'Unknown') AS views,
            COALESCE((SELECT STRING_AGG(o, ', ')
                        FROM UNNEST(SPLIT(REPLACE(REPLACE(View, '[', ''), ']', ''))) AS o
                        WHERE REGEXP_CONTAINS(o, r'orientation')), 'Unknown') AS Orientations
        FROM `bank-marketing-project-446413.houses.house_airflow`
        ),
        cleaned_rating_energy AS (
        SELECT
            Name,
            CASE
                WHEN `Energy efficiency rating` LIKE 'A%' THEN 'A'
                WHEN `Energy efficiency rating` LIKE 'B%' THEN 'B'
                WHEN `Energy efficiency rating` LIKE 'C%' THEN 'C'
                WHEN `Energy efficiency rating` LIKE 'D%' THEN 'D'
                WHEN `Energy efficiency rating` LIKE 'E%' THEN 'E'
                WHEN `Energy efficiency rating` LIKE 'F%' THEN 'F'
                WHEN `Energy efficiency rating` LIKE 'G%' THEN 'G'
                ELSE NULL
            END AS Energy_efficiency_cleaned
        FROM `bank-marketing-project-446413.houses.house_airflow`),
        mode_data_energy AS(
        SELECT Energy_efficiency_cleaned AS mode_value_energy
        FROM cleaned_rating_energy
        WHERE Energy_efficiency_cleaned IS NOT NULL
        GROUP BY Energy_efficiency_cleaned
        ORDER BY COUNT(*) DESC
        LIMIT 1),
        cleaned_rating_dioxide AS (
        SELECT
            Name,
            CASE
                WHEN `Environmental carbon dioxide impact rating` LIKE 'A%' THEN 'A'
                WHEN `Environmental carbon dioxide impact rating` LIKE 'B%' THEN 'B'
                WHEN `Environmental carbon dioxide impact rating` LIKE 'C%' THEN 'C'
                WHEN `Environmental carbon dioxide impact rating` LIKE 'D%' THEN 'D'
                WHEN `Environmental carbon dioxide impact rating` LIKE 'E%' THEN 'E'
                WHEN `Environmental carbon dioxide impact rating` LIKE 'F%' THEN 'F'
                WHEN `Environmental carbon dioxide impact rating` LIKE 'G%' THEN 'G'
                ELSE NULL
            END AS Energy_dioxide_cleaned
        FROM `bank-marketing-project-446413.houses.house_airflow`),
        mode_data_dioxide AS(
        SELECT Energy_dioxide_cleaned AS mode_dioxide
        FROM cleaned_rating_dioxide
        WHERE Energy_dioxide_cleaned IS NOT NULL
        GROUP BY Energy_dioxide_cleaned
        ORDER BY COUNT(*) DESC
        LIMIT 1)


        SELECT h.*,
        COALESCE(ce.Energy_efficiency_cleaned, (SELECT mode_value_energy FROM mode_data_energy)) AS energy_efficiency_rating,
        COALESCE(cd.Energy_dioxide_cleaned, (SELECT mode_dioxide FROM mode_data_dioxide)) AS Environmental_carbon_dioxide_impact_rating
        FROM cleaned_houses AS h
        LEFT JOIN cleaned_rating_energy ce ON h.Name = ce.Name
        LEFT JOIN cleaned_rating_dioxide cd ON h.Name = cd.Name
        ;

    '''
    return query