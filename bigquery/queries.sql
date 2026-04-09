-- bigquery/queries.sql
-- Analytical queries for surge pricing dashboard
-- Run these in BigQuery console or connect to Looker Studio


-- 1. average surge by location today
SELECT
    location,
    ROUND(AVG(surge_multiplier), 2) AS avg_surge,
    MAX(surge_multiplier)           AS peak_surge,
    COUNT(*)                        AS snapshot_count
FROM `dynamic_pricing.surge_analytics`
WHERE DATE(recorded_at) = CURRENT_DATE()
GROUP BY location
ORDER BY avg_surge DESC;


-- 2. surge by hour of day — peak hour correlation
SELECT
    hour_of_day,
    is_peak_hour,
    ROUND(AVG(surge_multiplier), 2) AS avg_surge,
    ROUND(AVG(rider_count), 0)      AS avg_riders,
    ROUND(AVG(driver_count), 0)     AS avg_drivers
FROM `dynamic_pricing.surge_analytics`
WHERE DATE(recorded_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY hour_of_day, is_peak_hour
ORDER BY hour_of_day;


-- 3. highest surge zones last 24 hours
SELECT
    location,
    MAX(surge_multiplier)           AS max_surge,
    ROUND(AVG(surge_multiplier), 2) AS avg_surge,
    COUNTIF(surge_multiplier >= 2)  AS high_surge_count
FROM `dynamic_pricing.surge_analytics`
WHERE recorded_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY location
ORDER BY max_surge DESC;


-- 4. supply vs demand ratio over time
SELECT
    recorded_at,
    location,
    rider_count,
    driver_count,
    ROUND(SAFE_DIVIDE(rider_count, driver_count), 2) AS demand_supply_ratio,
    surge_multiplier
FROM `dynamic_pricing.surge_analytics`
WHERE DATE(recorded_at) = CURRENT_DATE()
ORDER BY recorded_at DESC;


-- 5. surge trend last 2 hours — for real-time dashboard
SELECT
    TIMESTAMP_TRUNC(recorded_at, MINUTE)    AS minute,
    location,
    ROUND(AVG(surge_multiplier), 2)         AS avg_surge
FROM `dynamic_pricing.surge_analytics`
WHERE recorded_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
GROUP BY minute, location
ORDER BY minute DESC, location;