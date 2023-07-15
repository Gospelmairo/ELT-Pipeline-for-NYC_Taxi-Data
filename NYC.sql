/*.
Answer the following questions: from the tables on bigquery
*/

-- Question 1
--What season has the highest number of pickup rides (Winter, Summer, Autumn and Spring)

-- GREEN
SELECT season, count(*)
FROM `myproject-389323.Rio3631.green_tripdata`
GROUP BY season
ORDER BY 2 desc
LIMIT 1;
-- For green service the season is winter

-- YELLOW
SELECT season, count(*)
FROM `myproject-38932.Rio3631.yellow_tripdata`
GROUP BY season
ORDER BY 2 desc
LIMIT 1;
-- For yellow service the season is spring

--FHV
SELECT season, count(*)
FROM `myproject-38932.Rio3631.fhv_tripdata`
GROUP BY season
ORDER BY 2 desc
LIMIT 1;
-- For for_hire_vehicle the season is winter


--Question 2
-- What period of the day has the highest pickup number

--GREEN
SELECT period_of_day
FROM(
  SELECT
    pickup_time,
    CASE
      WHEN EXTRACT(HOUR FROM pickup_time) >= 0 AND EXTRACT(HOUR FROM pickup_time) < 6 THEN 'Night'
      WHEN EXTRACT(HOUR FROM pickup_time) >= 6 AND EXTRACT(HOUR FROM pickup_time) < 12 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM pickup_time) >= 12 AND EXTRACT(HOUR FROM pickup_time) < 18 THEN 'Afternoon'
      ELSE 'Evening'
    END AS period_of_day
  FROM `myproject-38932.Rio3631.green_tripdata`
) AS p
GROUP BY 1
LIMIT 1;
-- The period of the day that has the highest pickup number in Green services is: Afternoon

--YELLOW
SELECT period_of_day
FROM(
  SELECT
    pickup_time,
    CASE
      WHEN EXTRACT(HOUR FROM pickup_time) >= 0 AND EXTRACT(HOUR FROM pickup_time) < 6 THEN 'Night'
      WHEN EXTRACT(HOUR FROM pickup_time) >= 6 AND EXTRACT(HOUR FROM pickup_time) < 12 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM pickup_time) >= 12 AND EXTRACT(HOUR FROM pickup_time) < 18 THEN 'Afternoon'
      ELSE 'Evening'
    END AS period_of_day
  FROM `myproject-38932.Rio3631.yellow_tripdata`
) AS p
GROUP BY 1
LIMIT 1;
-- Night as a period of the day has the highest pickup number in Yellow services

--FHV
SELECT period_of_day
FROM(
  SELECT
    pickup_time,
    CASE
      WHEN EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) >= 0 AND EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) < 6 THEN 'Night'
      WHEN EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) >= 6 AND EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) < 12 THEN 'Morning'
      WHEN EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) >= 12 AND EXTRACT(HOUR FROM TIMESTAMP(pickup_time)) < 18 THEN 'Afternoon'
      ELSE 'Evening'
    END AS period_of_day
  FROM `myproject-38932.Rio3631.fhv_tripdata`
) AS p
GROUP BY 1
LIMIT 1;
-- The period of the day that has the highest pickup number in fhv services is Morning


--Question 3
-- What day of the week (Monday- Sunday) has the highest pickup number
--GREEN
SELECT 
  num_day_of_week,
  CASE
    WHEN num_day_of_week = 1 then "Sunday"
    WHEN num_day_of_week = 2 then "Monday"
    WHEN num_day_of_week = 3 then "Tuesday"
    WHEN num_day_of_week = 4 then "Wednesday"
    WHEN num_day_of_week = 5 then "Thursday"
    WHEN num_day_of_week = 6 then "Friday"
    ELSE "Saturday"
  END AS day_of_week,
FROM (
  SELECT
    pickup_time,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP(pickup_time)) AS num_day_of_week
  FROM `myproject-38932.Rio3631.green_tripdata`
  LIMIT 1000
) AS num
GROUP BY 1
LIMIT 1;
-- The day of the week (Monday- Sunday) that has the highest pickup number in green services is Friday

--YELLOW
SELECT 
  num_day_of_week,
  CASE
    WHEN num_day_of_week = 1 then "Sunday"
    WHEN num_day_of_week = 2 then "Monday"
    WHEN num_day_of_week = 3 then "Tuesday"
    WHEN num_day_of_week = 4 then "Wednesday"
    WHEN num_day_of_week = 5 then "Thursday"
    WHEN num_day_of_week = 6 then "Friday"
    ELSE "Saturday"
  END AS day_of_week,
FROM (
  SELECT
    pickup_time,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP(pickup_time)) AS num_day_of_week
  FROM `myproject-38932.Rio3631.fhv_tripdata`
  LIMIT 1000
) AS num
GROUP BY 1
LIMIT 1;
-- The day of the week (Monday- Sunday) that has the highest pickup number in yellow services is Saturday

--FHV
SELECT 
  num_day_of_week,
  CASE
    WHEN num_day_of_week = 1 then "Sunday"
    WHEN num_day_of_week = 2 then "Monday"
    WHEN num_day_of_week = 3 then "Tuesday"
    WHEN num_day_of_week = 4 then "Wednesday"
    WHEN num_day_of_week = 5 then "Thursday"
    WHEN num_day_of_week = 6 then "Friday"
    ELSE "Saturday"
  END AS day_of_week,
FROM (
  SELECT
    pickup_time,
    EXTRACT(DAYOFWEEK FROM TIMESTAMP(pickup_time)) AS num_day_of_week
  FROM `myproject-38932.Rio3631.fhv_tripdata`
  LIMIT 1000
) AS num
GROUP BY 1
LIMIT 1;
-- The day of the week from (Monday - Sunday) that has the highest pickup number in fhv services is Saturday



