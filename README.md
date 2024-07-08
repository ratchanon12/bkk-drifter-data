# bkk-drifter-data

This repository contains data related to drifters in Chao Phraya River. The primary dataset is stored in a `.csv` file located in the `data` folder. This file includes various metrics and information collected from drifters navigating through Chao Phraya River's waterways and canals, providing insights into patterns, behaviors, and possibly environmental factors affecting drifting activities in the area.

## Drifter Transport Performance Analysis

The query retrieves data related to drifter transport, calculating the velocity, distance, and time differences between consecutive GPS records for each tracker in the drifter_history table. It utilizes window functions to fetch the necessary data and perform calculations.

```sql
SELECT
   `a`.`id` AS `id`,
   `a`.`drifter_id` AS `drifter_id`,
   `a`.`latitude` AS `latitude`,
   `a`.`longitude` AS `longitude`,
   `a`.`recorded_timestamp` AS `recorded_timestamp`,
   `a`.`nlat` AS `nlat`,
   `a`.`nlng` AS `nlng`,
   `a`.`nrecorded_timestamp` AS `nrecorded_timestamp`,
   `a`.`distance` AS `distance`,
   timestampdiff(SECOND, `a`.`recorded_timestamp`, `a`.`nrecorded_timestamp`) AS `second_diff`,
   IFNULL(ABS(`a`.`distance` / timestampdiff(SECOND, `a`.`recorded_timestamp`, `a`.`nrecorded_timestamp`)), 0) AS `velocity`
FROM (
   SELECT 
      `drifter_history`.`id` AS `id`,
      `drifter_history`.`drifter_id` AS `drifter_id`,
      `drifter_history`.`lat` AS `latitude`,
      `drifter_history`.`lng` AS `longitude`,
      `drifter_history`.`recorded_timestamp` AS `recorded_timestamp`,
      LEAD(`drifter_history`.`lat`, 1) OVER (PARTITION BY `drifter_history`.`drifter_id` ORDER BY `drifter_history`.`recorded_timestamp`) AS `nlat`,
      LEAD(`drifter_history`.`lng`, 1) OVER (PARTITION BY `drifter_history`.`drifter_id` ORDER BY `drifter_history`.`recorded_timestamp`) AS `nlng`,
      LEAD(`drifter_history`.`recorded_timestamp`, 1) OVER (PARTITION BY `drifter_history`.`drifter_id` ORDER BY `drifter_history`.`recorded_timestamp`) AS `nrecorded_timestamp`,
      IF(
         ST_DISTANCE_SPHERE(
            POINT(`drifter_history`.`lng`, `drifter_history`.`lat`),
            POINT(
               LEAD(`drifter_history`.`lng`, 1) OVER (PARTITION BY `drifter_history`.`drifter_id` ORDER BY `drifter_history`.`recorded_timestamp`),
               LEAD(`drifter_history`.`lat`, 1) OVER (PARTITION BY `drifter_history`.`drifter_id` ORDER BY `drifter_history`.`recorded_timestamp`)
            )
         ) > `drifter_history`.`acc`,
         ST_DISTANCE_SPHERE(
            POINT(`drifter_history`.`lng`, `drifter_history`.`lat`),
            POINT(
               LEAD(`drifter_history`.`lng`, 1) OVER (PARTITION BY `drifter_history`.`drifter_id` ORDER BY `drifter_history`.`recorded_timestamp`),
               LEAD(`drifter_history`.`lat`, 1) OVER (PARTITION BY `drifter_history`.`drifter_id` ORDER BY `drifter_history`.`recorded_timestamp`)
            )
         ),
         0
      ) AS `distance`
   FROM `drifter_history`
   WHERE `drifter_history`.`loc` = 'gps'
) `a`;
```

### Query Explained

### Columns Selected

* id: Unique identifier for the record.
* drifter_id: Identifier for the tracker.
* latitude: Latitude coordinate of the tracker.
* longitude: Longitude coordinate of the tracker.
* address: Address associated with the coordinate.
* battery: Battery level of the tracker.
* timestamp: Timestamp when the data was recorded.
* recorded_timestamp: Timestamp when the data was stored in the database.
* gsm: GSM signal strength.
* sats: Number of satellites used to obtain the GPS fix.
* reason: Reason for recording the data.
* accuracy: Accuracy of the GPS coordinate.
* connection: Type of location data ('gps' here).
* nlat: Latitude of the next record for the same tracker ordered by recorded_timestamp.
* nlng: Longitude of the next record for the same tracker ordered by recorded_timestamp.
* nrecorded_timestamp: Recorded timestamp of the next record for the same tracker.
* distance: Calculated distance between current and subsequent point using ST_DISTANCE_SPHERE function.
* second_diff: Time difference in seconds between the current and next recorded timestamps.
* velocity: Calculated velocity (distance divided by time difference).

### Calculations and Conditions
* Distance Calculation: The ST_DISTANCE_SPHERE function calculates the distance between two points on the Earth's surface, considering the spherical shape of the Earth. The condition ensures that only distances greater than the accuracy threshold are considered.
* Window Functions: The LEAD function is used to fetch subsequent latitude, longitude, and recorded timestamps within the same drifter_id group, ordered by recorded_timestamp. This is essential for comparing consecutive records.
* Velocity Calculation: This measures how fast the drifter moves from one point to the next by dividing the distance traveled by the time difference between points.
* Handling Null Values: IFNULL is used to handle cases where time difference (second_diff) is zero or null, ensuring a zero velocity is returned in such cases.

## Data Preparation

To successfully execute this query, ensure the raw data is formatted and loaded into the MySQL database. The following steps outline the data preparation requirements:



Database Creation: If not already done, create a database for storing tracker history data.
```sql
CREATE DATABASE drifter_db;
USE drifter_db;
```

### Table Creation: Create the drifter_history table with the appropriate schema.
```sql
CREATE TABLE drifters (
id INT PRIMARY KEY,
drifter_id INT
);
```
```sql
CREATE TABLE drifter_history (
id INT PRIMARY KEY,
drifter_id INT,
lat DOUBLE,
lng DOUBLE,
recorded_timestamp DATETIME
);
```


### Data Insertion: Insert raw data into the drifter_history table.
```sql
INSERT INTO drifter_history (id, drifter_id, lat, lng, recorded_timestamp)
VALUES (1, 20001339, 12.9731, 77.5933,'2023-10-01 12:01:00');
-- ... (more data here)
```


## Retention episodes frequencies and durations

Python script used to fetch drifter transport data from a MySQL database, process it, and generate summary statistics and CSV files for further analysis.

### Prerequisites

Before executing this script, ensure that the following conditions are met:

1. MySQL Database: The raw tracker history data must be available in a MySQL database.
2. Environment Variables: Database connection details should be stored in a .env file.
3. Python Libraries: Install the required Python libraries using:
pip install pandas numpy pymysql python-dotenv
   Script Overview

The script fetches data related to drifter transport from a MySQL database, processes the data to calculate various metrics, and saves the processed data and summary statistics to CSV files.