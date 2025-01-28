-- We want to have data per value
CREATE TABLE `gko-csta-flink-sql-flatten` AS
SELECT `sensorId`,
	CAST(ROW(`flatten`.`type`, `flatten`.`unit`, `flatten`.`value`) AS ROW < `type` STRING, `unit` STRING, `value` DOUBLE >) AS `value`,
	`timestamp`
FROM `team-global`.`csta_global_cluster`.`gko-csta-raw`,
	UNNEST(`team-global`.`csta_global_cluster`.`gko-csta-raw`.`value`) AS `flatten`;


-- We need to transform from Fahrenheit to Celsius
CREATE table `gko-csta-flink-sql-transformed` AS SELECT
  `sensorId`,
  `type`,
  CASE
    WHEN `value`.`unit` = 'Fahrenheit' AND `value`.`type`= 'temperature'
     THEN 'Celsius'
    ELSE `value`.`unit`
   END AS `unit`,
  CASE
    WHEN `value`.`unit` = 'Fahrenheit' AND `value`.`type`= 'temperature'
    THEN (`value`.`value` - 32) / 1.8
    ELSE `value`.`value`
  END AS `value`,
  `timestamp`
  FROM `team-global`.`csta_global_cluster`.`gko-csta-flink-sql-flatten`;


-- We aggregate over the window
CREATE TABLE `team-global`.`csta_global_cluster`.`gko-csta-flink-sql-windowed` AS SELECT
    `sensorId`,
    `type`,
    ROUND(AVG(`value`), 1) AS `valueAvg`,
    COUNT(`value`) AS `count`,
    LAST_VALUE(`unit`) AS `unit`,
    LAST_VALUE(`timestamp`) AS `timestamp`,
    window_start,
    window_end
  FROM TABLE(
   TUMBLE(
     DATA => TABLE `team-global`.`csta_global_cluster`.`gko-csta-flink-sql-transformed`,
     TIMECOL => DESCRIPTOR($rowtime),
     SIZE => INTERVAL '10' SECONDS))
  GROUP BY window_start, window_end, `sensorId`, `type`;


-- We restructure it into the desired form
CREATE TABLE `team-global`.`csta_global_cluster`.`gko-csta-flink-sql-aggregation` AS SELECT
    `sensorId`,
    `type`,
    window_start,
    window_end,
  	CAST(ROW(`valueAvg`, `count`, `unit`, `timestamp`) AS ROW < `valueAvg` DOUBLE, `count` DOUBLE, `unit` STRING, `timestamp` STRING >) AS `value`
FROM `team-global`.`csta_global_cluster`.`gko-csta-flink-sql-windowed`;



