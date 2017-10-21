create database IF NOT EXISTS ${hiveconf:databaseName};

use ${hiveconf:databaseName};

Create TABLE IF NOT EXISTS archiveLogData
(
name string,
hitRate float,
errorRate float,
year int,
month int, 
day int,
hour int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '${hiveconf:pigOutputDir}/${hiveconf:jobDate}' OVERWRITE INTO TABLE archiveLogData;

Create TABLE IF NOT EXISTS logData
(
name string,
hitRate float,
errorRate float
)
Partitioned by (year int, month int, day int, hour int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS orc;

insert into table logData partition(year=${hiveconf:year}, month=${hiveconf:month}, day=${hiveconf:day}, hour=${hiveconf:hour}) select name,hitRate, errorRate from archiveLogData where year=${hiveconf:year}, month=${hiveconf:month}, day=${hiveconf:day}, hour=${hiveconf:hour};

select COUNT(*) from archiveLogData;
select COUNT(*) from logData;