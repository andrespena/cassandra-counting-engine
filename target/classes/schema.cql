
DROP KEYSPACE counting;
CREATE KEYSPACE counting WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

USE counting;

DROP TABLE counts;
CREATE TABLE counts (
  name        varchar,
  type        varchar,
  granularity varchar,
  time        timestamp,
  value       counter,
  PRIMARY KEY (name, type, granularity, time)
);


