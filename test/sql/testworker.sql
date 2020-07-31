--
-- Test the relation and worker process mapping
--
-- NOTE: default 'max_worker_processes' configuration is 8.
--

CREATE DATABASE relworker;
\c relworker

CREATE EXTENSION kv_fdw;
CREATE SERVER kv_server FOREIGN DATA WRAPPER kv_fdw;

CREATE FOREIGN TABLE test1(key TEXT, value TEXT) SERVER kv_server;
INSERT INTO test1 VALUES('YC', 'VidarDB');
SELECT * FROM test1;

CREATE FOREIGN TABLE test2(key TEXT, value TEXT) SERVER kv_server;
INSERT INTO test2 VALUES('YC', 'VidarDB');
SELECT * FROM test2;

CREATE FOREIGN TABLE test3(key TEXT, value TEXT) SERVER kv_server;
INSERT INTO test3 VALUES('YC', 'VidarDB');
SELECT * FROM test3;

CREATE DATABASE relworker2;
\c relworker2

CREATE EXTENSION kv_fdw;
CREATE SERVER kv_server FOREIGN DATA WRAPPER kv_fdw;

CREATE FOREIGN TABLE test1(key TEXT, value TEXT) SERVER kv_server;
INSERT INTO test1 VALUES('YC', 'VidarDB');
SELECT * FROM test1;

CREATE FOREIGN TABLE test2(key TEXT, value TEXT) SERVER kv_server;
INSERT INTO test2 VALUES('YC', 'VidarDB');
SELECT * FROM test2;

CREATE FOREIGN TABLE test3(key TEXT, value TEXT) SERVER kv_server;
INSERT INTO test3 VALUES('YC', 'VidarDB');
SELECT * FROM test3;

\c postgres
DROP DATABASE relworker;
DROP DATABASE relworker2;
