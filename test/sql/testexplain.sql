--
-- Test select, insert, delete, update explains
--

\c kvtest

CREATE FOREIGN TABLE test(key TEXT, value TEXT) SERVER kv_server;

EXPLAIN INSERT INTO test VALUES('California', 'Waterloo');  

EXPLAIN SELECT * FROM test;  

EXPLAIN UPDATE test SET value='VidarSQL';  

EXPLAIN DELETE FROM test WHERE key='California';

DROP FOREIGN TABLE test;  
