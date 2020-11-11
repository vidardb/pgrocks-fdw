--
-- Test select, insert, delete, update explains
--

\c kvtest

CREATE FOREIGN TABLE test(key TEXT, value TEXT) SERVER kv_server;

EXPLAIN INSERT INTO test VALUES('VidarDB', 'Database');  

EXPLAIN SELECT * FROM test;  

EXPLAIN UPDATE test SET value='Machine Learning Database';  

EXPLAIN DELETE FROM test WHERE key='VidarDB';

DROP FOREIGN TABLE test;  
