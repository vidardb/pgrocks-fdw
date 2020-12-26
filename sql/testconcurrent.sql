--
-- Test Concurrent Operation Manually
--

\c kvtest

-- clear --
DROP FOREIGN TABLE item;

-- create table --
CREATE FOREIGN TABLE item(id BIGINT, val BIGINT) SERVER kv_server;

--------------------------------------------------------------------------------
-- NOTE: run the following commands simultaneously in three terminals --
-- run in terminal #1 --
insert into item values (generate_series(1,1000000), generate_series(1,1000000));

-- run in terminal #2 --
insert into item values (generate_series(1000001,2000000), generate_series(1000001,2000000));

-- run in terminal #3 --
insert into item values (generate_series(2000001,3000000), generate_series(2000001,3000000));
--------------------------------------------------------------------------------

-- only run in any terminal --
select count(*) from item;
