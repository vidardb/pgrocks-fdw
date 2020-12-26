--
-- Test create, alter, and drop a table
--

\c kvtest

CREATE FOREIGN TABLE testddl(id SERIAL, name VARCHAR(20), price NUMERIC(10,2), inventory INT, stime TIMESTAMP, flag BOOLEAN) SERVER kv_server;

INSERT INTO testddl VALUES (DEFAULT, 'Name1', 10.00, 50, current_timestamp(2), false);
INSERT INTO testddl VALUES (DEFAULT, 'Name2', 20.00, 50, current_timestamp(2), false);
INSERT INTO testddl VALUES (DEFAULT, 'Name3', 30.00, 50, current_timestamp(2), false);

\d testddl

SELECT * FROM testddl;

ALTER FOREIGN TABLE testddl RENAME TO item;

\d testddl

\d item

ALTER FOREIGN TABLE item RENAME COLUMN stime TO stamp;

\d item

ALTER FOREIGN TABLE item ALTER COLUMN name TYPE TEXT;

\d item

SELECT * FROM item;

ALTER FOREIGN TABLE item DROP COLUMN flag;

\d item

DROP FOREIGN TABLE item;
