--
-- Test basic select, insert, delete, update operations
--

\c kvtest

CREATE FOREIGN TABLE student(id INTEGER, name TEXT) SERVER kv_server;

INSERT INTO student VALUES(20757123, 'Rafferty');
SELECT * FROM student;

INSERT INTO student VALUES(20777345, 'Heisenberg');
SELECT * FROM student;

DELETE FROM student WHERE id=20777345;
SELECT * FROM student;

UPDATE student SET name='Jones' WHERE id=20757123;
SELECT * FROM student;

DROP FOREIGN TABLE student;  
