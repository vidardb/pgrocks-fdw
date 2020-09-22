--
-- Test with clause
--

\c kvtest

-- Prepare dataset --
DROP FOREIGN TABLE COMPANY;
CREATE FOREIGN TABLE COMPANY (
    ID             INT     NOT NULL,
    NAME           TEXT    NOT NULL,
    AGE            INT     NOT NULL,
    ADDRESS        CHAR(50),
    SALARY         REAL
) SERVER kv_server;

INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES (1, 'Paul', 32, 'California', 20000.00);
INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES (2, 'Allen', 25, 'Texas', 15000.00);
INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES (3, 'Teddy', 23, 'Norway', 20000.00);
INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES (4, 'Mark', 25, 'Rich-Mond ', 65000.00);
INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES (5, 'David', 27, 'Texas', 85000.00);
INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES (6, 'Kim', 22, 'South-Hall', 45000.00);
INSERT INTO COMPANY (ID, NAME, AGE, ADDRESS, SALARY) VALUES (7, 'James', 24, 'Houston', 10000.00);

-- Show all records --
With CTE AS (
    Select ID, NAME, AGE, ADDRESS, SALARY
    FROM COMPANY
)
Select * From CTE;

-- Sum the salary that is less than 20000 --
WITH RECURSIVE t(n) AS (
    VALUES (0)
    UNION ALL
    SELECT SALARY FROM COMPANY WHERE SALARY < 20000
)
SELECT sum(n) FROM t;

--  Move the record which's salary is more than 30000 --
DROP FOREIGN TABLE COMPANY1;
CREATE FOREIGN TABLE COMPANY1 (
    ID             INT     NOT NULL,
    NAME           TEXT    NOT NULL,
    AGE            INT     NOT NULL,
    ADDRESS        CHAR(50),
    SALARY         REAL
) SERVER kv_server;

WITH moved_rows AS (
    DELETE FROM COMPANY
    WHERE SALARY >= 30000
    RETURNING *
)
INSERT INTO COMPANY1 (SELECT * FROM moved_rows);

SELECT * FROM COMPANY;
SELECT * FROM COMPANY1;
