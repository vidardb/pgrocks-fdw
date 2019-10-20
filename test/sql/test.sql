--
-- Test Insert, Select, Update, Delete, Data Types 
--

\c kvtest

CREATE FOREIGN TABLE item (id SERIAL, name VARCHAR(20), price NUMERIC(10,2), inventory INT, stime TIMESTAMP, flag BOOLEAN) SERVER kv_server;

INSERT INTO item VALUES (DEFAULT, 'Name1', 10.00, 50, current_timestamp(2), false);
INSERT INTO item VALUES (DEFAULT, 'Name2', 20.00, 50, current_timestamp(2), false);
INSERT INTO item VALUES (DEFAULT, 'Name3', 30.00, 50, current_timestamp(2), false);
INSERT INTO item VALUES (DEFAULT, 'Name1', 40.00, 10,  '2016-06-22 19:10:25', false);
SELECT * FROM item;

SELECT COUNT(*) FROM item WHERE name='Name1';
SELECT COUNT(*) FROM item WHERE name='Name1' AND inventory=10;
SELECT * FROM item WHERE name='Name2' OR inventory=10;
SELECT * FROM item WHERE stime > timestamp '2019-01-01 00:00:00';
SELECT * FROM item WHERE name LIKE '%2';
SELECT * FROM item WHERE id IN (2,3);
SELECT * FROM item LIMIT 1;
SELECT * FROM item LIMIT 1 OFFSET 1;

SELECT DISTINCT inventory FROM item;
SELECT MIN(price) FROM item;
SELECT MAX(price) FROM item;
SELECT name, SUM(inventory) FROM item GROUP BY name;
SELECT name, AVG(price) FROM item GROUP BY name;
SELECT name, AVG(price) FROM item GROUP BY name HAVING AVG(price) > 20;
SELECT id, name, price FROM item WHERE price > 10.00 ORDER BY price;
SELECT id, name, price FROM item WHERE price > 10.00 ORDER BY price ASC;
SELECT id, name, price FROM item WHERE price > 10.00 ORDER BY price DESC;

INSERT INTO item VALUES (DEFAULT, 'Name2', 20.00, 50, current_timestamp(2), NULL);
SELECT * FROM item;
SELECT id, name FROM item WHERE flag IS NULL;
UPDATE item SET inventory=40 WHERE id=5;
UPDATE item SET inventory=NULL WHERE id=5;
SELECT * FROM item;
DELETE FROM item WHERE id=5;
SELECT * FROM item;

CREATE FOREIGN TABLE product(name VARCHAR(20), make CHAR(50), product_id UUID) SERVER kv_server;
INSERT INTO product VALUES ('Name1', 'ComA', '40e6215d-b5c6-4896-987c-f30f3678f608');
INSERT INTO product VALUES ('Name2', 'ComB', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');
INSERT INTO product VALUES ('Name3', 'ComC', 'b0eebc88-4501-4ef8-bb6d-6bb9bd380a22');
SELECT * FROM product;
SELECT item.name,item.inventory,product.product_id FROM item INNER JOIN product ON item.name=product.name;
SELECT item.name,item.inventory,product.product_id FROM item CROSS JOIN product;

INSERT INTO product VALUES ('Name4', 'ComD', 'b532bc88-4502-1ef8-bc6c-6cb9bd300a23');
INSERT INTO item VALUES (DEFAULT, 'Name5', 30.00, 50, current_timestamp(2), false);
SELECT item.name,item.inventory,product.product_id FROM item LEFT OUTER JOIN product ON item.name=product.name;
SELECT item.name,item.inventory,product.product_id FROM item RIGHT OUTER JOIN product ON item.name=product.name;
SELECT item.name,item.inventory,product.product_id FROM item FULL OUTER JOIN product ON item.name=product.name;

DROP FOREIGN TABLE item;
DROP FOREIGN TABLE product;
