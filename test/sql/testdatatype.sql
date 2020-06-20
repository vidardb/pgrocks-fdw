--
-- Test PG Data Type
--

\c kvtest

-- clear --
DROP FOREIGN TABLE item;

-- integer --
CREATE FOREIGN TABLE item(id INTEGER, name TEXT) SERVER kv_server;
INSERT INTO item VALUES(987654321, 'test1');
INSERT INTO item VALUES(587654321, 'test2');
INSERT INTO item VALUES(787654321, 'test3');
INSERT INTO item VALUES(187654321, 'test4');
INSERT INTO item VALUES(387654321, 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- numeric --
CREATE FOREIGN TABLE item(id NUMERIC(9, 3), name TEXT) SERVER kv_server;
INSERT INTO item VALUES(987654.321, 'test1');
INSERT INTO item VALUES(587654.321, 'test2');
INSERT INTO item VALUES(787654.321, 'test3');
INSERT INTO item VALUES(187654.321, 'test4');
INSERT INTO item VALUES(387654.321, 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- floating --
CREATE FOREIGN TABLE item(id FLOAT(25), name TEXT) SERVER kv_server;
INSERT INTO item VALUES(987654.321, 'test1');
INSERT INTO item VALUES(587654.321, 'test2');
INSERT INTO item VALUES(787654.321, 'test3');
INSERT INTO item VALUES(187654.321, 'test4');
INSERT INTO item VALUES(387654.321, 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- serial --
CREATE FOREIGN TABLE item(id SERIAL, name TEXT) SERVER kv_server;
INSERT INTO item VALUES(DEFAULT, 'test1');
INSERT INTO item VALUES(DEFAULT, 'test2');
INSERT INTO item VALUES(DEFAULT, 'test3');
INSERT INTO item VALUES(DEFAULT, 'test4');
INSERT INTO item VALUES(DEFAULT, 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- money --
CREATE FOREIGN TABLE item(id MONEY, name TEXT) SERVER kv_server;
INSERT INTO item VALUES(987654.321, 'test1');
INSERT INTO item VALUES(587654.321, 'test2');
INSERT INTO item VALUES(787654.321, 'test3');
INSERT INTO item VALUES(187654.321, 'test4');
INSERT INTO item VALUES(387654.321, 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- char --
CREATE FOREIGN TABLE item(id CHAR(10), name TEXT) SERVER kv_server;
INSERT INTO item VALUES('987654.321', 'test1');
INSERT INTO item VALUES('587654.321', 'test2');
INSERT INTO item VALUES('787654.321', 'test3');
INSERT INTO item VALUES('187654.321', 'test4');
INSERT INTO item VALUES('387654.321', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- text --
CREATE FOREIGN TABLE item(id TEXT, name TEXT) SERVER kv_server;
INSERT INTO item VALUES('zzzyx', 'test1');
INSERT INTO item VALUES('aaabc', 'test2');
INSERT INTO item VALUES('wwwoq', 'test3');
INSERT INTO item VALUES('wwwab', 'test4');
INSERT INTO item VALUES('hhhij', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- bytea --
CREATE FOREIGN TABLE item(id BYTEA, name TEXT) SERVER kv_server;
INSERT INTO item VALUES('\xFFFDED', 'test1');
INSERT INTO item VALUES('\xAAAFCD', 'test2');
INSERT INTO item VALUES('\xDDDFAD', 'test3');
INSERT INTO item VALUES('\xDDDAFD', 'test4');
INSERT INTO item VALUES('\xBBBDDD', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- timestamp --
CREATE FOREIGN TABLE item(id TIMESTAMP, name TEXT) SERVER kv_server;
INSERT INTO item VALUES('2020-06-28 06:48:11', 'test1');
INSERT INTO item VALUES('2010-06-28 06:48:11', 'test2');
INSERT INTO item VALUES('2018-06-28 06:48:11', 'test3');
INSERT INTO item VALUES('2014-06-28 06:48:11', 'test4');
INSERT INTO item VALUES('2000-06-28 06:48:11', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- boolean --
CREATE FOREIGN TABLE item(id BOOLEAN, name TEXT) SERVER kv_server;
INSERT INTO item VALUES(TRUE, 'test1');
INSERT INTO item VALUES(FALSE, 'test2');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- enum --
CREATE TYPE fruit AS ENUM ('orange', 'banana', 'peach', 'apple', 'pear');
CREATE FOREIGN TABLE item(id fruit, name TEXT) SERVER kv_server;
INSERT INTO item VALUES('pear', 'test1');
INSERT INTO item VALUES('apple', 'test2');
INSERT INTO item VALUES('peach', 'test3');
INSERT INTO item VALUES('banana', 'test4');
INSERT INTO item VALUES('orange', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;
DROP TYPE fruit;

-- geo point --
-- comparison func for point does not exist --
CREATE FOREIGN TABLE item(id POINT, name TEXT) SERVER kv_server;
INSERT INTO item VALUES('(100, 100)', 'test1');
INSERT INTO item VALUES('(10, 10)', 'test2');
INSERT INTO item VALUES('(90, 10)', 'test3');
INSERT INTO item VALUES('(1, 1)', 'test4');
INSERT INTO item VALUES('(90, 100)', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- network --
CREATE FOREIGN TABLE item(id INET, name TEXT) SERVER kv_server;
INSERT INTO item VALUES('192.168.1.106/32', 'test1');
INSERT INTO item VALUES('10.0.0.100/32', 'test2');
INSERT INTO item VALUES('172.16.10.150/32', 'test3');
INSERT INTO item VALUES('192.168.1.2/32', 'test4');
INSERT INTO item VALUES('172.16.10.16/32', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- bit --
CREATE FOREIGN TABLE item(id BIT VARYING(5), name TEXT) SERVER kv_server;
INSERT INTO item VALUES(B'11111', 'test1');
INSERT INTO item VALUES(B'00000', 'test2');
INSERT INTO item VALUES(B'100', 'test3');
INSERT INTO item VALUES(B'00100', 'test4');
INSERT INTO item VALUES(B'01', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- uuid --
CREATE FOREIGN TABLE item(id UUID, name TEXT) SERVER kv_server;
INSERT INTO item VALUES('96b7b283-9db3-4ab1-b495-9416c35e6fc8', 'test1');
INSERT INTO item VALUES('56b7b283-9db3-4ab1-b495-9416c35e6fc8', 'test2');
INSERT INTO item VALUES('76b7b283-9db3-4ab1-b495-9416c35e6fc8', 'test3');
INSERT INTO item VALUES('16b7b283-9db3-4ab1-b495-9416c35e6fc8', 'test4');
INSERT INTO item VALUES('36b7b283-9db3-4ab1-b495-9416c35e6fc8', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- json --
CREATE FOREIGN TABLE item(id JSON, name TEXT) SERVER kv_server;
INSERT INTO item VALUES('{"uuid": "96b7b283-9db3-4ab1-b495-9416c35e6fc8"}', 'test1');
INSERT INTO item VALUES('{"uuid": "56b7b283-9db3-4ab1-b495-9416c35e6fc8"}', 'test2');
INSERT INTO item VALUES('{"uuid": "76b7b283-9db3-4ab1-b495-9416c35e6fc8"}', 'test3');
INSERT INTO item VALUES('{"uuid": "16b7b283-9db3-4ab1-b495-9416c35e6fc8"}', 'test4');
INSERT INTO item VALUES('{"uuid": "36b7b283-9db3-4ab1-b495-9416c35e6fc8"}', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- domain --
CREATE DOMAIN domid AS integer CHECK (VALUE > 0);
CREATE FOREIGN TABLE item(id domid, name TEXT) SERVER kv_server;
INSERT INTO item VALUES(987654321, 'test1');
INSERT INTO item VALUES(587654321, 'test2');
INSERT INTO item VALUES(787654321, 'test3');
INSERT INTO item VALUES(187654321, 'test4');
INSERT INTO item VALUES(387654321, 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;
DROP DOMAIN domid;

-- xml --
CREATE FOREIGN TABLE item(id XML, name TEXT) SERVER kv_server;
INSERT INTO item VALUES(xml'<zzz>yyy</zzz>', 'test1');
INSERT INTO item VALUES(xml'<aaa>xxx</aaa>', 'test2');
INSERT INTO item VALUES(xml'<hhh>zzz</hhh>', 'test3');
INSERT INTO item VALUES(xml'<ddd>ccc</ddd>', 'test4');
INSERT INTO item VALUES(xml'<bbb>yyy</bbb>', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- tsvector --
CREATE FOREIGN TABLE item(id TSVECTOR, name TEXT) SERVER kv_server;
INSERT INTO item VALUES('z zero zoom zero zoom', 'test1');
INSERT INTO item VALUES('b bird body bird banana', 'test2');
INSERT INTO item VALUES('k kill kernel kernel', 'test3');
INSERT INTO item VALUES('a aha apple also', 'test4');
INSERT INTO item VALUES('o open oppo ooh', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- array --
CREATE FOREIGN TABLE item(id INTEGER[], name TEXT) SERVER kv_server;
INSERT INTO item VALUES('{100, 100}', 'test1');
INSERT INTO item VALUES('{10, 10}', 'test2');
INSERT INTO item VALUES('{90, 10}', 'test3');
INSERT INTO item VALUES('{1, 1}', 'test4');
INSERT INTO item VALUES('{90, 100}', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- range --
CREATE FOREIGN TABLE item(id int4range, name TEXT) SERVER kv_server;
INSERT INTO item VALUES('[70000, 90000]', 'test1');
INSERT INTO item VALUES('[10000, 20000]', 'test2');
INSERT INTO item VALUES('[60000, 70000)', 'test3');
INSERT INTO item VALUES('(30000, 50000)', 'test4');
INSERT INTO item VALUES('(20000, 30000)', 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;

-- composite --
CREATE TYPE comtype AS (
    width   INTEGER,
    height  INTEGER
);
CREATE FOREIGN TABLE item(id comtype, name TEXT) SERVER kv_server;
INSERT INTO item VALUES((100,100), 'test1');
INSERT INTO item VALUES((10,10), 'test2');
INSERT INTO item VALUES((90,10), 'test3');
INSERT INTO item VALUES((1,1), 'test4');
INSERT INTO item VALUES((90,100), 'test5');
SELECT * FROM item;
DROP FOREIGN TABLE item;
DROP TYPE comtype;
