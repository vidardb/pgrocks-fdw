--
-- Test the copy command 
--

\c kvtest

CREATE FOREIGN TABLE product(name VARCHAR(20), make CHAR(50), product_id UUID) SERVER kv_server;
\copy product FROM 'sql/products.csv' WITH CSV;
COPY (SELECT * FROM product) TO '/tmp/products.csv' WITH (FORMAT CSV);
DROP FOREIGN TABLE product;
\! diff -uN /tmp/products.csv sql/products.csv
