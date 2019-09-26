--
--Create the extension and server for tests
--


CREATE DATABASE kvtest;  
\c kvtest  

CREATE EXTENSION kv_fdw;  
CREATE SERVER kv_server FOREIGN DATA WRAPPER kv_fdw;  
