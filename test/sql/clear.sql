--
-- Delete the extension and server 
--

DROP SERVER kv_server;  
DROP EXTENSION kv_fdw;  
  
\c postgres  
DROP DATABASE kvtest;  
