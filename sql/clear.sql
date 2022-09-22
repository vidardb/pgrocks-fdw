--
-- Delete the extension and the server 
--

\c kvtest

DROP SERVER kv_server;  
DROP EXTENSION kv_fdw;  
  
\c postgres  
DROP DATABASE kvtest;  
