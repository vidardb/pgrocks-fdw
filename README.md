# PostgresForeignDataWrapper

sudo service postgresql restart
sudo -u postgres psql

CREATE EXTENSION kv_fdw;
CREATE SERVER kv_server FOREIGN DATA WRAPPER kv_fdw;
CREATE FOREIGN TABLE test(key TEXT, value TEXT) SERVER kv_server;
INSERT INTO test VALUES('a', 'a');
SELECT * FROM test;

# delete all the stuff
DROP FOREIGN TABLE test;
DROP SERVER kv_server;
DROP EXTENSION kv_fdw;

# start PostgreSQL with debug mode
sudo -u postgres /usr/lib/postgresql/11/bin/postgres -d 0 -D /var/lib/postgresql/11/main -c config_file=/etc/postgresql/11/main/postgresql.conf
