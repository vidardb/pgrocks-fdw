# Run PostgreSQL with RocksDB using Docker

We can easily deploy PostgreSQL with RocksDB using Docker. This section will guide you to install and play with RocksDB in one minute.

## Requirement

Docker is the only requirement. Install it at [https://docs.docker.com/get-docker/](https://docs.docker.com/get-docker/) .

## Run a RocksDB's Docker container:

*Before running any of the following commands, please ensure that both the container name `postgresql` and the port `5432` have not been used in your environment.*

To run a RocksDB's Docker container:

```sh
docker run -d --name postgresql -p 5432:5432 vidardb/postgresql:rocksdb-6.2.4
```

After doing that, a `postgresql` container will start and the database will be initialized.

## Connect to the PostgreSQL:

For the users who don't have the PostgreSQL client installed:

```sh
docker exec -it postgresql sh -c 'psql -h 127.0.0.1 -p 5432 -U postgres'
```

For the others who have the PostgreSQL client installed:

```sh
psql -h 127.0.0.1 -p 5432 -U postgres
```

## Build your own Docker image

You may want to create your custom Docker image. Do it by '1-click' in the root directory of `PostgresForeignDataWrapper` repository:

```sh
# Build a docker image with the default name 'vidardb/postgresql:rocksdb-6.2.4'
make docker-image
```

Some available build parameters:

```sh
REGISTRY=<YOUR REGISTRY ADDRESS> IMAGE=<YOUR IMAGE NAME> TAG=<YOUR IMAGE TAG> make docker-image 
```
