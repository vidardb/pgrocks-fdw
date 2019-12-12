# Run PostgreSQL with RocksDB in Docker

We can also deploy the PostgreSQL with RocksDB in Docker container.

## Testing

We can start a PostgreSQL container with the following official docker image.

- Run a docker container:

    ```sh
    docker run -d --name postgresql -p 5432:5432 registry.cn-shanghai.aliyuncs.com/vidardb/postgresql:rocksdb-6.2.4
    ```

- Connect to the PostgreSQL:

    ```sh
    psql -h 127.0.0.1 -p 5432 -U postgres
    ```

    Please note that you have already install the PostgreSQL client and you can execute any sql in it.

## Building

We can also build a new docker image in the following way. But before it, we need to install docker engine in our building machine.

- Install docker engine:

    Docker Engine is available on multiple platforms. Just follow the [official doc](https://docs.docker.com/install/#supported-platforms) to choose the best installation path for you.

- Build docker image:

    ```sh
    make docker-image
    ```

    After executing the previous command, it will build docker image with the default image repository and name: `vidardb/postgresql:rocksdb-6.2.4`.

    But you can also specify your own build parameters:

    ```sh
    REGISTRY=<YOUR REGISTRY ADDRESS> IMAGE=<YOUR IMAGE NAME> TAG=<YOUR IMAGE TAG> make docker-image 
    ```
