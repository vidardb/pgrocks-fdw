# Run PostgreSQL with RocksDB in Container

We can also deploy the PostgreSQL with RocksDB in Docker container.

## Building

Before building docker image, we need to install docker engine in your building machine.

- Install docker engine:

    Docker Engine is available on multiple platforms. Just follow the [official doc](https://docs.docker.com/install/#supported-platforms) to choose the best installation path for you.

- Build docker image:

    ```sh
    make docker-image
    ```

    When executing the before command, it will build docker image with the default image name `vidardb/postgresql:rocksdb-6.2.4`.

    You can also specify your own build parameters:

    ```sh
    REGISTRY=<YOUR REGISTRY ADDRESS> IMAGE=<YOUR IMAGE NAME> TAG=<YOUR IMAGE TAG> make docker-image 
    ```

## Testing

After building docker image successfully, we can run a PostgreSQL container with the image.

- Run a container:

    ```sh
    docker run -d --name postgresql -p 5432:5432 vidardb/postgresql:rocksdb-6.2.4
    ```

    Also, you can run a PostgreSQL container with the official docker image:

    ```sh
    docker run -d --name postgresql -p 5432:5432 registry.cn-shanghai.aliyuncs.com/vidardb/postgresql:rocksdb-6.2.4
    ```

- Connect to the PostgreSQL:

    ```sh
    psql -h 127.0.0.1 -p 5432 -U postgres
    ```

    Please note that you have already install the PostgreSQL client and then you can execute any sql in it.