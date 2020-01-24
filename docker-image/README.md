# Run PostgreSQL with RocksDB in Docker

We can deploy PostgreSQL with RocksDB in Docker container.

## Testing

Start the container with the following Docker image we have provided.

- Run a Docker container:

    ```sh
    docker run -d --name postgresql -p 5432:5432 vidardb/postgresql:rocksdb-6.2.4
    ```

- Connect to the PostgreSQL:

    ```sh
    psql -h 127.0.0.1 -p 5432 -U postgres
    ```

    Please note that PostgreSQL client should already be installed before running the container.

## Building

We can build a new Docker image in the following way. It is the prerequisite that install docker engine in the building machine.

- Install Docker engine:

    Docker engine is available on multiple platforms. Just follow the [official doc](https://docs.docker.com/install/#supported-platforms) to choose the best installation option for you.

- Build Docker image:

    ```sh
    make docker-image
    ```

    After executing the previous command, it will build docker image with the default image repository and name: `vidardb/postgresql:rocksdb-6.2.4`.

    We can also specify the build parameters:

    ```sh
    REGISTRY=<YOUR REGISTRY ADDRESS> IMAGE=<YOUR IMAGE NAME> TAG=<YOUR IMAGE TAG> make docker-image 
    ```
