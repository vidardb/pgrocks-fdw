# Run PostgreSQL with RocksDB in Docker

We can easily deploy PostgreSQL with RocksDB in a Docker container.

## Get Started

The following document will guide you to install and play with RocksDB in a few minutes:

- Docker is the only requirement. Install it at https://docs.docker.com/install/ .

- Run a RocksDB Docker container:

    ```sh
    docker run -d --name postgresql -p 5432:5432 vidardb/postgresql:rocksdb-6.2.4
    ```

- Connect to the PostgreSQL after the container is ready:

    ```sh
    psql -h 127.0.0.1 -p 5432 -U postgres
    ```

    *Please note that the PostgreSQL client should already be installed before connecting the container.*

- For the users who don't have the PostgreSQL client installed:

    ```sh
    docker exec -it postgresql /bin/bash
    ```
    
    Then we can connect to the PostgreSQL successfully inside the container (retry the second command again). 

## Building your own Docker image

You may want to create your custom Docker image. Do it by '1-click':

    ```sh
    # Building a Docker image with the default name: 'vidardb/postgresql:rocksdb-6.2.4'
    make docker-image
    ```

Some available build parameters:

    ```sh
    REGISTRY=<YOUR REGISTRY ADDRESS> IMAGE=<YOUR IMAGE NAME> TAG=<YOUR IMAGE TAG> make docker-image 
    ```
