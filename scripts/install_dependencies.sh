#!/bin/bash

sudo apt update
sudo apt install -y gcc g++ clang flex libbison-dev libxml2-dev zlib1g-dev libreadline-dev m4 cmake build-essential git wget
sudo tee /etc/apt/sources.list.d/pgdg.list << END
deb http://apt.postgresql.org/pub/repos/apt/ focal-pgdg main
END
wget https://www.postgresql.org/media/keys/ACCC4CF8.asc
sudo apt-key add ACCC4CF8.asc
sudo apt update
sudo apt install -y postgresql-client-13 postgresql-server-dev-13
