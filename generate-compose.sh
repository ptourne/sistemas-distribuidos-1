#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Error: Incorrect number of arguments"
    echo "Use: ./generar-compose.sh <file_name> <number_of_workers>"
    exit 1
fi
file_name=$1
number_of_workers=$2

compose_header() {
    echo "name: analisis-peliculas"
    echo "services:"
    echo "  coordinator:"
    echo "    container_name: coordinator"
    echo "    build:"
    echo "      context: ./coordinator"
    echo "    entrypoint: /coordinator"
    echo "    networks:"
    echo "      - local_net"
    echo "    environment:"
    echo "      - NUMBER_OF_WORKERS=$number_of_workers"
    echo ""
}

compose_workers() {
    local worker_id=$1
    echo "  worker$worker_id:"
    echo "    container_name: worker$worker_id"
    echo "    image: worker:latest"
    echo "    entrypoint: /worker"
    echo "    networks:"
    echo "      - local_net"
    echo "    depends_on:"
    echo "      - coordinator"
    echo ""
}

compose_network() {
    echo "networks:"
    echo "  local_net:"
    echo "    name: local_net"
    echo "    ipam:"
    echo "      driver: default"
    echo "      config:"
    echo "        - subnet: 172.25.125.0/24"
}

compose_header > $file_name
for i in $(seq 1 $number_of_workers); do
    compose_workers $i >> $file_name
done
compose_network >> $file_name
