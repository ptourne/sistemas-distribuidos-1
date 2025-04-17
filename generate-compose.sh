#!/bin/bash

if [ "$#" -eq 0 ]||[ "$#" -gt 2 ]; then
    echo "Error: Incorrect number of arguments"
    echo "Use: ./generar-compose.sh [file_name] <number_of_workers>"
    exit 1
elif [ "$#" -eq 1 ]; then
    file_name=./docker-compose.yml
    number_of_workers=$1
elif [ "$#" -eq 2 ]; then
    file_name=$1
    number_of_workers=$2
fi

# Verify number_of_workers is a positive integer
if ! [[ "$number_of_workers" =~ ^[0-9]+$ ]] || [ "$number_of_workers" -le -1 ]; then
    echo "Error: Number of workers must be a positive integer"
    exit 1
fi

compose_header() {
    echo "name: analisis-peliculas
services:"
}

compose_rabbitmq() {
    echo "    rabbitmq:
        container_name: rabbitmq
        image: rabbitmq:latest
        ports:
            - \"5672:5672\"
            - \"15672:15672\"
        networks:
            - local_net
        healthcheck:
            test: rabbitmq-diagnostics -q ping
            interval: 10s
            timeout: 10s
            retries: 10
"
}

compose_coordinator() {
    echo "    coordinator:
        container_name: coordinator
        build:
            context: .
            dockerfile: coordinator/Dockerfile
        command: /bin/sh -c '(test -f /datasets/credits.csv || unzip /datasets/credits.zip -d /datasets) && (test -f /datasets/ratings.csv || unzip /datasets/ratings.zip -d /datasets) && /coordinator'        
        networks:
            - local_net
        environment:
            - NUMBER_OF_WORKERS=$number_of_workers
        depends_on:
            rabbitmq:
                condition: service_healthy
        volumes:
            - ${PWD}/datasets:/datasets
"
}
# TODO: entrypoint: /coordinator en compose_coordinador() y el command en compose_client()

compose_workers() {
    local worker_id=$1
    echo "    worker$worker_id:
        container_name: worker$worker_id
        build:
            context: .
            dockerfile: worker/Dockerfile
        entrypoint: /worker
        environment:
            - WORKER_ID=$worker_id
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
"
}

compose_network() {
    echo "networks:
    local_net:
        name: local_net
        ipam:
            driver: default
            config:
                - subnet: 172.25.125.0/24
"
}

compose_header > $file_name
compose_rabbitmq >> $file_name
compose_coordinator >> $file_name
for i in $(seq 1 $number_of_workers); do
    compose_workers $i >> $file_name
done
compose_network >> $file_name
