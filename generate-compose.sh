#!/bin/bash

if [ "$#" -eq 3 ]; then
    file_name=./docker-compose.yml
    number_of_workers=$1
    number_of_reduce_by_country_sum_budgets=$2
    number_of_reduce_top_5_by_budgets=$3
elif [ "$#" -eq 4 ]; then
    file_name=$1
    number_of_workers=$2
    number_of_reduce_by_country_sum_budgets=$3
    number_of_reduce_top_5_by_budgets=$4

else
    echo "Error: Incorrect number of arguments"
    echo "Use: ./generar-compose.sh [file_name] <number_of_workers>"
    exit 1
fi

# Verify number_of_workers is a positive integer
if ! [[ "$number_of_workers" =~ ^[0-9]+$ ]] || [ "$number_of_workers" -le -1 ]; then
    echo "Error: Number of filters must be a positive integer"
    exit 1
fi
# Verify number_of_reduce_by_country_sum_budgets is a positive integer
if ! [[ "$number_of_reduce_by_country_sum_budgets" =~ ^[0-9]+$ ]] || [ "$number_of_reduce_by_country_sum_budgets" -le -1 ]; then
    echo "Error: Number of reduce by country sum budgets must be a positive integer"
    exit 1
fi
# Verify number_of_reduce_top_5_by_budgets is a positive integer
if ! [[ "$number_of_reduce_top_5_by_budgets" =~ ^[0-9]+$ ]] || [ "$number_of_reduce_top_5_by_budgets" -le -1 ]; then
    echo "Error: Number of reduce top 5 by budgets must be a positive integer"
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
        entrypoint: /coordinator
        networks:
            - local_net
        environment:
            - NUMBER_OF_WORKERS=$number_of_workers
            - NUMBER_OF_REDUCE_BY_COUNTRY_SUM_BUDGETS=$number_of_reduce_by_country_sum_budgets
            - NUMBER_OF_REDUCE_TOP_5_BY_BUDGETS=$number_of_reduce_top_5_by_budgets
        depends_on:
            rabbitmq:
                condition: service_healthy
            sentiment_server:
                condition: service_healthy
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
            - WORKER_ID=$
            - N_JOINERS=$number_of_workers
            - NLP_GRPC_ADDR=sentiment_server:50051
            - SERVER_PORT=1234
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
"
}

compose_client() {
    echo "    client:
        container_name: client
        build:
            context: .
            dockerfile: client/Dockerfile
        entrypoint: /client
        environment:
            - SERVER_PORT=endpoint:9876
        networks:
            - local_net
        depends_on:
            - endpoint
        volumes:
            - ${PWD}/client/datasets:/datasets
"
}

compose_endpoint() {
    echo "    endpoint:
        container_name: endpoint
        build:
            context: .
            dockerfile: endpoint/Dockerfile
        entrypoint: /endpoint
        environment:
            - ENDPOINT_PORT=9876
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
"
}

compose_reduce_top_5_by_budgets() {
    local worker_id=$1
    echo "    reduce_top_5_by_budget$worker_id:
        container_name: reduce_top_5_by_budget$worker_id
        build:
            context: .
            dockerfile: map_reducer/main/reduce_top_5_by_budget/Dockerfile
        entrypoint: /map_reducer
        environment:
            - WORKER_ID=$worker_id
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
"
}

compose_reduce_by_country_sum_budgets() {
    local worker_id=$1
    echo "    reduce_by_country_sum_budget$worker_id:
        container_name: reduce_by_country_sum_budget$worker_id
        build:
            context: .
            dockerfile: map_reducer/main/reduce_by_country_sum_budget/Dockerfile
        entrypoint: /map_reducer
        environment:
            - WORKER_ID=$worker_id
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
            sentiment_server:
                condition: service_healthy
        volumes:
            - ${PWD}/joiner_credits:/joiner_credits
            - ${PWD}/joiner_ratings:/joiner_ratings
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

compose_sentiment_server() {
    echo "    sentiment_server:
        container_name: sentiment_server
        build:
            context: .
            dockerfile: worker/nlp/python_server/Dockerfile
        ports:
            - \"50051:50051\"
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
        healthcheck:
            test: ncat -zv localhost 50051
            interval: 10s
            timeout: 10s
            retries: 10
"
}


compose_header > $file_name
compose_rabbitmq >> $file_name
compose_sentiment_server >> $file_name
compose_coordinator >> $file_name
for i in $(seq 1 $number_of_workers); do
    compose_workers $i >> $file_name
done
for i in $(seq 1 $number_of_reduce_by_country_sum_budgets); do
    compose_reduce_by_country_sum_budgets $i >> $file_name
done
for i in $(seq 1 $number_of_reduce_top_5_by_budgets); do
    compose_reduce_top_5_by_budgets $i >> $file_name
done
compose_client >> $file_name
compose_endpoint >> $file_name
compose_network >> $file_name
