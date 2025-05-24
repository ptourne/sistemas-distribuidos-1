#!/bin/bash

if [ "$#" -eq 11 ]; then
    file_name=./docker-compose.yml
    number_of_workers=$1
    number_of_joiners_credits=$2
    number_of_joiners_ratings=$3
    number_of_reduce_by_country_sum_budgets=$4
    number_of_reduce_top_5_by_budgets=$5
    number_of_reduce_by_sentiment=$6
    number_of_reduce_by_actor=$7
    number_of_reduce_top_10_by_actor=$8
    number_of_reduce_top_bottom_avg_ratings=${9}
    number_of_clients=${10}
    number_of_nlp_workers=${11}

elif [ "$#" -eq 12 ]; then
    file_name=$1
    number_of_workers=$2
    number_of_joiners_credits=$3
    number_of_joiners_ratings=$4
    number_of_reduce_by_country_sum_budgets=$5
    number_of_reduce_top_5_by_budgets=$6
    number_of_reduce_by_sentiment=$7
    number_of_reduce_by_actor=$8
    number_of_reduce_top_10_by_actor=${9}
    number_of_reduce_top_bottom_avg_ratings=${10}
    number_of_clients=${11}
    number_of_nlp_workers=${12}
else
    echo "Error: Incorrect number of arguments"
    echo "Use: ./generar-compose.sh [file_name] <number_of_workers>,<number_of_joiners_credits>,<number_of_joiners_ratings>,
    <number_of_reduce_by_country_sum_budgets>, <number_of_reduce_top_5_by_budgets>,
    <number_of_reduce_by_sentiment>, <number_of_reduce_by_actor>, <number_of_reduce_top_10_by_actor>,
    <number_of_reduce_top_bottom_avg_ratings>, <number_of_clients>, <number_of_nlp_workers>"
    exit 1
fi

# Verify number_of_workers is a positive integer
if ! [[ "$number_of_workers" =~ ^[0-9]+$ ]] || [ "$number_of_workers" -le -1 ]; then
    echo "Error: Number of workers must be a positive integer"
    exit 1
fi

# Verify number_of_joiners_ratings is a positive integer
if ! [[ "$number_of_joiners_ratings" =~ ^[0-9]+$ ]] || [ "$number_of_joiners_ratings" -le -1 ]; then
    echo "Error: Number of joiners raitings must be a positive integer"
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
# Verify number_of_reduce_by_sentiment is a positive integer
if ! [[ "$number_of_reduce_by_sentiment" =~ ^[0-9]+$ ]] || [ "$number_of_reduce_by_sentiment" -le -1 ]; then
    echo "Error: Number of reduce by sentiment must be a positive integer"
    exit 1
fi

# Verify number_of_reduce_by_actor is a positive integer
if ! [[ "$number_of_reduce_by_actor" =~ ^[0-9]+$ ]] || [ "$number_of_reduce_by_actor" -le -1 ]; then
    echo "Error: Number of reduce by actor must be a positive integer"
    exit 1
fi

# Verify number_of_reduce_top_10_by_actor is a positive integer
if ! [[ "$number_of_reduce_top_10_by_actor" =~ ^[0-9]+$ ]] || [ "$number_of_reduce_top_10_by_actor" -le -1 ]; then
    echo "Error: Number of reduce top 10 by actor must be a positive integer"
    exit 1
fi

# Verify number_of_clients is a positive integer
if ! [[ "$number_of_clients" =~ ^[0-9]+$ ]] || [ "$number_of_clients" -le -1 ]; then
    echo "Error: Number of clients must be a positive integer"
    exit 1
fi

if ! [[ "$number_of_nlp_workers" =~ ^[0-9]+$ ]] || [ "$number_of_nlp_workers" -le -1 ]; then
    echo "Error: Number of nlp_workers must be a positive integer"
    exit 1
fi



compose_header() {
    echo "name: analisis-peliculas
services:"
}

compose_rabbitmq() {
    echo "    rabbitmq:
        container_name: rabbitmq
        image: rabbitmq:4.1.0-management
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
        volumes:
            - ${PWD}/rabbitmq/rabbitmq.conf:/etc/rabbitmq/conf.d/rabbitmq.conf
"
}
# volumes:
    # - ${PWD}/rabbitmq_config/rabbitmq.conf:/etc/rabbitmq/conf.d/rabbitmq.conf

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
            - PREFETCH=1 # Potential optimization
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
    local monitor_addresses=$2
    echo "    worker$worker_id:
        container_name: worker$worker_id
        build:
            context: .
            dockerfile: worker/Dockerfile
        entrypoint: /worker
        environment: 
            - WORKER_ID=$worker_id
            - N_JOINERS_CREDITS=$number_of_joiners_credits
            - N_JOINERS_RATINGS=$number_of_joiners_ratings
            - N_WORKERS=$number_of_workers
            - SERVER_PORT=1234
            - PREFETCH=1 # Potential optimization
            - WORKER_NAME=worker$worker_id
            - MONITOR_ADDRESSES=$monitor_addresses
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
            sentiment_server:
                condition: service_healthy
"
}

compose_nlp_workers() {
    local worker_id=$1
    echo "    nlp_worker$worker_id:
        container_name: nlp_worker$worker_id
        build:
            context: .
            dockerfile: nlp/Dockerfile
        entrypoint: /nlp
        environment: 
            - WORKER_ID=$worker_id
            - N_WORKERS=$number_of_nlp_workers
            - NLP_GRPC_ADDR=sentiment_server:50051
            - SERVER_PORT=1234
            - PREFETCH=1 # Potential optimization
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
            sentiment_server:
                condition: service_healthy
"
}

#compose_lean_workers() {
#    local worker_id=$1
#    echo "    lean_worker$worker_id:
#        container_name: lean_worker$worker_id
#        build:
#            context: .
#            dockerfile: lean_worker/Dockerfile
#        entrypoint: /lean_worker
#        environment:
#            - WORKER_ID=$worker_id
#            - SERVER_PORT=1234
#            - N_WORKERS=$number_of_lean_workers
#            - PREFETCH=30 # Potential optimization
#        networks:
#            - local_net
#        depends_on:
#            rabbitmq:
#                condition: service_healthy
#            sentiment_server:
#                condition: service_healthy
#"
#}

compose_joiner_rating() {
    local worker_id=$1
    local worker_count=$2
    echo "    joiner_rating$worker_id:
        container_name: joiner_rating$worker_id
        build:
            context: .
            dockerfile: joiners/joiner_ratings_worker/Dockerfile
        entrypoint: /joiners
        environment:
            - WORKER_ID=$worker_id
            - SERVER_PORT=1234
            - WORKER_COUNT=$worker_count
            - PREFETCH=1
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
            sentiment_server:
                condition: service_healthy
        volumes:
            - ${PWD}/joiner_ratings:/joiner_ratings
"
}

compose_joiner_credits() {
    local worker_id=$1
    local worker_count=$2
    echo "    joiner_credits$worker_id:
        container_name: joiner_credits$worker_id
        build:
            context: .
            dockerfile: joiners/joiner_credits_worker/Dockerfile
        entrypoint: /joiners
        environment:
            - WORKER_ID=$worker_id
            - SERVER_PORT=1234
            - WORKER_COUNT=$worker_count
            - PREFETCH=1
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
            sentiment_server:
                condition: service_healthy
        volumes:
            - ${PWD}/joiner_credits:/joiner_credits
"
}

compose_client() {
    local client_id=$1
    echo "    client$client_id:
        container_name: client$client_id
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
    compose_reduce $1 $2 reduce_top_5_by_budget map_reducer/main/reduce_top_5_by_budget map_reducer
}

compose_reduce_top_bottom_avg_ratings() {
    compose_reduce $1 $2 reduce_top_bottom_avg_rating map_reducer/main/reduce_top_bottom_avg_rating map_reducer
}

compose_reduce_by_country_sum_budgets() {
    compose_reduce $1 $2 reduce_by_country_sum_budget map_reducer/main/reduce_by_country_sum_budget map_reducer
}

compose_reduce_by_sentiment() {
    compose_reduce $1 $2 reduce_by_sentiment map_reducer/main/reduce_by_sentiment map_reducer
}

compose_reduce_by_actor() {
    compose_reduce $1 $2 reduce_by_actor map_reducer/main/reduce_by_actor map_reducer
}


compose_reduce() {
    local worker_id=$1
    local worker_count=$2
    local name=$3
    local dockerfile_path=$4
    local entrypoint=$5
    echo "    $name$worker_id:
        container_name: $name$worker_id
        build:
            context: .
            dockerfile: $dockerfile_path/Dockerfile
        entrypoint: /map_reducer
        environment:
            - WORKER_ID=$worker_id
            - WORKER_COUNT=$worker_count
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

compose_sentiment_server() {
    echo "    sentiment_server:
        container_name: sentiment_server
        build:
            context: .
            dockerfile: nlp/python_server/Dockerfile
        ports:
            - \"50051:50051\"
        networks:
            - local_net
        environment:
            - GRPC_PORT=50051
            - GRPC_WORKERS=30
        healthcheck:
            test: ncat -zv localhost 50051
            interval: 10s
            timeout: 10s
            retries: 10
"
}

compose_reduce_top_10_by_actor() {
    local worker_id=$1
    echo "    reduce_top_10_by_actor$worker_id:
        container_name: reduce_top_10_by_actor$worker_id
        build:
            context: .
            dockerfile: map_reducer/main/reduce_top_10_by_actor/Dockerfile
        entrypoint: /map_reducer
        environment:
            - WORKER_ID=$worker_id
            - WORKER_COUNT=$number_of_reduce_top_10_by_actor
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
"
}

compose_reduce_by_movieId() {
    local worker_id=$1
    echo "    reduce_by_movieid$worker_id:
        container_name: reduce_by_movieid$worker_id
        build:
            context: .
            dockerfile: map_reducer/main/reduce_by_movieId/Dockerfile
        entrypoint: /map_reducer
        environment:
            - WORKER_ID=$worker_id
            - WORKER_CONDI=1
            - WORKER_COUNT=$NUMBER_OF_REDUCE_BY_MOVIEID
            - PREFETCH=1
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
"
}

compose_monitor(){
    local worker_id=$1
    local number_of_monitors=$2
    local port=$3
    echo "    monitor$worker_id:
        container_name: monitor$worker_id
        build:
            context: .
            dockerfile: monitor/Dockerfile
        entrypoint: /monitor
        networks:
            - local_net
        environment:
            - PORT=$port
            - MONITOR_COUNT=$number_of_monitors
            - MONITOR_ID=$worker_id
        depends_on:
            rabbitmq:
                condition: service_healthy
            sentiment_server:
                condition: service_healthy
        volumes:
            - /var/run/docker.sock:/var/run/docker.sock
    "
}


NUMBER_OF_MONITORS=3
MONITOR_PORT_BASE=9000
monitor_addresses=""
for i in $(seq 1 $NUMBER_OF_MONITORS); do
    port=$((MONITOR_PORT_BASE + i))
    monitor_addresses+="monitor$i:$port"
    if [[ $i -lt $NUMBER_OF_MONITORS ]]; then
        monitor_addresses+=","
    fi
done

compose_header > $file_name
compose_rabbitmq >> $file_name
compose_sentiment_server >> $file_name
compose_coordinator >> $file_name

for i in $(seq 1 $number_of_workers); do
    compose_workers $i "$monitor_addresses" >> $file_name
done
for i in $(seq 1 $number_of_nlp_workers); do
    compose_nlp_workers $i >> $file_name
done
#for i in $(seq 1 $number_of_lean_workers); do
#   compose_lean_workers $i >> $file_name
#done
for i in $(seq 1 $number_of_joiners_credits); do
    compose_joiner_credits $i $number_of_joiners_credits >> $file_name
done
for i in $(seq 1 $number_of_joiners_ratings); do
    compose_joiner_rating $i >> $file_name
done
for i in $(seq 1 $number_of_reduce_by_country_sum_budgets); do
    compose_reduce_by_country_sum_budgets $i $number_of_reduce_by_country_sum_budgets >> $file_name
done
for i in $(seq 1 $number_of_reduce_top_5_by_budgets); do
    compose_reduce_top_5_by_budgets $i $number_of_reduce_top_5_by_budgets >> $file_name
done
for i in $(seq 1 $number_of_reduce_top_bottom_avg_ratings); do
    compose_reduce_top_bottom_avg_ratings $i $number_of_reduce_top_bottom_avg_ratings >> $file_name
done
for i in $(seq 1 $number_of_reduce_by_sentiment); do
    compose_reduce_by_sentiment $i $number_of_reduce_by_sentiment >> $file_name
done
for i in $(seq 1 $number_of_reduce_by_actor); do
    compose_reduce_by_actor $i $number_of_reduce_by_actor >> $file_name
done
for i in $(seq 1 $number_of_reduce_top_10_by_actor); do
    compose_reduce_top_10_by_actor $i $number_of_reduce_top_10_by_actor >> $file_name
done
# NUMBER_OF_REDUCE_BY_MOVIEID=10
# for i in $(seq 1 $NUMBER_OF_REDUCE_BY_MOVIEID); do
#     compose_reduce_by_movieId $i $NUMBER_OF_REDUCE_BY_MOVIEID >> $file_name
# done
for i in $(seq 1 $NUMBER_OF_MONITORS); do
    compose_monitor $i $NUMBER_OF_MONITORS $((MONITOR_PORT_BASE + i)) >> $file_name
done
for i in $(seq 1 $number_of_clients); do
    compose_client $i $number_of_clients >> $file_name
done
compose_endpoint >> $file_name
compose_network >> $file_name
