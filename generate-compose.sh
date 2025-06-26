#!/bin/bash

if [ "$#" -eq 13 ]; then
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
    number_of_sentiment_servers=${12}
    number_of_monitors=${13}

elif [ "$#" -eq 14 ]; then
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
    number_of_sentiment_servers=${13}
    number_of_monitors=${14}
else
    echo "Error: Incorrect number of arguments"
    echo "Use: ./generar-compose.sh [file_name] <number_of_workers>,<number_of_joiners_credits>,<number_of_joiners_ratings>,
    <number_of_reduce_by_country_sum_budgets>, <number_of_reduce_top_5_by_budgets>,
    <number_of_reduce_by_sentiment>, <number_of_reduce_by_actor>, <number_of_reduce_top_10_by_actor>,
    <number_of_reduce_top_bottom_avg_ratings>, <number_of_clients>, <number_of_nlp_workers>,
    <number_of_sentiment_servers>, <number_of_monitors>"
    exit 1
fi

max_log_size=1000

# Verify number_of_workers is a positive integer
if ! [[ "$number_of_workers" =~ ^[0-9]+$ ]] || [ "$number_of_workers" -le -1 ]; then
    echo "Error: Number of workers must be a positive integer"
    echo $number_of_workers
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

# Verify number_of_sentiment_servers is a positive integer
if ! [[ "$number_of_sentiment_servers" =~ ^[0-9]+$ ]] || [ "$number_of_sentiment_servers" -lt 1 ]; then
    echo "Error: Number of sentiment servers must be an integer >= 1"
    exit 1
fi

# Verify number_of_monitors is a positive integer
if ! [[ "$number_of_monitors" =~ ^[0-9]+$ ]] || [ "$number_of_monitors" -lt 1 ]; then
    echo "Error: Number of monitors must be a an integer >= 1"
    exit 1
fi

MONITOR_PORT_BASE=9000
SENTIMENT_SERVER_PORT_BASE=50050
monitor_addresses=""
sentimen_server_addresses=""
monitor_peers=""

for i in $(seq 0 $((number_of_monitors-1))); do
    port=$((MONITOR_PORT_BASE + i))
    monitor_addresses+="monitor$i:$port"
    monitor_peers+="$i:monitor$i:$port"
    if [[ $i -lt $((number_of_monitors-1)) ]]; then
        monitor_addresses+=","
        monitor_peers+=","
    fi
done

for i in $(seq 0 $((number_of_sentiment_servers-1))); do
    port=$((SENTIMENT_SERVER_PORT_BASE + i))
    sentimen_server_addresses+="sentiment_server$i:$port"
    if [[ $i -lt $number_of_sentiment_servers ]]; then
        sentimen_server_addresses+=","
    fi
done

generate_services_list() {
    local services=""

    for i in $(seq 0 $((number_of_workers-1))); do
        services+="worker$i,"
    done

    for i in $(seq 0 $((number_of_joiners_credits-1))); do
        services+="joiner_credits$i,"
    done

    for i in $(seq 0 $((number_of_joiners_ratings-1))); do
        services+="joiner_rating$i,"
    done

    for i in $(seq 0 $((number_of_reduce_by_country_sum_budgets-1))); do
        services+="reduce_by_country_sum_budget$i,"
    done

    for i in $(seq 0 $((number_of_reduce_top_5_by_budgets-1))); do
        services+="reduce_top_5_by_budget$i,"
    done

    for i in $(seq 0 $((number_of_reduce_by_actor-1))); do
        services+="reduce_by_actor$i,"
    done

    for i in $(seq 0 $((number_of_reduce_top_10_by_actor-1))); do
        services+="reduce_top_10_by_actor$i,"
    done

    for i in $(seq 0 $((number_of_reduce_top_bottom_avg_ratings-1))); do
        services+="reduce_top_bottom_avg_rating$i,"
    done

    for i in $(seq 0 $((number_of_reduce_by_sentiment-1))); do
        services+="reduce_by_sentiment$i,"
    done

    for i in $(seq 0 $((number_of_nlp_workers-1))); do
        services+="nlp_worker$i,"
    done

    for i in $(seq 0 $((number_of_sentiment_servers-1))); do
        services+="sentiment_server$i,"
    done

    for i in $(seq 0 $((number_of_monitors-1))); do
        services+="monitor$i,"
    done

    services+="coordinator,endpoint"

    echo "$services"
}

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

NUMBER_OF_REDUCE_BY_MOVIEID=10
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
            - N_WORKERS=$number_of_workers
            - N_RATINGS_CONSUMERS=$NUMBER_OF_REDUCE_BY_MOVIEID
            - PREFETCH=1 # Potential optimization
            - NAME=coordinator
            - MONITOR_ADDRESSES=$monitor_addresses
        volumes:
            - ${PWD}/coordinator:/coordinator-dir
        depends_on:
            rabbitmq:
                condition: service_healthy
"
}

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
            - N_WORKERS=$number_of_workers
            - N_REDUCERS_BY_COUNTRY_SUM_BUDGETS=$number_of_reduce_by_country_sum_budgets
            - SERVER_PORT=1234
            - PREFETCH=1 # Potential optimization
            - NAME=worker$worker_id
            - MONITOR_ADDRESSES=$monitor_addresses
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
"
}

generate_sentiment_dependencies() {
    for i in $(seq 0 $((number_of_sentiment_servers-1))); do
        echo "            sentiment_server$i:"
        echo "                condition: service_healthy"
    done
}

compose_nlp_workers() {
    local worker_id=$1
    local consumer_count=$2
    echo "    nlp_worker$worker_id:
        container_name: nlp_worker$worker_id
        build:
            context: .
            dockerfile: nlp/Dockerfile
        entrypoint: /nlp
        environment: 
            - WORKER_ID=$worker_id
            - CONSUMER_COUNT=$consumer_count
            - N_WORKERS=$number_of_nlp_workers
            - NLP_GRPC_ADDRS=$sentimen_server_addresses
            - SERVER_PORT=1234
            - PREFETCH=1 # Potential optimization
            - NAME=nlp_worker$worker_id
            - MONITOR_ADDRESSES=$monitor_addresses
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy"
    generate_sentiment_dependencies
    echo ""
}

compose_joiner_rating() {
    local worker_id=$1
    local worker_count=$2
    local consumer_count=$3
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
            - CONSUMER_COUNT=$consumer_count
            - PREFETCH=1
            - NAME=joiner_rating$worker_id
            - MONITOR_ADDRESSES=$monitor_addresses
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
        volumes:
            - ${PWD}/joiner_ratings:/joiner_ratings
"
}

compose_joiner_credits() {
    local worker_id=$1
    local worker_count=$2
    local consumer_count=$3
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
            - CONSUMER_COUNT=$consumer_count
            - PREFETCH=1
            - NAME=joiner_credits$worker_id
            - MONITOR_ADDRESSES=$monitor_addresses
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
        volumes:
            - ${PWD}/joiner_credits:/joiner_credits
"
}

compose_client() {
    local client_id=$1
    local client_count=$2
    echo "    client$client_id:
        container_name: client$client_id
        build:
            context: .
            dockerfile: client/Dockerfile
        entrypoint: /client
        environment:
            - SERVER_PORT=endpoint:9876
            - NAME=client$client_id
            - MONITOR_ADDRESSES=$monitor_addresses
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
            - NAME=endpoint
            - MONITOR_ADDRESSES=$monitor_addresses
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
        volumes:
            - ${PWD}/endpoint:/endpoint-dir-ids
"
}

compose_reduce_top_5_by_budgets() {
    compose_reduce $1 $2 $3 reduce_top_5_by_budget map_reducer/main/reduce_top_5_by_budget map_reducer
}

compose_reduce_top_bottom_avg_ratings() {
    compose_reduce $1 $2 $3 reduce_top_bottom_avg_rating map_reducer/main/reduce_top_bottom_avg_rating map_reducer
}

compose_reduce_by_country_sum_budgets() {
    compose_reduce $1 $2 $3 reduce_by_country_sum_budget map_reducer/main/reduce_by_country_sum_budget map_reducer
}

compose_reduce_by_sentiment() {
    compose_reduce $1 $2 $3 reduce_by_sentiment map_reducer/main/reduce_by_sentiment map_reducer
}

compose_reduce_by_actor() {
    compose_reduce $1 $2 $3 reduce_by_actor map_reducer/main/reduce_by_actor map_reducer
}


compose_reduce() {
    local worker_id=$1
    local worker_count=$2
    local worker_output_count=$3
    local name=$4
    local dockerfile_path=$5
    local entrypoint=$6
    local container_name="${name}${worker_id}"
    echo "    $container_name:
        container_name: $container_name
        build:
            context: .
            dockerfile: $dockerfile_path/Dockerfile
        entrypoint: /map_reducer
        environment:
            - WORKER_ID=$worker_id
            - WORKER_COUNT=$worker_count
            - WORKER_OUTPUT_COUNT=$worker_output_count
            - NAME=$container_name
            - MONITOR_ADDRESSES=$monitor_addresses
            - MAX_LOG_SIZE=$max_log_size
        networks:
            - local_net
        volumes:
            - ${PWD}/reducer_volumes/$container_name:/transaction_log
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
    local id=$1
    local port=$3
    echo "    sentiment_server$id:
        container_name: sentiment_server$id
        build:
            context: .
            dockerfile: nlp/python_server/Dockerfile
        networks:
            - local_net
        environment:
            - GRPC_PORT=$port
            - GRPC_WORKERS=30
            - NAME=sentiment_server$id
            - MONITOR_ADDRESSES=$monitor_addresses
        healthcheck:
            test: ['CMD', 'sh', '-c', 'nc -z localhost $port']
            interval: 10s
            timeout: 10s
            retries: 10
"
}

compose_reduce_top_10_by_actor() {
    local worker_id=$1
    local worker_count=$2
    local worker_output_count=$3
    echo "    reduce_top_10_by_actor$worker_id:
        container_name: reduce_top_10_by_actor$worker_id
        build:
            context: .
            dockerfile: map_reducer/main/reduce_top_10_by_actor/Dockerfile
        entrypoint: /map_reducer
        environment:
            - WORKER_ID=$worker_id
            - WORKER_COUNT=$number_of_reduce_top_10_by_actor
            - WORKER_OUTPUT_COUNT=$worker_output_count
            - NAME=reduce_top_10_by_actor$worker_id
            - MONITOR_ADDRESSES=$monitor_addresses
            - MAX_LOG_SIZE=$max_log_size
        networks:
            - local_net
        depends_on:
            rabbitmq:
                condition: service_healthy
"
}

compose_reduce_by_movieId() {
    local worker_id=$1
    local worker_count=$2
    local consumer_count=$3
    echo "    reduce_by_movieid$worker_id:
        container_name: reduce_by_movieid$worker_id
        build:
            context: .
            dockerfile: map_reducer/main/reduce_by_movieId/Dockerfile
        entrypoint: /map_reducer
        environment:
            - WORKER_ID=$worker_id
            - WORKER_COUNT=$worker_count
            - WORKER_OUTPUT_COUNT=$consumer_count
            - PREFETCH=1
            - NAME=reduce_by_movieid$worker_id
            - MONITOR_ADDRESSES=$monitor_addresses
            - MAX_LOG_SIZE=$max_log_size
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
    local monitor_peers=$4
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
            - PEERS=$monitor_peers
            - SERVICES=$(generate_services_list)
        depends_on:
            rabbitmq:
                condition: service_healthy
        volumes:
            - /var/run/docker.sock:/var/run/docker.sock
    "
}

compose_header > $file_name
compose_rabbitmq >> $file_name
for i in $(seq 0 $((number_of_sentiment_servers-1))); do
    compose_sentiment_server $i $number_of_sentiment_servers $((SENTIMENT_SERVER_PORT_BASE + i)) >> $file_name
done
compose_coordinator >> $file_name
for i in $(seq 0 $((number_of_workers-1))); do
    compose_workers $i >> $file_name
done
for i in $(seq 0 $((number_of_nlp_workers-1))); do
    compose_nlp_workers $i $number_of_reduce_by_sentiment >> $file_name
done
for i in $(seq 0 $((number_of_joiners_credits-1))); do
    compose_joiner_credits $i $number_of_joiners_credits $number_of_reduce_by_actor >> $file_name
done
for i in $(seq 0 $((number_of_joiners_ratings-1))); do
    compose_joiner_rating $i $number_of_joiners_ratings $number_of_reduce_top_bottom_avg_ratings >> $file_name
done
for i in $(seq 0 $((number_of_reduce_by_country_sum_budgets-1))); do
    compose_reduce_by_country_sum_budgets $i $number_of_reduce_by_country_sum_budgets $number_of_reduce_top_5_by_budgets >> $file_name
done
for i in $(seq 0 $((number_of_reduce_top_5_by_budgets-1))); do
    compose_reduce_top_5_by_budgets $i $number_of_reduce_top_5_by_budgets 1 >> $file_name
done
for i in $(seq 0 $((number_of_reduce_top_bottom_avg_ratings-1))); do
    compose_reduce_top_bottom_avg_ratings $i $number_of_reduce_top_bottom_avg_ratings 1 >> $file_name
done
for i in $(seq 0 $((number_of_reduce_by_sentiment-1))); do
    compose_reduce_by_sentiment $i $number_of_reduce_by_sentiment $number_of_workers >> $file_name
done
for i in $(seq 0 $((number_of_reduce_by_actor-1))); do
    compose_reduce_by_actor $i $number_of_reduce_by_actor $number_of_reduce_top_10_by_actor >> $file_name
done
for i in $(seq 0 $((number_of_reduce_top_10_by_actor-1))); do
    compose_reduce_top_10_by_actor $i $number_of_reduce_top_10_by_actor 1 >> $file_name
done
for i in $(seq 0 $((NUMBER_OF_REDUCE_BY_MOVIEID-1))); do
    compose_reduce_by_movieId $i $NUMBER_OF_REDUCE_BY_MOVIEID $number_of_workers>> $file_name
done
for i in $(seq 0 $((number_of_clients-1))); do
    compose_client $i $number_of_clients >> $file_name
done
for i in $(seq 0 $((number_of_monitors-1))); do
    compose_monitor $i $number_of_monitors $((MONITOR_PORT_BASE + i)) $monitor_peers >> $file_name
done
compose_endpoint >> $file_name
compose_network >> $file_name
