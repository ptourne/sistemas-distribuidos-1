#!/bin/bash

if [ "$1" == "" ]; then
    docker exec -it rabbitmq rabbitmqctl help
else
    docker exec -it rabbitmq rabbitmqctl $1
fi
