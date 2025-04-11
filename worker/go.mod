module github.com/ptourne/sistemas-distribuidos-1/worker

go 1.24.1

replace github.com/ptourne/sistemas-distribuidos-1/common => ../common

require github.com/ptourne/sistemas-distribuidos-1/common v0.0.0-00010101000000-000000000000

require github.com/rabbitmq/amqp091-go v1.10.0 // indirect
