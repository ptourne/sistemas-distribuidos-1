module github.com/ptourne/sistemas-distribuidos-1/coordinator

go 1.24.1

require github.com/rabbitmq/amqp091-go v1.10.0 // indirect

replace github.com/ptourne/sistemas-distribuidos-1/common => ../common

replace github.com/ptourne/sistemas-distribuidos-1/middleware => ../middleware

require github.com/ptourne/sistemas-distribuidos-1/common v0.0.0-00010101000000-000000000000

require github.com/ptourne/sistemas-distribuidos-1/middleware v0.0.0-00010101000000-000000000000

require (
	github.com/fatih/color v1.18.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/sys v0.32.0 // indirect
)
