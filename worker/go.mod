module github.com/ptourne/sistemas-distribuidos-1/worker

go 1.24.1

replace github.com/ptourne/sistemas-distribuidos-1 => ../

replace github.com/ptourne/sistemas-distribuidos-1/common => ../common

replace github.com/ptourne/sistemas-distribuidos-1/middleware => ../middleware

require github.com/ptourne/sistemas-distribuidos-1/common v0.0.0-00010101000000-000000000000

require (
	github.com/ptourne/sistemas-distribuidos-1/middleware v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.72.0
	google.golang.org/protobuf v1.36.6
)

require (
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250218202821-56aae31c358a // indirect
)

require (
	github.com/fatih/color v1.18.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/rabbitmq/amqp091-go v1.10.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
)
