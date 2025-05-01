module github.com/ptourne/sistemas-distribuidos-1/middleware

go 1.24.1

require github.com/rabbitmq/amqp091-go v1.10.0

replace github.com/ptourne/sistemas-distribuidos-1/common => ../common

require github.com/ptourne/sistemas-distribuidos-1/common v0.0.0-00010101000000-000000000000

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	golang.org/x/sys v0.25.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
