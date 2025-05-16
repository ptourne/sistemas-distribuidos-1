module github.com/ptourne/sistemas-distribuidos-1/common

go 1.24.1

require (
	github.com/fatih/color v1.18.0
	github.com/ptourne/sistemas-distribuidos-1/middleware v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.10.0
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/ptourne/sistemas-distribuidos-1/middleware => ../middleware

require (
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/sys v0.32.0 // indirect
)
