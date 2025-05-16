pushd client
go mod tidy
go get
go mod download
popd
pushd common
go mod tidy
go get
go mod download
popd
pushd coordinator
go mod tidy
go get
go mod download
popd
pushd endpoint
go mod tidy
go get
go mod download
popd
pushd joiner_credits
go mod tidy
go get
go mod download
popd
pushd joiner_ratings
go mod tidy
go get
go mod download
popd
pushd joiners
go mod tidy
go get
go mod download
popd
pushd lean_worker
go mod tidy
go get
go mod download
popd
pushd logger_test
go mod tidy
go get
go mod download
popd
pushd map_reducer
go mod tidy
go get
go mod download
popd
pushd middleware
go mod tidy
go get
go mod download
popd
pushd nlp
go mod tidy
go get
go mod download
popd
pushd rabbitmq
go mod tidy
go get
go mod download
popd
pushd worker
go mod tidy
go get
go mod download
popd
