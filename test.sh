NUMBER_OF_THREADS=20

echo "Running tests"

echo "Commom package tests"
pushd common/
GOMAXPROCS=$NUMBER_OF_THREADS go test ./...
res_common=$?
popd

echo "Codec package tests"
pushd middleware/codec/
GOMAXPROCS=$NUMBER_OF_THREADS go test
res_codec=$?
popd

echo "RabbitMQ tests"
pushd middleware/middleware/rabbitmq/
GOMAXPROCS=$NUMBER_OF_THREADS go test
res_rabbitmq=$?
popd

echo "Map-Reducer tests"
pushd map_reducer/
GOMAXPROCS=$NUMBER_OF_THREADS go test
res_map_reducer=$?
popd

echo "All tests completed"
echo "Common package tests: $res_common"
echo "Codec package tests: $res_codec"
echo "RabbitMQ tests: $res_rabbitmq"
echo "Map-Reducer tests: $res_map_reducer"
