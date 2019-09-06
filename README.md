## Setup

- Install Go 1.11 or higher https://golang.org/doc/install
- Run `make localstack` to set up localstack using docker-compose

### Tests

Run `go test` to run the main test suite. Test suite (mostly) handles resource set up and clean up.

### Running manually

Run `go build .` and then `./localstack_test` to start the application. If you do so you'll need to manually manage the Kinesis resources.

```
aws kinesis create-stream --stream-name localstack_stream --shard-count 1 --endpoint http://localhost:4568
aws kinesis delete-stream --stream-name localstack_stream --endpoint http://localhost:4568
aws kinesis describe-stream --stream-name localstack_stream --endpoint http://localhost:4568
```
