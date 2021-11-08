all: bin

bin: bin/mcfly

bin/mcfly: cmd/mcfly/main.go pkg/rewind/*.go pkg/store/*.go
	go build -ldflags "-w -s" -o bin/mcfly cmd/mcfly/main.go

test:
	go test -v -covermode=atomic ./pkg/...

clean:
	rm -rf bin rewind.json
	docker-compose down
	rm -rf mysqldata
