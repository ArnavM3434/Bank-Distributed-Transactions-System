.PHONY: all clean

all: server client

server: server.go
	go build -o server server.go

client: client.go
	go build -o client client.go

clean:
	rm -f server client
