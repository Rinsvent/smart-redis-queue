.PHONY: build clean docker-up docker-down test test-short test-coverage

build:
	go build -o bin/smart-redis-queue .

clean:
	rm -rf bin/

docker-up:
	docker compose up -d

docker-down:
	docker compose down

test:
	go test ./... -v

test-short:
	go test ./... -short -v

test-coverage:
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html
