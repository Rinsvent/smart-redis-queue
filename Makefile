.PHONY: build clean docker-up docker-down test test-short test-coverage example

# Библиотека: проверка сборки
build:
	go build ./...

clean:
	rm -rf bin/ coverage.out coverage.html

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

example:
	go run ./examples/basic
