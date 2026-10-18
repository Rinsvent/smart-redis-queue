.PHONY: build clean docker-up docker-down test test-short test-coverage example manager desktop frontend desktop-darwin

# Библиотека: проверка сборки
build:
	go build ./...

# Собрать CLI manager
manager:
	go build -o bin/manager ./cmd/manager

# Frontend для desktop UI (нужен Node.js 18+)
frontend:
	cd internal/desktopui/frontend && npm install && npm run build

# GUI manager (CGO + Wails; webkit2_41 для Ubuntu 24.04+)
# Бинарь smart-redis-queue — WM_CLASS совпадает с .desktop / иконкой в доке.
# WEBKIT_DISABLE_DMABUF_RENDERER выставляется в коде (NVIDIA GBM).
desktop: frontend
	go build -tags wails,production,webkit2_41 -o bin/smart-redis-queue ./cmd/manager
	cp -f bin/smart-redis-queue bin/manager-desktop

# Локальная сборка macOS (запускать на Mac; CGO + Xcode CLT).
# GOARCH=arm64|amd64 make desktop-darwin
desktop-darwin: frontend
	CGO_ENABLED=1 go build -tags wails,production \
		-ldflags "-s -w" \
		-o bin/smart-redis-queue-desktop-darwin-$${GOARCH:-$$(go env GOARCH)} \
		./cmd/manager

# Быстрый запуск без ручного env (frontend должен быть собран: make frontend)
desktop-run: desktop
	./bin/smart-redis-queue desktop

# go run (иконка: WM_CLASS=manager → пишется runtime .desktop)
desktop-gorun: frontend
	go run -tags wails,production,webkit2_41 ./cmd/manager desktop

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
