.PHONY: build clean docker-up docker-down test test-short test-coverage example manager server desktop frontend desktop-darwin desktop-android desktop-ios

# Библиотека: проверка сборки
build:
	go build ./...

# CLI manager (без Wails/CGO; server/desktop — stubs)
manager:
	go build -ldflags "-s -w" -o bin/manager ./cmd/manager

# Web UI (тот же frontend). CGO не нужен. Слушайте 0.0.0.0 для коллег в LAN.
server: frontend
	go build -tags server -ldflags "-s -w" -o bin/manager-server ./cmd/manager

# Frontend для desktop/mobile/server UI (нужен Node.js 18+)
frontend:
	cd internal/desktopui/frontend && npm install && npm run build

# GUI: Wails v3, entrypoint ./cmd/desktop — запуск без подкоманд.
# Linux: legacy GTK3 (`gtk3`) пока нет gtk4/webkitgtk-6.0 в CI/дефолтном окружении.
desktop: frontend
	go build -tags wails,production,gtk3 -ldflags "-s -w" \
		-o bin/smart-redis-queue ./cmd/desktop
	cp -f bin/smart-redis-queue bin/manager-desktop

# Локальная сборка macOS desktop (запускать на Mac; CGO + Xcode CLT).
# GOARCH=arm64|amd64 make desktop-darwin
desktop-darwin: frontend
	CGO_ENABLED=1 go build -tags wails,production \
		-ldflags "-s -w" \
		-o bin/smart-redis-queue-desktop-darwin-$${GOARCH:-$$(go env GOARCH)} \
		./cmd/desktop

# Android APK (experimental Wails v3). Нужны SDK/NDK + `wails3` / Task.
desktop-android:
	wails3 task android:package:fat

# iOS Simulator .app (experimental; только macOS + Xcode).
desktop-ios:
	wails3 task ios:package IOS_PLATFORM=simulator

# Быстрый запуск UI
desktop-run: desktop
	./bin/smart-redis-queue

# go run UI
desktop-gorun: frontend
	go run -tags wails,production,gtk3 ./cmd/desktop

# go run web UI
server-gorun: frontend
	go run -tags server ./cmd/manager server --host 127.0.0.1 --port 8080

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
