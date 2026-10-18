//go:build android

package main

import "github.com/wailsapp/wails/v3/pkg/application"

func init() {
	// c-shared: main() не вызывается автоматически — регистрируем для Android host.
	application.RegisterAndroidMain(main)
}
