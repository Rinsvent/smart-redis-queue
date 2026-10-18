//go:build wails || android || ios || server

package desktopui

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/wailsapp/wails/v3/pkg/application"
)

//go:embed all:frontend/dist
var assets embed.FS

//go:embed appicon.png
var appIcon []byte

// RunConfig — параметры запуска UI (desktop / mobile / server).
type RunConfig struct {
	// Host/Port используются только в server-сборке (-tags server).
	Host string
	Port int
	// WebSocketAllowAllOrigins — ослабить origin-check (LAN / reverse-proxy).
	// По умолчанию false: same-origin.
	WebSocketAllowAllOrigins bool
}

// Run запускает UI с дефолтным конфигом.
func Run() error {
	return RunWithConfig(RunConfig{})
}

// RunWithConfig запускает Wails v3 UI (desktop / mobile / HTTP server).
// Тот же frontend/dist и тот же App-сервис.
func RunWithConfig(cfg RunConfig) error {
	if !application.System.IsServer() {
		prepareLinuxWebKitEnv()
	}

	store, err := NewStore(resolveStorePath())
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	svc := NewApp(store)
	if !application.System.IsServer() {
		ensureLinuxDesktopEntry(appIcon)
	}

	host := cfg.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := cfg.Port
	if port == 0 {
		port = 8080
	}

	opts := application.Options{
		Name:        "Smart Redis Queue",
		Description: "Desktop manager for Smart Redis Queue",
		Icon:        appIcon,
		Services: []application.Service{
			application.NewService(svc),
		},
		Assets: application.AssetOptions{
			Handler: application.AssetFileServerFS(assets),
		},
		Mac: application.MacOptions{
			ApplicationShouldTerminateAfterLastWindowClosed: true,
		},
		Linux: application.LinuxOptions{
			ProgramName:   "smart-redis-queue",
			ApplicationID: "com.rinsvent.smartredisqueue",
		},
		IOS:     application.IOSOptions{},
		Android: application.AndroidOptions{},
		Server: application.ServerOptions{
			Host:                     host,
			Port:                     port,
			WebSocketAllowAllOrigins: cfg.WebSocketAllowAllOrigins,
		},
	}
	if application.System.IsPlatform(application.PlatformIOS) {
		opts.DisableDefaultSignalHandler = true
	}
	app := application.New(opts)

	winOpts := application.WebviewWindowOptions{
		Title:            "Smart Redis Queue",
		Width:            1280,
		Height:           800,
		BackgroundColour: application.NewRGB(245, 247, 250),
		URL:              "/",
	}
	if runtime.GOOS == "linux" && !application.System.IsServer() {
		// NVIDIA/GBM: без Never WebKit часто abort'ится (EGL_NOT_INITIALIZED).
		winOpts.Linux = application.LinuxWindow{
			WebviewGpuPolicy: application.WebviewGpuPolicyNever,
		}
	}
	app.Window.NewWithOptions(winOpts)

	if application.System.IsServer() {
		fmt.Fprintf(os.Stderr, "Smart Redis Queue web UI: http://%s:%d/\n", host, port)
		fmt.Fprintf(os.Stderr, "Health: http://%s:%d/health\n", host, port)
	}

	return app.Run()
}

// prepareLinuxWebKitEnv — до инициализации GTK/WebKit.
// Wails v3 сам ставит DMABUF=1 на NVIDIA; оставляем явный fallback для всех Linux desktop.
func prepareLinuxWebKitEnv() {
	if runtime.GOOS != "linux" {
		return
	}
	if os.Getenv("WEBKIT_DISABLE_DMABUF_RENDERER") == "" {
		_ = os.Setenv("WEBKIT_DISABLE_DMABUF_RENDERER", "1")
	}
}

// resolveStorePath — на mobile нет $HOME/UserConfigDir; пишем в app sandbox.
// Пустая строка → NewStore использует defaultStorePath (desktop/server).
func resolveStorePath() string {
	if application.System.IsMobile() {
		if p := application.Mobile.StoragePath(); p != "" {
			return filepath.Join(p, "connections.json")
		}
	}
	return ""
}
