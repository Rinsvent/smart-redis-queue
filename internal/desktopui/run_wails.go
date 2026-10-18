//go:build wails || android || ios

package desktopui

import (
	"embed"
	"fmt"
	"os"
	"runtime"

	"github.com/wailsapp/wails/v3/pkg/application"
)

//go:embed all:frontend/dist
var assets embed.FS

//go:embed appicon.png
var appIcon []byte

// Run запускает Wails v3 desktop / mobile UI.
func Run() error {
	prepareLinuxWebKitEnv()

	store, err := NewStore("")
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	svc := NewApp(store)
	ensureLinuxDesktopEntry(appIcon)

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
	if runtime.GOOS == "linux" {
		// NVIDIA/GBM: без Never WebKit часто abort'ится (EGL_NOT_INITIALIZED).
		winOpts.Linux = application.LinuxWindow{
			WebviewGpuPolicy: application.WebviewGpuPolicyNever,
		}
	}
	app.Window.NewWithOptions(winOpts)

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
