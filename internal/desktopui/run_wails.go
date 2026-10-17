//go:build wails

package desktopui

import (
	"context"
	"embed"
	"fmt"
	"os"
	"runtime"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
	"github.com/wailsapp/wails/v2/pkg/options/linux"
	"github.com/wailsapp/wails/v2/pkg/options/mac"
)

//go:embed all:frontend/dist
var assets embed.FS

//go:embed appicon.png
var appIcon []byte

// Run запускает Wails desktop UI.
func Run() error {
	prepareLinuxWebKitEnv()

	store, err := NewStore("")
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	app := NewApp(store)
	ensureLinuxDesktopEntry(appIcon)

	opts := &options.App{
		Title:  "Smart Redis Queue",
		Width:  1280,
		Height: 800,
		AssetServer: &assetserver.Options{
			Assets: assets,
		},
		BackgroundColour: &options.RGBA{R: 245, G: 247, B: 250, A: 255},
		OnStartup: func(ctx context.Context) {
			app.startup(ctx)
			_ = app.EnsureActive()
		},
		Bind: []interface{}{app},
		Mac: &mac.Options{
			About: &mac.AboutInfo{
				Title:   "Smart Redis Queue",
				Message: "Desktop manager",
				Icon:    appIcon,
			},
		},
	}
	if runtime.GOOS == "linux" {
		opts.Linux = &linux.Options{
			Icon: appIcon,
			// Не влияет на WM_CLASS (он = basename argv[0]), но полезно для группировки.
			ProgramName: "smart-redis-queue",
			// NVIDIA/GBM: без этого WebKit часто abort'ится (EGL_NOT_INITIALIZED).
			WebviewGpuPolicy: linux.WebviewGpuPolicyNever,
		}
	}

	return wails.Run(opts)
}

// prepareLinuxWebKitEnv — до инициализации GTK/WebKit.
// Раньше это делали вручную в shell; теперь бинарь сам переживает NVIDIA GBM.
func prepareLinuxWebKitEnv() {
	if runtime.GOOS != "linux" {
		return
	}
	if os.Getenv("WEBKIT_DISABLE_DMABUF_RENDERER") == "" {
		_ = os.Setenv("WEBKIT_DISABLE_DMABUF_RENDERER", "1")
	}
}
