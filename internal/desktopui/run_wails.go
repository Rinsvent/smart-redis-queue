//go:build wails

package desktopui

import (
	"context"
	"embed"
	"fmt"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
)

//go:embed all:frontend/dist
var assets embed.FS

// Run запускает Wails desktop UI.
func Run() error {
	store, err := NewStore("")
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	app := NewApp(store)

	return wails.Run(&options.App{
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
	})
}
