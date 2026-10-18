//go:build wails || android || ios

package desktopui

import (
	"context"

	"github.com/wailsapp/wails/v3/pkg/application"
)

// ServiceStartup — Wails v3 lifecycle (вместо v2 OnStartup / startup).
func (a *App) ServiceStartup(ctx context.Context, _ application.ServiceOptions) error {
	a.ctx = ctx
	_ = a.EnsureActive()
	return nil
}
