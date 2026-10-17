//go:build wails && linux

package desktopui

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const linuxAppID = "smart-redis-queue"

// ensureLinuxDesktopEntry пишет иконку + .desktop под реальный WM_CLASS
// текущего процесса (basename бинаря). При `go run ./cmd/manager` это
// обычно "manager"; при `./bin/smart-redis-queue` — "smart-redis-queue".
// Без совпадения StartupWMClass↔WM_CLASS Ubuntu рисует шестерёнку.
func ensureLinuxDesktopEntry(iconPNG []byte) {
	if len(iconPNG) == 0 {
		return
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return
	}

	iconPath := filepath.Join(home, ".local/share/icons/hicolor/512x512/apps", linuxAppID+".png")
	for _, size := range []int{512, 256, 128, 64} {
		dir := filepath.Join(home, ".local/share/icons/hicolor", fmt.Sprintf("%dx%d", size, size), "apps")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			continue
		}
		_ = os.WriteFile(filepath.Join(dir, linuxAppID+".png"), iconPNG, 0o644)
	}

	exe, err := os.Executable()
	if err != nil || exe == "" {
		return
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	wmClass := filepath.Base(exe)
	// иконка также под именем WM_CLASS (на случай Icon=<wmClass>)
	for _, size := range []int{512, 256, 128, 64} {
		dir := filepath.Join(home, ".local/share/icons/hicolor", fmt.Sprintf("%dx%d", size, size), "apps")
		_ = os.WriteFile(filepath.Join(dir, wmClass+".png"), iconPNG, 0o644)
	}

	appsDir := filepath.Join(home, ".local/share/applications")
	if err := os.MkdirAll(appsDir, 0o755); err != nil {
		return
	}

	writeDesktop := func(filename, execPath, class string, noDisplay bool) {
		lines := []string{
			"[Desktop Entry]",
			"Type=Application",
			"Version=1.0",
			"Name=Smart Redis Queue",
			"Comment=Desktop manager for Smart Redis Queue",
			"Exec=" + quoteDesktopExec(execPath) + " desktop",
			"Icon=" + iconPath,
			"Terminal=false",
			"Categories=Development;Utility;",
			"StartupNotify=true",
			"StartupWMClass=" + class,
		}
		if noDisplay {
			lines = append(lines, "NoDisplay=true")
		}
		lines = append(lines, "")
		_ = os.WriteFile(filepath.Join(appsDir, filename), []byte(strings.Join(lines, "\n")), 0o644)
	}

	// Запись под текущий процесс — главное для иконки в доке.
	writeDesktop(linuxAppID+"-runtime.desktop", exe, wmClass, true)

	// Удобный ярлык в меню: предпочитаем стабильный бинарь рядом, иначе текущий exe.
	menuExec := exe
	if stable := filepath.Join(filepath.Dir(exe), "smart-redis-queue"); fileExists(stable) {
		menuExec = stable
		writeDesktop(linuxAppID+".desktop", menuExec, "smart-redis-queue", false)
	} else if wmClass == "smart-redis-queue" {
		writeDesktop(linuxAppID+".desktop", exe, "smart-redis-queue", false)
	} else {
		// go run / manager: ярлык в меню тоже на текущий exe, но видимый
		writeDesktop(linuxAppID+".desktop", exe, wmClass, false)
	}

	// Алиас manager-desktop, если лежит рядом
	if alias := filepath.Join(filepath.Dir(exe), "manager-desktop"); fileExists(alias) {
		writeDesktop(linuxAppID+"-manager-desktop.desktop", alias, "manager-desktop", true)
	}

	_ = exec.Command("update-desktop-database", appsDir).Run()
	_ = exec.Command("gtk-update-icon-cache", "-f", filepath.Join(home, ".local/share/icons/hicolor")).Run()
}

func fileExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}

func quoteDesktopExec(path string) string {
	if strings.ContainsAny(path, " \t\"'\\") {
		return `"` + strings.ReplaceAll(path, `"`, `\"`) + `"`
	}
	return path
}
