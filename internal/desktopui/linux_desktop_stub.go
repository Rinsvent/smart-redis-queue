//go:build !((wails || android || ios) && linux && !android)

package desktopui

func ensureLinuxDesktopEntry(_ []byte) {}
