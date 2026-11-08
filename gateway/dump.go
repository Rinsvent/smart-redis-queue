package gateway

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Dump — graceful snapshot op-log + loss-window.
type Dump struct {
	Version      int         `json:"version"`
	SavedAt      time.Time   `json:"saved_at"`
	NextOffset   uint64      `json:"next_offset"`
	Ops          []Op        `json:"ops"`
	Loss         []lossEntry `json:"loss_window"`
	LossWindowNS int64       `json:"loss_window_ns"`
}

const dumpVersion = 1

func writeDump(path string, d Dump) error {
	d.Version = dumpVersion
	if d.SavedAt.IsZero() {
		d.SavedAt = time.Now().UTC()
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	raw, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readDump(path string) (Dump, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Dump{}, err
	}
	var d Dump
	if err := json.Unmarshal(raw, &d); err != nil {
		return Dump{}, err
	}
	if d.Version != 0 && d.Version != dumpVersion {
		return Dump{}, fmt.Errorf("gateway: unsupported dump version %d", d.Version)
	}
	return d, nil
}
