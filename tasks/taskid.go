package tasks

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// BuildTaskID — ID для Publish.
//
//	name              — обычный лок по имени
//	name~o{hash8}     — лок по набору args (ByOptions)
//	name~r{rand8}     — полный bypass (BypassLock); ByOptions игнорируется
func BuildTaskID(name string, args map[string]string, bypassLock, byOptions bool) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", fmt.Errorf("tasks: empty name")
	}
	if bypassLock {
		suf, err := randomSuffix(4)
		if err != nil {
			return "", err
		}
		return name + "~r" + suf, nil
	}
	if byOptions {
		return name + "~o" + OptionsHash(args), nil
	}
	return name, nil
}

// OptionsHash — стабильный короткий хэш args (sorted keys).
func OptionsHash(args map[string]string) string {
	if len(args) == 0 {
		sum := sha256.Sum256(nil)
		return hex.EncodeToString(sum[:4])
	}
	keys := make([]string, 0, len(args))
	for k := range args {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(args[k])
		b.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:4])
}

func randomSuffix(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
