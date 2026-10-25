package journal

import (
	"fmt"
	"strconv"
	"strings"
)

const sep = "\x1f"

// Cursor — позиция в журнале.
// Offset (+ Partition) — kafka/pg/mem; Key — opaque monotonic id (mongo ObjectID hex, 24 символа).
type Cursor struct {
	Offset    int64
	Partition int32
	Key       string
}

// Encode — монотонная строка для Redis.
// Offset: 20 десятичных цифр; Key: как есть (для ObjectID — 24 hex).
func (c Cursor) Encode() string {
	if c.Key != "" {
		return fmt.Sprintf("%010d%s%s", c.Partition, sep, c.Key)
	}
	return fmt.Sprintf("%010d%s%020d", c.Partition, sep, c.Offset)
}

// After — строго больше other (лексикографически по Encode).
func (c Cursor) After(other Cursor) bool {
	return c.Encode() > other.Encode()
}

// DecodeCursor разбирает Encode().
func DecodeCursor(s string) (Cursor, error) {
	if s == "" {
		return Cursor{}, nil
	}
	parts := strings.Split(s, sep)
	if len(parts) != 2 {
		return Cursor{}, fmt.Errorf("journal: bad cursor %q", s)
	}
	p, err := strconv.ParseInt(parts[0], 10, 32)
	if err != nil {
		return Cursor{}, err
	}
	tail := parts[1]
	// %020d → ровно 20 цифр; ObjectID hex → 24 символа
	if len(tail) == 20 {
		o, err := strconv.ParseInt(tail, 10, 64)
		if err != nil {
			return Cursor{}, err
		}
		return Cursor{Partition: int32(p), Offset: o}, nil
	}
	return Cursor{Partition: int32(p), Key: tail}, nil
}

// OffsetCursor — pg/mem (partition=0).
func OffsetCursor(offset int64) Cursor {
	return Cursor{Offset: offset}
}

// KeyCursor — mongo ObjectID hex и др. opaque id.
func KeyCursor(key string) Cursor {
	return Cursor{Key: key}
}

// KafkaCursor — kafka partition+offset.
func KafkaCursor(partition int32, offset int64) Cursor {
	return Cursor{Partition: partition, Offset: offset}
}
