package journal

import (
	"fmt"
	"strconv"
	"strings"
)

const sep = "\x1f"

// Cursor — позиция в журнале по offset (единый подход для kafka/pg/mongo).
type Cursor struct {
	Offset    int64
	Partition int32 // kafka; 0 для pg/mongo
}

// Encode — монотонная строка для Redis.
func (c Cursor) Encode() string {
	return fmt.Sprintf("%010d%s%020d", c.Partition, sep, c.Offset)
}

// After — строго больше other.
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
	o, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return Cursor{}, err
	}
	return Cursor{Partition: int32(p), Offset: o}, nil
}

// OffsetCursor — pg/mongo (partition=0).
func OffsetCursor(offset int64) Cursor {
	return Cursor{Offset: offset}
}

// KafkaCursor — kafka partition+offset.
func KafkaCursor(partition int32, offset int64) Cursor {
	return Cursor{Partition: partition, Offset: offset}
}
