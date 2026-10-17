package redisqueue

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

// Маркер сжатого payload в Redis (обратная совместимость: без маркера = raw).
// Формат: magic(3) + algo(1) + compressed body.
var payloadCodecMagic = []byte{'R', 'Q', 'C'}

const (
	payloadAlgoRaw  byte = 0
	payloadAlgoZstd byte = 1
)

// Compression — алгоритм сжатия payload на Publish (opt-in).
type Compression int

const (
	// CompressionNone — без сжатия (дефолт).
	CompressionNone Compression = iota
	// CompressionZstd — zstd; в Redis пишется с маркером RQC\x01.
	CompressionZstd
)

const defaultCompressMinBytes = 1024

var (
	zstdEncoder *zstd.Encoder
	zstdDecoder *zstd.Decoder
)

func init() {
	var err error
	zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic("redisqueue: zstd encoder: " + err.Error())
	}
	zstdDecoder, err = zstd.NewReader(nil)
	if err != nil {
		panic("redisqueue: zstd decoder: " + err.Error())
	}
}

// encodePayloadForRedis — сжатие перед Publish. minBytes: сжимать только если len >= min (0 → default 1KiB).
// Если сжатый размер не меньше исходного — оставляем raw (без маркера).
func encodePayloadForRedis(raw []byte, c Compression, minBytes int) ([]byte, error) {
	if c == CompressionNone || len(raw) == 0 {
		return raw, nil
	}
	if minBytes <= 0 {
		minBytes = defaultCompressMinBytes
	}
	if len(raw) < minBytes {
		return raw, nil
	}
	switch c {
	case CompressionZstd:
		comp := zstdEncoder.EncodeAll(raw, make([]byte, 0, len(raw)/2))
		if len(comp)+len(payloadCodecMagic)+1 >= len(raw) {
			return raw, nil
		}
		out := make([]byte, 0, len(payloadCodecMagic)+1+len(comp))
		out = append(out, payloadCodecMagic...)
		out = append(out, payloadAlgoZstd)
		out = append(out, comp...)
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported compression %d", c)
	}
}

// decodePayloadFromRedis — автодетект маркера; неизвестный/битый → ошибка только если magic совпал.
func decodePayloadFromRedis(stored []byte) ([]byte, error) {
	if len(stored) < len(payloadCodecMagic)+1 {
		return stored, nil
	}
	if !bytes.Equal(stored[:len(payloadCodecMagic)], payloadCodecMagic) {
		return stored, nil
	}
	algo := stored[len(payloadCodecMagic)]
	body := stored[len(payloadCodecMagic)+1:]
	switch algo {
	case payloadAlgoRaw:
		return body, nil
	case payloadAlgoZstd:
		return zstdDecoder.DecodeAll(body, nil)
	default:
		return nil, fmt.Errorf("unknown payload compression algo %d", algo)
	}
}
