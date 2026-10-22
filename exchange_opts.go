package redisqueue

// ExchangePublishOption — опции Exchange.PublishWith.
type ExchangePublishOption func(*exchangePublishConfig)

type exchangePublishConfig struct {
	atomic       bool // default true
	checkVersion bool
	publishConfig
}

func (c *exchangePublishConfig) apply(opts []ExchangePublishOption) {
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
}

// WithDeliveryAtomic — один EVAL на все dest (default).
func WithDeliveryAtomic() ExchangePublishOption {
	return func(c *exchangePublishConfig) { c.atomic = true }
}

// WithDeliveryBestEffort — N× Publish в каждую очередь (частичные копии возможны).
func WithDeliveryBestEffort() ExchangePublishOption {
	return func(c *exchangePublishConfig) { c.atomic = false }
}

// WithCheckVersion — слать ver в Lua; при mismatch Load+rematch+retry.
// Без флага GET ver не делается (статичные bindings после Load).
func WithCheckVersion() ExchangePublishOption {
	return func(c *exchangePublishConfig) { c.checkVersion = true }
}

// WithExchangeTagBackpressure — backpressure по тегу на каждой dest-очереди (AND до записи).
func WithExchangeTagBackpressure(tag string, max int64) ExchangePublishOption {
	return func(c *exchangePublishConfig) {
		WithTagBackpressure(tag, max)(&c.publishConfig)
	}
}

// WithExchangeCompression — сжатие для этого Publish.
func WithExchangeCompression(comp Compression) ExchangePublishOption {
	return func(c *exchangePublishConfig) {
		c.compression = comp
	}
}

func (c *exchangePublishConfig) asPublishOptions() []PublishOption {
	var opts []PublishOption
	if len(c.tagLimits) > 0 {
		limits := c.tagLimits
		opts = append(opts, WithTagLimits(limits))
	}
	if c.compression != CompressionNone && c.compression != 0 {
		opts = append(opts, WithCompression(c.compression))
	}
	if c.compressMinBytes > 0 {
		opts = append(opts, WithCompressMinBytes(c.compressMinBytes))
	}
	return opts
}
