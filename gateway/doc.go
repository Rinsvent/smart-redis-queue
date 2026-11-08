// Package gateway — opt-in single-active SRQ Gateway перед Redis.
//
// Data-plane (Publish/Get/Ack/Reject) через gRPC; Admin UI ходит в Redis напрямую.
// После Redis OK publish держится в RAM loss-window (≤1s); mutating ops — в едином op-log.
package gateway
