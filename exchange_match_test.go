package redisqueue

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopicMatch(t *testing.T) {
	cases := []struct {
		pat, key string
		want     bool
	}{
		{"order.created", "order.created", true},
		{"order.created", "order.updated", false},
		{"order.*", "order.created", true},
		{"order.*", "order.created.eu", false},
		{"*.created", "order.created", true},
		{"#", "a.b.c", true},
		{"#", "", true},
		{"order.#", "order", true},
		{"order.#", "order.created", true},
		{"order.#", "order.created.eu", true},
		{"order.*.eu", "order.created.eu", true},
		{"order.*.eu", "order.created.us", false},
		{"#.eu", "order.created.eu", true},
		{"a.#.c", "a.b.c", true},
		{"a.#.c", "a.b.x.c", true},
		{"a.#.c", "a.c", true},
		{"*", "x", true},
		{"*", "x.y", false},
		{"", "", true},
		{"", "x", false},
	}
	for _, tc := range cases {
		assert.Equalf(t, tc.want, topicMatch(tc.pat, tc.key), "pat=%q key=%q", tc.pat, tc.key)
	}
}

func TestMatchBindings(t *testing.T) {
	bs := []Binding{
		{Pattern: "order.created", Queue: "billing"},
		{Pattern: "order.created", Queue: "analytics"},
		{Pattern: "order.*.eu", Queue: "eu"},
		{Pattern: "", Queue: "audit"},
	}

	assert.Equal(t, []string{"billing", "analytics"}, matchBindings(ExchangeDirect, "order.created", bs))
	assert.Empty(t, matchBindings(ExchangeDirect, "other", bs))

	fan := matchBindings(ExchangeFanout, "ignored", bs)
	assert.ElementsMatch(t, []string{"billing", "analytics", "eu", "audit"}, fan)

	topic := matchBindings(ExchangeTopic, "order.paid.eu", bs)
	assert.Equal(t, []string{"eu"}, topic)
}
