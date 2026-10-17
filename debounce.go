package redisqueue

import "strings"

// DebouncePrefix — opt-in debounce на Publish: id с этим префиксом всегда
// перезаписывает payload / двигает score; in-flight ставит флаг reactivate на Ack.
// Без префикса — прежний SET NX (дубликаты → not added).
const DebouncePrefix = "deb!"

// HasDebouncePrefix сообщает, включён ли debounce-режим для task id.
func HasDebouncePrefix(id string) bool {
	return strings.HasPrefix(id, DebouncePrefix)
}

// DebounceID возвращает id с префиксом (идемпотентно, если префикс уже есть).
func DebounceID(logical string) string {
	if HasDebouncePrefix(logical) {
		return logical
	}
	return DebouncePrefix + logical
}
