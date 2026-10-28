package tasks

import "strings"

// Политики когда cron-тик наступил, а предыдущий прогон ещё жив (лок payload).
const (
	// ConcurrencyForbid — пропустить тик (как сейчас / k8s Forbid).
	ConcurrencyForbid = "forbid"
	// ConcurrencyAfter — после завершения текущего сразу Enqueue ещё раз (pending_rerun).
	// Не debounce: deb! перезаписывает payload in-flight и ставит повтор после Ack с новым телом;
	// after = «один отложенный прогон теми же defaults», без параллели.
	ConcurrencyAfter = "after"
	// ConcurrencyAllow — параллельный прогон (~r на тике), как k8s Allow.
	ConcurrencyAllow = "allow"
)

func normalizeConcurrency(s string) string {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case ConcurrencyAfter:
		return ConcurrencyAfter
	case ConcurrencyAllow:
		return ConcurrencyAllow
	default:
		return ConcurrencyForbid
	}
}
