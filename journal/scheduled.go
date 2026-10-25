package journal

import "time"

// ScheduledOrNow — zero → now; иначе исходное значение (в том числе в прошлом).
func ScheduledOrNow(t time.Time) time.Time {
	if t.IsZero() {
		return time.Now()
	}
	return t
}
