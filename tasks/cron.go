package tasks

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

type cronSchedule interface {
	Next(time.Time) time.Time
}

func parseCron(expr string) (cronSchedule, error) {
	sched, err := cronParser.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid cron %q: %w", expr, err)
	}
	return sched, nil
}

// NextRunAfter — следующий запуск после t (для UI / тестов).
func NextRunAfter(cronExpr string, t time.Time) (time.Time, error) {
	sched, err := parseCron(cronExpr)
	if err != nil {
		return time.Time{}, err
	}
	return sched.Next(t), nil
}
