
package main
import (
  "context"
  "fmt"
  redisqueue "github.com/Rinsvent/smart-redis-queue"
  "github.com/redis/go-redis/v9"
)
func main() {
  rdb := redis.NewClient(&redis.Options{Addr:"localhost:6379", DB:0})
  defer rdb.Close()
  a := redisqueue.NewAdmin(rdb)
  s, err := a.QueueOverview(context.Background(), "demo-mail")
  if err != nil { panic(err) }
  fmt.Printf("pending=%d inProgress=%d consumers=%d parts=%d\n", s.TotalPending, s.TotalInProgress, len(s.Consumers), s.PartitionTotal)
  from, to, _ := a.QueueStatsBounds(context.Background(), "demo-mail")
  fmt.Printf("stats bounds %d..%d\n", from, to)
}
