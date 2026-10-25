package mongo

import (
	"context"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/Rinsvent/smart-redis-queue/journal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Config struct {
	URI        string
	Database   string
	Collection string
}

type Source struct {
	client *mongo.Client
	col    *mongo.Collection
	ctr    *mongo.Collection
}

type doc struct {
	Offset    int64      `bson:"journal_offset"`
	TaskID    string     `bson:"task_id"`
	Partition string     `bson:"partition"`
	Priority  int        `bson:"priority"`
	Payload   []byte     `bson:"payload"`
	Tags      []string   `bson:"tags"`
	Groups    []string   `bson:"groups"`
	Scheduled time.Time  `bson:"scheduled"`
	CreatedAt time.Time  `bson:"created_at"`
	AckedAt   *time.Time `bson:"acked_at"` // null до Ack — для partial index
}

func New(ctx context.Context, cfg Config) (*Source, error) {
	if cfg.Collection == "" {
		cfg.Collection = "srq_journal_tasks"
	}
	if cfg.Database == "" {
		cfg.Database = "srq"
	}
	cl, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, err
	}
	db := cl.Database(cfg.Database)
	return &Source{
		client: cl,
		col:    db.Collection(cfg.Collection),
		ctr:    db.Collection(cfg.Collection + "_counters"),
	}, nil
}

func (s *Source) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }

func (s *Source) Init(ctx context.Context) error {
	// partial $exists:false не поддерживается → храним acked_at: null до Ack.
	_, err := s.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "journal_offset", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "task_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "scheduled", Value: 1}}, Options: options.Index().SetPartialFilterExpression(bson.M{"acked_at": nil})},
		{Keys: bson.D{{Key: "journal_offset", Value: 1}, {Key: "acked_at", Value: 1}}},
	})
	return err
}

func (s *Source) nextOffset(ctx context.Context) (int64, error) {
	res := s.ctr.FindOneAndUpdate(ctx,
		bson.M{"_id": "journal_offset"},
		bson.M{"$inc": bson.M{"seq": int64(1)}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	)
	var out struct {
		Seq int64 `bson:"seq"`
	}
	if err := res.Decode(&out); err != nil {
		return 0, err
	}
	return out.Seq, nil
}

func clampScheduled(sch time.Time) time.Time {
	now := time.Now()
	if sch.IsZero() || sch.Before(now) {
		return now
	}
	return sch
}

func (s *Source) Insert(ctx context.Context, tasks ...*redisqueue.Task) error {
	for _, t := range tasks {
		off, err := s.nextOffset(ctx)
		if err != nil {
			return err
		}
		tags, groups := t.Tags, t.Groups
		if tags == nil {
			tags = []string{}
		}
		if groups == nil {
			groups = []string{}
		}
		_, err = s.col.InsertOne(ctx, doc{
			Offset: off, TaskID: t.ID, Partition: t.Partition, Priority: t.Priority,
			Payload: t.Payload, Tags: tags, Groups: groups,
			Scheduled: clampScheduled(t.Scheduled), CreatedAt: time.Now(),
			AckedAt: nil,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) ReadBatch(ctx context.Context, after journal.Cursor, n int) ([]journal.Record, error) {
	if n < 1 {
		n = 32
	}
	afterOff := after.Offset
	if afterOff < 0 {
		afterOff = 0
	}
	cur, err := s.col.Find(ctx,
		bson.M{"acked_at": nil, "journal_offset": bson.M{"$gt": afterOff}},
		options.Find().SetSort(bson.D{{Key: "journal_offset", Value: 1}}).SetLimit(int64(n)),
	)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var out []journal.Record
	for cur.Next(ctx) {
		var d doc
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		out = append(out, toRec(d))
	}
	return out, cur.Err()
}

func (s *Source) ReadByOffset(ctx context.Context, c journal.Cursor) (journal.Record, error) {
	var d doc
	err := s.col.FindOne(ctx, bson.M{"journal_offset": c.Offset}).Decode(&d)
	if err != nil {
		return journal.Record{}, err
	}
	return toRec(d), nil
}

func toRec(d doc) journal.Record {
	return journal.Record{
		Task: &redisqueue.Task{
			ID: d.TaskID, Partition: d.Partition, Priority: d.Priority,
			Payload: d.Payload, Tags: d.Tags, Groups: d.Groups, Scheduled: d.Scheduled,
		},
		Cursor: journal.OffsetCursor(d.Offset),
	}
}

func (s *Source) Ack(ctx context.Context, taskID string, _ map[string]string) error {
	now := time.Now()
	_, err := s.col.UpdateOne(ctx, bson.M{"task_id": taskID, "acked_at": nil},
		bson.M{"$set": bson.M{"acked_at": now}})
	return err
}

func (s *Source) ReadAcks(context.Context, journal.Cursor, int) ([]journal.AckEvent, error) {
	return nil, nil
}
