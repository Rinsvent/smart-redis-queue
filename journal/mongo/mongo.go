package mongo

import (
	"context"
	"fmt"
	"time"

	redisqueue "github.com/Rinsvent/smart-redis-queue"
	"github.com/Rinsvent/smart-redis-queue/journal"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
}

type doc struct {
	ID        primitive.ObjectID `bson:"_id"`
	TaskID    string             `bson:"task_id"`
	Partition string             `bson:"partition"`
	Priority  int                `bson:"priority"`
	Payload   []byte             `bson:"payload"`
	Tags      []string           `bson:"tags"`
	Groups    []string           `bson:"groups"`
	Scheduled time.Time          `bson:"scheduled"`
	CreatedAt time.Time          `bson:"created_at"`
	AckedAt   *time.Time         `bson:"acked_at"` // null до Ack — для partial index
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
	return &Source{
		client: cl,
		col:    cl.Database(cfg.Database).Collection(cfg.Collection),
	}, nil
}

func (s *Source) Close(ctx context.Context) error { return s.client.Disconnect(ctx) }

func (s *Source) Init(ctx context.Context) error {
	// _id (ObjectID) — монотонный offset, отдельный counter не нужен.
	// partial $exists:false не поддерживается → храним acked_at: null до Ack.
	_, err := s.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "task_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "scheduled", Value: 1}}, Options: options.Index().SetPartialFilterExpression(bson.M{"acked_at": nil})},
		{Keys: bson.D{{Key: "_id", Value: 1}, {Key: "acked_at", Value: 1}}},
	})
	return err
}

func (s *Source) Insert(ctx context.Context, tasks ...*redisqueue.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	now := time.Now()
	docs := make([]interface{}, 0, len(tasks))
	for _, t := range tasks {
		tags, groups := t.Tags, t.Groups
		if tags == nil {
			tags = []string{}
		}
		if groups == nil {
			groups = []string{}
		}
		docs = append(docs, doc{
			ID:        primitive.NewObjectID(),
			TaskID:    t.ID,
			Partition: t.Partition,
			Priority:  t.Priority,
			Payload:   t.Payload,
			Tags:      tags,
			Groups:    groups,
			Scheduled: journal.ScheduledOrNow(t.Scheduled),
			CreatedAt: now,
			AckedAt:   nil,
		})
	}
	_, err := s.col.InsertMany(ctx, docs, options.InsertMany().SetOrdered(true))
	return err
}

func afterObjectID(c journal.Cursor) (primitive.ObjectID, error) {
	if c.Key == "" {
		return primitive.NilObjectID, nil
	}
	id, err := primitive.ObjectIDFromHex(c.Key)
	if err != nil {
		return primitive.NilObjectID, fmt.Errorf("mongo journal cursor: %w", err)
	}
	return id, nil
}

func (s *Source) ReadBatch(ctx context.Context, after journal.Cursor, n int) ([]journal.Record, error) {
	if n < 1 {
		n = 32
	}
	afterID, err := afterObjectID(after)
	if err != nil {
		return nil, err
	}
	cur, err := s.col.Find(ctx,
		bson.M{"acked_at": nil, "_id": bson.M{"$gt": afterID}},
		options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(n)),
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
	id, err := afterObjectID(c)
	if err != nil {
		return journal.Record{}, err
	}
	if id.IsZero() {
		return journal.Record{}, fmt.Errorf("mongo: empty ObjectID cursor")
	}
	var d doc
	if err := s.col.FindOne(ctx, bson.M{"_id": id}).Decode(&d); err != nil {
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
		Cursor: journal.KeyCursor(d.ID.Hex()),
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
