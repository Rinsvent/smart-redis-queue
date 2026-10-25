package mongo

import (
	"context"
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
	ctr    *mongo.Collection
}

// _id — стабильный ObjectID (не курсор). journal_offset — ось pump (int из batch $inc).
type doc struct {
	ID            primitive.ObjectID `bson:"_id"`
	JournalOffset *int64             `bson:"journal_offset"`
	State         string             `bson:"state"`
	TaskID        string             `bson:"task_id"`
	Partition     string             `bson:"partition"`
	Priority      int                `bson:"priority"`
	Payload       []byte             `bson:"payload"`
	Tags          []string           `bson:"tags"`
	Groups        []string           `bson:"groups"`
	Scheduled     time.Time          `bson:"scheduled"`
	CreatedAt     time.Time          `bson:"created_at"`
	AckedAt       *time.Time         `bson:"acked_at"`
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
	_, err := s.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "task_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "journal_offset", Value: 1}}, Options: options.Index().SetUnique(true).SetSparse(true)},
		{Keys: bson.D{{Key: "scheduled", Value: 1}}, Options: options.Index().SetPartialFilterExpression(bson.M{
			"state": journal.StateScheduled, "acked_at": nil,
		})},
		{Keys: bson.D{{Key: "journal_offset", Value: 1}, {Key: "state", Value: 1}, {Key: "acked_at", Value: 1}}},
	})
	return err
}

// reserveOffsets — один $inc на весь батч, возвращает первый id диапазона.
func (s *Source) reserveOffsets(ctx context.Context, n int64) (start int64, err error) {
	if n < 1 {
		return 0, nil
	}
	res := s.ctr.FindOneAndUpdate(ctx,
		bson.M{"_id": "journal_offset"},
		bson.M{"$inc": bson.M{"seq": n}},
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(options.After),
	)
	var out struct {
		Seq int64 `bson:"seq"`
	}
	if err := res.Decode(&out); err != nil {
		return 0, err
	}
	return out.Seq - n + 1, nil
}

func (s *Source) Insert(ctx context.Context, tasks ...*redisqueue.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	now := time.Now()
	type item struct {
		t   *redisqueue.Task
		sch time.Time
		due bool
	}
	items := make([]item, len(tasks))
	dueN := int64(0)
	for i, t := range tasks {
		sch := journal.ScheduledOrNow(t.Scheduled)
		due := journal.IsDue(sch, now)
		items[i] = item{t: t, sch: sch, due: due}
		if due {
			dueN++
		}
	}
	var start int64
	var err error
	if dueN > 0 {
		start, err = s.reserveOffsets(ctx, dueN)
		if err != nil {
			return err
		}
	}
	docs := make([]interface{}, 0, len(items))
	var dueI int64
	for _, it := range items {
		tags, groups := it.t.Tags, it.t.Groups
		if tags == nil {
			tags = []string{}
		}
		if groups == nil {
			groups = []string{}
		}
		d := doc{
			ID:        primitive.NewObjectID(),
			TaskID:    it.t.ID,
			Partition: it.t.Partition,
			Priority:  it.t.Priority,
			Payload:   it.t.Payload,
			Tags:      tags,
			Groups:    groups,
			Scheduled: it.sch,
			CreatedAt: now,
			AckedAt:   nil,
		}
		if it.due {
			off := start + dueI
			dueI++
			d.JournalOffset = &off
			d.State = journal.StateReady
		} else {
			d.State = journal.StateScheduled
		}
		docs = append(docs, d)
	}
	_, err = s.col.InsertMany(ctx, docs, options.InsertMany().SetOrdered(true))
	return err
}

func (s *Source) ReadBatch(ctx context.Context, after journal.Cursor, n int) ([]journal.Record, error) {
	if n < 1 {
		n = 32
	}
	afterOff := after.Offset
	if afterOff < 0 {
		afterOff = 0
	}
	// Только ready-stream. Не фильтруем scheduled<=now здесь — иначе дыра в курсоре.
	cur, err := s.col.Find(ctx,
		bson.M{
			"state":          journal.StateReady,
			"acked_at":       nil,
			"journal_offset": bson.M{"$gt": afterOff},
		},
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
	var off int64
	if d.JournalOffset != nil {
		off = *d.JournalOffset
	}
	return journal.Record{
		Task: &redisqueue.Task{
			ID: d.TaskID, Partition: d.Partition, Priority: d.Priority,
			Payload: d.Payload, Tags: d.Tags, Groups: d.Groups, Scheduled: d.Scheduled,
		},
		Cursor: journal.OffsetCursor(off),
	}
}

// PromoteDue — due scheduled → ready: один $inc на батч, без копирования payload.
func (s *Source) PromoteDue(ctx context.Context, limit int) (int, error) {
	if limit < 1 {
		limit = 64
	}
	dueAt := time.Now().Add(journal.DueSkew)
	cur, err := s.col.Find(ctx,
		bson.M{"state": journal.StateScheduled, "acked_at": nil, "scheduled": bson.M{"$lte": dueAt}},
		options.Find().SetSort(bson.D{{Key: "scheduled", Value: 1}}).SetLimit(int64(limit)),
	)
	if err != nil {
		return 0, err
	}
	defer cur.Close(ctx)
	var ids []primitive.ObjectID
	for cur.Next(ctx) {
		var d doc
		if err := cur.Decode(&d); err != nil {
			return 0, err
		}
		ids = append(ids, d.ID)
	}
	if err := cur.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	start, err := s.reserveOffsets(ctx, int64(len(ids)))
	if err != nil {
		return 0, err
	}
	models := make([]mongo.WriteModel, 0, len(ids))
	for i, id := range ids {
		off := start + int64(i)
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": id, "state": journal.StateScheduled}).
			SetUpdate(bson.M{"$set": bson.M{
				"state":          journal.StateReady,
				"journal_offset": off,
			}}))
	}
	res, err := s.col.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, err
	}
	return int(res.ModifiedCount), nil
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

var _ journal.DuePromoter = (*Source)(nil)
