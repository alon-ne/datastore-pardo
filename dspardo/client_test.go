package dspardo

import (
	"cloud.google.com/go/datastore"
	"context"
	"fmt"
	"github.com/alon-ne/datastore-pardo/lang"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"net/http"
	"os"
	"sort"
	"sync/atomic"
	"testing"
)

var dsClient *datastore.Client

const (
	kind        = "TestEntity"
	ancestor    = "ancestor"
	numEntities = 4000
)

var ancestorKey = datastore.NameKey(ancestor, ancestor, nil)
var testEntityKeys sortableKeys
var datastoreEmulatorHost = os.Getenv("DATASTORE_EMULATOR_HOST")

type Entity struct {
	N int    `datastore:"n"`
	S string `datastore:"s"`
}

type sortableKeys []*datastore.Key

func (s sortableKeys) Len() int {
	return len(s)
}

func (s sortableKeys) Less(i, j int) bool {
	return s[i].Name < s[j].Name
}

func (s sortableKeys) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	dsClient, err = datastore.NewClient(ctx, "dspardo")
	lang.PanicOnError(err)

	resp, err := http.Post("http://"+datastoreEmulatorHost+"/reset", "", nil)
	lang.PanicOnError(err)
	if resp.StatusCode != http.StatusOK {
		panic("response from datastore emulator: " + resp.Status)
	}
	validateAllEntitiesDeleted(ctx)

	testEntityKeys = putTestEntities(ctx, 1, numEntities)

	m.Run()
	os.Exit(0)
}

func Test_ParDoGetMulti(t *testing.T) {
	ctx := context.Background()
	numWorkers := 4
	batchSize := 79

	client := New(dsClient, numWorkers, batchSize)

	keys, err := dsClient.GetAll(ctx, testEntitiesQuery(), nil)
	require.NoError(t, err)
	shards := make([]int, numWorkers)

	err = ParDoGetMulti(ctx, client, keys, func(ctx context.Context, worker int, entities []Entity) error {
		fmt.Printf("Worker %v got %v entities\n", worker, len(entities))
		for _, entity := range entities {
			shards[worker] += entity.N
		}
		return nil
	})
	require.NoError(t, err)
	var sum int
	for _, shard := range shards {
		sum += shard
	}
	assert.EqualValues(t, numEntities, sum)
}

func Test_ParDoQuery_4_workers_1000_batch(t *testing.T) {
	testParDoQuery(t, context.Background(), 4, 1000, numEntities)
}

func Test_ParDoQuery_1_worker_1000_batch(t *testing.T) {
	testParDoQuery(t, context.Background(), 1, 1000, numEntities)
}

func Test_ParDoQuery_1_worker_1_batch(t *testing.T) {
	testParDoQuery(t, context.Background(), 1, 1, 4)
}

func TestClient_DeleteByQuery(t *testing.T) {
	keys := putTestEntities(context.Background(), 2, 2000)
	client := New(dsClient, 16, 60)
	err := client.DeleteByQuery(context.Background(),
		datastore.NewQuery(kind).FilterField("n", "=", 2).Order("__key__"),
		"deleted %v entities",
	)
	require.NoError(t, err)

	for _, k := range keys {
		var entity Entity
		err = dsClient.Get(context.Background(), k, &entity)
		require.ErrorIs(t, err, datastore.ErrNoSuchEntity)
	}
}

func testParDoQuery(t *testing.T, ctx context.Context, numWorkers int, batchSize int, entities int) {
	var totalProcessed int64
	batches := make(chan []*datastore.Key)

	var allKeys []*datastore.Key
	var collectKeys errgroup.Group
	collectKeys.Go(func() error {
		for batch := range batches {
			//log.Printf("appending %v", batch)
			allKeys = append(allKeys, batch...)
		}
		return nil
	})

	client := New(dsClient, numWorkers, batchSize)
	err := client.ParDoQuery(
		ctx,
		datastore.NewQuery(kind).Order("__key__").Limit(entities),
		func(ctx context.Context, batchIndex int, batch []*datastore.Key) error {
			//log.Printf("sending %v,%v", batchIndex, batch)
			batches <- batch
			return nil
		},
		func(ctx context.Context, processed int) {
			atomic.StoreInt64(&totalProcessed, int64(processed))
		},
	)
	close(batches)
	require.NoError(t, err)

	_ = collectKeys.Wait()

	assert.EqualValues(t, entities, totalProcessed)
	assert.Equal(t, entities, len(allKeys))
	assert.ElementsMatch(t, testEntityKeys[:entities], allKeys)
}

func deleteTestEntities(ctx context.Context) {
	fmt.Printf("Deleting test entities...\n")
	it := dsClient.Run(ctx, testEntitiesQuery())
	for {
		key, err := it.Next(nil)
		if err == iterator.Done {
			break
		}
		lang.PanicOnError(err)
		err = dsClient.Delete(ctx, key)
		lang.PanicOnError(err)
	}

	validateAllEntitiesDeleted(ctx)

}

func validateAllEntitiesDeleted(ctx context.Context) {
	query := testEntitiesQuery()

	existingEntities, err := dsClient.Count(ctx, query.Limit(1))
	lang.PanicOnError(err)
	if existingEntities > 0 {
		panic("not all entities deleted")
	}
}

func testEntitiesQuery() *datastore.Query {
	return datastore.NewQuery(kind).Ancestor(ancestorKey).KeysOnly()
}

func putTestEntities(ctx context.Context, n int, numEntities int) (allKeys sortableKeys) {
	fmt.Printf("Creating test entities...\n")
	var entities []Entity
	var keys []*datastore.Key

	for i := 0; i < numEntities; i++ {
		keys = append(keys, testKey())
		entities = append(entities, Entity{N: n, S: "text"})
		if len(keys) == 500 || i == numEntities-1 {
			fmt.Printf("Putting %v test entities:...\n", len(keys))
			_, err := dsClient.PutMulti(ctx, keys, entities)
			lang.PanicOnError(err)
			allKeys = append(allKeys, keys...)
			keys = nil
			entities = nil
		}
	}

	sort.Sort(allKeys)

	return
}

func testKey() *datastore.Key {
	return datastore.NameKey(kind, uuid.New().String(), ancestorKey)
}
