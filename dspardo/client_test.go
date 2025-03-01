package dspardo

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"fmt"
	"github.com/alon-ne/datastore-pardo/lang"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"log"
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

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	dsClient, err = datastore.NewClient(ctx, "dspardo")
	lang.PanicOnError(err)

	if datastoreEmulatorHost != "" {
		resp, err := http.Post("http://"+datastoreEmulatorHost+"/reset", "", nil)
		lang.PanicOnError(err)
		if resp.StatusCode != http.StatusOK {
			panic("response from datastore emulator: " + resp.Status)
		}
		validateAllEntitiesDeleted(ctx)
		testEntityKeys = putTestEntities(ctx, 1, numEntities)
	}

	m.Run()
	os.Exit(0)
}

func Test_ParDoGetMulti(t *testing.T) {
	ctx := context.Background()
	numWorkers := 4
	batchSize := 79

	client := New(dsClient, numWorkers, batchSize, false)

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

func TestClient_ParDoQuery_4_workers_1000_batch(t *testing.T) {
	testParDoQuery(t, 4, 1000, numEntities, 0)
}

func TestClient_ParDoQuery_4_workers_1000_batch_withErrors(t *testing.T) {
	testParDoQuery(t, 4, 1000, numEntities, 10)
}

func TestClient_ParDoQuery_unlimited_workers_500_batch_4_entities(t *testing.T) {
	testParDoQuery(t, 4, 500, 4, 0)
}

func TestClient_ParDoQuery_1_worker_1000_batch(t *testing.T) {
	testParDoQuery(t, 1, 1000, numEntities, 0)
}

func TestClient_ParDoQuery_1_worker_1_batch(t *testing.T) {
	testParDoQuery(t, 1, 1, 4, 0)
}

func TestClient_ParDoQuery_1_worker_1_batch_notKeysOnly(t *testing.T) {
	var batchSize int = 1
	numEntities := 1
	var totalProcessed int64
	batches := make(chan Batch)

	var allKeys []*datastore.Key
	var allProperties []datastore.PropertyList
	var collectResults errgroup.Group

	expectedBatches := numEntities / batchSize

	batchIndexes := map[int]struct{}{}
	collectResults.Go(func() error {
		for batch := range batches {
			//log.Printf("appending %v", batch)
			allKeys = append(allKeys, batch.Keys...)
			allProperties = append(allProperties, batch.Properties...)
			batchIndexes[batch.Index] = struct{}{}
		}
		return nil
	})

	client := New(dsClient, 1, batchSize, false)
	client.KeysOnly = false
	err := client.ParDoQuery(
		context.Background(),
		datastore.NewQuery(kind).Order("__key__").Limit(numEntities),
		func(ctx context.Context, batch Batch) error {
			log.Printf("%v got batch %#v", t.Name(), batch)
			batches <- batch
			return nil
		},
		func(ctx context.Context, processed int) {
			atomic.StoreInt64(&totalProcessed, int64(processed))
		},
	)
	close(batches)
	require.NoError(t, err)

	_ = collectResults.Wait()

	for i := 0; i < expectedBatches; i++ {
		assert.Contains(t, batchIndexes, i)
	}
	assert.EqualValues(t, numEntities, totalProcessed)
	assert.Equal(t, numEntities, len(allKeys))
	expectedKeys := testEntityKeys[:numEntities]
	assert.ElementsMatch(t, expectedKeys, allKeys, "expected:\t%v\nactual:\t\t%v", expectedKeys, allKeys)
	for i := 0; i < numEntities; i++ {
		//assert.ElementsMatch(t, i, allProperties[i], datastore.PropertyList{{Name: "n", Value: 1}, {Name: "s", Value: "text"}})
		for _, prop := range allProperties[i] {
			switch prop.Name {
			case "n":
				assert.EqualValues(t, 1, prop.Value)
			case "s":
				assert.Equal(t, "text", prop.Value)
			}
		}
	}
}

func TestClient_DeleteByQuery(t *testing.T) {
	putTestEntities(context.Background(), 2, 2000)
	client := New(dsClient, 16, 500, false)
	query := datastore.NewQuery(kind).FilterField("n", "=", 2).Order("__key__")
	err := client.DeleteByQuery(context.Background(), query, "deleted %v entities")
	require.NoError(t, err)

	//goland:noinspection GoDeprecation
	notDeleted, err := client.Count(context.Background(), query.Limit(1))
	require.NoError(t, err)
	require.Zero(t, notDeleted)
}

func testParDoQuery(t *testing.T, numWorkers, batchSize, numEntities, errorsInterval int) {
	var totalProcessed int64
	batches := make(chan Batch)

	var allKeys []*datastore.Key
	var collectResults errgroup.Group

	expectedBatches := numEntities / batchSize

	batchIndexes := map[int]struct{}{}
	collectResults.Go(func() error {
		for batch := range batches {
			//log.Printf("appending %v", batch)
			allKeys = append(allKeys, batch.Keys...)
			batchIndexes[batch.Index] = struct{}{}
		}
		return nil
	})

	testError := errors.New("test error")

	client := New(dsClient, numWorkers, batchSize, true)
	err := client.ParDoQuery(
		context.Background(),
		datastore.NewQuery(kind).Order("__key__").Limit(numEntities),
		func(ctx context.Context, batch Batch) error {
			//log.Printf("sending %v,%v", batchIndex, batch)
			batches <- batch
			if errorsInterval > 0 && batch.Index%errorsInterval == 0 {
				return testError
			}
			return nil
		},
		func(ctx context.Context, processed int) {
			atomic.StoreInt64(&totalProcessed, int64(processed))
		},
	)
	close(batches)

	if errorsInterval > 0 {
		require.ErrorIs(t, err, testError)
	} else {
		require.NoError(t, err)
	}

	_ = collectResults.Wait()

	for i := 0; i < expectedBatches; i++ {
		assert.Contains(t, batchIndexes, i)
	}
	assert.EqualValues(t, numEntities, totalProcessed)
	assert.Equal(t, numEntities, len(allKeys))
	expectedKeys := testEntityKeys[:numEntities]
	assert.ElementsMatch(t, expectedKeys, allKeys, "expected:\t%v\nactual:\t\t%v", expectedKeys, allKeys)
}

func validateAllEntitiesDeleted(ctx context.Context) {
	query := testEntitiesQuery()

	//goland:noinspection GoDeprecation
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
