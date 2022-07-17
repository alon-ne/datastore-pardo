package dspardo

import (
	"cloud.google.com/go/datastore"
	"context"
	"fmt"
	"github.com/alon-ne/datastore-pardo/lang"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"net/http"
	"os"
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
var testEntityKeys []*datastore.Key

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	dsClient, err = datastore.NewClient(ctx, "dspardo")
	lang.PanicOnError(err)

	resp, err := http.Post("http://"+os.Getenv("DATASTORE_EMULATOR_HOST")+"/reset", "", nil)
	lang.PanicOnError(err)
	if resp.StatusCode != http.StatusOK {
		panic("response from datastore emulator: " + resp.Status)
	}
	validateAllEntitiesDeleted(ctx)

	testEntityKeys = putTestEntities(numEntities, ctx)

	m.Run()
	os.Exit(0)
}

func Test_Count(t *testing.T) {
	ctx := context.Background()
	numWorkers := 4
	batchSize := 1000

	client := New(dsClient, numWorkers, batchSize)
	count, err := client.Count(ctx, testEntitiesQuery())
	require.NoError(t, err)
	assert.Equal(t, count, numEntities)
}

func Test_ParDoKeysWithProgress(t *testing.T) {
	ctx := context.Background()
	numWorkers := 4
	batchSize := 1000

	var totalProcessed int64
	workerKeys := make([][]*datastore.Key, numWorkers)
	client := New(dsClient, numWorkers, batchSize)
	err := client.ParDoKeysWithProgress(
		ctx,
		datastore.NewQuery(kind),
		func(ctx context.Context, worker int, keys []*datastore.Key) error {
			workerKeys[worker] = append(workerKeys[worker], keys...)
			return nil
		},
		func(ctx context.Context, processed int) {
			atomic.StoreInt64(&totalProcessed, int64(processed))
		},
	)
	require.NoError(t, err)

	var allWorkerKeys []*datastore.Key
	for i := 0; i < numWorkers; i++ {
		fmt.Printf("Worker %v got %v keys\n", i, len(workerKeys[i]))
		allWorkerKeys = append(allWorkerKeys, workerKeys[i]...)
	}
	assert.EqualValues(t, numEntities, totalProcessed)
	assert.Equal(t, len(testEntityKeys), len(allWorkerKeys))
	//assert.ElementsMatch(t, testEntityKeys, allWorkerKeys)

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

func putTestEntities(numEntities int, ctx context.Context) (allKeys []*datastore.Key) {
	fmt.Printf("Creating test entities...\n")
	var entities []datastore.PropertyList
	var keys []*datastore.Key

	for i := 0; i < numEntities; i++ {
		keys = append(keys, testKey())
		entities = append(entities, datastore.PropertyList{})
		if len(keys) == 500 || i == numEntities-1 {
			fmt.Printf("Putting %v test entities...\n", len(keys))
			_, err := dsClient.PutMulti(ctx, keys, entities)
			lang.PanicOnError(err)
			allKeys = append(allKeys, keys...)
			keys = nil
			entities = nil
		}
	}
	return
}

func testKey() *datastore.Key {
	return datastore.NameKey(kind, uuid.New().String(), ancestorKey)
}
