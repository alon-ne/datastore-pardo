package dspardo

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"sync/atomic"
)

type ParDoKeysFunc func(ctx context.Context, worker int, keys []*datastore.Key) error
type ParDoEntitiesFunc func(ctx context.Context, worker int, entities []datastore.PropertyList) error
type ProgressCallback func(ctx context.Context, processed int)

const DatastorePutMaxBatchSize = 500

type Client struct {
	*datastore.Client
	numWorkers int
	batchSize  int
}

func New(dsClient *datastore.Client, numWorkers, batchSize int) *Client {
	return &Client{
		Client:     dsClient,
		numWorkers: numWorkers,
		batchSize:  batchSize,
	}
}

func (c *Client) Count(ctx context.Context, query *datastore.Query) (count int, err error) {
	var count64 int64
	err = c.ParDoQuery(ctx, query,
		func(_ context.Context, _ int, keys []*datastore.Key) error {
			atomic.AddInt64(&count64, int64(len(keys)))
			return nil
		},
		func(ctx context.Context, processed int) {},
	)
	count = int(count64)
	return
}

func (c *Client) ParDoQuery(ctx context.Context, query *datastore.Query,
	do ParDoKeysFunc, progress ProgressCallback) (err error) {
	var errGroup errgroup.Group

	batches := c.startWorkers(ctx, &errGroup, do, progress)
	err = c.sendBatches(ctx, query, batches)
	close(batches)

	workersErr := errGroup.Wait()
	if errors.Is(err, iterator.Done) {
		err = nil
	}

	if err == nil {
		err = workersErr
	}

	return
}

func (c *Client) startWorkers(ctx context.Context, errGroup *errgroup.Group, do ParDoKeysFunc, progress ProgressCallback) chan []*datastore.Key {
	var entitiesProcessed int64
	batches := make(chan []*datastore.Key, c.numWorkers)
	for i := 0; i < c.numWorkers; i++ {
		worker := i
		errGroup.Go(func() error {
			for batch := range batches {
				if err := do(ctx, worker, batch); err != nil {
					return err
				}
				entitiesProcessed := atomic.AddInt64(&entitiesProcessed, int64(len(batch)))
				progress(ctx, int(entitiesProcessed))

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
			return nil
		})
	}
	return batches
}

func (c *Client) sendBatches(ctx context.Context, query *datastore.Query, batches chan []*datastore.Key) (err error) {
	query = query.KeysOnly()
	it := c.Client.Run(ctx, query)
	keys := c.makeKeys()

	for err == nil {
		var key *datastore.Key
		key, err = it.Next(nil)

		batchSize := c.batchSize
		if err == nil {
			keys = append(keys, key)
		} else if errors.Is(err, iterator.Done) {
			batchSize = len(keys)
		}

		if len(keys) < batchSize {
			continue
		}

		select {
		case batches <- keys:
			keys = c.makeKeys()
		case <-ctx.Done():
			if err == nil {
				err = ctx.Err()
			}
		}
	}
	return
}

func (c *Client) makeKeys() []*datastore.Key {
	return make([]*datastore.Key, 0, c.batchSize)
}
