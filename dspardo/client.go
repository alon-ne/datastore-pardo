package dspardo

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"sync/atomic"
)

type ParDoKeysFunc func(ctx context.Context, batchIndex int, batch []*datastore.Key) error
type ProgressCallback func(ctx context.Context, processed int)

//goland:noinspection GoUnusedConst
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

func (c *Client) DeleteByQuery(ctx context.Context, query *datastore.Query, progressFormat string) error {
	return c.ParDoQuery(ctx, query,
		func(ctx context.Context, _ int, keys []*datastore.Key) error {
			return c.DeleteMulti(ctx, keys)
		},
		func(_ context.Context, processed int) {
			if progressFormat != "" {
				fmt.Printf(progressFormat+"\n", processed)
			}
		},
	)
}

func (c *Client) ParDoQuery(ctx context.Context, query *datastore.Query, do ParDoKeysFunc, progress ProgressCallback) (err error) {
	var errGroup errgroup.Group
	errGroup.SetLimit(c.numWorkers)

	batch := c.newBatch()

	var i int
	var entitiesProcessed int64
	batchSize := c.batchSize

	it := c.Client.Run(ctx, query.KeysOnly())
	for err == nil {
		var key *datastore.Key
		key, err = it.Next(nil)
		//log.Printf("got %v,%v,%v", i, key, err)

		if err == nil {
			batch = append(batch, key)
		} else if errors.Is(err, iterator.Done) {
			batchSize = len(batch)
		}

		if len(batch) < batchSize || batchSize == 0 {
			continue
		}

		select {
		case <-ctx.Done():
			if err == nil {
				err = ctx.Err()
			}
		default:
		}

		batchIndex, readyBatch := i, batch
		errGroup.Go(func() error {
			//log.Printf("doing %v, %v", batchIndex, readyBatch)
			if err := do(ctx, batchIndex, readyBatch); err != nil {
				return err
			}

			entitiesProcessed := atomic.AddInt64(&entitiesProcessed, int64(len(readyBatch)))
			progress(ctx, int(entitiesProcessed))
			return nil
		})

		i++
		batch = c.newBatch()
	}

	if errors.Is(err, iterator.Done) {
		err = nil
	}

	return errGroup.Wait()
}

func (c *Client) newBatch() []*datastore.Key {
	return make([]*datastore.Key, 0, c.batchSize)
}
