package dspardo

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"log"
	"sync/atomic"
)

type ParDoKeysFunc func(ctx context.Context, batchIndex int, keys []*datastore.Key) error
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

func (c *Client) ParDoQuery(ctx context.Context, query *datastore.Query,
	do ParDoKeysFunc, progress ProgressCallback) error {
	var errGroup errgroup.Group
	errGroup.SetLimit(c.numWorkers)

	query = query.KeysOnly()

	it := c.Client.Run(ctx, query)
	batch := c.newBatch()
	var err error
	var batchIndex int
	var entitiesProcessed int64

	for err == nil {
		var key *datastore.Key
		key, err = it.Next(nil)
		log.Printf("got %v,%v,%v", batchIndex, key, err)

		batchSize := c.batchSize
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

		goIndex := batchIndex
		goBatch := batch
		errGroup.Go(func() error {
			log.Printf("doing %v, %v", goIndex, goBatch)
			if err := do(ctx, goIndex, goBatch); err != nil {
				return err
			}

			entitiesProcessed := atomic.AddInt64(&entitiesProcessed, int64(len(goBatch)))
			progress(ctx, int(entitiesProcessed))

			return nil
		})

		batch = c.newBatch()
		batchIndex++
	}
	if errors.Is(err, iterator.Done) {
		err = nil
	}

	return errGroup.Wait()
}

func (c *Client) newBatch() []*datastore.Key {
	return make([]*datastore.Key, 0, c.batchSize)
}
