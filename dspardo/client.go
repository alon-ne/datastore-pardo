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

func (c *Client) DeleteByQuery(ctx context.Context, query *datastore.Query, progressFormat string) error {
	return c.ParDoQuery(ctx, query,
		func(ctx context.Context, worker int, keys []*datastore.Key) error {
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
	errGroup, errCtx := errgroup.WithContext(ctx)

	batches := c.sendBatches(errCtx, errGroup, query)
	c.startWorkers(ctx, errGroup, do, progress, batches)

	return errGroup.Wait()
}

func (c *Client) startWorkers(ctx context.Context, errGroup *errgroup.Group, do ParDoKeysFunc, progress ProgressCallback, batches chan []*datastore.Key) {
	var entitiesProcessed int64
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

}

func (c *Client) sendBatches(ctx context.Context, errGroup *errgroup.Group, query *datastore.Query) (batches chan []*datastore.Key) {
	bufferSize := c.numWorkers
	if bufferSize == -1 {
		bufferSize = 0
	}

	batches = make(chan []*datastore.Key, bufferSize)
	query = query.KeysOnly()

	errGroup.Go(func() (err error) {
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

			if len(keys) < batchSize || batchSize == 0 {
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
		close(batches)
		if errors.Is(err, iterator.Done) {
			err = nil
		}

		return
	})
	return
}

func (c *Client) makeKeys() []*datastore.Key {
	return make([]*datastore.Key, 0, c.batchSize)
}
