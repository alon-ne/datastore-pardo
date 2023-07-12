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

type ParDoKeysFunc func(ctx context.Context, batch Batch) error
type ProgressCallback func(ctx context.Context, processed int)

//goland:noinspection GoUnusedConst
const (
	DatastorePutMaxBatchSize = 500
	UnlimitedWorkers         = -1
)

type Client struct {
	*datastore.Client
	numWorkers int
	batchSize  int
}

func New(dsClient *datastore.Client, numWorkers, batchSize int) *Client {
	if numWorkers == 0 {
		numWorkers = UnlimitedWorkers
	}

	return &Client{
		Client:     dsClient,
		numWorkers: numWorkers,
		batchSize:  batchSize,
	}
}

func (c *Client) DeleteByQuery(ctx context.Context, query *datastore.Query, progressFormat string) error {
	return c.ParDoQuery(ctx, query,
		func(ctx context.Context, batch Batch) error {
			return c.DeleteMulti(ctx, batch.Keys)
		},
		func(_ context.Context, processed int) {
			if progressFormat != "" {
				fmt.Printf(progressFormat+"\n", processed)
			}
		},
	)
}

func (c *Client) ParDoQuery(ctx context.Context, query *datastore.Query, do ParDoKeysFunc, progress ProgressCallback) (err error) {
	errGroup, errGroupCtx := errgroup.WithContext(ctx)
	errGroup.SetLimit(c.numWorkers)

	batch := c.newBatch(0)
	entitiesProcessed := int64(0)
	batchSize := c.batchSize
	it := c.Client.Run(ctx, query.KeysOnly())
	for err == nil {
		var key *datastore.Key
		batch.StartCursor, err = it.Cursor()
		if err == nil {
			key, err = it.Next(nil)
		}
		//log.Printf("got %v", batch, err)

		if err == nil {
			batch.Add(key)
		} else if errors.Is(err, iterator.Done) {
			batchSize = batch.Len()
		}

		if batch.Len() < batchSize || batchSize == 0 {
			continue
		}

		select {
		case <-errGroupCtx.Done():
			if err == nil {
				err = errGroupCtx.Err()
			}
		default:
		}

		readyBatch, err := batch.Finalize(it)
		if err != nil {
			return err
		}

		errGroup.Go(func() error {
			//log.Printf("doing %v", readyBatch)
			if err := do(ctx, readyBatch); err != nil {
				return err
			}

			entitiesProcessed := atomic.AddInt64(&entitiesProcessed, int64(readyBatch.Len()))
			progress(ctx, int(entitiesProcessed))
			return nil
		})

		batch = c.newBatch(batch.Index + 1)
	}

	if errors.Is(err, iterator.Done) {
		err = nil
	}

	if err == nil {
		err = errGroup.Wait()
	}

	return
}

func (c *Client) newBatch(index int) Batch {
	return Batch{Index: index, Keys: make([]*datastore.Key, 0, c.batchSize)}
}
