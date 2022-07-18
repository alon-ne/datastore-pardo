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

//func (c *Client) ParDoGetMulti(ctx context.Context, keys *[]datastore.Key)

func ParDoGetMulti[T interface{}](
	ctx context.Context,
	c *Client,
	keys []*datastore.Key,
	do func(ctx context.Context, worker int, entities []T) error,
) error {
	var consumers errgroup.Group
	keyBatches := make(chan []*datastore.Key, c.numWorkers)
	for i := 0; i < c.numWorkers; i++ {
		worker := i
		consumers.Go(func() error {
			for keyBatch := range keyBatches {
				entitiesBatch := make([]T, len(keyBatch))
				if err := c.GetMulti(ctx, keyBatch, entitiesBatch); err != nil {
					return err
				}

				if err := do(ctx, worker, entitiesBatch); err != nil {
					return err
				}
			}
			return nil
		})
	}

	var err error
	batchSize := c.batchSize
	for offset := 0; offset < len(keys) && err == nil; offset += batchSize {
		remaining := len(keys) - offset
		if batchSize > remaining {
			batchSize = remaining
		}

		select {
		case keyBatches <- keys[offset : offset+batchSize]:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}

	close(keyBatches)
	if err != nil {
		return err
	}

	if err = consumers.Wait(); err != nil {
		return err
	}

	return nil
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
