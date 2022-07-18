package dspardo

import (
	"cloud.google.com/go/datastore"
	"context"
	"golang.org/x/sync/errgroup"
)

func ParDoGetMulti[T interface{}](
	ctx context.Context,
	c *Client,
	keys []*datastore.Key,
	do func(ctx context.Context, worker int, entities []T) error,
) error {
	consumers, keyBatches := startConsumers(ctx, c, do)
	keyBatches, err := sendBatches(ctx, c, keys, keyBatches)

	close(keyBatches)
	if err != nil {
		return err
	}

	if err = consumers.Wait(); err != nil {
		return err
	}

	return nil
}

func sendBatches(ctx context.Context, c *Client, keys []*datastore.Key, keyBatches chan []*datastore.Key) (chan []*datastore.Key, error) {
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
	return keyBatches, err
}

func startConsumers[T interface{}](
	ctx context.Context,
	c *Client,
	do func(ctx context.Context, worker int, entities []T) error,
) (*errgroup.Group, chan []*datastore.Key) {
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
	return &consumers, keyBatches
}
