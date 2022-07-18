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
