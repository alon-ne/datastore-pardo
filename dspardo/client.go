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
	KeysOnly bool

	numWorkers     int
	maxBatchSize   int
	cursorsEnabled bool
}

func New(dsClient *datastore.Client, numWorkers, batchSize int, cursorsEnabled bool) *Client {
	if numWorkers == 0 {
		numWorkers = UnlimitedWorkers
	}

	return &Client{
		KeysOnly:       true,
		Client:         dsClient,
		numWorkers:     numWorkers,
		maxBatchSize:   batchSize,
		cursorsEnabled: cursorsEnabled,
	}
}

func (c *Client) ParDoQuery(ctx context.Context, query *datastore.Query, do ParDoKeysFunc, progress ProgressCallback) (err error) {
	var entitiesProcessed int64
	var key *datastore.Key

	var errGroup errgroup.Group
	errGroup.SetLimit(c.numWorkers)

	desiredBatchSize := c.maxBatchSize
	if c.KeysOnly {
		query = query.KeysOnly()
	}
	it := c.Client.Run(ctx, query)
	batch := c.newBatch(0)

	var iteratorErr error
	var cursorErr error

	for iteratorErr == nil {
		var properties datastore.PropertyList

		dst := &properties
		if c.KeysOnly {
			dst = nil
		}
		key, iteratorErr = it.Next(dst)
		if iteratorErr == nil {
			batch.Keys = append(batch.Keys, key)
			batch.Properties = append(batch.Properties, properties)
		} else if errors.Is(iteratorErr, iterator.Done) {
			desiredBatchSize = batch.Len()
		}

		if batch.Len() < desiredBatchSize || desiredBatchSize == 0 {
			continue
		}

		readyBatch := batch
		if c.cursorsEnabled {
			if readyBatch.EndCursor, cursorErr = it.Cursor(); cursorErr != nil {
				break
			}
		}

		errGroup.Go(func() error {
			defer progress(ctx, int(atomic.AddInt64(&entitiesProcessed, int64(readyBatch.Len()))))
			return do(ctx, readyBatch)
		})

		batch = c.newBatch(batch.Index + 1)
	}

	if errGroupErr := errGroup.Wait(); errGroupErr != nil {
		return errGroupErr
	}

	if iteratorErr != nil && !errors.Is(iteratorErr, iterator.Done) {
		return iteratorErr
	}

	if cursorErr != nil {
		return cursorErr
	}

	return nil
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

func (c *Client) newBatch(index int) Batch {
	return Batch{
		Index:      index,
		Keys:       make([]*datastore.Key, 0, c.maxBatchSize),
		Properties: make([]datastore.PropertyList, 0, c.maxBatchSize),
	}
}
