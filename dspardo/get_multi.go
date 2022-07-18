package dspardo

import (
	"cloud.google.com/go/datastore"
	"context"
	"golang.org/x/sync/errgroup"
)

func ParDoGetMulti[T interface{}](
	ctx context.Context,
	client *Client,
	keys []*datastore.Key,
	do func(ctx context.Context, worker int, entities []T) error,

) error {
	return (&parDoGetMulti[T]{
		client:     client,
		keys:       keys,
		do:         do,
		keyBatches: make(chan []*datastore.Key, client.numWorkers),
	}).run(ctx)
}

type parDoGetMulti[T interface{}] struct {
	client *Client
	keys   []*datastore.Key
	do     func(ctx context.Context, worker int, entities []T) error

	keyBatches chan []*datastore.Key
}

func (p *parDoGetMulti[T]) run(ctx context.Context) error {
	consumers := p.startConsumers(ctx)
	err := p.sendBatches(ctx)

	close(p.keyBatches)
	if err != nil {
		return err
	}

	if err = consumers.Wait(); err != nil {
		return err
	}

	return nil
}

func (p *parDoGetMulti[T]) startConsumers(ctx context.Context) *errgroup.Group {
	var consumers errgroup.Group
	for i := 0; i < p.client.numWorkers; i++ {
		worker := i
		consumers.Go(func() error {
			for keyBatch := range p.keyBatches {
				entitiesBatch := make([]T, len(keyBatch))
				if err := p.client.GetMulti(ctx, keyBatch, entitiesBatch); err != nil {
					return err
				}

				if err := p.do(ctx, worker, entitiesBatch); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return &consumers
}

func (p *parDoGetMulti[T]) sendBatches(ctx context.Context) error {
	var err error
	batchSize := p.client.batchSize
	for offset := 0; offset < len(p.keys) && err == nil; offset += batchSize {
		remaining := len(p.keys) - offset
		if batchSize > remaining {
			batchSize = remaining
		}

		select {
		case p.keyBatches <- p.keys[offset : offset+batchSize]:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	return err
}
