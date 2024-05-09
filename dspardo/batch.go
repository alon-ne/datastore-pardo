package dspardo

import "cloud.google.com/go/datastore"

type Batch struct {
	Index     int
	Keys      []*datastore.Key
	EndCursor datastore.Cursor
}

func (b *Batch) Add(key *datastore.Key) {
	b.Keys = append(b.Keys, key)
}

func (b *Batch) Len() int {
	return len(b.Keys)
}
