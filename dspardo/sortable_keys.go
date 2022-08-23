package dspardo

import "cloud.google.com/go/datastore"

type sortableKeys []*datastore.Key

func (s sortableKeys) Len() int {
	return len(s)
}

func (s sortableKeys) Less(i, j int) bool {
	return s[i].Name < s[j].Name
}

func (s sortableKeys) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
