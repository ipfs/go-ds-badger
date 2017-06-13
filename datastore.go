package badger

import (
	badger "github.com/dgraph-io/badger/badger"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/pkg/errors"
)

type datastore struct {
	DB *badger.KV
}

func NewDatastore(path string) (*datastore, error) {
	opt := badger.DefaultOptions
	opt.Dir = path
	opt.ValueDir = path
	opt.ValueGCThreshold = 0

	kv, err := badger.NewKV(&opt)
	if err != nil {
		return nil, err
	}

	return &datastore{
		DB: kv,
	}, nil
}

func (d *datastore) Put(key ds.Key, value interface{}) (err error) {
	return errors.New("not implemented")
}

func (d *datastore) Get(key ds.Key) (value interface{}, err error) {
	return nil, errors.New("not implemented")
}

func (d *datastore) Has(key ds.Key) (exists bool, err error) {
	return nil, errors.New("not implemented")
}

func (d *datastore) Delete(key ds.Key) (err error) {
	return errors.New("not implemented")
}

func (d *datastore) Query(q dsq.Query) (dsq.Results, error) {
	return d.QueryNew(q)
}

func (d *datastore) QueryNew(q dsq.Query) (dsq.Results, error) {
	return nil, errors.New("not implemented")
}

func (d *datastore) QueryOrig(q dsq.Query) (dsq.Results, error) {
	return nil, errors.New("not implemented")
}

func (d *datastore) Close() (err error) {
	return d.DB.Close()
}

func (d *datastore) Batch() (ds.Batch, error) {
	return nil, errors.New("not implemented")
}
