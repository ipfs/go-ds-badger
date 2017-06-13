package badger

import (
	badger "github.com/dgraph-io/badger/badger"
	"github.com/pkg/errors"
	ds "gx/ipfs/QmRWDav6mzWseLWeYfVd5fvUKiVe9xNH29YfMF438fG364/go-datastore"
	dsq "gx/ipfs/QmRWDav6mzWseLWeYfVd5fvUKiVe9xNH29YfMF438fG364/go-datastore/query"
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
	val, ok := value.([]byte)
	if !ok {
		return ds.ErrInvalidType
	}

	return d.DB.Set(key.Bytes(), val)
}

func (d *datastore) Get(key ds.Key) (value interface{}, err error) {
	var item badger.KVItem
	err = d.DB.Get(key.Bytes(), &item)
	if err != nil {
		return nil, err
	}
	if item.Value() == nil {
		return nil, ds.ErrNotFound
	}
	return item.Value(), nil
}

func (d *datastore) Has(key ds.Key) (exists bool, err error) {
	var item badger.KVItem
	err = d.DB.Get(key.Bytes(), &item)
	if err != nil {
		return false, err
	}

	return item.Value() != nil, nil
}

func (d *datastore) Delete(key ds.Key) (err error) {
	return d.DB.Delete(key.Bytes())
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

func (d *datastore) IsThreadSafe() {}

func (d *datastore) Batch() (ds.Batch, error) {
	return nil, errors.New("not implemented")
}
