package badger

import (
	"bytes"
	"errors"
	badger "github.com/dgraph-io/badger/badger"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/jbenet/goprocess"
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
	if len(q.Filters) > 0 ||
		len(q.Orders) > 0 ||
		q.Limit > 0 ||
		q.Offset > 0 {
		return d.QueryOrig(q)
	}

	opt := badger.DefaultIteratorOptions
	opt.FetchValues = !q.KeysOnly
	it := d.DB.NewIterator(opt)
	it.Rewind()

	it.Seek([]byte(q.Prefix))

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Next: func() (dsq.Result, bool) {
			if !it.Valid() {
				return dsq.Result{}, false
			}
			item := it.Item()
			k := string(item.Key())
			e := dsq.Entry{Key: k}

			if !bytes.HasPrefix(item.Key(), []byte(q.Prefix)) {
				return dsq.Result{}, false
			}

			if !q.KeysOnly {
				buf := make([]byte, len(item.Value()))
				copy(buf, item.Value())
				e.Value = buf
			}

			it.Next()
			return dsq.Result{Entry: e}, true
		},
		Close: func() error {
			it.Close()
			return nil
		},
	}), nil
}

func (d *datastore) QueryOrig(q dsq.Query) (dsq.Results, error) {
	qrb := dsq.NewResultBuilder(q)
	qrb.Process.Go(func(worker goprocess.Process) {
		d.runQuery(worker, qrb)
	})

	// go wait on the worker (without signaling close)
	go qrb.Process.CloseAfterChildren()

	// Now, apply remaining things (filters, order)
	qr := qrb.Results()
	for _, f := range q.Filters {
		qr = dsq.NaiveFilter(qr, f)
	}
	for _, o := range q.Orders {
		qr = dsq.NaiveOrder(qr, o)
	}
	return qr, nil
}

func (d *datastore) runQuery(worker goprocess.Process, qrb *dsq.ResultBuilder) {
	opt := badger.DefaultIteratorOptions
	opt.FetchValues = !qrb.Query.KeysOnly
	it := d.DB.NewIterator(opt)
	defer it.Close()

	it.Rewind()
	it.Seek([]byte(qrb.Query.Prefix))
	if qrb.Query.Offset > 0 {
		for j := 0; j < qrb.Query.Offset; j++ {
			it.Next()
		}
	}

	for sent := 0; it.Valid(); sent++ {
		if qrb.Query.Limit > 0 && sent >= qrb.Query.Limit {
			break
		}

		k := string(it.Item().Key())
		e := dsq.Entry{Key: k}

		if !qrb.Query.KeysOnly {
			buf := make([]byte, len(it.Item().Value()))
			copy(buf, it.Item().Value())
			e.Value = buf
		}

		select {
		case qrb.Output <- dsq.Result{Entry: e}: // we sent it out
		case <-worker.Closing(): // client told us to end early.
			break
		}
		it.Next()
	}
}

func (d *datastore) Close() (err error) {
	return d.DB.Close()
}

func (d *datastore) IsThreadSafe() {}

func (d *datastore) Batch() (ds.Batch, error) {
	return nil, errors.New("not implemented")
}
