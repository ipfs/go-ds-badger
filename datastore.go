package badger

import (
	"fmt"
	"strings"
	"time"

	osh "github.com/Kubuxu/go-os-helper"
	badger "github.com/dgraph-io/badger"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	goprocess "github.com/jbenet/goprocess"
)

type Datastore struct {
	DB *badger.DB

	gcDiscardRatio float64
}

type txn struct {
	txn *badger.Txn
}

// Options are the badger datastore options, reexported here for convenience.
type Options struct {
	gcDiscardRatio float64

	badger.Options
}

var DefaultOptions = Options{
	gcDiscardRatio: 0.1,

	Options: badger.DefaultOptions,
}

// NewDatastore creates a new badger datastore.
//
// DO NOT set the Dir and/or ValuePath fields of opt, they will be set for you.
func NewDatastore(path string, options *Options) (*Datastore, error) {
	// Copy the options because we modify them.
	var opt badger.Options
	var gcDiscardRatio float64
	if options == nil {
		opt = badger.DefaultOptions
		gcDiscardRatio = DefaultOptions.gcDiscardRatio
	} else {
		opt = options.Options
		gcDiscardRatio = options.gcDiscardRatio
	}

	if osh.IsWindows() && opt.SyncWrites {
		opt.Truncate = true
	}

	opt.Dir = path
	opt.ValueDir = path

	kv, err := badger.Open(opt)
	if err != nil {
		if strings.HasPrefix(err.Error(), "manifest has unsupported version:") {
			err = fmt.Errorf("unsupported badger version, use github.com/ipfs/badgerds-upgrade to upgrade: %s", err.Error())
		}
		return nil, err
	}

	return &Datastore{
		DB: kv,

		gcDiscardRatio: gcDiscardRatio,
	}, nil
}

func (d *Datastore) NewTransaction(readOnly bool) ds.Txn {
	return &txn{d.DB.NewTransaction(!readOnly)}
}

func (d *Datastore) Put(key ds.Key, value interface{}) error {
	txn := d.NewTransaction(false)
	defer txn.Discard()

	if err := txn.Put(key, value); err != nil {
		return err
	}

	return txn.Commit()
}

func (d *Datastore) PutWithTTL(key ds.Key, value interface{}, ttl time.Duration) error {
	txn := d.NewTransaction(false).(*txn)
	defer txn.Discard()

	if err := txn.PutWithTTL(key, value, ttl); err != nil {
		return err
	}

	return txn.Commit()
}

func (d *Datastore) SetTTL(key ds.Key, ttl time.Duration) error {
	txn := d.NewTransaction(false).(*txn)
	defer txn.Discard()

	if err := txn.SetTTL(key, ttl); err != nil {
		return err
	}

	return txn.Commit()
}

func (d *Datastore) Get(key ds.Key) (value interface{}, err error) {
	txn := d.NewTransaction(true)
	defer txn.Discard()

	return txn.Get(key)
}

func (d *Datastore) Has(key ds.Key) (bool, error) {
	txn := d.NewTransaction(true)
	defer txn.Discard()

	return txn.Has(key)
}

func (d *Datastore) Delete(key ds.Key) error {
	txn := d.NewTransaction(false)
	defer txn.Discard()

	err := txn.Delete(key)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (d *Datastore) Query(q dsq.Query) (dsq.Results, error) {
	txn := d.NewTransaction(true)
	defer txn.Discard()

	return txn.Query(q)
}

// DiskUsage implements the PersistentDatastore interface.
// It returns the sum of lsm and value log files sizes in bytes.
func (d *Datastore) DiskUsage() (uint64, error) {
	lsm, vlog := d.DB.Size()
	return uint64(lsm + vlog), nil
}

func (d *Datastore) Close() error {
	return d.DB.Close()
}

func (d *Datastore) IsThreadSafe() {}

func (d *Datastore) Batch() (ds.Batch, error) {
	return d.NewTransaction(false), nil
}

func (d *Datastore) CollectGarbage() error {
	err := d.DB.RunValueLogGC(d.gcDiscardRatio)
	if err == badger.ErrNoRewrite {
		err = nil
	}
	return err
}

func (t *txn) Put(key ds.Key, value interface{}) error {
	bytes, ok := value.([]byte)
	if !ok {
		return ds.ErrInvalidType
	}

	return t.txn.Set(key.Bytes(), bytes)
}

func (t *txn) PutWithTTL(key ds.Key, value interface{}, ttl time.Duration) error {
	bytes, ok := value.([]byte)
	if !ok {
		return ds.ErrInvalidType
	}

	return t.txn.SetWithTTL(key.Bytes(), bytes, ttl)
}

func (t *txn) SetTTL(key ds.Key, ttl time.Duration) error {
	data, err := t.Get(key)
	if err != nil {
		return err
	}

	return t.PutWithTTL(key, data, ttl)
}

func (t *txn) Get(key ds.Key) (interface{}, error) {
	item, err := t.txn.Get(key.Bytes())
	if err == badger.ErrKeyNotFound {
		err = ds.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	val, err := item.Value()
	if err != nil {
		return nil, err
	}

	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

func (t *txn) Has(key ds.Key) (bool, error) {
	_, err := t.Get(key)

	if err == nil {
		return true, nil
	} else if err == ds.ErrNotFound {
		return false, nil
	}

	return false, err
}

func (t *txn) Delete(key ds.Key) error {
	return t.txn.Delete(key.Bytes())
}

func (t *txn) Query(q dsq.Query) (dsq.Results, error) {
	prefix := []byte(q.Prefix)
	opt := badger.DefaultIteratorOptions
	opt.PrefetchValues = !q.KeysOnly

	txn := t.txn

	it := txn.NewIterator(opt)
	it.Seek([]byte(q.Prefix))
	if q.Offset > 0 {
		for j := 0; j < q.Offset; j++ {
			it.Next()
		}
	}

	qrb := dsq.NewResultBuilder(q)

	qrb.Process.Go(func(worker goprocess.Process) {
		defer it.Close()

		for sent := 0; it.ValidForPrefix(prefix); sent++ {
			if qrb.Query.Limit > 0 && sent >= qrb.Query.Limit {
				break
			}

			item := it.Item()

			k := string(item.Key())
			e := dsq.Entry{Key: k}

			var result dsq.Result
			if !q.KeysOnly {
				b, err := item.Value()
				if err != nil {
					result = dsq.Result{Error: err}
				} else {
					bytes := make([]byte, len(b))
					copy(bytes, b)
					e.Value = bytes
					result = dsq.Result{Entry: e}
				}
			} else {
				result = dsq.Result{Entry: e}
			}

			select {
			case qrb.Output <- result:
			case <-worker.Closing(): // client told us to close early
				return
			}
			it.Next()
		}

		return
	})

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

func (t *txn) Commit() error {
	return t.txn.Commit(nil)
}

func (t *txn) Discard() {
	t.txn.Discard()
}
