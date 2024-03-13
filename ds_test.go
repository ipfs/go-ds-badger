package badger

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	dstest "github.com/ipfs/go-datastore/test"
)

var bg = context.Background()

var testcases = map[string]string{
	"/a":     "a",
	"/a/b":   "ab",
	"/a/b/c": "abc",
	"/a/b/d": "a/b/d",
	"/a/c":   "ac",
	"/a/d":   "ad",
	"/e":     "e",
	"/f":     "f",
	"/g":     "",
}

// returns datastore, and a function to call on exit.
// (this garbage collects). So:
//
//	d, close := newDS(t)
//	defer close()
func newDS(t *testing.T) (*Datastore, func()) {
	path, err := os.MkdirTemp(os.TempDir(), "testing_badger_")
	if err != nil {
		t.Fatal(err)
	}

	d, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	return d, func() {
		d.Close()
		os.RemoveAll(path)
	}
}

func addTestCases(t *testing.T, d *Datastore, testcases map[string]string) {
	for k, v := range testcases {
		dsk := ds.NewKey(k)
		if err := d.Put(bg, dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := ds.NewKey(k)
		v2, err := d.Get(bg, dsk)
		if err != nil {
			t.Fatal(err)
		}
		if string(v2) != v {
			t.Errorf("%s values differ: %s != %s", k, v, v2)
		}
	}
}
func TestQuery(t *testing.T) {
	d, done := newDS(t)
	defer done()

	addTestCases(t, d, testcases)

	rs, err := d.Query(bg, dsq.Query{Prefix: "/a/"})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b",
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	}, rs)

	// test offset and limit

	rs, err = d.Query(bg, dsq.Query{Prefix: "/a/", Offset: 2, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a/b/d",
		"/a/c",
	}, rs)
}

func TestHas(t *testing.T) {
	d, done := newDS(t)
	defer done()
	addTestCases(t, d, testcases)

	has, err := d.Has(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}

	if !has {
		t.Error("Key should be found")
	}

	has, err = d.Has(bg, ds.NewKey("/a/b/c/d"))
	if err != nil {
		t.Error(err)
	}

	if has {
		t.Error("Key should not be found")
	}
}

func TestGetSize(t *testing.T) {
	d, done := newDS(t)
	defer done()
	addTestCases(t, d, testcases)

	size, err := d.GetSize(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}

	if size != len(testcases["/a/b/c"]) {
		t.Error("")
	}

	_, err = d.GetSize(bg, ds.NewKey("/a/b/c/d"))
	if err != ds.ErrNotFound {
		t.Error(err)
	}
}

func TestNotExistGet(t *testing.T) {
	d, done := newDS(t)
	defer done()
	addTestCases(t, d, testcases)

	has, err := d.Has(bg, ds.NewKey("/a/b/c/d"))
	if err != nil {
		t.Error(err)
	}

	if has {
		t.Error("Key should not be found")
	}

	val, err := d.Get(bg, ds.NewKey("/a/b/c/d"))
	if val != nil {
		t.Error("Key should not be found")
	}

	if err != ds.ErrNotFound {
		t.Error("Error was not set to ds.ErrNotFound")
		if err != nil {
			t.Error(err)
		}
	}
}

func TestDelete(t *testing.T) {
	d, done := newDS(t)
	defer done()
	addTestCases(t, d, testcases)

	has, err := d.Has(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}
	if !has {
		t.Error("Key should be found")
	}

	err = d.Delete(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}

	has, err = d.Has(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Error(err)
	}
	if has {
		t.Error("Key should not be found")
	}
}

func TestGetEmpty(t *testing.T) {
	d, done := newDS(t)
	defer done()

	err := d.Put(bg, ds.NewKey("/a"), []byte{})
	if err != nil {
		t.Error(err)
	}

	v, err := d.Get(bg, ds.NewKey("/a"))
	if err != nil {
		t.Error(err)
	}

	if len(v) != 0 {
		t.Error("expected 0 len []byte form get")
	}
}

func expectMatches(t *testing.T, expect []string, actualR dsq.Results) {
	actual, err := actualR.Rest()
	if err != nil {
		t.Error(err)
	}

	if len(actual) != len(expect) {
		t.Error("not enough", expect, actual)
	}
	for _, k := range expect {
		found := false
		for _, e := range actual {
			if e.Key == k {
				found = true
			}
		}
		if !found {
			t.Error(k, "not found")
		}
	}
}

func TestBatching(t *testing.T) {
	d, done := newDS(t)
	defer done()

	b, err := d.Batch(bg)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		err := b.Put(bg, ds.NewKey(k), []byte(v))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		val, err := d.Get(bg, ds.NewKey(k))
		if err != nil {
			t.Fatal(err)
		}

		if v != string(val) {
			t.Fatal("got wrong data!")
		}
	}

	//Test delete

	b, err = d.Batch(bg)
	if err != nil {
		t.Fatal(err)
	}

	err = b.Delete(bg, ds.NewKey("/a/b"))
	if err != nil {
		t.Fatal(err)
	}

	err = b.Delete(bg, ds.NewKey("/a/b/c"))
	if err != nil {
		t.Fatal(err)
	}

	err = b.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}

	rs, err := d.Query(bg, dsq.Query{Prefix: "/"})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, []string{
		"/a",
		"/a/b/d",
		"/a/c",
		"/a/d",
		"/e",
		"/f",
		"/g",
	}, rs)

	//Test cancel

	b, err = d.Batch(bg)
	if err != nil {
		t.Fatal(err)
	}

	const key = "/xyz"

	err = b.Put(bg, ds.NewKey(key), []byte("/x/y/z"))
	if err != nil {
		t.Fatal(err)
	}

	// TODO: remove type assertion once datastore.Batch interface has Cancel
	err = b.(*batch).Cancel()
	if err != nil {
		t.Fatal(err)
	}

	_, err = d.Get(bg, ds.NewKey(key))
	if err == nil {
		t.Fatal("expected error trying to get uncommited data")
	}
}

func TestBatchingRequired(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "testing_badger_")
	if err != nil {
		t.Fatal(err)
	}

	dsOpts := DefaultOptions
	d, err := NewDatastore(path, &dsOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		d.Close()
		os.RemoveAll(path)
	}()

	const valSize = 1000

	// Check that transaction fails when there are too many writes.  This is
	// not testing batching logic, but is here to prove that batching works
	// where a transaction fails.
	t.Logf("putting %d byte values until transaction overflows", valSize)
	tx, err := d.NewTransaction(bg, false)
	if err != nil {
		t.Fatal(err)
	}
	var puts int
	for ; puts < 10000000; puts++ {
		buf := make([]byte, valSize)
		rand.Read(buf)
		err = tx.Put(bg, ds.NewKey(fmt.Sprintf("/key%d", puts)), buf)
		if err != nil {
			break
		}
		puts++
	}
	if err == nil {
		t.Error("expected transaction to fail")
	} else {
		t.Logf("OK - transaction cannot handle %d puts: %s", puts, err)
	}
	tx.Discard(bg)

	// Check that batch succeeds with the same number of writes that caused a
	// transaction to fail.
	t.Logf("putting %d %d byte values using batch", puts, valSize)
	b, err := d.Batch(bg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < puts; i++ {
		buf := make([]byte, valSize)
		rand.Read(buf)
		err = b.Put(bg, ds.NewKey(fmt.Sprintf("/key%d", i)), buf)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}
}

// Tests from basic_tests from go-datastore

func TestBasicPutGet(t *testing.T) {
	d, done := newDS(t)
	defer done()

	k := ds.NewKey("foo")
	val := []byte("Hello Datastore!")

	err := d.Put(bg, k, val)
	if err != nil {
		t.Fatal("error putting to datastore: ", err)
	}

	have, err := d.Has(bg, k)
	if err != nil {
		t.Fatal("error calling has on key we just put: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	out, err := d.Get(bg, k)
	if err != nil {
		t.Fatal("error getting value after put: ", err)
	}

	if !bytes.Equal(out, val) {
		t.Fatal("value received on get wasnt what we expected:", out)
	}

	have, err = d.Has(bg, k)
	if err != nil {
		t.Fatal("error calling has after get: ", err)
	}

	if !have {
		t.Fatal("should have key foo, has returned false")
	}

	err = d.Delete(bg, k)
	if err != nil {
		t.Fatal("error calling delete: ", err)
	}

	have, err = d.Has(bg, k)
	if err != nil {
		t.Fatal("error calling has after delete: ", err)
	}

	if have {
		t.Fatal("should not have key foo, has returned true")
	}
}

func TestNotFounds(t *testing.T) {
	d, done := newDS(t)
	defer done()

	badk := ds.NewKey("notreal")

	val, err := d.Get(bg, badk)
	if err != ds.ErrNotFound {
		t.Fatal("expected ErrNotFound for key that doesnt exist, got: ", err)
	}

	if val != nil {
		t.Fatal("get should always return nil for not found values")
	}

	have, err := d.Has(bg, badk)
	if err != nil {
		t.Fatal("error calling has on not found key: ", err)
	}
	if have {
		t.Fatal("has returned true for key we don't have")
	}
}

func TestManyKeysAndQuery(t *testing.T) {
	d, done := newDS(t)
	defer done()

	var keys []ds.Key
	var keystrs []string
	var values [][]byte
	count := 100
	for i := 0; i < count; i++ {
		s := fmt.Sprintf("%dkey%d", i, i)
		dsk := ds.NewKey(s)
		keystrs = append(keystrs, dsk.String())
		keys = append(keys, dsk)
		buf := make([]byte, 64)
		rand.Read(buf)
		values = append(values, buf)
	}

	t.Logf("putting %d values", count)
	for i, k := range keys {
		err := d.Put(bg, k, values[i])
		if err != nil {
			t.Fatalf("error on put[%d]: %s", i, err)
		}
	}

	t.Log("getting values back")
	for i, k := range keys {
		val, err := d.Get(bg, k)
		if err != nil {
			t.Fatalf("error on get[%d]: %s", i, err)
		}

		if !bytes.Equal(val, values[i]) {
			t.Fatal("input value didnt match the one returned from Get")
		}
	}

	t.Log("querying values")
	q := dsq.Query{KeysOnly: true}
	resp, err := d.Query(bg, q)
	if err != nil {
		t.Fatal("calling query: ", err)
	}

	t.Log("aggregating query results")
	var outkeys []string
	for {
		res, ok := resp.NextSync()
		if res.Error != nil {
			t.Fatal("query result error: ", res.Error)
		}
		if !ok {
			break
		}

		outkeys = append(outkeys, res.Key)
	}

	t.Log("verifying query output")
	sort.Strings(keystrs)
	sort.Strings(outkeys)

	if len(keystrs) != len(outkeys) {
		t.Fatalf("got wrong number of keys back, %d != %d", len(keystrs), len(outkeys))
	}

	for i, s := range keystrs {
		if outkeys[i] != s {
			t.Fatalf("in key output, got %s but expected %s", outkeys[i], s)
		}
	}

	t.Log("deleting all keys")
	for _, k := range keys {
		if err := d.Delete(bg, k); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGC(t *testing.T) {
	d, done := newDS(t)
	defer done()

	count := 10000

	b, err := d.Batch(bg)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("putting %d values", count)
	for i := 0; i < count; i++ {
		buf := make([]byte, 6400)
		rand.Read(buf)
		err = b.Put(bg, ds.NewKey(fmt.Sprintf("/key%d", i)), buf)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}

	b, err = d.Batch(bg)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("deleting %d values", count)
	for i := 0; i < count; i++ {
		err := b.Delete(bg, ds.NewKey(fmt.Sprintf("/key%d", i)))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}

	if err := d.CollectGarbage(bg); err != nil {
		t.Fatal(err)
	}
}

// TestDiskUsage verifies we fetch some badger size correctly.
// Because the Size metric is only updated every minute in badger and
// this interval is not configurable, we re-open the database
// (the size is always calculated on Open) to make things quick.
func TestDiskUsage(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "testing_badger_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	d, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err != nil {
		t.Fatal(err)
	}
	addTestCases(t, d, testcases)
	d.Close()

	d, err = NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := d.DiskUsage(bg)
	if s == 0 {
		t.Error("expected some size")
	}
	d.Close()
}

func TestTxnDiscard(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "testing_badger_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	d, err := NewDatastore(path, nil)
	defer os.RemoveAll(path)
	if err != nil {
		t.Fatal(err)
	}

	txn, err := d.NewTransaction(bg, false)
	if err != nil {
		t.Fatal(err)
	}
	key := ds.NewKey("/test/thing")
	if err := txn.Put(bg, key, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	txn.Discard(bg)
	has, err := d.Has(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	if has {
		t.Fatal("key written in aborted transaction still exists")
	}

	d.Close()
}

func TestTxnCommit(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "testing_badger_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	d, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	txn, err := d.NewTransaction(bg, false)
	if err != nil {
		t.Fatal(err)
	}
	key := ds.NewKey("/test/thing")
	if err := txn.Put(bg, key, []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	err = txn.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}
	has, err := d.Has(bg, key)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatal("key written in committed transaction does not exist")
	}

	d.Close()
}

func TestTxnBatch(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "testing_badger_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	d, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	txn, err := d.NewTransaction(bg, false)
	if err != nil {
		t.Fatal(err)
	}
	data := make(map[ds.Key][]byte)
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("/test/%d", i))
		bytes := make([]byte, 16)
		_, err := rand.Read(bytes)
		if err != nil {
			t.Fatal(err)
		}
		data[key] = bytes

		err = txn.Put(bg, key, bytes)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = txn.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}

	for key, bytes := range data {
		retrieved, err := d.Get(bg, key)
		if err != nil {
			t.Fatal(err)
		}
		if len(retrieved) != len(bytes) {
			t.Fatal("bytes stored different length from bytes generated")
		}
		for i, b := range retrieved {
			if bytes[i] != b {
				t.Fatal("bytes stored different content from bytes generated")
			}
		}
	}

	d.Close()
}

func TestTTL(t *testing.T) {
	path, err := os.MkdirTemp(os.TempDir(), "testing_badger_")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)

	d, err := NewDatastore(path, nil)
	if err != nil {
		t.Fatal(err)
	}

	txn, err := d.NewTransaction(bg, false)
	if err != nil {
		t.Fatal(err)
	}

	data := make(map[ds.Key][]byte)
	for i := 0; i < 10; i++ {
		key := ds.NewKey(fmt.Sprintf("/test/%d", i))
		bytes := make([]byte, 16)
		_, err := rand.Read(bytes)
		if err != nil {
			t.Fatal(err)
		}
		data[key] = bytes
	}

	// write data
	for key, bytes := range data {
		err = txn.(ds.TTL).PutWithTTL(bg, key, bytes, time.Second)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = txn.Commit(bg)
	if err != nil {
		t.Fatal(err)
	}

	txn, err = d.NewTransaction(bg, true)
	if err != nil {
		t.Fatal(err)
	}
	for key := range data {
		_, err := txn.Get(bg, key)
		if err != nil {
			t.Fatal(err)
		}
	}
	txn.Discard(bg)

	time.Sleep(time.Second)

	for key := range data {
		has, err := d.Has(bg, key)
		if err != nil {
			t.Fatal(err)
		}
		if has {
			t.Fatal("record with ttl did not expire")
		}
	}

	d.Close()
}

func TestExpirations(t *testing.T) {
	var err error

	d, done := newDS(t)
	defer done()

	txn, err := d.NewTransaction(bg, false)
	if err != nil {
		t.Fatal(err)
	}
	ttltxn := txn.(ds.TTL)
	defer txn.Discard(bg)

	key := ds.NewKey("/abc/def")
	val := make([]byte, 32)
	if n, err := rand.Read(val); n != 32 || err != nil {
		t.Fatal("source of randomness failed")
	}

	ttl := time.Hour
	now := time.Now()
	tgt := now.Add(ttl)

	if err = ttltxn.PutWithTTL(bg, key, val, ttl); err != nil {
		t.Fatalf("adding with ttl failed: %v", err)
	}

	if err = txn.Commit(bg); err != nil {
		t.Fatalf("commiting transaction failed: %v", err)
	}

	// Second transaction to retrieve expirations.
	txn, err = d.NewTransaction(bg, true)
	if err != nil {
		t.Fatal(err)
	}
	ttltxn = txn.(ds.TTL)
	defer txn.Discard(bg)

	// GetExpiration returns expected value.
	var dsExp time.Time
	if dsExp, err = ttltxn.GetExpiration(bg, key); err != nil {
		t.Fatalf("getting expiration failed: %v", err)
	} else if tgt.Sub(dsExp) >= 5*time.Second {
		t.Fatal("expiration returned by datastore not within the expected range (tolerance: 5 seconds)")
	} else if tgt.Sub(dsExp) < 0 {
		t.Fatal("expiration returned by datastore was earlier than expected")
	}

	// Iterator returns expected value.
	q := dsq.Query{
		ReturnExpirations: true,
		KeysOnly:          true,
	}
	var ress dsq.Results
	if ress, err = txn.Query(bg, q); err != nil {
		t.Fatalf("querying datastore failed: %v", err)
	}

	defer ress.Close()
	if res, ok := ress.NextSync(); !ok {
		t.Fatal("expected 1 result in iterator")
	} else if res.Expiration != dsExp {
		t.Fatalf("expiration returned from iterator differs from GetExpiration, expected: %v, actual: %v", dsExp, res.Expiration)
	}

	if _, ok := ress.NextSync(); ok {
		t.Fatal("expected no more results in iterator")
	}

	// Datastore->GetExpiration()
	if exp, err := d.GetExpiration(bg, key); err != nil {
		t.Fatalf("querying datastore failed: %v", err)
	} else if exp != dsExp {
		t.Fatalf("expiration returned from DB differs from that returned by txn, expected: %v, actual: %v", dsExp, exp)
	}

	if _, err := d.GetExpiration(bg, ds.NewKey("/foo/bar")); err != ds.ErrNotFound {
		t.Fatalf("wrong error type: %v", err)
	}
}

func TestSuite(t *testing.T) {
	d, done := newDS(t)
	defer done()

	dstest.SubtestAll(t, d)
}
