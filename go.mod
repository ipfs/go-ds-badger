module github.com/ipfs/go-ds-badger

replace github.com/ipfs/go-datastore v0.4.4 => github.com/textileio/go-datastore v0.4.5-0.20200728205504-ffeb3591b248

require (
	github.com/dgraph-io/badger v1.6.1
	github.com/ipfs/go-datastore v0.4.4
	github.com/ipfs/go-log/v2 v2.0.5
	github.com/jbenet/goprocess v0.1.4
)

go 1.13
