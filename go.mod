module github.com/ipfs/go-ds-badger

require (
	github.com/dgraph-io/badger v1.6.0
	github.com/ipfs/go-datastore v0.1.1
	github.com/ipfs/go-log v0.0.1
	github.com/jbenet/goprocess v0.0.0-20160826012719-b497e2f366b8
)

replace github.com/ipfs/go-datastore => github.com/MichaelMure/go-datastore v0.1.1-0.20191122134937-68a77964d1eb

go 1.12
