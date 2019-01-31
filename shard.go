package zyxindex

import "os"

/*
	divided hash64 into diffent shard.
*/

const shardMusk = 8

// parse key from bytes to uint64
func littleEndianKey(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48
}

// parse key from uint64 to bytes
func littleEndianPutKey(b []byte, k uint64) {
	_ = b[6] // early bounds check to guarantee safety of writes below
	b[0] = byte(k)
	b[1] = byte(k >> 8)
	b[2] = byte(k >> 16)
	b[3] = byte(k >> 24)
	b[4] = byte(k >> 32)
	b[5] = byte(k >> 40)
	b[6] = byte(k >> 48)
}

//parse offset from bytes to uint64
func littleEndianOffset(b []byte) uint64 {
	return uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32
}

// parse offset from uint64 to bytes
func littleEndianPutOffset(b []byte, v uint64) {
	_ = b[4] // early bounds check to guarantee safety of writes below
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
	b[4] = byte(v >> 32)
}

type HashTabler interface {
	Get(k []byte) (v []byte, err error)
	Close() error
}

type Shards [1 << shardMusk]HashTabler

// load shards from manifest.
// manifest must not be null
func LoadFromManifest(dir string, manifest *Manifest) (shards Shards, err error) {
	if manifest.Version != version {
		panic("unknown version")
	}
	if manifest.ShardNum != 1<<shardMusk {
		panic("shardnum not equal")
	}
	for i := 0; i < 1<<shardMusk; i++ {
		f, e := os.Open(HashTablePath(dir, i))
		if err != nil {
			err = e
			return
		}
		hashtable, e := OpenHashTable(f)
		if err != nil {
			err = e
			return
		}
		shards[i] = hashtable
	}
	return
}

//calcShard calculates shardId and key in this shard.
func calcShard(hash64 uint64) (shardId int, key []byte) {
	key = make([]byte, kLen)
	littleEndianPutKey(key, hash64)
	shardId = int(hash64 >> (64 - shardMusk))
	return
}

// Get gets the offset of the key hashd
func (shards *Shards) Get(hash64 uint64) (offset uint64, err error) {
	shardId, key := calcShard(hash64)
	v, err := shards[shardId].Get(key)
	if err != nil {
		return
	}
	offset = littleEndianOffset(v)
	return
}
