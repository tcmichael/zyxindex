package zyxindex

import (
	"os"
	"testing"
)

const (
	testDir = "test"
)

func init() {
	os.RemoveAll(testDir)
}

func TestHashTableBuilder(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.Mkdir(testDir, 0755)
	tmp, err := os.Create(testDir + "/tmp")
	if err != nil {
		t.Error("create failed:", err)
	}
	hashTable, err := os.Create(testDir + "/table")
	if err != nil {
		t.Error("create failed:", err)
	}
	builder := NewBuilder(tmp, hashTable)

	v := make([]byte, 5)
	put := func(hash64 uint64, offset uint64) {
		_, k := calcShard(hash64)
		littleEndianPutOffset(v, offset)
		err := builder.Put(k, v)
		if err != nil {
			t.Error("put failed:", hash64, offset, err)
		}
	}
	put(0, 100)
	put(200, 300)
	put(400, 600)
	err = builder.Finish()
	if err != nil {
		t.Error("can't finish", err)
	}
	hashTable.Seek(0, 0)
	table, err := OpenHashTable(hashTable)
	if err != nil {
		t.Error("can't open hash Table:", err)
	}
	get := func(hash64 uint64, expected uint64) {
		_, k := calcShard(hash64)
		v, err := table.Get(k)
		if err != nil {
			t.Error("read failed:", err)
		}
		offset := littleEndianOffset(v)
		if offset != expected {
			t.Error("get not same:", offset, expected)
		}
	}
	get(0, 100)
	get(200, 300)
	get(400, 600)
}

func TestHashTableBuilders(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.Mkdir(testDir, 0755)
	builders, err := NewShardsBuilder(testDir)
	if err != nil {
		t.Error("NewHashTableBuilders failed:", err)
	}
	put := func(hash64 uint64, offset uint64) {
		err := builders.Put(hash64, offset)
		if err != nil {
			t.Error("put failed:", hash64, offset, err)
		}
	}
	put(0, 100)
	put(1>>56|200, 300)
	put(2>>56|400, 600)
	shards, err := builders.BuildShards()
	if err != nil {
		t.Error("Create fails:", err)
	}
	get := func(hash64 uint64, expected uint64) {
		offset, err := shards.Get(hash64)
		if err != nil {
			t.Error("read failed:", err)
		}
		if offset != expected {
			t.Error("get not same:", offset, expected)
		}
	}
	get(0, 100)
	get(1>>56|200, 300)
	get(2>>56|400, 600)
}
