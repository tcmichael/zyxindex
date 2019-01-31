package zyxindex

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestLittleEndianKey(t *testing.T) {
	hash64 := uint64(28572051027328338)
	b := make([]byte, kLen)
	littleEndianPutKey(b, hash64)
	b2 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b2, hash64)
	if !bytes.Equal(b, b2[:kLen]) {
		t.Errorf("%v should equal expected(%v)", b, b2[:kLen])
	}
	expected := littleEndianKey(b)
	if hash64<<8>>8 != expected {
		t.Errorf("%v should equal expected(%v)", hash64<<8>>8, expected)
	}
}

func TestLittleEndianOffset(t *testing.T) {
	offset := uint64(28572051027328338)
	b := make([]byte, vLen)
	littleEndianPutOffset(b, offset)
	b2 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b2, offset)
	if !bytes.Equal(b, b2[:vLen]) {
		t.Errorf("%v should equal expected(%v)", b, b2[:kLen])
	}
	expected := littleEndianOffset(b)
	if offset<<24>>24 != expected {
		t.Errorf("%v should equal expected(%v)", offset<<8>>8, expected)
	}
}

func TestCalcShard(t *testing.T) {
	hash64 := uint64(28572051027328338)
	shardId, key := calcShard(hash64)
	b := make([]byte, kLen)
	littleEndianPutKey(b, hash64)
	if !bytes.Equal(b, key) {
		t.Errorf("%v should equal expected(%v)", b, key)
	}
	b2 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b2, hash64)
	if int(b2[7]) != shardId {
		t.Errorf("%v should equal expected(%v)", int(b2[7]), shardId)
	}
}

type MapHashTable struct {
	Map map[uint64][]byte
}

func (m *MapHashTable) Get(k []byte) (v []byte, err error) {
	key := littleEndianKey(k)
	v = m.Map[key]
	return
}

func (m *MapHashTable) Close() error {
	return nil
}

func TestShards_Get(t *testing.T) {
	var shards Shards
	b1 := make([]byte, 7)
	littleEndianPutOffset(b1, 0)
	b2 := make([]byte, 7)
	littleEndianPutOffset(b2, 100)
	b3 := make([]byte, 7)
	littleEndianPutOffset(b3, 200)

	shards[0] = &MapHashTable{
		Map: map[uint64][]byte{
			0:   b1,
			256: b2,
		},
	}
	shards[127] = &MapHashTable{
		Map: map[uint64][]byte{
			101: b3,
		},
	}
	if v, _ := shards.Get(0); v != 0 {
		t.Errorf("%v should equal expected(%v)", v, 0)
	}
	if v, _ := shards.Get(256); v != 100 {
		t.Errorf("%v should equal expected(%v)", v, 100)
	}
	if v, _ := shards.Get(127<<56 | 101); v != 200 {
		t.Errorf("%v should equal expected(%v)", v, 200)
	}
}
