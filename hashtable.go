package zyxindex

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

/*
 Hashtable is the implementation of HashTabler,
 Hashtable is closed chain hash table: the key is fixed 7 bytes and the value is fixed 5 bytes.

 Generate a hashTable from a shardfile:
  hashTable, err := Generate(source, keycount, writer);
 Open a hashTable:
  hashTable, err := HashTable.Open(reader);
 Get from hashTable:
  value, err := hashTable.Get(key);

 The HashTable structure:

		+--------------+--------------+--------------+--------------+--------------+ --------------+
		|  slot count  |     slot 1   |  slot 2      |    ......    |    slot n    | TODO: checksum|
		+--------------+--------------+--------------+--------------+--------------+ --------------+

 The slot count is 8 bytes. belows
 The slot structure:

		+--------------+--------------+
		|    key(7)    |  value(5)    |
		+--------------+--------------+


 TODO(tcmichael): Should slots are 4k alignment?
*/

const (
	kLen = 7
	vLen = 5
)

// NotExistSlot when a slot has nothing.
var NotExistSlot = make([]byte, kLen+vLen)

func init() {
	for i := kLen; i < kLen+vLen; i++ {
		NotExistSlot[i] = 0xf
	}
}

type HashTable struct {
	slotCount uint64
	r         io.ReaderAt
}

type KV struct {
	k []byte
	v []byte
}

type kvReader interface {
	readNext(k, v []byte) (err error)
}

// next slot
func nextSlot(slot uint64, slotCount uint64) uint64 {
	slot++
	if slot >= slotCount {
		slot = 0
	}
	return slot
}

// Generate generates a HashTable of a shard.
// @param source [in], a shard kv reader.
// @param keycount [in], key count of the reader.
// @param w [out], implements the file writer of the HashTable.
// @return err, nil means success, other means fail.
func Generate(source kvReader, keycount int, w io.Writer) (err error) {
	slotCount := uint64(keycount * 3)
	musk := uint64(0)
	for ; 1<<musk < slotCount; musk++ {
	}
	slotCount = 1 << musk

	slots := make([][]byte, slotCount)
	hit := make([]bool, slotCount)

	for i := 0; i < keycount; i++ {
		slotData := make([]byte, kLen+vLen)
		k, v := slotData[:kLen], slotData[kLen:]
		err = source.readNext(k, v)
		if err != nil {
			return
		}

		slot := littleEndianKey(k) & (slotCount - 1)
		for j := uint64(0); j < slotCount; j++ {
			if !hit[slot] {
				hit[slot] = true
				slots[slot] = slotData
				break
			}
			slot = nextSlot(slot, slotCount)
		}
	}

	// flush
	slotCountB := make([]byte, 8)
	binary.LittleEndian.PutUint64(slotCountB, slotCount)
	_, err = w.Write(slotCountB)
	if err != nil {
		return
	}

	for i, slot := range slots {
		if hit[i] {
			_, err = w.Write(slot)
		} else {
			_, err = w.Write(NotExistSlot)
		}
		if err != nil {
			return
		}
	}
	return
}

// Open opens a hash table from a file, which implements the io.ReaderAt
func OpenHashTable(r io.ReaderAt) (h *HashTable, err error) {
	slotCountB := make([]byte, 8)
	_, err = r.ReadAt(slotCountB, 0)
	if err != nil {
		return
	}
	slotCount := binary.LittleEndian.Uint64(slotCountB)
	return &HashTable{slotCount: slotCount, r: r}, nil
}

// Get gets value of the key from hash table
// @param k, the key
// @return v, the value
// @return err, nil when the key exists, os.ErrNotExist when the key miss. or return other
func (h *HashTable) Get(k []byte) (v []byte, err error) {
	slot := littleEndianKey(k) & (h.slotCount - 1)
	b := make([]byte, kLen+vLen)
	for i := uint64(0); i < h.slotCount; i++ {
		off := int64(8 + slot*(kLen+vLen))
		_, err = h.r.ReadAt(b, off)
		if err != nil {
			return
		}
		if bytes.Equal(b, NotExistSlot) {
			return nil, os.ErrNotExist
		}
		if bytes.Equal(b[:kLen], k) {
			return b[kLen:], nil
		}
		slot = nextSlot(slot, h.slotCount)
	}
	return nil, os.ErrNotExist
}

func (h *HashTable) Close() error {
	if closer, ok := h.r.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
