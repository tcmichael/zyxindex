package zyxindex

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
)

type Source struct {
	keys  []int
	index int
}

func (s *Source) readNext(k, v []byte) (err error) {
	key := s.keys[s.index]
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(key))
	copy(k, b[:kLen])
	copy(v, b[:vLen])
	s.index++
	return nil
}

func TestHashTable(t *testing.T) {
	source := &Source{
		keys: []int{0, 1, 2, 3, 4, 5, 6, 33, 31, 63},
	}
	buffer := new(bytes.Buffer)
	N := len(source.keys)
	err := Generate(source, N, buffer)
	if err != nil {
		t.Fatal(err)
	}

	h, err := OpenHashTable(bytes.NewReader(buffer.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]int, len(source.keys))
	copy(keys, source.keys)
	keys = append(keys, 7)
	for _, key := range keys {
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(key))
		k := b[:kLen]

		if key == 7 {
			_, err := h.Get(k)
			if err != os.ErrNotExist {
				t.Errorf("%v: data shoule not exist", key)
			}
		} else {
			v, err := h.Get(k)
			if err != nil {
				t.Error(err)
			}
			expected := b[:vLen]
			if !bytes.Equal(v[:], expected) {
				t.Errorf("%v: data is not equal", key)
			}
		}
	}
}
