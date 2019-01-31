/*
* usage:
* db, err := open(path)
* if err != nil {
* 	return err;
* }
* value, err = DB.get(key)
* TODO(zhaoyanxing): values, err = DB.gets(key)
 */

package zyxindex

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
	"path/filepath"
)

// DB is the database
type DB struct {
	path   string
	shards Shards

	// data source
	// (keysize: uint64, key: bytes, valuesize: uint64, value: bytes)
	file *os.File
}

// Open opens or creates a DB for the given storage.
// The DB will be created if not exist, unless ErrorIfMissing is true.
// Also, if ErrorIfExist is true and the DB exist Open will returns
// os.ErrExist error.
//
// Open will create indexes of the database.
//
// The returned DB instance is safe for concurrent use.
// The DB must be closed after use, by calling Close method.
func Open(path string) (db *DB, err error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	db = &DB{
		path: path,
		file: file,
	}
	manifest, err := loadManifest(filepath.Dir(path))
	if err != nil {
		// if manifest not exist, build indexes
		if os.IsNotExist(err) {
			log.Println("start build indexes")
			err = db.preLoad()
		}
		return
	}
	db.shards, err = LoadFromManifest(filepath.Dir(path), manifest)
	return
}

const sizeOfuint64 = 8

func (db *DB) preLoad() (err error) {
	builder, err := NewShardsBuilder(filepath.Dir(db.path))
	if err != nil {
		return
	}

	key := make([]byte, 1<<10) // 1k
	var offset uint64
	file := db.file

	//iterator:  (keysize: uint64, key: bytes, valuesize: uint64, value: bytes)
	for {
		var keySize, valueSize uint64
		err = binary.Read(file, binary.LittleEndian, &keySize)
		if err != nil {
			break
		}
		_, err = io.ReadFull(file, key[:int(keySize)])
		if err != nil {
			break
		}
		err = binary.Read(file, binary.LittleEndian, &valueSize)
		if err != nil {
			break
		}
		_, err = file.Seek(int64(valueSize), 1)
		if err != nil {
			break
		}
		hash64 := fnvHash64(key[:keySize])
		builder.Put(hash64, offset)
		offset += sizeOfuint64 + keySize + sizeOfuint64 + valueSize
	}
	if err != io.EOF {
		return
	}
	db.shards, err = builder.BuildShards()
	if err != nil {
		return
	}
	return buildManifest(filepath.Dir(db.path))
}

func buildManifest(dir string) error {
	mainfest := &Manifest{
		Version:  version,
		ShardNum: 1 << shardMusk,
	}
	return CreateManifestFile(dir, mainfest)
}

// Close closes the DB.
//
// It is valid to call Close multiple times. Other methods should not be
// called after the DB has been closed.
func (db *DB) Close() error {
	for i := 0; i < 1<<shardMusk; i++ {
		err := db.shards[i].Close()
		if err != nil {
			return err
		}
	}
	return db.file.Close()
}

// Get gets the value for the given key. It returns ErrNotFound if the
// DB does not contains the key.
//
// The returned slice is its own copy, it is safe to modify the contents
// of the returned slice.
// It is safe to modify the contents of the argument after Get returns.
//
// @return err, os.ErrNotExist if the key is not found.
func (db *DB) Get(key []byte) (value []byte, err error) {
	hash64 := fnvHash64(key)
	offset, err := db.shards.Get(hash64)
	if err != nil {
		return
	}
	uint64Buffer := make([]byte, 8)
	_, err = db.file.ReadAt(uint64Buffer, int64(offset))
	if err != nil {
		return
	}
	keySize := binary.LittleEndian.Uint64(uint64Buffer)
	if int(keySize) != len(key) {
		err = os.ErrNotExist
		return
	}
	keyBuffer := make([]byte, len(key))
	_, err = db.file.ReadAt(keyBuffer, int64(offset+8))
	if err != nil {
		return
	}
	if !bytes.Equal(keyBuffer, key) {
		err = os.ErrNotExist
		return
	}

	_, err = db.file.ReadAt(uint64Buffer, int64(offset+8+keySize))
	if err != nil {
		return
	}
	valueSize := binary.LittleEndian.Uint64(uint64Buffer)
	valueBuffer := make([]byte, int(valueSize))
	_, err = db.file.ReadAt(valueBuffer, int64(offset+16+keySize))
	if err != nil {
		return
	}
	value = valueBuffer[:int(valueSize)]
	return
}
