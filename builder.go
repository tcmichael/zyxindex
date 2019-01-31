package zyxindex

/*
	build shards.
*/

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

const (
	tmp       = "tmp"
	hashTable = "hashTable"
)

// build indexes as shards
type ShardsBuilder [1 << shardMusk]*ShardBuilder

// NewShardsBuilder creates a shards builder
// @param dir [in], which dictionary for building shards
// @return builder
// @return err
func NewShardsBuilder(dir string) (builder *ShardsBuilder, err error) {
	builder = new(ShardsBuilder)
	for i := 0; i < 1<<shardMusk; i++ {
		tmpFile, e := os.Create(filepath.Join(dir, tmp+strconv.Itoa(i)))
		if e != nil {
			err = e
			return
		}
		// {$dir}/hashTable/{$shardId}
		hashTableFile, e := os.Create(HashTablePath(dir, i))
		if e != nil {
			err = e
			return
		}
		builder[i] = NewBuilder(tmpFile, hashTableFile)
	}
	return
}

func HashTablePath(dir string, i int) string {
	return filepath.Join(dir, hashTable+strconv.Itoa(i))
}

var vBuf = make([]byte, vLen)

// Put puts hash64 and offset into shardsbuilder
// @param hash64, the key in shards
// @param offset, the value in shards
// @return err, error
func (b *ShardsBuilder) Put(hash64 uint64, offset uint64) (err error) {
	shardId, key := calcShard(hash64)
	littleEndianPutOffset(vBuf, offset)
	return b[shardId].Put(key, vBuf)
}

const cpuCores = 8

// BuildShards builds shards and Finshes building.
// BuildShards use cpuCores concurrent
// @return shards
// @return err
func (b *ShardsBuilder) BuildShards() (shards Shards, err error) {
	task := make(chan int, 1<<shardMusk)
	for i := 0; i < 1<<shardMusk; i++ {
		task <- i
	}
	close(task)
	var wg sync.WaitGroup
	for i := 0; i < cpuCores; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx, ok := <-task
				if !ok {
					break
				}
				err = b[idx].Finish()
				if err != nil {
					return
				}
				if file, ok := b[idx].hashTableWriter.(*os.File); ok {
					hashTable, e := OpenHashTable(file)
					if e != nil {
						return
					}
					shards[idx] = hashTable
				}
			}
		}()
	}
	wg.Wait()
	return
}

// build hashTable, also one shard.
type ShardBuilder struct {
	// the template file
	tmpFile *os.File

	// use buffer to write the tmpFile for reducing random writes
	bufioWriter *bufio.Writer

	//hashtable writer
	hashTableWriter io.Writer

	// key count in hashtable
	keycount int
}

// 8M * 256 = 2G < 4G
const bufioSize = 8 << 20

// NewBuilder creates a shard builder
// @param tmpFile[in], template file
// @param hashTableWriter, the writer of hash table
// @return builder
func NewBuilder(tmpFile *os.File, hashTableWriter io.Writer) *ShardBuilder {
	return &ShardBuilder{
		tmpFile:         tmpFile,
		hashTableWriter: hashTableWriter,
		bufioWriter:     bufio.NewWriterSize(tmpFile, bufioSize),
	}
}

// Put puts k and v into builder
// @param k, the key in hash table
// @param v, the value in hash table
// @return err, error
func (b *ShardBuilder) Put(k, v []byte) (err error) {
	_, err = b.bufioWriter.Write(k[:kLen])
	if err != nil {
		return
	}
	_, err = b.bufioWriter.Write(v[:vLen])
	if err != nil {
		return
	}
	b.keycount++
	return
}

// Finish Finshes building and closes the temp file.
// @return err
func (b *ShardBuilder) Finish() (err error) {
	// TODO(tcmichael): do not flush and reuse the buffer.
	err = b.bufioWriter.Flush()
	if err != nil {
		return
	}
	_, err = b.tmpFile.Seek(0, 0)
	if err != nil {
		return
	}
	err = Generate(b, b.keycount, b.hashTableWriter)
	if err != nil {
		return
	}
	b.tmpFile.Close()
	err = os.Remove(b.tmpFile.Name())
	return
}

// readNext implements kvReader for generating a hash table
// @param k[in], the key in hash table
// @param v[in], the value in hash table
// @return err, error.
func (b *ShardBuilder) readNext(k, v []byte) (err error) {
	_, err = io.ReadFull(b.tmpFile, k)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(b.tmpFile, v)
	if err != nil {
		return err
	}
	return
}
