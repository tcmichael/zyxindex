package main

import (
	"bytes"
	rand "crypto/rand"
	"encoding/binary"
	"log"
	rand2 "math/rand"
	"os"
	"tcmichael/zyxindex"
	"time"
)

func main() {
	rand2.Seed(time.Now().Unix())
	filepath := "test/data"
	os.RemoveAll("test")
	os.Mkdir("test", 0755)
	dataFile, err := os.Create(filepath)
	if err != nil {
		return
	}
	type doc struct {
		k, v []byte
	}

	// build test data
	data := make([]doc, 100)
	for i := 0; i < 100; i++ {
		log.Print(i)
		keySize := 1 + rand2.Intn(1<<10-1)
		data[i].k = make([]byte, keySize)
		rand.Read(data[i].k)
		valueSize := 1 + rand2.Intn(1<<20-1)
		data[i].v = make([]byte, valueSize)
		rand.Read(data[i].v)

		binary.Write(dataFile, binary.LittleEndian, uint64(keySize))
		dataFile.Write([]byte(data[i].k))
		binary.Write(dataFile, binary.LittleEndian, uint64(valueSize))
		dataFile.Write([]byte(data[i].v))
	}
	dataFile.Close()

	// test build indexes
	db, err := zyxindex.Open(filepath)
	if err != nil {
		log.Print(err)
		return
	}
	seq := rand2.Perm(100)
	for i := 0; i < 100; i++ {
		d := data[seq[i]]
		value, _ := db.Get(d.k)
		if !bytes.Equal(value, d.v) {
			log.Print(d.k, d.v, value)
			panic("failed")
		}
	}
	db.Close()

	// test read from manifest
	db, err = zyxindex.Open(filepath)
	if err != nil {
		log.Print(err)
		return
	}
	seq = rand2.Perm(100)
	for i := 0; i < 100; i++ {
		d := data[seq[i]]
		value, _ := db.Get(d.k)
		if !bytes.Equal(value, d.v) {
			log.Print(d.k, d.v, value)
			panic("failed")
		}
	}
	log.Print("test success")
}
