package zyxindex

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"
)

func TestDB(t *testing.T) {
	defer os.RemoveAll(testDir)
	os.Mkdir(testDir, 0755)
	dataPath := testDir + "/data"
	file, err := os.Create(dataPath)
	if err != nil {
		t.Error("create file failed", err)
	}
	data := map[string]string{
		"123":        "456",
		"helloworld": "!",
		"uesrname":   "password",
	}
	for k, v := range data {
		binary.Write(file, binary.LittleEndian, uint64(len(k)))
		file.Write([]byte(k))
		binary.Write(file, binary.LittleEndian, uint64(len(v)))
		file.Write([]byte(v))
	}
	db, err := Open(dataPath)
	if err != nil {
		return
	}
	for k, v := range data {
		value, err := db.Get([]byte(k))
		if err != nil {
			t.Error("get failed", err)
		}
		if !bytes.Equal(value, []byte(v)) {
			t.Error("should equal", value, v)
		}
	}
}
