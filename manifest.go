package zyxindex

import (
	"encoding/json"
	"os"
	"path/filepath"
)

/*
a manifest is a json file, which descriptes the indexes of database;
it looks like below:
{
	"version": 1,
	"shard_num": 256
}
*/

const version = 1

type Manifest struct {
	Version  int `json:"version"`
	ShardNum int `json:"shard_num"`
}

func ManifestPath(dir string) string {
	return filepath.Join(dir, "manifest")
}

func CreateManifestFile(dir string, manifest *Manifest) (err error) {
	file, err := os.Create(ManifestPath(dir))
	if err != nil {
		return
	}
	enc := json.NewEncoder(file)
	err = enc.Encode(manifest)
	return
}

func loadManifest(dir string) (manifest *Manifest, err error) {
	manifest = new(Manifest)
	manifestPath := ManifestPath(dir)
	file, err := os.Open(manifestPath)
	if err != nil {
		return
	}
	dec := json.NewDecoder(file)
	err = dec.Decode(manifest)
	return
}
