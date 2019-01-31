package zyxindex

import "hash/fnv"

// fnv hash 64
func fnvHash64(key []byte) uint64 {
	hash := fnv.New64()
	hash.Write(key)
	return hash.Sum64()
}
