package gotx

import (
	"hash/crc32"
)

func GetHashSlotRange(size int, maxSlot uint32) []uint32 {
	average := maxSlot / uint32(size)
	maxValuePerShard := make([]uint32, size)
	lastIndex := size - 1
	for i := range maxValuePerShard {
		if i == lastIndex {
			maxValuePerShard[i] = maxSlot
		} else {
			maxValuePerShard[i] = average * uint32(i+1)
		}
	}
	return maxValuePerShard
}

func GetIndexByHash(hashSlotRange []uint32, shardKey []byte, maxSlot uint32) int {
	slot := crc32.ChecksumIEEE(shardKey) % maxSlot
	for i, v := range hashSlotRange {
		if slot < v {
			return i
		}
	}
	return -1
}
