package redis

import (
	"context"
	"crypto/sha256"
	"encoding/binary"

	"github.com/go-redis/redis"
)

type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) *redis.Client
}

// get redis client from field
type DefaultConnectionProvider struct {
	client *redis.Client
}

func NewDefaultConnectionProvider(client *redis.Client) *DefaultConnectionProvider {
	return &DefaultConnectionProvider{
		client: client,
	}
}

func (p *DefaultConnectionProvider) CurrentConnection(_ context.Context) *redis.Client {
	return p.client
}

// get db by hash slot
type ShardConnectionProvider struct {
	db               []*redis.Client
	hashSlot         []uint32
	shardKeyProvider ShardKeyProvider
	maxSlot          uint32
}

func NewShardConnectionProvider(db []*redis.Client, maxSlot uint32, shardKeyProvider ShardKeyProvider) *ShardConnectionProvider {
	average := maxSlot / uint32(len(db))
	maxValuePerShard := make([]uint32, len(db))
	for i := range maxValuePerShard {
		maxValuePerShard[i] = average * uint32(i+1)
	}
	return &ShardConnectionProvider{
		db:               db,
		shardKeyProvider: shardKeyProvider,
		hashSlot:         maxValuePerShard,
		maxSlot:          maxSlot,
	}
}

func (p *ShardConnectionProvider) CurrentConnection(ctx context.Context) *redis.Client {
	shardKey := p.shardKeyProvider(ctx)
	hashByte := sha256.Sum256([]byte(shardKey))
	hashInt := binary.BigEndian.Uint32(hashByte[:])
	slot := hashInt % p.maxSlot
	for i, v := range p.hashSlot {
		if slot < v {
			return p.db[i]
		}
	}
	return nil
}
