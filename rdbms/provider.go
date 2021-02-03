package rdbms

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
)

type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) Conn
}

type ClientProvider interface {
	CurrentClient(ctx context.Context) Client
}

// get db connection from field
type DefaultConnectionProvider struct {
	db Conn
}

func NewDefaultConnectionProvider(db Conn) *DefaultConnectionProvider {
	return &DefaultConnectionProvider{
		db: db,
	}
}

func (p *DefaultConnectionProvider) CurrentConnection(_ context.Context) Conn {
	return p.db
}

type ShardKeyProvider func(ctx context.Context) string

// get db by hash slot
type ShardConnectionProvider struct {
	db               []Conn
	hashSlot         []uint32
	shardKeyProvider ShardKeyProvider
	maxSlot          uint32
}

func NewShardConnectionProvider(db []Conn, maxSlot uint32, shardKeyProvider ShardKeyProvider) *ShardConnectionProvider {
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

func (p *ShardConnectionProvider) CurrentConnection(ctx context.Context) Conn {
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
