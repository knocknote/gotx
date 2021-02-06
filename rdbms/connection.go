package rdbms

import (
	"context"
	"database/sql"
	"hash/crc32"
)

type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) Conn
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
type ShardingConnectionProvider struct {
	db               []*sql.DB
	hashSlot         []uint32
	shardKeyProvider ShardKeyProvider
	maxSlot          uint32
}

func NewShardingConnectionProvider(db []*sql.DB, maxSlot uint32, shardKeyProvider ShardKeyProvider) *ShardingConnectionProvider {
	average := maxSlot / uint32(len(db))
	maxValuePerShard := make([]uint32, len(db))
	for i := range maxValuePerShard {
		maxValuePerShard[i] = average * uint32(i+1)
	}
	return &ShardingConnectionProvider{
		db:               db,
		shardKeyProvider: shardKeyProvider,
		hashSlot:         maxValuePerShard,
		maxSlot:          maxSlot,
	}
}

func (p *ShardingConnectionProvider) CurrentConnection(ctx context.Context) Conn {
	shardKey := p.shardKeyProvider(ctx)
	slot := crc32.ChecksumIEEE([]byte(shardKey)) % p.maxSlot
	for i, v := range p.hashSlot {
		if slot < v {
			return p.db[i]
		}
	}
	return nil
}
