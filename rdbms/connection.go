package rdbms

import (
	"context"
	"database/sql"

	"github.com/knocknote/gotx"
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
	return &ShardingConnectionProvider{
		db:               db,
		shardKeyProvider: shardKeyProvider,
		hashSlot:         gotx.GetHashSlotRange(len(db), maxSlot),
		maxSlot:          maxSlot,
	}
}

func (p *ShardingConnectionProvider) CurrentConnection(ctx context.Context) Conn {
	shardKey := p.shardKeyProvider(ctx)
	index := gotx.GetIndexByHash(p.hashSlot, []byte(shardKey), p.maxSlot)
	return p.db[index]
}
