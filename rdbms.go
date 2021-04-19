package gotx

import (
	"context"
	"fmt"

	"database/sql"
)

type contextRdbmsTransactionKey string

func rdbmsTransactionKey(shardKey string) contextRdbmsTransactionKey {
	return contextRdbmsTransactionKey(fmt.Sprintf("current_%s_tx", shardKey))
}

type rdbmsShardKeyProvider func(ctx context.Context) string

var defaultRdbmsShardKeyProvider = func(ctx context.Context) string {
	return "rdbms"
}

// --------------------------
// Connection
// --------------------------
type RdbmsConn interface {
	RdbmsClient
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type RdbmsConnectionProvider interface {
	CurrentConnection(ctx context.Context) RdbmsConn
}

// get db connection from field
type DefaultRdbmsConnectionProvider struct {
	db RdbmsConn
}

func NewDefaultRdbmsConnectionProvider(db RdbmsConn) RdbmsConnectionProvider {
	return &DefaultRdbmsConnectionProvider{
		db: db,
	}
}

func (p *DefaultRdbmsConnectionProvider) CurrentConnection(_ context.Context) RdbmsConn {
	return p.db
}

// get db by hash slot
type ShardingRdbmsConnectionProvider struct {
	db               []*sql.DB
	hashSlot         []uint32
	shardKeyProvider rdbmsShardKeyProvider
	maxSlot          uint32
}

func NewShardingRdbmsConnectionProvider(db []*sql.DB, maxSlot uint32, shardKeyProvider rdbmsShardKeyProvider) RdbmsConnectionProvider {
	return &ShardingRdbmsConnectionProvider{
		db:               db,
		shardKeyProvider: shardKeyProvider,
		hashSlot:         GetHashSlotRange(len(db), maxSlot),
		maxSlot:          maxSlot,
	}
}

func (p *ShardingRdbmsConnectionProvider) CurrentConnection(ctx context.Context) RdbmsConn {
	shardKey := p.shardKeyProvider(ctx)
	index := GetIndexByHash(p.hashSlot, []byte(shardKey), p.maxSlot)
	return p.db[index]
}

// --------------------------
// Client
// --------------------------
type RdbmsClient interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type RdbmsClientProvider interface {
	CurrentClient(ctx context.Context) RdbmsClient
}

type DefaultRdbmsClientProvider struct {
	shardKeyProvider   func(ctx context.Context) string
	connectionProvider RdbmsConnectionProvider
}

func NewDefaultRdbmsClientProvider(connectionProvider RdbmsConnectionProvider) RdbmsClientProvider {
	return NewShardingRdbmsClientProvider(connectionProvider, defaultRdbmsShardKeyProvider)
}

func NewShardingRdbmsClientProvider(connectionProvider RdbmsConnectionProvider, shardKeyProvider rdbmsShardKeyProvider) RdbmsClientProvider {
	return &DefaultRdbmsClientProvider{
		shardKeyProvider:   shardKeyProvider,
		connectionProvider: connectionProvider,
	}
}

func (p *DefaultRdbmsClientProvider) CurrentClient(ctx context.Context) RdbmsClient {
	key := rdbmsTransactionKey(p.shardKeyProvider(ctx))
	transaction := ctx.Value(key)
	if transaction == nil {
		return p.connectionProvider.CurrentConnection(ctx)
	}
	return transaction.(RdbmsClient)
}

// --------------------------
// Transactor
// --------------------------
type RdbmsTransactor struct {
	shardKeyProvider   func(ctx context.Context) string
	connectionProvider RdbmsConnectionProvider
}

func NewRdbmsTransactor(connectionProvider RdbmsConnectionProvider) Transactor {
	return NewShardingRdbmsTransactor(connectionProvider, defaultRdbmsShardKeyProvider)
}

func NewShardingRdbmsTransactor(connectionProvider RdbmsConnectionProvider, shardKeyProvider rdbmsShardKeyProvider) Transactor {
	return &RdbmsTransactor{
		shardKeyProvider:   shardKeyProvider,
		connectionProvider: connectionProvider,
	}
}

func (t *RdbmsTransactor) Required(ctx context.Context, fn DoInTransaction, options ...Option) error {
	if ctx.Value(rdbmsTransactionKey(t.shardKeyProvider(ctx))) != nil {
		return fn(ctx)
	}
	return t.RequiresNew(ctx, fn, options...)
}

func (t *RdbmsTransactor) RequiresNew(ctx context.Context, fn DoInTransaction, options ...Option) (err error) {
	config := NewDefaultConfig()
	for _, opt := range options {
		opt.Apply(&config)
	}
	db := t.connectionProvider.CurrentConnection(ctx)
	tx, e := db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: config.ReadOnly,
	})
	if e != nil {
		err = e
		return
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		} else {
			if config.RollbackOnly {
				_ = tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}
	}()
	err = fn(context.WithValue(ctx, rdbmsTransactionKey(t.shardKeyProvider(ctx)), tx))
	return
}
