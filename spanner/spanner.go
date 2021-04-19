package gotx

import (
	"context"
	"errors"

	"github.com/knocknote/gotx"

	"cloud.google.com/go/spanner"
)

type contextCurrentTransactionKey string

const currentTransactionKey contextCurrentTransactionKey = "current_spanner_transaction"

// ------------------------------------
// Connection
// ------------------------------------
type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) *spanner.Client
}

type DefaultConnectionProvider struct {
	client *spanner.Client
}

func NewDefaultConnectionProvider(client *spanner.Client) ConnectionProvider {
	return &DefaultConnectionProvider{
		client: client,
	}
}

func (p *DefaultConnectionProvider) CurrentConnection(_ context.Context) *spanner.Client {
	return p.client
}

// ------------------------------------
// Client
// ------------------------------------
type Reader interface {
	Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator
	ReadUsingIndex(ctx context.Context, table, index string, keys spanner.KeySet, columns []string) (ri *spanner.RowIterator)
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
	QueryWithOptions(ctx context.Context, statement spanner.Statement, opts spanner.QueryOptions) *spanner.RowIterator
	QueryWithStats(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
}

type Client interface {
	Reader(ctx context.Context) Reader
	ApplyOrBufferWrite(context.Context, ...*spanner.Mutation) error
	Update(ctx context.Context, statement spanner.Statement) (int64, error)
	UpdateWithOption(ctx context.Context, statement spanner.Statement, options spanner.QueryOptions) (int64, error)
	BatchUpdate(ctx context.Context, statement []spanner.Statement) ([]int64, error)
	PartitionedUpdate(ctx context.Context, statement spanner.Statement) (int64, error)
	PartitionedUpdateWithOptions(ctx context.Context, statement spanner.Statement, options spanner.QueryOptions) (int64, error)
}

type DefaultTxClient struct {
	spannerClient *spanner.Client
	txRW          *spanner.ReadWriteTransaction
	txRO          *spanner.ReadOnlyTransaction
}

func NewDefaultTxClient(client *spanner.Client, rw *spanner.ReadWriteTransaction, ro *spanner.ReadOnlyTransaction) *DefaultTxClient {
	return &DefaultTxClient{
		spannerClient: client,
		txRW:          rw,
		txRO:          ro,
	}
}

func (e *DefaultTxClient) ApplyOrBufferWrite(ctx context.Context, data ...*spanner.Mutation) error {
	if e.isInReadWriteTransaction() {
		return e.txRW.BufferWrite(data)
	}
	if e.isInReadOnlyTransaction() {
		return errors.New("read only transaction doesn't support write operation")
	}
	_, err := e.spannerClient.Apply(ctx, data)
	return err
}

func (e *DefaultTxClient) Update(ctx context.Context, stmt spanner.Statement) (int64, error) {
	if e.isInReadWriteTransaction() {
		return e.txRW.Update(ctx, stmt)
	}
	return -1, errors.New("read write transaction is required to use statement")
}

func (e *DefaultTxClient) UpdateWithOption(ctx context.Context, stmt spanner.Statement, opts spanner.QueryOptions) (int64, error) {
	if e.isInReadWriteTransaction() {
		return e.txRW.UpdateWithOptions(ctx, stmt, opts)
	}
	return -1, errors.New("read write transaction is required to use statement")
}

func (e *DefaultTxClient) BatchUpdate(ctx context.Context, stmts []spanner.Statement) ([]int64, error) {
	if e.isInReadWriteTransaction() {
		return e.txRW.BatchUpdate(ctx, stmts)
	}
	return nil, errors.New("read write transaction is required to use statement")
}

func (e *DefaultTxClient) PartitionedUpdate(ctx context.Context, stmt spanner.Statement) (int64, error) {
	if e.isInReadWriteTransaction() {
		//spanner doesn't support nested transaction
		return -1, errors.New("partitioned update is unsupported in read write transaction")
	}
	return e.spannerClient.PartitionedUpdate(ctx, stmt)
}

func (e *DefaultTxClient) PartitionedUpdateWithOptions(ctx context.Context, stmt spanner.Statement, options spanner.QueryOptions) (int64, error) {
	if e.isInReadWriteTransaction() {
		//spanner doesn't support nested transaction
		return -1, errors.New("partitioned update is unsupported in read write transaction")
	}
	return e.spannerClient.PartitionedUpdateWithOptions(ctx, stmt, options)
}

func (e *DefaultTxClient) Reader(_ context.Context) Reader {
	if e.isInReadWriteTransaction() {
		return e.txRW
	}
	if e.isInReadOnlyTransaction() {
		return e.txRO
	}
	return e.spannerClient.Single()
}

func (e *DefaultTxClient) isInReadWriteTransaction() bool {
	return e.txRW != nil
}

func (e *DefaultTxClient) isInReadOnlyTransaction() bool {
	return e.txRO != nil
}

type ClientProvider interface {
	CurrentClient(ctx context.Context) Client
}

type DefaultClientProvider struct {
	connectionProvider ConnectionProvider
}

func NewDefaultClientProvider(connectionProvider ConnectionProvider) ClientProvider {
	return &DefaultClientProvider{
		connectionProvider: connectionProvider,
	}
}

func (p *DefaultClientProvider) CurrentClient(ctx context.Context) Client {
	transaction := ctx.Value(currentTransactionKey)
	if transaction == nil {
		return NewDefaultTxClient(p.connectionProvider.CurrentConnection(ctx), nil, nil)
	}
	return transaction.(Client)
}

// ------------------------------------
// Transactor
// ------------------------------------
type Transactor struct {
	provider ConnectionProvider
}

func NewTransactor(provider ConnectionProvider) gotx.Transactor {
	return &Transactor{
		provider: provider,
	}
}

func (t *Transactor) Required(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) error {
	if ctx.Value(currentTransactionKey) != nil {
		return fn(ctx)
	}
	return t.RequiresNew(ctx, fn, options...)
}

var rollbackOnly = errors.New("rollback only transaction")

func (t *Transactor) RequiresNew(ctx context.Context, fn gotx.DoInTransaction, options ...gotx.Option) error {

	config := gotx.NewDefaultConfig()
	for _, opt := range options {
		opt.Apply(&config)
	}

	spannerClient := t.provider.CurrentConnection(ctx)
	if config.ReadOnly {
		txn := spannerClient.ReadOnlyTransaction()
		defer txn.Close()
		executor := NewDefaultTxClient(spannerClient, nil, txn)
		return fn(context.WithValue(ctx, currentTransactionKey, executor))
	}
	_, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		executor := NewDefaultTxClient(spannerClient, txn, nil)
		err := fn(context.WithValue(ctx, currentTransactionKey, executor))
		if err != nil {
			return err
		}
		if config.RollbackOnly {
			return rollbackOnly
		}
		return nil
	})
	// rollback only transaction
	if err != nil && errors.Is(err, rollbackOnly) {
		return nil
	}
	return err
}
