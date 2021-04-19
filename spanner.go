package gotx

import (
	"context"
	"errors"

	"golang.org/x/xerrors"

	"cloud.google.com/go/spanner"
)

type contextCurrentTransactionKey string

const currentTransactionKey contextCurrentTransactionKey = "current_spanner_transaction"

// -----------------------------------
// Connection
// -----------------------------------
type SpannerConnectionProvider interface {
	CurrentConnection(ctx context.Context) *spanner.Client
}

type DefaultSpannerConnectionProvider struct {
	client *spanner.Client
}

func NewDefaultSpannerConnectionProvider(client *spanner.Client) SpannerConnectionProvider {
	return &DefaultSpannerConnectionProvider{
		client: client,
	}
}

func (p *DefaultSpannerConnectionProvider) CurrentConnection(_ context.Context) *spanner.Client {
	return p.client
}

// -----------------------------------
// Client
// -----------------------------------
type SpannerReader interface {
	Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator
	ReadUsingIndex(ctx context.Context, table, index string, keys spanner.KeySet, columns []string) (ri *spanner.RowIterator)
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
	QueryWithOptions(ctx context.Context, statement spanner.Statement, opts spanner.QueryOptions) *spanner.RowIterator
	QueryWithStats(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
}

type SpannerClient interface {
	Reader(ctx context.Context) SpannerReader
	ApplyOrBufferWrite(context.Context, ...*spanner.Mutation) error
	Update(ctx context.Context, statement spanner.Statement) (int64, error)
	UpdateWithOption(ctx context.Context, statement spanner.Statement, options spanner.QueryOptions) (int64, error)
	BatchUpdate(ctx context.Context, statement []spanner.Statement) ([]int64, error)
	PartitionedUpdate(ctx context.Context, statement spanner.Statement) (int64, error)
	PartitionedUpdateWithOptions(ctx context.Context, statement spanner.Statement, options spanner.QueryOptions) (int64, error)
}

type DefaultSpannerClient struct {
	spannerClient *spanner.Client
	txRW          *spanner.ReadWriteTransaction
	txRO          *spanner.ReadOnlyTransaction
}

func NewDefaultSpannerClient(client *spanner.Client, rw *spanner.ReadWriteTransaction, ro *spanner.ReadOnlyTransaction) SpannerClient {
	return &DefaultSpannerClient{
		spannerClient: client,
		txRW:          rw,
		txRO:          ro,
	}
}

func (e *DefaultSpannerClient) ApplyOrBufferWrite(ctx context.Context, data ...*spanner.Mutation) error {
	if e.isInReadWriteTransaction() {
		return e.txRW.BufferWrite(data)
	}
	if e.isInReadOnlyTransaction() {
		return xerrors.New("read only transaction doesn't support write operation")
	}
	_, err := e.spannerClient.Apply(ctx, data)
	return err
}

func (e *DefaultSpannerClient) Update(ctx context.Context, stmt spanner.Statement) (int64, error) {
	if e.isInReadWriteTransaction() {
		return e.txRW.Update(ctx, stmt)
	}
	return -1, xerrors.New("read write transaction is required to use statement")
}

func (e *DefaultSpannerClient) UpdateWithOption(ctx context.Context, stmt spanner.Statement, opts spanner.QueryOptions) (int64, error) {
	if e.isInReadWriteTransaction() {
		return e.txRW.UpdateWithOptions(ctx, stmt, opts)
	}
	return -1, xerrors.New("read write transaction is required to use statement")
}

func (e *DefaultSpannerClient) BatchUpdate(ctx context.Context, stmts []spanner.Statement) ([]int64, error) {
	if e.isInReadWriteTransaction() {
		return e.txRW.BatchUpdate(ctx, stmts)
	}
	return nil, xerrors.New("read write transaction is required to use statement")
}

func (e *DefaultSpannerClient) PartitionedUpdate(ctx context.Context, stmt spanner.Statement) (int64, error) {
	if e.isInReadWriteTransaction() {
		//spanner doesn't support nested transaction
		return -1, xerrors.New("partitioned update is unsupported in read write transaction")
	}
	return e.spannerClient.PartitionedUpdate(ctx, stmt)
}

func (e *DefaultSpannerClient) PartitionedUpdateWithOptions(ctx context.Context, stmt spanner.Statement, options spanner.QueryOptions) (int64, error) {
	if e.isInReadWriteTransaction() {
		//spanner doesn't support nested transaction
		return -1, xerrors.New("partitioned update is unsupported in read write transaction")
	}
	return e.spannerClient.PartitionedUpdateWithOptions(ctx, stmt, options)
}

func (e *DefaultSpannerClient) Reader(_ context.Context) SpannerReader {
	if e.isInReadWriteTransaction() {
		return e.txRW
	}
	if e.isInReadOnlyTransaction() {
		return e.txRO
	}
	return e.spannerClient.Single()
}

func (e *DefaultSpannerClient) isInReadWriteTransaction() bool {
	return e.txRW != nil
}

func (e *DefaultSpannerClient) isInReadOnlyTransaction() bool {
	return e.txRO != nil
}

type SpannerClientProvider interface {
	CurrentClient(ctx context.Context) SpannerClient
}

type DefaultSpannerClientProvider struct {
	connectionProvider SpannerConnectionProvider
}

func NewDefaultSpannerClientProvider(connectionProvider SpannerConnectionProvider) SpannerClientProvider {
	return &DefaultSpannerClientProvider{
		connectionProvider: connectionProvider,
	}
}

func (p *DefaultSpannerClientProvider) CurrentClient(ctx context.Context) SpannerClient {
	transaction := ctx.Value(currentTransactionKey)
	if transaction == nil {
		return NewDefaultSpannerClient(p.connectionProvider.CurrentConnection(ctx), nil, nil)
	}
	return transaction.(SpannerClient)
}

// -----------------------------------
// Transactor
// -----------------------------------
type SpannerTransactor struct {
	provider SpannerConnectionProvider
}

func NewSpannerTransactor(provider SpannerConnectionProvider) Transactor {
	return &SpannerTransactor{
		provider: provider,
	}
}

func (t *SpannerTransactor) Required(ctx context.Context, fn DoInTransaction, options ...Option) error {
	if ctx.Value(currentTransactionKey) != nil {
		return fn(ctx)
	}
	return t.RequiresNew(ctx, fn, options...)
}

var rollbackOnly = errors.New("rollback only transaction")

func (t *SpannerTransactor) RequiresNew(ctx context.Context, fn DoInTransaction, options ...Option) error {

	config := NewDefaultConfig()
	for _, opt := range options {
		opt.Apply(&config)
	}

	spannerClient := t.provider.CurrentConnection(ctx)
	if config.ReadOnly {
		txn := spannerClient.ReadOnlyTransaction()
		defer txn.Close()
		executor := NewDefaultSpannerClient(spannerClient, nil, txn)
		return fn(context.WithValue(ctx, currentTransactionKey, executor))
	}
	_, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		executor := NewDefaultSpannerClient(spannerClient, txn, nil)
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
