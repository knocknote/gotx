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

type DefaultClient struct {
	spannerClient *spanner.Client
	txRW          *spanner.ReadWriteTransaction
	txRO          *spanner.ReadOnlyTransaction
}

func NewDefaultClient(client *spanner.Client, rw *spanner.ReadWriteTransaction, ro *spanner.ReadOnlyTransaction) Client {
	return &DefaultClient{
		spannerClient: client,
		txRW:          rw,
		txRO:          ro,
	}
}

func (e *DefaultClient) ApplyOrBufferWrite(ctx context.Context, data ...*spanner.Mutation) error {
	if e.isInReadWriteTransaction() {
		return e.txRW.BufferWrite(data)
	}
	if e.isInReadOnlyTransaction() {
		return errors.New("read only transaction doesn't support write operation")
	}
	_, err := e.spannerClient.Apply(ctx, data)
	return err
}

func (e *DefaultClient) Update(ctx context.Context, stmt spanner.Statement) (int64, error) {
	if e.isInReadWriteTransaction() {
		return e.txRW.Update(ctx, stmt)
	}
	return -1, errors.New("read write transaction is required to use statement")
}

func (e *DefaultClient) UpdateWithOption(ctx context.Context, stmt spanner.Statement, opts spanner.QueryOptions) (int64, error) {
	if e.isInReadWriteTransaction() {
		return e.txRW.UpdateWithOptions(ctx, stmt, opts)
	}
	return -1, errors.New("read write transaction is required to use statement")
}

func (e *DefaultClient) BatchUpdate(ctx context.Context, stmts []spanner.Statement) ([]int64, error) {
	if e.isInReadWriteTransaction() {
		return e.txRW.BatchUpdate(ctx, stmts)
	}
	return nil, errors.New("read write transaction is required to use statement")
}

func (e *DefaultClient) PartitionedUpdate(ctx context.Context, stmt spanner.Statement) (int64, error) {
	if e.isInReadWriteTransaction() {
		//spanner doesn't support nested transaction
		return -1, errors.New("partitioned update is unsupported in read write transaction")
	}
	return e.spannerClient.PartitionedUpdate(ctx, stmt)
}

func (e *DefaultClient) PartitionedUpdateWithOptions(ctx context.Context, stmt spanner.Statement, options spanner.QueryOptions) (int64, error) {
	if e.isInReadWriteTransaction() {
		//spanner doesn't support nested transaction
		return -1, errors.New("partitioned update is unsupported in read write transaction")
	}
	return e.spannerClient.PartitionedUpdateWithOptions(ctx, stmt, options)
}

func (e *DefaultClient) Reader(_ context.Context) Reader {
	if e.isInReadWriteTransaction() {
		return e.txRW
	}
	if e.isInReadOnlyTransaction() {
		return e.txRO
	}
	return e.spannerClient.Single()
}

func (e *DefaultClient) isInReadWriteTransaction() bool {
	return e.txRW != nil
}

func (e *DefaultClient) isInReadOnlyTransaction() bool {
	return e.txRO != nil
}

type ClientProvider interface {
	CurrentClient(ctx context.Context) Client
}

type DefaultClientProvider struct {
	singleClient Client
}

func NewDefaultClientProvider(client *spanner.Client) ClientProvider {
	return NewDefaultClientProviderWithFactory(client, &DefaultClientFactory{})
}

func NewDefaultClientProviderWithFactory(client *spanner.Client, factory ClientFactory) ClientProvider {
	return &DefaultClientProvider{
		singleClient: factory.NewClient(client, nil, nil),
	}
}

func (p *DefaultClientProvider) CurrentClient(ctx context.Context) Client {
	transaction := ctx.Value(currentTransactionKey)
	if transaction == nil {
		return p.singleClient
	}
	return transaction.(Client)
}

type ClientFactory interface {
	NewClient(client *spanner.Client, rw *spanner.ReadWriteTransaction, ro *spanner.ReadOnlyTransaction) Client
}

type DefaultClientFactory struct {
}

func (c *DefaultClientFactory) NewClient(client *spanner.Client, rw *spanner.ReadWriteTransaction, ro *spanner.ReadOnlyTransaction) Client {
	return NewDefaultClient(client, rw, ro)
}

// ------------------------------------
// Transactor
// ------------------------------------

type TransactionOptions spanner.TransactionOptions

func (o TransactionOptions) Apply(c *gotx.Config) {
	c.VendorOption = spanner.TransactionOptions(o)
}

func OptionTransactionOptions(options spanner.TransactionOptions) TransactionOptions {
	return TransactionOptions(options)
}

type Transactor struct {
	spannerClient *spanner.Client
	clientFactory ClientFactory
	onCommit      func(commitResponse *spanner.CommitResponse)
}

type TransactorConfig struct {
	ClientFactory ClientFactory
	OnCommit      func(commitResponse *spanner.CommitResponse)
}

func NewTransactor(spannerClient *spanner.Client) gotx.Transactor {
	return NewTransactorWithConfig(spannerClient, TransactorConfig{})
}

func NewTransactorWithConfig(spannerClient *spanner.Client, config TransactorConfig) gotx.Transactor {
	var factory ClientFactory
	if config.ClientFactory == nil {
		factory = &DefaultClientFactory{}
	} else {
		factory = config.ClientFactory
	}
	return &Transactor{
		spannerClient: spannerClient,
		clientFactory: factory,
		onCommit:      config.OnCommit,
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
	config.VendorOption = spanner.TransactionOptions{}
	for _, opt := range options {
		opt.Apply(&config)
	}

	if config.ReadOnly {
		txn := t.spannerClient.ReadOnlyTransaction()
		defer txn.Close()
		executor := t.clientFactory.NewClient(t.spannerClient, nil, txn)
		return fn(context.WithValue(ctx, currentTransactionKey, executor))
	}
	commitResponse, err := t.spannerClient.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		executor := t.clientFactory.NewClient(t.spannerClient, txn, nil)
		err := fn(context.WithValue(ctx, currentTransactionKey, executor))
		if err != nil {
			return err
		}
		if config.RollbackOnly {
			return rollbackOnly
		}
		return nil
	}, config.VendorOption.(spanner.TransactionOptions))
	// rollback only transaction
	if err != nil && errors.Is(err, rollbackOnly) {
		return nil
	}
	// commit hook
	if err == nil && t.onCommit != nil {
		t.onCommit(&commitResponse)
	}
	return err
}
