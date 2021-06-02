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
	connectionProvider ConnectionProvider
	clientFactory      ClientFactory
}

func NewDefaultClientProvider(connectionProvider ConnectionProvider) ClientProvider {
	return NewDefaultClientProviderWithFactory(connectionProvider, &DefaultClientFactory{})
}

func NewDefaultClientProviderWithFactory(connectionProvider ConnectionProvider, clientFactory ClientFactory) ClientProvider {
	return &DefaultClientProvider{
		connectionProvider: connectionProvider,
		clientFactory:      clientFactory,
	}
}

func (p *DefaultClientProvider) CurrentClient(ctx context.Context) Client {
	transaction := ctx.Value(currentTransactionKey)
	if transaction == nil {
		return p.clientFactory.NewClient(p.connectionProvider.CurrentConnection(ctx), nil, nil)
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
	clientProvider ConnectionProvider
	clientFactory  ClientFactory
	onCommit       func(commitResponse *spanner.CommitResponse)
}

type TransactorConfig struct {
	ClientFactory ClientFactory
	OnCommit      func(commitResponse *spanner.CommitResponse)
}

func NewTransactor(clientProvider ConnectionProvider) gotx.Transactor {
	return NewTransactorWithConfig(clientProvider, TransactorConfig{
		ClientFactory: &DefaultClientFactory{},
	})
}

func NewTransactorWithConfig(clientProvider ConnectionProvider, config TransactorConfig) gotx.Transactor {
	return &Transactor{
		clientProvider: clientProvider,
		clientFactory:  config.ClientFactory,
		onCommit:       config.OnCommit,
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

	spannerClient := t.clientProvider.CurrentConnection(ctx)
	if config.ReadOnly {
		txn := spannerClient.ReadOnlyTransaction()
		defer txn.Close()
		executor := t.clientFactory.NewClient(spannerClient, nil, txn)
		return fn(context.WithValue(ctx, currentTransactionKey, executor))
	}
	commitResponse, err := spannerClient.ReadWriteTransactionWithOptions(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		executor := t.clientFactory.NewClient(spannerClient, txn, nil)
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
