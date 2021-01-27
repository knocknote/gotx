package spanner

import (
	"context"
	"errors"

	"cloud.google.com/go/spanner"
)

type Reader interface {
	Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator
	ReadUsingIndex(ctx context.Context, table, index string, keys spanner.KeySet, columns []string) (ri *spanner.RowIterator)
	Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
	QueryWithOptions(ctx context.Context, statement spanner.Statement, opts spanner.QueryOptions) *spanner.RowIterator
	QueryWithStats(ctx context.Context, statement spanner.Statement) *spanner.RowIterator
	ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error)
}

type TxClient interface {
	Reader
	WriteMutation(context.Context, ...*spanner.Mutation) error
}

type DefaultTxClient struct {
	spannerClient *spanner.Client
	txRW          *spanner.ReadWriteTransaction
	txRO          *spanner.ReadOnlyTransaction
	buffers       []*spanner.Mutation
}

func NewDefaultTxClient(client *spanner.Client, rw *spanner.ReadWriteTransaction, ro *spanner.ReadOnlyTransaction) *DefaultTxClient {
	return &DefaultTxClient{
		spannerClient: client,
		txRW:          rw,
		txRO:          ro,
		buffers:       make([]*spanner.Mutation, 0),
	}
}

func (e *DefaultTxClient) WriteMutation(ctx context.Context, data ...*spanner.Mutation) error {
	if e.isInReadWriteTransaction() {
		e.buffers = append(e.buffers, data...)
		return nil
	}
	if e.isInReadOnlyTransaction() {
		return errors.New("read only transaction doesn't support write operation")
	}
	_, err := e.spannerClient.Apply(ctx, e.buffers)
	return err
}

func (e *DefaultTxClient) Read(ctx context.Context, table string, keys spanner.KeySet, columns []string) *spanner.RowIterator {
	return e.reader().Read(ctx, table, keys, columns)
}

func (e *DefaultTxClient) ReadUsingIndex(ctx context.Context, table, index string, keys spanner.KeySet, columns []string) (ri *spanner.RowIterator) {
	return e.reader().ReadUsingIndex(ctx, table, index, keys, columns)
}

func (e *DefaultTxClient) Query(ctx context.Context, statement spanner.Statement) *spanner.RowIterator {
	return e.reader().Query(ctx, statement)
}

func (e *DefaultTxClient) QueryWithOptions(ctx context.Context, statement spanner.Statement, opts spanner.QueryOptions) *spanner.RowIterator {
	return e.reader().QueryWithOptions(ctx, statement, opts)
}

func (e *DefaultTxClient) QueryWithStats(ctx context.Context, statement spanner.Statement) *spanner.RowIterator {
	return e.reader().QueryWithStats(ctx, statement)
}

func (e *DefaultTxClient) ReadRow(ctx context.Context, table string, key spanner.Key, columns []string) (*spanner.Row, error) {
	return e.reader().ReadRow(ctx, table, key, columns)
}

func (e *DefaultTxClient) reader() Reader {
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
	return e.txRW != nil
}

func (e *DefaultTxClient) Flush(_ context.Context) error {
	if e.isInReadWriteTransaction() {
		err := e.txRW.BufferWrite(e.buffers)
		// clear is required for retrying transaction.
		e.buffers = make([]*spanner.Mutation, 0)
		return err
	}
	return nil
}
