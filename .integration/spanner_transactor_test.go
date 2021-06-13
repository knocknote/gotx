package _integration

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/knocknote/gotx"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/spanner"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	gotxspanner "github.com/knocknote/gotx/spanner"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type mutationHookClient struct {
	gotxspanner.Client
	Tables []string
}

func (c *mutationHookClient) ApplyOrBufferWrite(ctx context.Context, data ...*spanner.Mutation) error {
	for _, m := range data {
		c.Tables = append(c.Tables, reflect.ValueOf(m).Elem().FieldByName("table").String())
	}
	return c.Client.ApplyOrBufferWrite(ctx, data...)
}

type mutationHookClientFactory struct {
}

func (c *mutationHookClientFactory) NewClient(client *spanner.Client, rw *spanner.ReadWriteTransaction, ro *spanner.ReadOnlyTransaction) gotxspanner.Client {
	return &mutationHookClient{
		Client: gotxspanner.NewDefaultClient(client, rw, ro),
		Tables: []string{},
	}
}

func newSpannerConnection(db string) (*spanner.Client, error) {
	parent := "projects/local-project/instances/test-instance"

	ctx := context.Background()
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return nil, err
	}
	defer adminClient.Close()

	op, err := adminClient.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          parent,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", db),
		ExtraStatements: []string{`CREATE TABLE test ( id INT64 NOT NULL) PRIMARY KEY (id)`},
	})
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			return nil, err
		}
	} else {
		if _, err = op.Wait(ctx); err != nil {
			return nil, err
		}
	}
	return spanner.NewClient(context.Background(), parent+"/databases/"+db)
}

func TestSpannerCommit(t *testing.T) {

	ctx := context.Background()
	connectionPool, err := newSpannerConnection("local-test1")
	if err != nil {
		t.Error(err)
		return
	}
	transactor := gotxspanner.NewTransactorWithConfig(connectionPool, gotxspanner.TransactorConfig{
		OnCommit: func(commitResponse *spanner.CommitResponse) {
			t.Log(commitResponse.CommitTs)
		},
	})
	clientProvider := gotxspanner.NewDefaultClientProvider(connectionPool)
	err = transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		m := []*spanner.Mutation{
			spanner.InsertOrUpdate("test", []string{"id"}, []interface{}{100}),
		}
		return client.ApplyOrBufferWrite(ctx, m...)
	}, gotxspanner.OptionTransactionOptions(spanner.TransactionOptions{
		CommitOptions: spanner.CommitOptions{ReturnCommitStats: true},
	}))
	if err != nil {
		t.Error(err)
		return
	}

	client := clientProvider.CurrentClient(ctx)
	//key := spanner.Key{100}
	row, err := client.Reader(ctx).ReadRow(ctx, "test", spanner.Key{100}, []string{"id"})
	if err != nil {
		t.Error(err)
		return
	}
	var v int64
	err = row.ColumnByName("id", &v)
	if err != nil {
		t.Error(err)
		return
	}
	if v != 100 {
		t.Errorf("id must be 100")
		return
	}
}

func TestSpannerCommitBatchStatement(t *testing.T) {

	ctx := context.Background()
	connectionProvider, err := newSpannerConnection("local-test1")
	if err != nil {
		t.Error(err)
		return
	}

	transactor := gotxspanner.NewTransactor(connectionProvider)
	clientProvider := gotxspanner.NewDefaultClientProvider(connectionProvider)

	stmt := spanner.Statement{SQL: "DELETE FROM test WHERE id >= 100"}
	_, _ = clientProvider.CurrentClient(ctx).PartitionedUpdate(ctx, stmt)
	var count []int64
	err = transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		stmt1 := spanner.Statement{
			SQL: "INSERT INTO test (id) VALUES(@id)",
			Params: map[string]interface{}{
				"id": 100,
			},
		}
		stmt2 := spanner.Statement{
			SQL: "INSERT INTO test (id) VALUES(@id)",
			Params: map[string]interface{}{
				"id": 101,
			},
		}
		count, err = client.BatchUpdate(ctx, []spanner.Statement{stmt1, stmt2})
		return err
	})
	if err != nil {
		t.Error(err)
		return
	}
	if count[0] != 1 || count[1] != 1 {
		t.Error("update count must be 1")
		return
	}
}

func TestSpannerCommitStatement(t *testing.T) {

	ctx := context.Background()
	connectionProvider, err := newSpannerConnection("local-test1")
	if err != nil {
		t.Error(err)
		return
	}

	transactor := gotxspanner.NewTransactor(connectionProvider)
	clientProvider := gotxspanner.NewDefaultClientProvider(connectionProvider)

	stmt := spanner.Statement{SQL: "DELETE FROM test WHERE id = 100"}
	_, _ = clientProvider.CurrentClient(ctx).PartitionedUpdate(ctx, stmt)
	var count int64
	err = transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		stmt = spanner.Statement{
			SQL: "INSERT INTO test (id) VALUES(@id)",
			Params: map[string]interface{}{
				"id": 100,
			},
		}
		count, err = client.Update(ctx, stmt)
		return err
	})
	if count != 1 {
		t.Error("update count must be 1")
		return
	}

	if err != nil {
		t.Error(err)
		return
	}

}

func TestSpannerCommitStatementAndMutation(t *testing.T) {

	ctx := context.Background()
	connectionProvider, err := newSpannerConnection("local-test1")
	if err != nil {
		t.Error(err)
		return
	}

	clientFactory := &mutationHookClientFactory{}
	transactor := gotxspanner.NewTransactorWithConfig(connectionProvider, gotxspanner.TransactorConfig{
		ClientFactory: clientFactory,
	})
	clientProvider := gotxspanner.NewDefaultClientProviderWithFactory(connectionProvider, clientFactory)

	stmt := spanner.Statement{SQL: "DELETE FROM test WHERE id >= 100"}
	_, _ = clientProvider.CurrentClient(ctx).PartitionedUpdate(ctx, stmt)
	err = transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		stmt = spanner.Statement{
			SQL: "INSERT INTO test (id) VALUES(@id)",
			Params: map[string]interface{}{
				"id": 100,
			},
		}
		m := []*spanner.Mutation{
			spanner.InsertOrUpdate("test", []string{"id"}, []interface{}{101}),
		}
		_, err = client.Update(ctx, stmt)
		if err != nil {
			return err
		}
		err = client.ApplyOrBufferWrite(ctx, m...)
		if len(client.(*mutationHookClient).Tables) != 1 {
			t.Error("must contain table")
		}
		return err
	})
	if err != nil {
		t.Error(err)
		return
	}
	row, err := clientProvider.CurrentClient(ctx).Reader(ctx).ReadRow(ctx, "test", spanner.Key{100}, []string{"id"})
	if err != nil {
		t.Error(err)
		return
	}
	var v int64
	err = row.ColumnByName("id", &v)
	if err != nil {
		t.Error(err)
		return
	}
	if v != 100 {
		t.Errorf("id must be 100")
		return
	}

	row, err = clientProvider.CurrentClient(ctx).Reader(ctx).ReadRow(ctx, "test", spanner.Key{101}, []string{"id"})
	if err != nil {
		t.Error(err)
		return
	}
	err = row.ColumnByName("id", &v)
	if err != nil {
		t.Error(err)
		return
	}
	if v != 101 {
		t.Errorf("id must be 101")
		return
	}
}

func TestSpannerCommitApply(t *testing.T) {

	ctx := context.Background()
	connectionProvider, err := newSpannerConnection("local-test1")
	if err != nil {
		t.Error(err)
		return
	}

	clientProvider := gotxspanner.NewDefaultClientProvider(connectionProvider)

	stmt := spanner.Statement{SQL: "DELETE FROM test WHERE id >= 100"}
	_, _ = clientProvider.CurrentClient(ctx).PartitionedUpdate(ctx, stmt)
	client := clientProvider.CurrentClient(ctx)
	m := []*spanner.Mutation{
		spanner.InsertOrUpdate("test", []string{"id"}, []interface{}{100}),
	}
	err = client.ApplyOrBufferWrite(ctx, m...)
	if err != nil {
		t.Error(err)
		return
	}
	//key := spanner.Key{100}
	row, err := client.Reader(ctx).ReadRow(ctx, "test", spanner.Key{100}, []string{"id"})
	if err != nil {
		t.Error(err)
		return
	}
	var v int64
	err = row.ColumnByName("id", &v)
	if err != nil {
		t.Error(err)
		return
	}
	if v != 100 {
		t.Errorf("id must be 100")
		return
	}

}

func TestSpannerRollback(t *testing.T) {

	ctx := context.Background()
	connectionProvider, err := newSpannerConnection("local-test2")
	if err != nil {
		t.Error(err)
		return
	}

	transactor := gotxspanner.NewTransactor(connectionProvider)
	clientProvider := gotxspanner.NewDefaultClientProvider(connectionProvider)
	err = transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		m := []*spanner.Mutation{
			spanner.InsertOrUpdate("test", []string{"id"}, []interface{}{100}),
		}
		_ = client.ApplyOrBufferWrite(ctx, m...)
		return errors.New("rollback")
	})
	if err == nil {
		t.Error("must be error")
		return
	}

	client := clientProvider.CurrentClient(ctx)
	//key := spanner.Key{100}
	_, err = client.Reader(ctx).ReadRow(ctx, "test", spanner.Key{100}, []string{"id"})
	if err == nil {
		t.Error(err)
		return
	}
	fmt.Println(err)
}

func TestSpannerRollbackStatement(t *testing.T) {

	ctx := context.Background()
	connectionProvider, err := newSpannerConnection("local-test2")
	if err != nil {
		t.Error(err)
		return
	}

	transactor := gotxspanner.NewTransactor(connectionProvider)
	clientProvider := gotxspanner.NewDefaultClientProvider(connectionProvider)
	stmt := spanner.Statement{SQL: "DELETE FROM test WHERE id = 100"}
	_, _ = clientProvider.CurrentClient(ctx).PartitionedUpdate(ctx, stmt)
	var count int64
	err = transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		stmt = spanner.Statement{
			SQL: "INSERT INTO test (id) VALUES(@id)",
			Params: map[string]interface{}{
				"id": 100,
			},
		}
		count, _ = client.Update(ctx, stmt)
		return errors.New("error")
	})
	if err == nil {
		t.Error("must be error")
		return
	}

	if count != 1 {
		t.Error("update count must be 1")
		return
	}

	client := clientProvider.CurrentClient(ctx)
	//key := spanner.Key{100}
	_, err = client.Reader(ctx).ReadRow(ctx, "test", spanner.Key{100}, []string{"id"})
	if err == nil {
		t.Error(err)
		return
	}
	fmt.Println(err)
}

func TestSpannerRollbackOption(t *testing.T) {

	ctx := context.Background()
	connectionProvider, err := newSpannerConnection("local-test3")
	if err != nil {
		t.Error(err)
		return
	}

	transactor := gotxspanner.NewTransactor(connectionProvider)
	clientProvider := gotxspanner.NewDefaultClientProvider(connectionProvider)
	err = transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		m := []*spanner.Mutation{
			spanner.InsertOrUpdate("test", []string{"id"}, []interface{}{100}),
		}
		return client.ApplyOrBufferWrite(ctx, m...)
	}, gotx.OptionRollbackOnly())

	if err != nil {
		t.Error(err)
		return
	}

	client := clientProvider.CurrentClient(ctx)
	//key := spanner.Key{100}
	_, err = client.Reader(ctx).ReadRow(ctx, "test", spanner.Key{100}, []string{"id"})
	if err == nil {
		t.Error(err)
		return
	}
	fmt.Println(err)
}

func TestSpannerReadonlyOption(t *testing.T) {

	ctx := context.Background()
	connectionProvider, err := newSpannerConnection("local-test4")
	if err != nil {
		t.Error(err)
		return
	}

	transactor := gotxspanner.NewTransactor(connectionProvider)
	clientProvider := gotxspanner.NewDefaultClientProvider(connectionProvider)
	err = transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		m := []*spanner.Mutation{
			spanner.InsertOrUpdate("test", []string{"id"}, []interface{}{100}),
		}
		return client.ApplyOrBufferWrite(ctx, m...)
	}, gotx.OptionReadOnly())

	if err == nil || err.Error() != "read only transaction doesn't support write operation" {
		t.Error(err)
		return
	}
}

func TestSpannerErrorOnReadOnlyTransaction(t *testing.T) {

	ctx := context.Background()
	connectionProvider, err := newSpannerConnection("local-test1")
	if err != nil {
		t.Error(err)
		return
	}

	transactor := gotxspanner.NewTransactor(connectionProvider)
	clientProvider := gotxspanner.NewDefaultClientProvider(connectionProvider)

	stmt := spanner.Statement{SQL: "DELETE FROM test WHERE id = 100"}
	_, _ = clientProvider.CurrentClient(ctx).PartitionedUpdate(ctx, stmt)
	err = transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		stmt = spanner.Statement{
			SQL: "INSERT INTO test (id) VALUES(@id)",
			Params: map[string]interface{}{
				"id": 100,
			},
		}
		_, err = client.Update(ctx, stmt)
		return err
	}, gotx.OptionReadOnly())
	if err == nil {
		t.Error("must be error")
		return
	}
	fmt.Println(err)
}

func TestSpannerOnReadOnlyTransaction(t *testing.T) {

	ctx := context.Background()
	connectionPool, err := newSpannerConnection("local-test1")
	if err != nil {
		t.Error(err)
		return
	}

	transactor := gotxspanner.NewTransactor(connectionPool)
	clientProvider := gotxspanner.NewDefaultClientProvider(connectionPool)

	stmt := spanner.Statement{SQL: "DELETE FROM test WHERE id = 102"}
	_, err = clientProvider.CurrentClient(ctx).PartitionedUpdate(ctx, stmt)
	if err != nil {
		t.Error(err)
		return
	}
	m := []*spanner.Mutation{
		spanner.InsertOrUpdate("test", []string{"id"}, []interface{}{102}),
	}
	_, err = connectionPool.Apply(ctx, m)
	if err != nil {
		t.Error(err)
		return
	}
	var v int64
	err = transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		reader, ok := client.Reader(ctx).(*spanner.ReadOnlyTransaction)
		if !ok {
			return errors.New("must be read only transaction")
		}
		row, e := reader.WithTimestampBound(spanner.ExactStaleness(16*time.Second)).
			ReadRow(ctx, "test", spanner.Key{102}, []string{"id"})
		if e != nil {
			return e
		}
		return row.ColumnByName("id", &v)
	}, gotx.OptionReadOnly())
	if err != nil && status.Code(err) != codes.NotFound {
		t.Error(err)
		return
	}
}
