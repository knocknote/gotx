package _integration

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/knocknote/gotx"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/spanner"

	database "cloud.google.com/go/spanner/admin/database/apiv1"
	gotxspanner "github.com/knocknote/gotx/spanner"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"

	_ "github.com/lib/pq"
)

func newSpannerConnection(db string) (gotxspanner.ConnectionProvider, error) {
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
	connection, err := spanner.NewClient(context.Background(), parent+"/databases/"+db)
	if err != nil {
		return nil, err
	}
	return gotxspanner.NewDefaultConnectionProvider(connection), nil
}

func TestSpannerCommit(t *testing.T) {

	ctx := context.Background()
	connectionProvider, err := newSpannerConnection("local-test1")
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
	})
	if err != nil {
		t.Error(err)
		return
	}

	client := clientProvider.CurrentClient(ctx)
	//key := spanner.Key{100}
	row, err := client.ReadRow(ctx, "test", spanner.Key{100}, []string{"id"})
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
	_, err = client.ReadRow(ctx, "test", spanner.Key{100}, []string{"id"})
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
	_, err = client.ReadRow(ctx, "test", spanner.Key{100}, []string{"id"})
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
