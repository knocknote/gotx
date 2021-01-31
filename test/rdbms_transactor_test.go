package test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/knocknote/gotx/rdbms"

	_ "github.com/lib/pq"
)

func newConnection() rdbms.ConnectionProvider {
	connection, err := sql.Open("postgres", "postgres://postgres:password@localhost/testdb?sslmode=disable")
	if err != nil {
		fmt.Printf("open error %v", err)
		return nil
	}
	return rdbms.NewDefaultConnectionProvider(connection)
}

func createTable(ctx context.Context, connectionProvider rdbms.ConnectionProvider, name string) error {
	connection := connectionProvider.CurrentConnection(ctx)
	_, _ = connection.Exec(fmt.Sprintf("drop table %s", name))
	_, err := connection.Exec(fmt.Sprintf("create table %s ( id varchar(10))", name))
	return err
}

func TestCommit(t *testing.T) {

	ctx := context.Background()
	connectionProvider := newConnection()
	transactor := rdbms.NewTransactor(connectionProvider)
	clientProvider := rdbms.NewDefaultClientProvider(connectionProvider)
	if err := createTable(ctx, connectionProvider, "test1"); err != nil {
		t.Error(err)
		return
	}
	err := transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		_, err := client.Exec("INSERT into test1 values('1')")
		return err
	})
	if err != nil {
		t.Error(err)
		return
	}

	client := clientProvider.CurrentClient(ctx)
	rows, err := client.Query("SELECT * FROM test1")
	if err != nil {
		t.Error(err)
		return
	}
	if !rows.Next() {
		t.Errorf("row must exists")
		return
	}
}
