package _integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/knocknote/gotx"

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

func TestRequiresNew(t *testing.T) {

	ctx := context.Background()
	connectionProvider := newConnection()
	transactor := rdbms.NewTransactor(connectionProvider)
	clientProvider := rdbms.NewDefaultClientProvider(connectionProvider)
	if err := createTable(ctx, connectionProvider, "test5"); err != nil {
		t.Error(err)
		return
	}
	_ = transactor.RequiresNew(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		_, _ = client.Exec("INSERT into test5 values('1')")
		_ = transactor.RequiresNew(ctx, func(ctx context.Context) error {
			client2 := clientProvider.CurrentClient(ctx)
			_, _ = client2.Exec("INSERT into test5 values('2')")
			return nil
		})
		return errors.New("outer transaction failed")
	})

	client := clientProvider.CurrentClient(ctx)
	var id string
	err := client.QueryRow("SELECT id FROM test5 where id = '1'").Scan(&id)
	if err == nil {
		t.Error("id 1 must be rollback")
		return
	}
	err = client.QueryRow("SELECT * FROM test5 where id = '2'").Scan(&id)
	if err != nil {
		t.Error(err)
		return
	}
}

func TestRollbackOnError(t *testing.T) {

	ctx := context.Background()
	connectionProvider := newConnection()
	transactor := rdbms.NewTransactor(connectionProvider)
	clientProvider := rdbms.NewDefaultClientProvider(connectionProvider)
	if err := createTable(ctx, connectionProvider, "test2"); err != nil {
		t.Error(err)
		return
	}
	e := errors.New("error")
	err := transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		_, err := client.Exec("INSERT into test2 values('1')")
		if err != nil {
			return err
		}
		return e
	})
	if err != e {
		t.Error("unexpected error")
		return
	}
	client := clientProvider.CurrentClient(ctx)
	row, err := client.Query("SELECT * FROM test2")
	if err != nil {
		t.Error(err)
		return
	}
	if row.Next() {
		t.Errorf("rollback expected")
		return
	}
}

func TestRollbackOption(t *testing.T) {

	ctx := context.Background()
	connectionProvider := newConnection()
	transactor := rdbms.NewTransactor(connectionProvider)
	clientProvider := rdbms.NewDefaultClientProvider(connectionProvider)
	if err := createTable(ctx, connectionProvider, "test3"); err != nil {
		t.Error(err)
		return
	}
	err := transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		_, err := client.Exec("INSERT into test3 values('1')")
		return err
	}, gotx.OptionRollbackOnly())

	// no error expected
	if err != nil {
		t.Error(err)
		return
	}
	client := clientProvider.CurrentClient(ctx)
	row, err := client.Query("SELECT * FROM test3")
	if err != nil {
		t.Error(err)
		return
	}
	if row.Next() {
		t.Errorf("rollback expected")
		return
	}
}

func TestReadOnlyOption(t *testing.T) {

	ctx := context.Background()
	connectionProvider := newConnection()
	transactor := rdbms.NewTransactor(connectionProvider)
	clientProvider := rdbms.NewDefaultClientProvider(connectionProvider)
	if err := createTable(ctx, connectionProvider, "test4"); err != nil {
		t.Error(err)
		return
	}
	err := transactor.Required(ctx, func(ctx context.Context) error {
		client := clientProvider.CurrentClient(ctx)
		_, err := client.Exec("INSERT into test4 values('1')")
		return err
	}, gotx.OptionReadOnly())

	if err == nil || err.Error() != "pq: cannot execute INSERT in a read-only transaction" {
		t.Errorf("unexpected error %v", err)
		return
	}

}
