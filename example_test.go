package exceltesting_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/future-architect/go-exceltesting"
	_ "github.com/jackc/pgx/v4/stdlib"
)

var conn *sql.DB

func TestMain(m *testing.M) {
	uri := "postgres://excellocal:password@localhost:15432/excellocal"
	var err error
	conn, err = sql.Open("pgx", uri)
	if err != nil {
		os.Exit(1)
	}
	defer conn.Close()
	m.Run()

}

func TestExample_Load(t *testing.T) {
	e := exceltesting.New(conn)

	e.Load(t, exceltesting.LoadRequest{
		TargetBookPath: filepath.Join("testdata", "load.xlsx"),
		SheetPrefix:    "",
		IgnoreSheet:    nil,
	})
}
