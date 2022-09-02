package exceltesting_test

import (
	"database/sql"
	"encoding/csv"
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

func TestExample_LoadRawFromCSV(t *testing.T) {
	f, err := os.Open(filepath.Join("testdata", "sample.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	cr := csv.NewReader(f)
	rs, err := cr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}

	err = exceltesting.LoadRaw(tx, exceltesting.LoadRawRequest{
		TableName: "company",
		Columns:   rs[0],
		Values:    rs[1:],
	})

	if err != nil {
		t.Fatal(err)
	}

	tx.Rollback()
}
