package cli

import (
	"github.com/future-architect/go-exceltesting/testonly"
	"github.com/google/go-cmp/cmp"
	"github.com/xuri/excelize/v2"
	"path/filepath"
	"testing"
)

func Test_dump(t *testing.T) {
	type args struct {
		dbSource        string
		targetFile      string
		tableNameArg    string
		systemColumnArg string
	}
	tests := []struct {
		name      string
		args      args
		wantErr   bool
		wantSheet []string
	}{
		{
			name: "",
			args: args{
				dbSource:        "postgres://excellocal:password@localhost:15432/excellocal",
				targetFile:      "got1.xlsx",
				tableNameArg:    "company,test_x",
				systemColumnArg: "created_at,updated_at,revision",
			},
			wantErr:   false,
			wantSheet: []string{"company", "test_x"},
		},
	}

	// setup
	conn := testonly.OpenTestDB(t)
	t.Cleanup(func() { _ = conn.Close() })
	testonly.ExecSQLFile(t, conn, filepath.Join("..", "testdata", "schema", "ddl.sql"))

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			if err := Dump(tt.args.dbSource, tt.args.targetFile, tt.args.tableNameArg, tt.args.systemColumnArg); (err != nil) != tt.wantErr {
				t.Errorf("dump() error = %v, wantErr %v", err, tt.wantErr)
			}
		})

		gotFile, err := excelize.OpenFile("got1.xlsx")
		if err != nil {
			t.Fatalf("got1.xlsx open failed")
		}
		list := gotFile.GetSheetList()
		if diff := cmp.Diff(tt.wantSheet, list); diff != "" {
			t.Errorf("got sheet listmismatch (-want +got):\n%s", diff)
		}

	}
}
