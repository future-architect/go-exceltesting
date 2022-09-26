package cli

import (
	"github.com/future-architect/go-exceltesting"
	"github.com/future-architect/go-exceltesting/testonly"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	type args struct {
		dbSource string
		r        exceltesting.LoadRequest
	}
	tests := []struct {
		name      string
		args      args
		wantErr   bool
		wantCount int
	}{
		{
			name: "",
			args: args{
				dbSource: "postgres://excellocal:password@localhost:15432/excellocal",
				r: exceltesting.LoadRequest{
					TargetBookPath: "testdata/load_input.xlsx",
				},
			},
			wantErr:   false,
			wantCount: 3,
		},
	}

	// setup
	conn := testonly.OpenTestDB(t)
	t.Cleanup(func() { _ = conn.Close() })
	testonly.ExecSQLFile(t, conn, filepath.Join("..", "testdata", "schema", "ddl.sql"))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Load(tt.args.dbSource, tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
			}
		})

		var gotCount int32
		if err := conn.QueryRow("select count(company_cd) from company").Scan(&gotCount); err != nil {
			t.Fatal(err)
		}

		if tt.wantCount != int(gotCount) {
			t.Errorf("load count mismatch: got=%d, want=%d", gotCount, tt.wantCount)
		}
	}

}
