package cli

import (
	"github.com/future-architect/go-exceltesting"
	"github.com/future-architect/go-exceltesting/testonly"
	"path/filepath"
	"testing"
)

func TestCompare(t *testing.T) {
	type args struct {
		dbSource string
		r        exceltesting.CompareRequest
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "",
			args: args{
				dbSource: "postgres://excellocal:password@localhost:15432/excellocal",
				r: exceltesting.CompareRequest{
					TargetBookPath: "testdata/compare_want.xlsx",
				},
			},
			wantErr: false,
		},
	}

	// setup
	conn := testonly.OpenTestDB(t)
	t.Cleanup(func() { _ = conn.Close() })
	testonly.ExecSQLFile(t, conn, filepath.Join("..", "testdata", "schema", "ddl.sql"))
	testonly.ExecSQLFile(t, conn, filepath.Join("testdata", "compare_input.sql"))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Compare(tt.args.dbSource, tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("Compare() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
