package exceltesting

import (
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestLoadRaw(t *testing.T) {
	conn := openTestDB(t)
	t.Cleanup(func() { conn.Close() })

	execSQLFile(t, conn, filepath.Join("testdata", "schema", "ddl.sql"))

	type company struct {
		companyCD   string
		companyName string
		foundedYear int
	}

	tests := []struct {
		name    string
		r       LoadRawRequest
		want    []company
		wantErr bool
	}{
		{
			name: "inserted data",
			r: LoadRawRequest{
				TableName: "company",
				Columns:   []string{"company_cd", "company_name", "founded_year", "created_at", "updated_at", "revision"},
				Values: [][]string{
					{"00001", "Future", "1989", "current_timestamp", "current_timestamp", "1"},
					{"00002", "YDC", "1972", "current_timestamp", "current_timestamp", "1"},
				},
			},
			want: []company{
				{companyCD: "00001", companyName: "Future", foundedYear: 1989},
				{companyCD: "00002", companyName: "YDC", foundedYear: 1972},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := conn.Exec("TRUNCATE company;"); err != nil {
				t.Fatalf("truncate company: %v", err)
			}
			tx, err := conn.Begin()
			if err != nil {
				t.Fatalf("start transaction: %v", err)
			}
			if err := LoadRaw(tx, tt.r); (err != nil) != tt.wantErr {
				t.Errorf("LoadRaw() error = %v, wantErr %v", err, tt.wantErr)
			}

			rows, err := tx.Query("SELECT company_cd, company_name, founded_year FROM company ORDER BY company_cd;")
			if err != nil {
				t.Errorf("failed to query: %v", err)
			}
			defer rows.Close()
			var got []company
			for rows.Next() {
				var companyCD, companyName string
				var foundedYear int
				if err := rows.Scan(&companyCD, &companyName, &foundedYear); err != nil {
					t.Errorf("failed to scan: %v", err)
				}
				got = append(got, company{
					companyCD:   companyCD,
					companyName: companyName,
					foundedYear: foundedYear,
				})
			}

			if diff := cmp.Diff(tt.want, got, cmp.AllowUnexported(company{})); diff != "" {
				t.Errorf("got columns for table(company) mismatch (-want +got):\n%s", diff)
			}

			if err := tx.Rollback(); err != nil {
				t.Fatalf("rollback: %v", err)
			}
		})
	}
}
