package exceltesting

import (
	"database/sql"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v4/stdlib"
)

func Test_exceltesing_Load(t *testing.T) {

	conn := openTestDB(t)
	defer conn.Close()

	execSQLFile(t, conn, filepath.Join("testdata", "ddl.sql"))

	type fields struct {
		db *sql.DB
	}
	type args struct {
		t *testing.T
		r LoadRequest
	}
	type company struct {
		companyCD   string
		companyName string
		foundedYear int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []company
		wantErr bool
	}{
		{
			name:   "inserted excel data",
			fields: fields{db: conn},
			args: args{t, LoadRequest{
				TargetBookPath: filepath.Join("testdata", "load.xlsx"),
				SheetPrefix:    "",
				IgnoreSheet:    nil,
			}},
			want: []company{
				{companyCD: "00001", companyName: "Future", foundedYear: 1989},
				{companyCD: "00002", companyName: "YDC", foundedYear: 1972},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &exceltesing{
				db: tt.fields.db,
			}
			if err := e.Load(tt.args.t, tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("Load() error = %v, wantErr %v", err, tt.wantErr)
			}

			rows, err := conn.Query("SELECT company_cd, company_name, founded_year, created_at, updated_at, revision FROM company ORDER BY company_cd;")
			if err != nil {
				t.Errorf("failed to query: %v", err)
			}
			defer rows.Close()
			var got []company
			for rows.Next() {
				var companyCD, companyName string
				var foundedYear, revision int
				var createdAt, updatedAt time.Time
				if err := rows.Scan(&companyCD, &companyName, &foundedYear, &createdAt, &updatedAt, &revision); err != nil {
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
		})
	}
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	const (
		DBUser = "excellocal"
		DBPass = "password"
		DBHost = "localhost"
		DBPort = "15432"
		DBName = "excellocal"
	)

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", DBUser, DBPass, DBHost, DBPort, DBName)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	return db
}

func execSQLFile(t *testing.T, db *sql.DB, filePath string) {
	t.Helper()

	b, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("failed to read file: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to start transaction: %v", err)
	}

	queries := strings.Split(string(b), ";")
	for _, query := range queries {

		q := strings.TrimSpace(query)
		if q == "" {
			continue
		}
		if _, err = tx.Exec(q); err != nil {
			t.Errorf("failed to exec sql, query = %s: %v", q, err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Errorf("failed to commit: %v", err)
	}
}
