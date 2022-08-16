package exceltesting

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
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
		name   string
		fields fields
		args   args
		want   []company
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
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &exceltesing{
				db: tt.fields.db,
			}

			e.Load(tt.args.t, tt.args.r)

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

func Test_exceltesing_Compare(t *testing.T) {
	conn := openTestDB(t)
	defer conn.Close()

	execSQLFile(t, conn, filepath.Join("testdata", "ddl.sql"))

	// Even if there is a difference in Compare(), t.Errorf() prevents the test from failing.
	mockT := new(testing.T)

	tests := []struct {
		name     string
		input    func(t *testing.T)
		wantFile string
		equal    bool
	}{
		{
			name: "equal",
			input: func(t *testing.T) {
				t.Helper()
				tdb := openTestDB(t)
				defer tdb.Close()
				if _, err := tdb.Exec(`TRUNCATE company;`); err != nil {
					t.Fatal(err)
				}
				if _, err := tdb.Exec(`INSERT INTO company (company_cd,company_name,founded_year,created_at,updated_at,revision)
					VALUES ('00001','Future',1989,current_timestamp,current_timestamp,1),('00002','YDC',1972,current_timestamp,current_timestamp,1);`); err != nil {
					t.Fatal(err)
				}
			},
			wantFile: filepath.Join("testdata", "compare.xlsx"),
			equal:    true,
		},
		{
			name: "diff",
			input: func(t *testing.T) {
				t.Helper()
				tdb := openTestDB(t)
				defer tdb.Close()
				if _, err := tdb.Exec(`TRUNCATE company;`); err != nil {
					t.Fatal(err)
				}
				if _, err := tdb.Exec(`INSERT INTO company (company_cd,company_name,founded_year,created_at,updated_at,revision)
					VALUES ('00001','Future',9891,current_timestamp,current_timestamp,1),('00002','YDC',2791,current_timestamp,current_timestamp,2);`); err != nil {
					t.Fatal(err)
				}
			},
			wantFile: filepath.Join("testdata", "compare.xlsx"),
			equal:    false,
		},
		{
			name: "fewer records of results",
			input: func(t *testing.T) {
				t.Helper()
				tdb := openTestDB(t)
				defer tdb.Close()
				if _, err := tdb.Exec(`TRUNCATE company;`); err != nil {
					t.Fatal(err)
				}
				if _, err := tdb.Exec(`INSERT INTO company (company_cd,company_name,founded_year,created_at,updated_at,revision)
					VALUES ('00001','Future',1989,current_timestamp,current_timestamp,1);`); err != nil {
					t.Fatal(err)
				}
			},
			wantFile: filepath.Join("testdata", "compare.xlsx"),
			equal:    false,
		},
		{
			name: "many records of results",
			input: func(t *testing.T) {
				t.Helper()
				tdb := openTestDB(t)
				defer tdb.Close()
				if _, err := tdb.Exec(`TRUNCATE company;`); err != nil {
					t.Fatal(err)
				}
				if _, err := tdb.Exec(`INSERT INTO company (company_cd,company_name,founded_year,created_at,updated_at,revision)
					VALUES ('00001','Future',1989,current_timestamp,current_timestamp,1),('00002','YDC',1972,current_timestamp,current_timestamp,1),('00003','FutureOne',2002,current_timestamp,current_timestamp,1);`); err != nil {
					t.Fatal(err)
				}
			},
			wantFile: filepath.Join("testdata", "compare.xlsx"),
			equal:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.input(t)

			e := New(conn)
			got := e.Compare(mockT, CompareRequest{
				TargetBookPath: filepath.Join("testdata", "compare.xlsx"),
				SheetPrefix:    "",
				IgnoreSheet:    nil,
				IgnoreColumns:  []string{"created_at", "updated_at"},
			})

			if got != tt.equal {
				t.Errorf("Compare() should return %v but %v", tt.equal, got)
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
	defer tx.Rollback()

	queries := strings.Split(string(b), ";")
	for _, query := range queries {

		q := strings.TrimSpace(query)
		if q == "" {
			continue
		}
		if _, err = tx.Exec(q); err != nil {
			t.Fatalf("failed to exec sql, query = %s: %v", q, err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}
}

func Test_exceltesing_DumpCSV(t *testing.T) {
	type args struct {
		t *testing.T
		r DumpRequest
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "dumped",
			args: args{r: DumpRequest{TargetBookPaths: []string{filepath.Join("testdata", "dump.xlsx")}}},
			want: filepath.Join("testdata", "want_dump_会社.csv"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &exceltesing{nil}
			e.DumpCSV(t, tt.args.r)

			got := filepath.Join("testdata", "csv", "dump_会社.csv")

			b1, err := os.ReadFile(tt.want)
			if err != nil {
				t.Errorf("read file: %v", tt.want)
				return
			}
			b2, err := os.ReadFile(got)
			if err != nil {
				t.Errorf("read file: %v", got)
				return
			}
			if diff := cmp.Diff(b1, b2); diff != "" {
				t.Errorf("file %s and %s is mismatch (-want +got):\n%s", tt.want, got, diff)
			}
		})
	}
}
