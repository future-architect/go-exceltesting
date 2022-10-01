package exceltesting

import (
	"database/sql"
	"github.com/future-architect/go-exceltesting/testonly"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jackc/pgtype"
	_ "github.com/jackc/pgx/v4/stdlib"
)

func Test_exceltesing_Load(t *testing.T) {
	jst, _ := time.LoadLocation("Asia/Tokyo")

	conn := testonly.OpenTestDB(t)
	t.Cleanup(func() { conn.Close() })

	testonly.ExecSQLFile(t, conn, filepath.Join("testdata", "schema", "ddl.sql"))

	tests := []struct {
		name string
		r    LoadRequest
		want []testX
	}{
		{
			name: "inserted excel data",
			r: LoadRequest{
				TargetBookPath: filepath.Join("testdata", "load.xlsx"),
				SheetPrefix:    "normal-",
				IgnoreSheet:    nil,
			},
			want: []testX{
				{
					ID: "test1",
					A:  true,
					B:  []byte("bytea"),
					C:  "a",
					D:  time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
					E:  0.1,
					F:  0.01,
					G:  pgtype.JSON{Bytes: []uint8("{}"), Status: pgtype.Present},
					H:  pgtype.JSONB{Bytes: []uint8("{}"), Status: pgtype.Present},
					I:  pgtype.Inet{IPNet: &net.IPNet{IP: net.ParseIP("0.0.0.0"), Mask: net.IPv4Mask(255, 255, 255, 255)}, Status: pgtype.Present},
					J:  32767,
					K:  2147483647,
					L:  9223372036854775807,
					M:  "00:00:01",
					N:  11111,
					O:  0,
					P:  "test",
					Q:  "01:02:03",
					S:  time.Date(2022, 1, 1, 1, 2, 3, 0, time.UTC),
					T:  time.Date(2022, 1, 1, 1, 2, 3, 0, jst),
					U:  "cee0db76-d69c-4ae3-ae33-5b5970adde48",
					V:  "abc",
					W:  1,
					X:  1,
					Y:  1,
					Z:  1,
				},
			},
		},
		{
			name: "inserted excel data with default value option",
			r: LoadRequest{
				TargetBookPath:                  filepath.Join("testdata", "load.xlsx"),
				SheetPrefix:                     "option-",
				IgnoreSheet:                     nil,
				EnableAutoCompleteNotNullColumn: true,
			},
			want: []testX{
				{
					ID: "test-opt",
					A:  false,
					B:  []byte("0"),
					C:  "x",
					D:  time.Date(0001, 1, 1, 0, 0, 0, 0, time.UTC),
					E:  0,
					F:  0,
					G:  pgtype.JSON{Bytes: []uint8("{}"), Status: pgtype.Present},
					H:  pgtype.JSONB{Bytes: []uint8("{}"), Status: pgtype.Present},
					I:  pgtype.Inet{IPNet: &net.IPNet{IP: net.ParseIP("0.0.0.0"), Mask: net.IPv4Mask(255, 255, 255, 255)}, Status: pgtype.Present},
					J:  0,
					K:  0,
					L:  0,
					M:  "00:00:00",
					N:  0,
					O:  0,
					P:  "x",
					Q:  "00:00:00",
					S:  time.Date(0001, 1, 1, 0, 0, 0, 0, time.UTC),
					T:  time.Date(0001, 1, 1, 0, 0, 0, 0, jst),
					U:  "00000000-0000-0000-0000-000000000000",
					V:  "x",
					W:  1,
					X:  1,
					Y:  1,
					Z:  0,
				},
			},
		},
		{
			name: "inserted excel data using sheet format version2",
			r: LoadRequest{
				TargetBookPath: filepath.Join("testdata", "load_v2.xlsx"),
				SheetPrefix:    "normal-",
				IgnoreSheet:    nil,
			},
			want: []testX{
				{
					ID: "test1",
					A:  true,
					B:  []byte("bytea"),
					C:  "a",
					D:  time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),
					E:  0.1,
					F:  0.01,
					G:  pgtype.JSON{Bytes: []uint8("{}"), Status: pgtype.Present},
					H:  pgtype.JSONB{Bytes: []uint8("{}"), Status: pgtype.Present},
					I:  pgtype.Inet{IPNet: &net.IPNet{IP: net.ParseIP("0.0.0.0"), Mask: net.IPv4Mask(255, 255, 255, 255)}, Status: pgtype.Present},
					J:  32767,
					K:  2147483647,
					L:  9223372036854775807,
					M:  "00:00:01",
					N:  11111,
					O:  0,
					P:  "test",
					Q:  "01:02:03",
					S:  time.Date(2022, 1, 1, 1, 2, 3, 0, time.UTC),
					T:  time.Date(2022, 1, 1, 1, 2, 3, 0, jst),
					U:  "cee0db76-d69c-4ae3-ae33-5b5970adde48",
					V:  "abc",
					W:  1,
					X:  1,
					Y:  1,
					Z:  1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &exceltesing{db: conn}
			e.Load(t, tt.r)

			got, err := getTestX(t, conn)
			if err != nil {
				t.Fatalf("failed to get test_x: %v", err)
			}

			opts := []cmp.Option{
				cmp.AllowUnexported(testX{}),
				cmpopts.IgnoreFields(testX{}, "W", "X", "Y"), // auto increment type
			}
			if diff := cmp.Diff(tt.want, got, opts...); diff != "" {
				t.Errorf("got columns for table(test_x) mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func Test_exceltesing_Compare(t *testing.T) {
	conn := testonly.OpenTestDB(t)
	defer conn.Close()

	testonly.ExecSQLFile(t, conn, filepath.Join("testdata", "schema", "ddl.sql"))

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
				tdb := testonly.OpenTestDB(t)
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
				tdb := testonly.OpenTestDB(t)
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
				tdb := testonly.OpenTestDB(t)
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
				tdb := testonly.OpenTestDB(t)
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

type testX struct {
	ID string
	A  bool
	B  []byte
	C  string
	D  time.Time
	E  float32
	F  float64
	G  pgtype.JSON
	H  pgtype.JSONB
	I  pgtype.Inet
	J  int16
	K  int32
	L  int64
	M  string
	N  float64
	O  int64
	P  string
	Q  string
	S  time.Time
	T  time.Time
	U  string
	V  string
	W  int16
	X  int32
	Y  int64
	Z  int
}

func getTestX(t *testing.T, db *sql.DB) ([]testX, error) {
	t.Helper()

	rows, err := db.Query(`SELECT id, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, s, t, u, v, w, x, y, z FROM test_x ORDER BY id;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []testX
	for rows.Next() {
		var i testX
		if err := rows.Scan(
			&i.ID,
			&i.A,
			&i.B,
			&i.C,
			&i.D,
			&i.E,
			&i.F,
			&i.G,
			&i.H,
			&i.I,
			&i.J,
			&i.K,
			&i.L,
			&i.M,
			&i.N,
			&i.O,
			&i.P,
			&i.Q,
			&i.S,
			&i.T,
			&i.U,
			&i.V,
			&i.W,
			&i.X,
			&i.Y,
			&i.Z,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func Test_exceltesing_DumpCSV(t *testing.T) {
	type args struct {
		t *testing.T
		r DumpRequest
	}
	tests := []struct {
		name string
		args args
		want []string
		got  []string
	}{
		{
			name: "dumped",
			args: args{r: DumpRequest{TargetBookPaths: []string{filepath.Join("testdata", "dump.xlsx")}}},
			want: []string{filepath.Join("testdata", "want_dump_会社.csv")},
			got:  []string{filepath.Join("testdata", "csv", "dump_会社.csv")},
		},
		{
			name: "dumpedWithEmptyFile",
			args: args{r: DumpRequest{TargetBookPaths: []string{filepath.Join("testdata", "dumpWithEmptyFile.xlsx")}}},
			want: []string{filepath.Join("testdata", "want_dumpWithEmptyFile_会社.csv")},
			got:  []string{filepath.Join("testdata", "csv", "dumpWithEmptyFile_Sheet1.csv")},
		},
		{
			name: "dumpWithEmptyFileMultipleSheets",
			args: args{r: DumpRequest{TargetBookPaths: []string{filepath.Join("testdata", "dumpWithEmptyFileMultipleSheets.xlsx")}}},
			want: []string{
				filepath.Join("testdata", "want_dumpWithEmptyFileMultipleSheets_会社1.csv"),
				filepath.Join("testdata", "want_dumpWithEmptyFileMultipleSheets_会社2.csv"),
				filepath.Join("testdata", "want_dumpWithEmptyFileMultipleSheets_会社3.csv"),
			},
			got: []string{
				filepath.Join("testdata", "csv", "dumpWithEmptyFileMultipleSheets_会社1.csv"),
				filepath.Join("testdata", "csv", "dumpWithEmptyFileMultipleSheets_会社2.csv"),
				filepath.Join("testdata", "csv", "dumpWithEmptyFileMultipleSheets_会社3.csv"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &exceltesing{nil}
			e.DumpCSV(t, tt.args.r)

			for i := 0; i < len(tt.want); i++ {
				b1, err := os.ReadFile(tt.want[i])
				if err != nil {
					t.Errorf("read file: %v", tt.want[i])
					continue
				}
				_, err = os.Stat(tt.got[i])
				if os.IsNotExist(err) {
					if reflect.DeepEqual(b1, []byte("")) {
						t.Logf("%v is not found, because it is empty.\n", filepath.Base(tt.got[i]))
					} else {
						t.Errorf("%v is not found, but it must exist.\n", filepath.Base(tt.got[i]))
					}
					continue
				}
				b2, err := os.ReadFile(tt.got[i])
				if err != nil {
					t.Errorf("read file: %v", tt.got[i])
					continue
				}
				if diff := cmp.Diff(b1, b2); diff != "" {
					t.Errorf("file %s and %s is mismatch (-want +got):\n%s", tt.want[i], tt.got[i], diff)
				}
			}
		})
	}
}
