package exceltesting

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/xuri/excelize/v2"
	"golang.org/x/exp/slices"
)

const (
	tempTablePrefix = "temp_"
)

// New はExcelからテストデータを投入できる構造体のファクトリ関数です
func New(db *sql.DB) *exceltesing {
	if db == nil {
		panic("db is nil")
	}
	return &exceltesing{db}
}

type exceltesing struct {
	db *sql.DB
}

// Load はExcelのBookを読み込み、データベースに事前データを投入します。
func (e *exceltesing) Load(t *testing.T, r LoadRequest) {
	t.Helper()
	ctx := context.TODO()
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("exceltesing: start transaction: %v", err)
	}
	defer tx.Rollback()

	f, err := excelize.OpenFile(r.TargetBookPath)
	if err != nil {
		t.Fatalf("exceltesing: excelize.OpenFile: %v", err)
	}
	defer f.Close()
	for _, sheet := range f.GetSheetList() {
		if slices.Contains(r.IgnoreSheet, sheet) {
			continue
		}
		if strings.HasPrefix(sheet, r.SheetPrefix) {
			table, err := e.loadExcelSheet(f, sheet)
			if err != nil {
				t.Fatalf("exceltesing: load excel sheet, sheet = %s: %v", sheet, err)
			}

			if err := e.insertData(table); err != nil {
				t.Fatalf("exceltesing: insert data to %s: %v", table.name, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("exceltesing: commit: %v", err)
	}
}

// Compare はExcelの期待結果と実際にデータベースに登録されているデータを比較して
// 差分がある場合は報告します。
// 値の比較は go-cmp (https://github.com/google/go-cmp) を利用しています。
func (e *exceltesing) Compare(t *testing.T, r CompareRequest) bool {
	t.Helper()

	tx, err := e.db.Begin()
	if err != nil {
		t.Errorf("exceltesting: failed to start transaction: %v", err)
		return false
	}
	defer tx.Rollback()

	f, err := excelize.OpenFile(r.TargetBookPath)
	if err != nil {
		t.Errorf("exceltesting: failed to open excel file: %v", err)
		return false
	}
	defer f.Close()

	equal := true
	for _, sheet := range f.GetSheetList() {
		if slices.Contains(r.IgnoreSheet, sheet) {
			continue
		}
		if strings.HasPrefix(sheet, r.SheetPrefix) {
			table, err := e.loadExcelSheet(f, sheet)
			if err != nil {
				t.Errorf("exceltesting: failed to load excel sheet, sheet = %s: %v", sheet, err)
				equal = false
				continue
			}
			got, want, err := e.comparativeSource(table, &r)
			if err != nil {
				t.Errorf("exceltesting: failed to fetch comparative source: %v", err)
				equal = false
				continue
			}

			opts := []cmp.Option{
				cmpopts.EquateNaNs(),
				cmp.Comparer(func(x, y *big.Int) bool {
					return x.Cmp(y) == 0
				}),
				cmp.AllowUnexported(x{}),
			}
			if diff := cmp.Diff(want, got, opts...); diff != "" {
				t.Errorf("table(%s) mismatch (-want +got):\n%s", table.name, diff)
				equal = false
				continue
			}
		}
	}
	return equal
}

// DumpCSV はExcelブックの全シートをCSVにDumpします。
//
// DumpRequest.TargetBookPaths で指定されたパスに csv ディレクトリを作成し、
// csvディレクトリにDumpしたCSVファイルを作成します。
func (e *exceltesing) DumpCSV(t *testing.T, r DumpRequest) {
	// fmt.Println("debug")
	for _, path := range r.TargetBookPaths {
		ef, err := excelize.OpenFile(path)
		if err != nil {
			t.Errorf("exceltesing: excelize.OpenFile: %v", err)
			return
		}
		defer ef.Close()
		for _, sheet := range ef.GetSheetList() {
			rows, err := ef.Rows(sheet)
			if err != nil {
				t.Errorf("exceltesing: get rows: %v", err)
				return
			}
			outDir := filepath.Join(filepath.Dir(path), "csv")
			if _, err := os.Stat(outDir); os.IsNotExist(err) {
				if err := os.Mkdir(outDir, 0755); err != nil {
					t.Errorf("exceltesing: create directory: %v", err)
					return
				}
			}
			//outFileName = dump_会社_csv
			outFileName := fmt.Sprintf("%s_%s.csv", getFileNameWithoutExt(path), sheet)
			// fmt.Println(outFileName)
			// fmt.Println("###########################")

			f, err := os.Create(filepath.Join(outDir, outFileName))
			if err != nil {
				t.Errorf("exceltesing: create file: %v", err)
				return
			}
			defer f.Close()

			writer := csv.NewWriter(f)
			defer writer.Flush()

			// dumpedNew Case
			//ここ動かすとuint何も書きこまれなくなる
			// rowTot := len(rows)
			// for rows.Next() {
			// 	rowTot++
			// }
			// fmt.Println(rowTot)

			// if rowTot == 8 { //#7 コーナーケース
			// 	return
			// }

			rowCnt := 0
			for rows.Next() {
				cols, err := rows.Columns()
				if err != nil {
					t.Errorf("exceltesing: rows.Columns: %v", err)
					return
				}

				if 3 <= rowCnt && rowCnt <= 6 {
					rowCnt++
					continue
				}
				if rowCnt >= 7 {
					cols = cols[1:]
				}

				if err := writer.Write(cols); err != nil {
					t.Errorf("exceltesing: writer.Write(): %v", err)
					return
				}
				rowCnt++
			}
		}
	}
}

// LoadRequest はExcelからデータを投入するための設定です。
type LoadRequest struct {
	// ロード対象Excelパス
	TargetBookPath string
	// ロード対象シートプレフィックス
	SheetPrefix string
	// 無視シート
	IgnoreSheet []string
}

// CompareRequest はExcelとデータベースの値を比較するための設定です。
type CompareRequest struct {
	// ロード対象Excelパス
	TargetBookPath string
	// ロード対象シートプレフィックス
	SheetPrefix string
	// 無視シート
	IgnoreSheet []string
	// 無視するカラム名
	IgnoreColumns []string
}

// DumpRequest はExcelをCSVにDumpするための設定です。
type DumpRequest struct {
	// dump対象Excelパス
	TargetBookPaths []string
}

func (e *exceltesing) loadExcelSheet(f *excelize.File, targetSheet string) (*table, error) {
	const tableNmCell = "A2"
	const columnTypeDefineRowNum = 6
	const columnDefineRowNum = 9

	tableNm, err := f.GetCellValue(targetSheet, tableNmCell)
	if err != nil {
		return nil, fmt.Errorf("get cell value: %w", err)
	}
	if tableNm == "" {
		return nil, fmt.Errorf("table name is empty")
	}

	rows, err := f.GetRows(targetSheet)
	if err != nil {
		return nil, fmt.Errorf("get row: %w", err)
	}

	columnsType := getExcelColumns(rows, columnTypeDefineRowNum)
	columns := getExcelColumns(rows, columnDefineRowNum)
	data, err := getExcelData(rows, columnDefineRowNum)
	if err != nil {
		return nil, fmt.Errorf("get excel data: %w", err)
	}

	return &table{
		name:        tableNm,
		columnsType: columnsType,
		columns:     columns,
		data:        data,
	}, nil
}

// comparativeSource はデータベースに格納されている実際のテーブルの値と、Excelから取得した期待する結果の値を
// 比較可能な値として取得します。
func (e *exceltesing) comparativeSource(t *table, req *CompareRequest) ([][]x, [][]x, error) {
	var pk string
	err := e.db.QueryRow(getPrimaryKeyQuery, t.name).Scan(&pk)
	if err != nil {
		return nil, nil, err
	}

	q1, cs, err := e.buildComparingQuery(t, pk, req)
	if err != nil {
		return nil, nil, err
	}

	got, err := e.getComparingData(q1, len(cs))
	if err != nil {
		return nil, nil, err
	}

	if err := e.createTempTable(t.name); err != nil {
		return nil, nil, fmt.Errorf("create temporary table: %w", err)
	}

	c := t.DeepCopy()
	c.name = tempTablePrefix + c.name
	if err := e.insertData(&c); err != nil {
		return nil, nil, fmt.Errorf("insert data to %s: %w", c.name, err)
	}

	q2, _, err := e.buildComparingQuery(&c, pk, req)
	if err != nil {
		return nil, nil, err
	}

	want, err := e.getComparingData(q2, len(cs))
	if err != nil {
		return nil, nil, err
	}

	return convert(got, cs), convert(want, cs), nil
}

func (e *exceltesing) insertData(t *table) error {
	if _, err := e.db.ExecContext(context.TODO(), fmt.Sprintf(`TRUNCATE TABLE %s;`, t.name)); err != nil {
		return fmt.Errorf("truncate table %s: %w", t.name, err)
	}

	if len(t.data) == 0 {
		return nil
	}

	sql := t.buildInsertSQL()
	_, err := e.db.ExecContext(context.TODO(), sql)
	return err
}

func (e *exceltesing) createTempTable(tableName string) error {
	query := fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s AS SELECT * FROM %s WHERE 0 = 1;", tempTablePrefix+tableName, tableName)
	_, err := e.db.Exec(query)
	return err
}

func (e *exceltesing) buildComparingQuery(t *table, primaryKey string, req *CompareRequest) (string, []string, error) {
	columns := make([]string, 0, len(t.columns))
	for _, c := range t.columns {
		if slices.Contains(req.IgnoreColumns, c) {
			continue
		}
		columns = append(columns, c)
	}

	var sql string
	sql += "SELECT "
	for i, column := range columns {
		if i > 0 {
			sql += ", "
		}
		sql += column
	}
	sql += fmt.Sprintf(" FROM %s ORDER BY %s;", t.name, primaryKey)
	return sql, columns, nil
}

func (e *exceltesing) getComparingData(q string, len int) ([][]any, error) {
	var got [][]any

	rows, err := e.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		g := make([]any, len)
		for i := range g {
			g[i] = &g[i]
		}
		if err := rows.Scan(g...); err != nil {
			return nil, err
		}
		got = append(got, g)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return got, nil
}

func getExcelColumns(rows [][]string, rowNum int) []string {
	columns := make([]string, 0, len(rows[rowNum-1]))

	// 1列目は説明項目で値と無関係のため読み飛ばす
	for _, cell := range rows[rowNum-1][1:] {
		cell = strings.Trim(strings.Trim(cell, "　"), " ")
		if cell == "" {
			// 空カラムのskip
			continue
		}
		columns = append(columns, cell)
	}

	return columns
}

func getExcelData(rows [][]string, rowNum int) ([][]string, error) {
	columns := getExcelColumns(rows, rowNum)

	var data [][]string
	for i, row := range rows[rowNum:] {
		rowStr := ""
		for _, cell := range row {
			rowStr = rowStr + strings.Trim(strings.Trim(cell, "　"), " ")
		}
		if rowStr == "" {
			continue
		}
		if len(row) < len(columns) {
			return nil, fmt.Errorf("data size is smaller than defines. columns: %s row: %s data: %+v\n", fmt.Sprint(len(row)), fmt.Sprint(i+1), row)
		}
		// 1列目が空ならskip
		if row[0] == "" {
			continue
		}
		data = append(data, row[1:len(columns)+1])
	}
	return data, nil
}

func getFileNameWithoutExt(path string) string {
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	return base[0 : len(base)-len(ext)]
}

// x はDBの値にカラムを付与した構造体です。
// go-cmp で結果と期待値を比較するときに、値に差分があったときにカラムも表示するために column を付与しています。
type x struct {
	column any
	value  any
}

func convert(vs [][]any, columns []string) [][]x {
	resp := make([][]x, len(vs))
	for i, r := range vs {
		for j, v := range r {
			resp[i] = append(resp[i], x{
				column: columns[j],
				value:  v,
			})
		}
	}
	return resp
}
