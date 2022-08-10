package exceltesting

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/xuri/excelize/v2"
	"golang.org/x/exp/slices"
	"strings"
	"testing"
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
func (e *exceltesing) Compare(t *testing.T, r CompareRequest) error {
	return nil
}

// DumpCSV はExcelブックの全シートをCSVにDumpします。
func (e *exceltesing) DumpCSV(t *testing.T, r DumpRequest) error {
	return nil
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
	TargetBookPath string
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
