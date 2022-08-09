package exceltesting

import (
	"database/sql"
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
func (e *exceltesing) Load(t *testing.T, r LoadRequest) error {
	return nil
}

// Compare はExcelの期待結果と実際にデータベースに登録されているデータを比較して
// 差分がある場合は報告します。
// 値の比較は go-cmp (https://github.com/google/go-cmp) を利用しています。
func (e *exceltesing) Compare(t *testing.T, r CompareRequest) error {
	return nil
}

// DumpCSV はExcelブックの全シートをCSVにDumpします。
func (e *exceltesing) DumpCSV(t *testing.T, req DumpRequest) error {
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

// table は投入対象のテーブルです
type table struct {
	name    string
	columns []string
	data    [][]string
}
