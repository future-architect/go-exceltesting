package exceltesting

import "database/sql"

// LoadRaw はGoの値からデータベースにデータを投入します。コミットは行いません。
func LoadRaw(tx *sql.Tx, r LoadRawRequest) error {
	t := &table{
		name:    r.TableName,
		columns: r.Columns,
		data:    r.Values,
	}

	query := t.buildInsertSQL()
	_, err := tx.Exec(query)
	return err
}

// LoadRawRequest はGoの値から直接データベースにデータを投入するための設定です。
type LoadRawRequest struct {
	TableName string
	Columns   []string
	Values    [][]string
}
