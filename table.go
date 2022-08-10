package exceltesting

import (
	"fmt"
	"strings"
)

const (
	Char   = "C"
	Number = "N"
)

// table は投入対象のテーブルです
type table struct {
	name        string
	columnsType []string
	columns     []string
	data        [][]string
}

// buildSQL はINSERTステートメントを作成します
func (t *table) buildInsertSQL() string {
	var valueSQLExp string
	for j, row := range t.data {
		rowSQLExp := "("
		for i, cell := range row {
			if i >= 1 {
				rowSQLExp = fmt.Sprintf("%s, ", rowSQLExp)
			}
			if cell == "" {
				rowSQLExp += "null"
			} else if t.columnsType[i] == Number {
				rowSQLExp += cell
			} else {
				rowSQLExp += fmt.Sprintf("'%s'", cell)
			}
		}
		rowSQLExp += ")"
		if j == 0 {
			valueSQLExp = rowSQLExp
			continue
		}
		valueSQLExp = valueSQLExp + "," + rowSQLExp
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES%s;\n", t.name, t.sqlColumnExp(), valueSQLExp)
	return sql
}

func (t *table) sqlColumnExp() string {
	return strings.Join(t.columns, ",")
}
