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

// DeepCopy generates a deep copy of table
func (t table) DeepCopy() table {
	var cp table = t
	if t.columns != nil {
		cp.columns = make([]string, len(t.columns))
		copy(cp.columns, t.columns)
	}
	if t.data != nil {
		cp.data = make([][]string, len(t.data))
		copy(cp.data, t.data)
		for i2 := range t.data {
			if t.data[i2] != nil {
				cp.data[i2] = make([]string, len(t.data[i2]))
				copy(cp.data[i2], t.data[i2])
			}
		}
	}
	return cp
}
