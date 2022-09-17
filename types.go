package exceltesting

import (
	"fmt"
	"net"
	"time"
)

var (
	// TODO: go-exceltesingの実装上、一部の型で空文字がNULLとなるため "x" を明示している。本来は空文字が望ましい
	//
	// また幾何データ型などいくつかの型はサポートしていない
	dbType2GoDefaultValue = map[string]any{
		"bool":        false,
		"bit":         0,
		"bytea":       0,
		"bpchar":      "x",
		"char":        "",
		"date":        time.Time{}.Format("2006-01-02 15:04:05"),
		"float4":      0,
		"float8":      0,
		"json":        "{}",
		"jsonb":       "{}",
		"inet":        net.IPv4zero,
		"int2":        0,
		"int4":        0,
		"int8":        0,
		"interval":    0,
		"numeric":     0,
		"oid":         0,
		"text":        "x",
		"time":        time.Time{}.Format("2006-01-02 15:04:05"),
		"timestamp":   time.Time{}.Format("2006-01-02 15:04:05"),
		"timestamptz": time.Time{}.Format("2006-01-02 15:04:05"),
		"uuid":        "00000000-0000-0000-0000-000000000000",
		"varchar":     "x",
	}
)

func defaultValueFromDBType(dbType string) string {
	if s, exists := dbType2GoDefaultValue[dbType]; exists {
		return fmt.Sprintf("%v", s)
	}
	return ""
}
