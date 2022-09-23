package cli

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/xuri/excelize/v2"
	"golang.org/x/exp/slices"
	"os"
	"os/signal"
	"strings"
	"unicode/utf8"
)

const DefaultColumnCnt = 32

var query = `SELECT tab.relname        AS table_name
				 , tabdesc.description AS table_description
				 , col.column_name
				 , coldesc.description AS column_description
				 , col.data_type
				 , col.is_nullable
				 , col.column_default
			FROM pg_stat_user_tables tab
					 INNER JOIN pg_description tabdesc
								ON tab.relid = tabdesc.objoid
									AND tabdesc.objsubid = '0'
					 INNER JOIN information_schema.columns col
								ON tab.relname = col.table_name
									AND tab.schemaname = current_schema()
					 INNER JOIN pg_description coldesc
								ON tab.relid = coldesc.objoid
									AND col.ordinal_position = coldesc.objsubid
			WHERE exists(select 1 FROM tmp_exceltesting_dump_table_name WHERE tab.relname = name)
			ORDER BY tab.relname
				   , col.ordinal_position
			;
`

var queryAll = `SELECT tab.relname        AS table_name
				 , tabdesc.description AS table_description
				 , col.column_name
				 , coldesc.description AS column_description
				 , col.data_type
				 , col.is_nullable
				 , col.column_default
			FROM pg_stat_user_tables tab
					 INNER JOIN pg_description tabdesc
								ON tab.relid = tabdesc.objoid
									AND tabdesc.objsubid = '0'
					 INNER JOIN information_schema.columns col
								ON tab.relname = col.table_name
									AND tab.schemaname = current_schema()
					 INNER JOIN pg_description coldesc
								ON tab.relid = coldesc.objoid
									AND col.ordinal_position = coldesc.objsubid
			ORDER BY tab.relname
				   , col.ordinal_position
			;
`

type TableDef struct {
	TableName string
	Comment   string
	Columns   []ColumnDef
}

type ColumnDef struct {
	Name         string
	Comment      string
	DataType     string
	Constraint   string
	DefaultValue string
}

func dump(dbSource, targetFile string, tableNameArg, systemColumnArg string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var tableNames []string
	if len(tableNameArg) > 0 {
		tableNames = strings.Split(tableNameArg, ",")
	}

	var systemColumn []string
	if len(systemColumnArg) > 0 {
		systemColumn = strings.Split(systemColumnArg, ",")
	}

	conn, err := pgxpool.Connect(ctx, dbSource)
	if err != nil {
		return fmt.Errorf("pgxpool connect: %w", err)
	}

	if len(tableNames) > 0 {
		_, err = conn.Exec(ctx, `CREATE TEMPORARY TABLE tmp_exceltesting_dump_table_name(name varchar(256))`)
		if err != nil {
			return fmt.Errorf("cannot temporary table: %w", err)
		}

		batch := &pgx.Batch{}
		for _, v := range tableNames {
			batch.Queue("insert into tmp_exceltesting_dump_table_name values($1)", v)
		}
		if err := conn.SendBatch(ctx, batch).Close(); err != nil {
			return fmt.Errorf("batch insert: %w", err)
		}
	}

	defs, err := selectTabColumnDef(ctx, conn, tableNames)
	if err != nil {
		return err
	}

	if len(defs) == 0 {
		fmt.Println("table not found")
		return nil
	}

	f := excelize.NewFile()
	f.DeleteSheet("Sheet1")

	var (
		rowHeaderStyle, _ = f.NewStyle(&excelize.Style{
			Border: []excelize.Border{
				{Type: "top", Style: 1, Color: "000000"},
				{Type: "left", Style: 1, Color: "000000"},
				{Type: "right", Style: 1, Color: "000000"},
				{Type: "bottom", Style: 1, Color: "000000"},
			},
			Fill: excelize.Fill{Type: "pattern", Color: []string{"#D9D9D9"}, Pattern: 1},
		})

		columnHeaderStyle, _ = f.NewStyle(&excelize.Style{
			Border: []excelize.Border{
				{Type: "top", Style: 1, Color: "#000000"},
				{Type: "left", Style: 1, Color: "#000000"},
				{Type: "right", Style: 1, Color: "#000000"},
				{Type: "bottom", Style: 1, Color: "#000000"},
			},
			Fill: excelize.Fill{Type: "pattern", Color: []string{"#FCD5B4"}, Pattern: 1},
		})

		columnHeaderSystemStyle, _ = f.NewStyle(&excelize.Style{
			Border: []excelize.Border{
				{Type: "top", Style: 1, Color: "000000"},
				{Type: "left", Style: 1, Color: "000000"},
				{Type: "right", Style: 1, Color: "000000"},
				{Type: "bottom", Style: 1, Color: "000000"},
			},
			Fill: excelize.Fill{Type: "pattern", Color: []string{"#BFBFBF"}, Pattern: 1},
		})

		rowStyle, _ = f.NewStyle(&excelize.Style{
			Border: []excelize.Border{
				{Type: "top", Style: 1, Color: "000000"},
				{Type: "left", Style: 1, Color: "000000"},
				{Type: "right", Style: 1, Color: "000000"},
				{Type: "bottom", Style: 1, Color: "000000"},
			},
		})
	)

	for i, tableDef := range defs {
		tableName := tableDef.Comment
		index := f.NewSheet(tableName)

		if i == 0 {
			f.SetActiveSheet(index)
		}

		_ = f.SetCellValue(tableName, "A1", tableDef.Comment)
		_ = f.SetCellValue(tableName, "A2", tableDef.TableName)
		_ = f.SetCellValue(tableName, "A8", "項目名")
		_ = f.SetCellValue(tableName, "A9", "項目物理名")

		_ = f.SetColWidth(tableName, "A", "A", 12.86)
		_ = f.SetCellStyle(tableName, "A8", "A9", rowHeaderStyle)

		for i, columnDef := range tableDef.Columns {
			axisComment, _ := excelize.CoordinatesToCellName(2+i, 8)
			axisName, _ := excelize.CoordinatesToCellName(2+i, 9)

			_ = f.SetCellValue(tableName, axisComment, columnDef.Comment)
			_ = f.SetCellValue(tableName, axisName, columnDef.Name)

			currentCol, _ := excelize.ColumnNumberToName(2 + i)

			width := utf8.RuneCountInString(columnDef.Comment) * 2
			if width < utf8.RuneCountInString(columnDef.Name) {
				width = utf8.RuneCountInString(columnDef.Name)
			}
			width += 2 //  + 2 for margin

			_ = f.SetColWidth(tableName, currentCol, currentCol, float64(width))

			style := columnHeaderStyle
			if slices.Contains(systemColumn, columnDef.Name) {
				style = columnHeaderSystemStyle
			}
			_ = f.SetCellStyle(tableName, axisComment, axisName, style)

		}

		// Add 3 empty row
		vCell, _ := excelize.CoordinatesToCellName(1+len(tableDef.Columns), 12)
		_ = f.SetCellStyle(tableName, "A10", vCell, rowStyle)
		_ = f.SetCellValue(tableName, "A10", "1")
		_ = f.SetCellValue(tableName, "A11", "2")
		_ = f.SetCellValue(tableName, "A12", "3")

	}

	if err := f.SaveAs(targetFile); err != nil {
		return fmt.Errorf("dump result save: %w", err)
	}

	return nil
}

func selectTabColumnDef(ctx context.Context, conn *pgxpool.Pool, tableNames []string) ([]TableDef, error) {
	sql := query
	if len(tableNames) == 0 {
		sql = queryAll
	}

	rows, err := conn.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("db access: %w", err)
	}
	defer rows.Close()

	var (
		tableDefs    = make([]TableDef, 0, len(tableNames))
		columnDefs   []ColumnDef
		currentTable string
	)

	for rows.Next() {
		g := make([]any, 7) // TableName, Comment, ColumnName, ColumnComment, DataType, ColumnConstraint, DefaultValue
		for i := range g {
			g[i] = &g[i]
		}
		if err := rows.Scan(g...); err != nil {
			return nil, err
		}

		if currentTable != Str(g[0]) {
			columnDefs = make([]ColumnDef, 0, DefaultColumnCnt)

			tableDefs = append(tableDefs, TableDef{
				TableName: Str(g[0]),
				Comment:   Str(g[1]),
			})
			currentTable = Str(g[0])
		}

		columnDefs = append(columnDefs, ColumnDef{
			Name:         Str(g[2]),
			Comment:      Str(g[3]),
			DataType:     Str(g[4]),
			Constraint:   Str(g[5]),
			DefaultValue: Str(g[6]),
		})
		tableDefs[len(tableDefs)-1].Columns = columnDefs

	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tableDefs, nil
}

func Str(a any) string {
	if a == nil {
		return ""
	}
	return a.(string)
}
