package cli

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/xuri/excelize/v2"
	"golang.org/x/exp/slices"
	"os"
	"os/signal"
	"strings"
	"time"
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
					 LEFT OUTER JOIN pg_description tabdesc
								ON tab.relid = tabdesc.objoid
									AND tabdesc.objsubid = '0'
					 LEFT OUTER JOIN information_schema.columns col
								ON tab.relname = col.table_name
									AND tab.schemaname = current_schema()
					 LEFT OUTER JOIN pg_description coldesc
								ON tab.relid = coldesc.objoid
									AND col.ordinal_position = coldesc.objsubid
			         LEFT OUTER JOIN pg_class pc
                        ON tab.relid = pc.oid
			WHERE exists(select 1 FROM tmp_exceltesting_dump_table_name WHERE tab.relname = name)
				AND	tab.schemaname = current_schema()
				AND col.table_schema = current_schema()
				AND pc.relispartition = false
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
					 LEFT OUTER JOIN pg_description tabdesc
								ON tab.relid = tabdesc.objoid
									AND tabdesc.objsubid = '0'
					 LEFT OUTER JOIN information_schema.columns col
								ON tab.relname = col.table_name
									AND tab.schemaname = current_schema()
					 LEFT OUTER JOIN pg_description coldesc
								ON tab.relid = coldesc.objoid
									AND col.ordinal_position = coldesc.objsubid
			         LEFT OUTER JOIN pg_class pc
                        ON tab.relid = pc.oid
			WHERE
					tab.schemaname = current_schema()
				AND col.table_schema = current_schema()
			    AND pc.relispartition = false
			ORDER BY tab.relname
				   , col.ordinal_position
			;
`

type TableDef struct {
	Name    string
	Comment string
	Columns []ColumnDef
}

type ColumnDef struct {
	Name         string
	Comment      string
	DataType     string
	Constraint   string
	DefaultValue string
}

func Dump(dbSource, targetFile, tableNameArg, systemColumnArg string, maxDumpSize int) error {
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
		return errors.New("table not found")
	}

	f := excelize.NewFile()

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
		sheetName := tableDef.Comment
		if len(sheetName) == 0 {
			sheetName = tableDef.Name
		}

		index := f.NewSheet(sheetName)

		if i == 0 {
			f.SetActiveSheet(index)
		}

		_ = f.SetCellValue(sheetName, "A1", tableDef.Comment)
		_ = f.SetCellValue(sheetName, "A2", tableDef.Name)
		_ = f.SetCellValue(sheetName, "A3", "version")
		_ = f.SetCellValue(sheetName, "B3", "2.0")
		_ = f.SetCellValue(sheetName, "A5", "項目名")
		_ = f.SetCellValue(sheetName, "A6", "項目物理名")

		_ = f.SetColWidth(sheetName, "A", "A", 12.86)
		_ = f.SetCellStyle(sheetName, "A5", "A6", rowHeaderStyle)

		for i, columnDef := range tableDef.Columns {
			axisComment, _ := excelize.CoordinatesToCellName(2+i, 5)
			axisName, _ := excelize.CoordinatesToCellName(2+i, 6)

			_ = f.SetCellValue(sheetName, axisComment, columnDef.Comment)
			_ = f.SetCellValue(sheetName, axisName, columnDef.Name)

			currentCol, _ := excelize.ColumnNumberToName(2 + i)

			width := utf8.RuneCountInString(columnDef.Comment) * 2
			if width < utf8.RuneCountInString(columnDef.Name) {
				width = utf8.RuneCountInString(columnDef.Name)
			}
			width += 2 //  + 2 for margin

			_ = f.SetColWidth(sheetName, currentCol, currentCol, float64(width))

			style := columnHeaderStyle
			if slices.Contains(systemColumn, columnDef.Name) {
				style = columnHeaderSystemStyle
			}
			_ = f.SetCellStyle(sheetName, axisComment, axisName, style)
		}

		records, err := selectExistsRecords(ctx, conn, tableDef, maxDumpSize)
		if err != nil {
			return fmt.Errorf("select exists records: %w", err)
		}

		// Add 3 empty row
		vCell, _ := excelize.CoordinatesToCellName(1+len(tableDef.Columns), 12)
		_ = f.SetCellStyle(sheetName, "A7", vCell, rowStyle)
		_ = f.SetCellValue(sheetName, "A7", "1")
		_ = f.SetCellValue(sheetName, "A8", "2")
		_ = f.SetCellValue(sheetName, "A9", "3")

		// データレコードがあれば上書き
		if len(records) > 0 {

			// 枠線などのスタイルを設定
			vCell, _ := excelize.CoordinatesToCellName(len(records[0])+1, len(records)+9) // 9 is data record start position
			_ = f.SetCellStyle(sheetName, "A10", vCell, rowStyle)

			for i, record := range records {
				rowNum := 7 + i

				vCell, _ := excelize.CoordinatesToCellName(1, rowNum)
				_ = f.SetCellValue(sheetName, vCell, fmt.Sprint(i+1))

				for j, cell := range record {
					colNum := j + 2
					vCell, _ := excelize.CoordinatesToCellName(colNum, rowNum)
					_ = f.SetCellValue(sheetName, vCell, fmtCell(cell))
				}
			}
		}
	}

	f.DeleteSheet("Sheet1")

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
		g := make([]any, 7) // Name, Comment, ColumnName, ColumnComment, DataType, ColumnConstraint, DefaultValue
		for i := range g {
			g[i] = &g[i]
		}
		if err := rows.Scan(g...); err != nil {
			return nil, err
		}

		if currentTable != Str(g[0]) {
			columnDefs = make([]ColumnDef, 0, DefaultColumnCnt)

			tableDefs = append(tableDefs, TableDef{
				Name:    Str(g[0]),
				Comment: Str(g[1]),
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

func selectExistsRecords(ctx context.Context, conn *pgxpool.Pool, tableDef TableDef, maxDumpSize int) ([][]any, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(`select * from %s limit %d`, tableDef.Name, maxDumpSize))
	if err != nil {
		return nil, fmt.Errorf("db access: %w", err)
	}
	defer rows.Close()

	dataRecords := make([][]any, 0, maxDumpSize)

	for rows.Next() {
		g := make([]any, len(tableDef.Columns))
		for i := range g {
			g[i] = &g[i]
		}
		if err := rows.Scan(g...); err != nil {
			return nil, err
		}

		dataRecords = append(dataRecords, g)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return dataRecords, nil
}

func fmtCell(a any) string {
	switch v := a.(type) {
	case time.Time:
		return v.Format("2006-01-02 15:04:05")
	case pgtype.Numeric:
		var s string
		if err := v.AssignTo(&s); err != nil {
			return fmt.Sprintf("%v (fmtCell): %v", v, err)
		}
		return s
	default:
		return fmt.Sprint(v)
	}
}
