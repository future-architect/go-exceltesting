package cli

import (
	"os"

	"github.com/fatih/color"
	"github.com/future-architect/go-exceltesting"
	"github.com/joho/godotenv"
	"gopkg.in/alecthomas/kingpin.v2"

	_ "github.com/jackc/pgx/v4/pgxpool"
)

var (
	app    = kingpin.New("exceltesting", "Excel file driven testing helper tool")
	source = app.Flag("source", `Database source (e.g. postgres://user:pass@host/dbname?sslmode=disable). EXCELTESTING_CONNECTION envvar is acceptable.`).Short('c').Envar("EXCELTESTING_CONNECTION").String()

	dumpCommand = app.Command("dump", "Generate excel template file from database")
	dumpFile    = dumpCommand.Arg("file", "Target excel file path (e.g. dump.xlsx)").Required().NoEnvar().String()
	table       = dumpCommand.Flag("table", "Dump target table names (e.g. table1,table2,table3)").NoEnvar().String()
	systemcolum = dumpCommand.Flag("systemcolum", "Specific system columns for cell style (e.g. created_at,updated_at,revision)").NoEnvar().String()

	loadCommand                     = app.Command("load", "Load from excel file to database")
	loadFile                        = loadCommand.Arg("file", "Target excel file path (e.g. input.xlsx)").Required().NoEnvar().ExistingFile()
	enableAutoCompleteNotNullColumn = loadCommand.Flag("enableAutoCompleteNotNullColumn", "Enable auto insert to not null columns if excel the cell is undefined").NoEnvar().Bool()

	compareCommand = app.Command("compare", "Compare database to excel file")
	compareFile    = compareCommand.Arg("file", "Target excel file path (e.g. want.xlsx)").Required().NoEnvar().ExistingFile()
)

func Main() {
	_ = godotenv.Load(".env.local", ".env")

	var err error
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case dumpCommand.FullCommand():
		err = Dump(*source, *dumpFile, *table, *systemcolum)
	case compareCommand.FullCommand():
		req := exceltesting.CompareRequest{
			TargetBookPath: *compareFile,
			SheetPrefix:    "",
		}
		err = Compare(*source, req)
	case loadCommand.FullCommand():
		req := exceltesting.LoadRequest{
			TargetBookPath:                  *loadFile,
			EnableAutoCompleteNotNullColumn: *enableAutoCompleteNotNullColumn,
		}
		err = Load(*source, req)
	}
	if err != nil {
		_, _ = color.New(color.FgHiRed).Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
