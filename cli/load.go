package cli

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"

	"github.com/future-architect/go-exceltesting"

	_ "github.com/jackc/pgx/v4/stdlib"
)

func Load(dbSource string, r exceltesting.LoadRequest) error {

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	db, err := sql.Open("pgx", dbSource)
	if err != nil {
		return fmt.Errorf("postgres oepn: %w", err)
	}
	e := exceltesting.New(db)

	if err := e.LoadWithContext(ctx, r); err != nil {
		return fmt.Errorf("load: %w", err)
	}

	return nil
}
