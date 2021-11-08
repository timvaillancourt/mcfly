package rewind

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/replication"
)

func (r *Rewinder) queryEventCreateDatabaseRevertSQL(queryEvent *replication.QueryEvent) string {
	return fmt.Sprintf("DROP DATABASE %s", queryEvent.Schema)
}

func (r *Rewinder) queryEventCreateTableRevertSQL(createTable string, queryEvent *replication.QueryEvent) string {
	return fmt.Sprintf("DROP TABLE %s.%s", queryEvent.Schema, createTable)
}
