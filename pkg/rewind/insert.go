package rewind

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
)

func (r *Rewinder) rowsEventInsertRevertSQL(rowTable string, rowsEvent *replication.RowsEvent) (string, error) {
	columnsInfo, err := r.GetTableColumns(rowTable)
	if err != nil {
		return "", err
	}

	wheres := []string{}
	for _, row := range rowsEvent.Rows {
		rowWheres := []string{}
		for colNum, value := range row {
			column := columnsInfo[colNum]
			rowWheres = append(rowWheres, fmt.Sprintf("%s=%s", column.Name, column.ValueToSQL(value)))
		}
		wheres = append(wheres, strings.Join(rowWheres, " AND "))
	}

	if len(wheres) > 1 {
		return fmt.Sprintf("DELETE FROM %s WHERE (%s)", rowTable, strings.Join(wheres, ") OR (")), nil
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", rowTable, wheres[0]), nil
}
