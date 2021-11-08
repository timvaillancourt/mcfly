package rewind

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
)

func (r *Rewinder) rowsEventDeleteRevertSQL(rowTable string, rowsEvent *replication.RowsEvent) (string, error) {
	columnsInfo, err := r.GetTableColumns(rowTable)
	if err != nil {
		return "", err
	}
	columnNames, err := r.getTableColumnNames(rowTable)
	if err != nil {
		return "", err
	}

	values := []string{}
	for _, row := range rowsEvent.Rows {
		rowValues := []string{}
		for colNum, value := range row {
			column := columnsInfo[colNum]
			rowValues = append(rowValues, column.ValueToSQL(value))
		}
		values = append(values, fmt.Sprintf("(%s)", strings.Join(rowValues, ", ")))
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES%s", rowTable, strings.Join(columnNames, ", "), strings.Join(values, ", ")), nil
}
