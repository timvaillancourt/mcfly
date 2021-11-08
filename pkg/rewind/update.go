package rewind

import (
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
)

func (r *Rewinder) rowsEventUpdateRevertSQL(rowTable string, rowsEvent *replication.RowsEvent) (string, error) {
	columnsInfo, err := r.GetTableColumns(rowTable)
	if err != nil {
		return "", err
	}

	sets := []string{}
	wheres := []string{}
	for colNum, colVal := range rowsEvent.Rows[0] {
		column := columnsInfo[colNum]
		if colNum == 0 {
			wheres = append(wheres, fmt.Sprintf("%s=%s", column.Name, column.ValueToSQL(colVal)))
			continue
		}
		sets = append(sets, fmt.Sprintf("%s=%s", column.Name, column.ValueToSQL(colVal)))
		wheres = append(wheres, fmt.Sprintf("%s=%s", column.Name, column.ValueToSQL(rowsEvent.Rows[1][colNum])))
	}

	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", rowTable, strings.Join(sets, ", "), strings.Join(wheres, " AND ")), nil
}
