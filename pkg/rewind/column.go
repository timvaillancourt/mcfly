package rewind

import (
	"fmt"
	"strings"

	"github.com/patrickmn/go-cache"
	"github.com/go-mysql-org/go-mysql/mysql"
)

// See: https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
var columnTypeMap = map[byte]string{
	mysql.MYSQL_TYPE_LONG:    "int",
	mysql.MYSQL_TYPE_VARCHAR: "varchar",
}

func columnTypeByteToString(ct byte) string {
	return columnTypeMap[ct]
}

// Column is a represts a row of the information_schema.columns table
// See: https://dev.mysql.com/doc/refman/5.7/en/columns-table.html
type Column struct {
	Name            string `db:"column_name"`
	TableSchema     string `db:"table_schema"`
	TableName       string `db:"table_schema"`
	OrdinalPosition uint   `db:"ordinal_position"`
	DataType        string `db:"data_type"`
}

func (column *Column) IsNum() bool {
	switch column.DataType {
	case "int":
		return true
	}
	return false
}

func (column *Column) ValueToSQL(value interface{}) string {
	if column.IsNum() {
		return fmt.Sprintf("%d", value)
	}
	return fmt.Sprintf("'%s'", value)
}

func (r *Rewinder) getTableColumnNames(rowsTable string) (columnNames []string, err error) {
	columnsInfo, err := r.GetTableColumns(rowsTable)
	if err != nil {
		return nil, err
	}
	for _, columnInfo := range columnsInfo {
		columnNames = append(columnNames, columnInfo.Name)
	}
	return columnNames, nil
}

func (r *Rewinder) GetTableColumns(table string) (columns []Column, err error) {
	if result, found := r.tableColumnCache.Get(table); found {
		return result.([]Column), nil
	}

	// TODO: add all table columns
	splitTable := strings.SplitN(table, ".", 2)
	err = r.db.Select(
		&columns,
		fmt.Sprintf(`SELECT column_name, ordinal_position, data_type FROM information_schema.columns
			WHERE table_schema='%s' AND table_name='%s' ORDER BY ordinal_position`,
			splitTable[0],
			splitTable[1],
		),
	)
	if err != nil {
		return nil, err
	}

	if r.config.Debug > 0 {
		fmt.Printf("Fetched column info for %s from mysql\n", table)
	}

	r.tableColumnCache.Set(table, columns, cache.NoExpiration)
	return columns, nil
}
