package rewind

import (
	"testing"

	"github.com/patrickmn/go-cache"
	"github.com/stretchr/testify/assert"
)

func TestGetRowEventColumns(t *testing.T) {
	r := &Rewinder{
		tableColumnCache: cache.New(cache.NoExpiration, 0),
	}
	r.tableColumnCache.Set("test.test", testTableColumns, cache.NoExpiration)

	columns, err := r.getTableColumnNames("test.test")
	assert.Nil(t, err)
	assert.Equal(t, columns, []string{"id", "firstname", "lastname"})
}

func TestColumnValueToSQL(t *testing.T) {
	column := &Column{DataType: "int"}
	assert.Equal(t, `1`, column.ValueToSQL(1))

	column = &Column{DataType: "varchar"}
	assert.Equal(t, `'test'`, column.ValueToSQL("test"))
}
