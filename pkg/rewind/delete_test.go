package rewind

import (
	"testing"

	"github.com/patrickmn/go-cache"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
)

func TestRowsEventDeleteRevertSQL(t *testing.T) {
	r := &Rewinder{
		tableColumnCache: cache.New(cache.NoExpiration, 0),
	}
	r.tableColumnCache.Set("test.test", testTableColumns, cache.NoExpiration)

	t.Run("single-row", func(t *testing.T) {
		sql, err := r.rowsEventDeleteRevertSQL("test.test", &replication.RowsEvent{
			Table: &testTableMapEvent,
			Rows: [][]interface{}{
				{0, "hello", "world"},
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, `INSERT INTO test.test (id, firstname, lastname) VALUES(0, 'hello', 'world')`, sql)
	})

	t.Run("multi-row", func(t *testing.T) {
		sql, err := r.rowsEventDeleteRevertSQL("test.test", &replication.RowsEvent{
			Table: &testTableMapEvent,
			Rows: [][]interface{}{
				{0, "hello", "world"},
				{1, "test", "123456"},
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, `INSERT INTO test.test (id, firstname, lastname) VALUES(0, 'hello', 'world'), (1, 'test', '123456')`, sql)
	})
}
