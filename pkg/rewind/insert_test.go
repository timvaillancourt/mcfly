package rewind

import (
	"testing"

	"github.com/patrickmn/go-cache"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
)

func TestRowsEventInsertRevertSQL(t *testing.T) {
	r := &Rewinder{
		tableColumnCache: cache.New(cache.NoExpiration, 0),
	}
	r.tableColumnCache.Set("test.test", testTableColumns, cache.NoExpiration)

	t.Run("single-row", func(t *testing.T) {
		sql, err := r.rowsEventInsertRevertSQL("test.test", &replication.RowsEvent{
			Table: &testTableMapEvent,
			Rows: [][]interface{}{
				{0, "hello", "world"},
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, `DELETE FROM test.test WHERE id=0 AND firstname='hello' AND lastname='world'`, sql)
	})

	t.Run("multi-row", func(t *testing.T) {
		sql, err := r.rowsEventInsertRevertSQL("test.test", &replication.RowsEvent{
			Table: &testTableMapEvent,
			Rows: [][]interface{}{
				{0, "hello", "world"},
				{1, "test", "123456"},
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, `DELETE FROM test.test WHERE (id=0 AND firstname='hello' AND lastname='world') OR (id=1 AND firstname='test' AND lastname='123456')`, sql)
	})
}
