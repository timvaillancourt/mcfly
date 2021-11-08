package rewind

import (
	"testing"

	"github.com/patrickmn/go-cache"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
)

func TestRowsEventUpdateRevertSQL(t *testing.T) {
	r := &Rewinder{
		tableColumnCache: cache.New(cache.NoExpiration, 0),
	}
	r.tableColumnCache.Set("test.test", testTableColumns, cache.NoExpiration)

	sql, err := r.rowsEventUpdateRevertSQL("test.test", &replication.RowsEvent{
		Table: &testTableMapEvent,
		Rows: [][]interface{}{
			{0, "hello", "world"},    // UPDATE "before" row image
			{0, "hello", "world!!!"}, // UPDATE "after" row image
		},
	})
	assert.Nil(t, err)
	assert.Equal(t, `UPDATE test.test SET firstname='hello', lastname='world' WHERE id=0 AND firstname='hello' AND lastname='world!!!'`, sql)
}
