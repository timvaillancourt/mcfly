package rewind

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
)

func TestQueryEventCreateTableRevertSQL(t *testing.T) {
	r := &Rewinder{}
	sql := r.queryEventCreateTableRevertSQL("test", &replication.QueryEvent{
		Schema: []byte("test"),
	})
	assert.Equal(t, sql, `DROP TABLE test.test`)
}
