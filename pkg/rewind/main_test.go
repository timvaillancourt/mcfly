package rewind

import "github.com/go-mysql-org/go-mysql/replication"

var testTableColumns = []Column{
	{Name: "id", DataType: "int"},
	{Name: "firstname"},
	{Name: "lastname"},
}

var testTableMapEvent = replication.TableMapEvent{
	Schema: []byte("test"),
	Table:  []byte("test"),
}
