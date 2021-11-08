package store

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestNextReverseIndexPoint(t *testing.T) {
	store := &Store{
		indexPoints: []indexPoint{
			{0, 10},
			{10, 20},
			{30, 40},
		},
	}
	point, found := store.nextReverseIndexPoint()
	assert.True(t, found)
	assert.Equal(t, indexPoint{30, 40}, point)
	assert.Equal(t, []indexPoint{{0, 10}, {10, 20}}, store.indexPoints)
}

func TestStoreWriteAndRead(t *testing.T) {
	tmpFile, err := ioutil.TempFile("", t.Name())
	if err != nil {
		t.Fatalf("failed to create tmp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	store, err := NewFileStore(tmpFile.Name())
	assert.Nil(t, err)
	defer store.Close()

	// event 1
	gtid1, _ := uuid.FromString("72bedaed-a21f-11ea-b27c-0242c0a8d002")
	event1 := Event{
		Schema:   "test",
		Table:    "test",
		Query:    "DROP TABLE test",
		GTIDNext: &gtid1,
	}
	eventData1, _ := json.Marshal(event1)
	n, err := store.Write(eventData1)
	assert.Nil(t, err)
	assert.Equal(t, 91, n)
	assert.Equal(t, int64(91), store.offset)
	assert.Equal(t, []indexPoint{{0, 91}}, store.indexPoints)

	// event 2: test an event larger the json.Decoder 512 byte buffer (eg: test unread buffer)
	gtid2, _ := uuid.FromString("72bedaed-a21f-11ea-b27c-0242c0a8d005")
	event2 := Event{
		Schema: "test",
		Table:  "test",
		Query: `CREATE TABLE test (
				id INT,
				firstname VARCHAR(255),
				lastname VARCHAR(255),
				city VARCHAR(255),
				province VARCHAR(255),
				country VARCHAR(255),
				continent VARCHAR(255),
				PRIMARY KEY (id),
				UNIQUE KEY something1 (id, firstname, lastname),
				UNIQUE KEY something2 (id, firstname, lastname, city, province, country),
				UNIQUE KEY something3 (id, firstname, lastname, city, province, country, continent)
			) ENGINE=InnoDB`,
		GTIDNext: &gtid2,
	}
	eventData2, _ := json.Marshal(event2)
	n, err = store.Write(eventData2)
	assert.Nil(t, err)
	assert.Equal(t, 585, n)
	assert.Equal(t, int64(676), store.offset)
	assert.Equal(t, []indexPoint{{0, 91}, {91, 585}}, store.indexPoints)

	// read and test events
	expect := []Event{event2, event1}
	decoder := json.NewDecoder(store)
	var i int
	for {
		assert.NotEqual(t, 3, i)
		var data Event
		if err := decoder.Decode(&data); err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		t.Logf("StoreEvent: %v\n", data)
		assert.Equal(t, expect[i], data)
		i += 1
	}

	assert.Len(t, store.indexPoints, 0)
	assert.Equal(t, int64(0), store.offset)
}
