package store

import (
	"encoding/json"

	"github.com/satori/go.uuid"
)

// Event represents a stored event
type Event struct {
	Schema   string     `json:"s"`
	Table    string     `json:"t"`
	Query    string     `json:"q"`
	GTIDNext *uuid.UUID `json:"gtid,omitempty"`
}

type EventStore struct {
	store *FileStore
}

func NewEventStore(fs *FileStore) *EventStore {
	return &EventStore{fs}
}

func (es *EventStore) Close() {
	if es.store != nil {
		es.store.Close()
	}
}

func (es *EventStore) Append(event Event) error {
	bytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = es.store.Write(bytes)
	return err
}
