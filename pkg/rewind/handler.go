package rewind

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/patrickmn/go-cache"
	"github.com/satori/go.uuid"
	"github.com/timvaillancourt/mcfly/pkg/store"
)

var setVars = []string{
	"SET @@SESSION.sql_log_bin=0",
}

func parseEventTime(ts uint32) time.Time {
	return time.Unix(int64(ts), 0)
}

func rowsEventRowFormat(rowsEvent *replication.RowsEvent) string {
	if len(rowsEvent.Rows) == 2 {
		return replication.BINLOG_ROW_IMAGE_FULL
	}
	return replication.BINLOG_ROW_IAMGE_MINIMAL
}

type Rewinder struct {
	config *Config
	//
	db               *sqlx.DB
	firstGTIDNext    *uuid.UUID
	lastGTIDNext     *uuid.UUID
	eventStore       *store.EventStore
	parser           *replication.BinlogParser
	tableColumnCache *cache.Cache
}

func New(config *Config, db *sqlx.DB) (*Rewinder, error) {
	if err := db.Ping(); err != nil {
		return nil, err
	}

	fileStore, err := store.NewFileStore(config.StoreFile)
	if err != nil {
		return nil, err
	}
	eventStore := store.NewEventStore(fileStore)
	eventStore.Append(store.Event{Query: "BEGIN; COMMIT;"})

	return &Rewinder{
		config:           config,
		db:               db,
		eventStore:       eventStore,
		parser:           replication.NewBinlogParser(),
		tableColumnCache: cache.New(cache.NoExpiration, 0),
	}, nil
}

func (r *Rewinder) Close() {
	if r.eventStore != nil {
		for _, setVar := range setVars {
			r.eventStore.Append(store.Event{Query: setVar})
		}
		r.eventStore.Close()
	}
}

func (r *Rewinder) HandleBinlogEvent(event *replication.BinlogEvent) error {
	//print all events
	if r.config.Debug >= 3 {
		event.Dump(os.Stdout)
	}

	logPos := event.Header.LogPos
	if r.config.StopPosition > 0 && logPos >= uint32(r.config.StopPosition) {
		return fmt.Errorf("reached stop position")
	}

	eventType := event.Header.EventType
	eventTime := parseEventTime(event.Header.Timestamp)
	switch eventType {
	case replication.GTID_EVENT:
		gtidEvent := event.Event.(*replication.GTIDEvent)
		gtid, _ := uuid.FromBytes(gtidEvent.SID)
		r.lastGTIDNext = &gtid
		if r.firstGTIDNext == nil {
			r.firstGTIDNext = &gtid
			r.eventStore.Append(store.Event{
				Query:    fmt.Sprintf("SET @@SESSION.GTID_NEXT='%s:%d';\nRESET MASTER", gtid, gtidEvent.SequenceNumber),
				GTIDNext: &gtid,
			})
		}
		fmt.Printf("[%s] LogPos=%d EventType=%s: GTID_NEXT=%s\n", eventTime, logPos, eventType, gtid.String())
	case replication.DELETE_ROWS_EVENTv2:
		deleteEvent := event.Event.(*replication.RowsEvent)
		rowTable := fmt.Sprintf("%s.%s", deleteEvent.Table.Schema, deleteEvent.Table.Table)
		fmt.Printf("[%s] LogPos=%d EventType=%s on %s\n", eventTime, logPos, eventType, rowTable)
		if r.config.Debug >= 1 {
			deleteEvent.Dump(os.Stdout)
		}
		revertSQL, err := r.rowsEventDeleteRevertSQL(rowTable, deleteEvent)
		if err != nil {
			return err
		}
		r.eventStore.Append(store.Event{
			Schema: string(deleteEvent.Table.Schema),
			Table:  string(deleteEvent.Table.Table),
			Query:  revertSQL,
		})
		fmt.Printf("[%s] LogPos=%d EventType=%s DELETE revert SQL:\n\t%s\n", eventTime, logPos, eventType, revertSQL)
	case replication.ROTATE_EVENT:
		rotateEvent := event.Event.(*replication.RotateEvent)
		fmt.Printf("[%s] LogPos=%d EventType=%s next binlog %q, position %d\n", eventTime, logPos, eventType,
			string(rotateEvent.NextLogName),
			rotateEvent.Position,
		)
		return fmt.Errorf("got binlog rotate event, unsupported")
	case replication.QUERY_EVENT:
		queryEvent := event.Event.(*replication.QueryEvent)
		queryElems := strings.Split(string(queryEvent.Query), " ")
		switch strings.ToUpper(queryElems[0]) {
		case "ALTER":
			fmt.Printf("[%s] LogPos=%d EventType=%s query: %q\n", eventTime, logPos, eventType, string(queryEvent.Query))
			// TODO: invalidate tableColumnCache
			return fmt.Errorf("ALTER operations are unsupported")
		case "CREATE":
			switch strings.ToUpper(queryElems[1]) {
			case "DATABASE":
				revertSQL := r.queryEventCreateDatabaseRevertSQL(queryEvent)
				fmt.Printf("[%s] LogPos=%d EventType=%s query: %s\n", eventTime, logPos, eventType, string(queryEvent.Query))
				fmt.Printf("[%s] LogPos=%d EventType=%s CREATE TABLE revert SQL: %s\n", eventTime, logPos, eventType, revertSQL)
				r.eventStore.Append(store.Event{Query: revertSQL})
			case "TABLE":
				tableName := queryElems[2]
				revertSQL := r.queryEventCreateTableRevertSQL(tableName, queryEvent)
				fmt.Printf("[%s] LogPos=%d EventType=%s query: %s\n", eventTime, logPos, eventType, string(queryEvent.Query))
				fmt.Printf("[%s] LogPos=%d EventType=%s CREATE TABLE revert SQL: %s\n", eventTime, logPos, eventType, revertSQL)
				r.eventStore.Append(store.Event{Query: revertSQL})
			}
		case "RENAME":
			return fmt.Errorf("RENAME operations are unsupported")
		}
	case replication.TABLE_MAP_EVENT:
		tableMapEvent := event.Event.(*replication.TableMapEvent)
		columnTypes := []string{}
		for _, columnTypeByte := range tableMapEvent.ColumnType {
			columnTypes = append(columnTypes, columnTypeByteToString(columnTypeByte))
		}
		fmt.Printf("[%s] LogPos=%d EventType=%s column types: %q\n", eventTime, logPos, eventType, columnTypes)
	case replication.UPDATE_ROWS_EVENTv2:
		rowsEvent := event.Event.(*replication.RowsEvent)
		rowTable := fmt.Sprintf("%s.%s", rowsEvent.Table.Schema, rowsEvent.Table.Table)
		rowFormat := rowsEventRowFormat(rowsEvent)
		if rowFormat != replication.BINLOG_ROW_IMAGE_FULL {
			fmt.Printf("[%s] %d %s ERROR: unsupported row format: %q", eventTime, logPos, eventType, rowFormat)
		}
		fmt.Printf("[%s] LogPos=%d EventType=%s on %s\n", eventTime, logPos, eventType,
			rowTable,
		)

		beforeRows := rowsEvent.Rows[0]
		afterRows := rowsEvent.Rows[1]
		revertSQL, err := r.rowsEventUpdateRevertSQL(rowTable, rowsEvent)
		if err != nil {
			return err
		}
		r.eventStore.Append(store.Event{
			Schema: string(rowsEvent.Table.Schema),
			Table:  string(rowsEvent.Table.Table),
			Query:  revertSQL,
		})
		if r.config.Debug >= 1 {
			rowsEvent.Dump(os.Stdout)
			fmt.Printf("[%s] LogPos=%d EventType=%s before row-image: '%v'\n", eventTime, logPos, eventType, beforeRows)
			fmt.Printf("[%s] LogPos=%d EventType=%s after row-image: '%v'\n", eventTime, logPos, eventType, afterRows)
		}
		fmt.Printf("[%s] LogPos=%d EventType=%s UPDATE revert SQL:\n\t%s\n", eventTime, logPos, eventType, revertSQL)
	case replication.WRITE_ROWS_EVENTv2:
		insertEvent := event.Event.(*replication.RowsEvent)
		rowTable := fmt.Sprintf("%s.%s", insertEvent.Table.Schema, insertEvent.Table.Table)
		fmt.Printf("[%s] LogPos=%d EventType=%s on %s\n", eventTime, logPos, eventType,
			rowTable,
		)
		if r.config.Debug >= 1 {
			insertEvent.Dump(os.Stdout)
		}
		revertSQL, err := r.rowsEventInsertRevertSQL(rowTable, insertEvent)
		if err != nil {
			return err
		}
		r.eventStore.Append(store.Event{
			Schema: string(insertEvent.Table.Schema),
			Table:  string(insertEvent.Table.Table),
			Query:  revertSQL,
		})
		fmt.Printf("[%s] LogPos=%d EventType=%s INSERT revert SQL:\n\t%s\n", eventTime, logPos, eventType, revertSQL)
	default:
		if r.config.Debug > 1 {
			fmt.Printf("[%s] LogPos=%d unsupported EventType=%s\n", eventTime, logPos, eventType)
		}
	}
	return nil
}

func (r *Rewinder) Rewind() error {
	return r.parser.ParseFile(
		r.config.BinlogFile,
		r.config.StartPosition,
		r.HandleBinlogEvent,
	)
}
