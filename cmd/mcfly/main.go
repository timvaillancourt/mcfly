package main

import (
	"flag"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/timvaillancourt/mcfly/pkg/rewind"
)

func main() {
	cnf := &rewind.Config{}
	flag.StringVar(&cnf.BinlogFile, "binlog", "mysqldata/mysql-bin.000003", "binlog file to rewind")
	flag.Int64Var(&cnf.StartPosition, "start-position", 0, "starting binlog position")
	flag.StringVar(&cnf.StoreFile, "store-file", "rewind.json", "rewind store file")
	flag.Int64Var(&cnf.StopPosition, "stop-position", 0, "binlog stop position, 0 = no limit")
	flag.StringVar(&cnf.MySQLHost, "mysql-host", "127.0.0.1", "mysql server host/ip")
	flag.UintVar(&cnf.MySQLPort, "mysql-port", 3306, "mysql server port")
	flag.StringVar(&cnf.MySQLUser, "mysql-user", "root", "mysql username")
	flag.StringVar(&cnf.MySQLPassword, "mysql-password", "", "mysql password")
	flag.UintVar(&cnf.Debug, "debug", 1, "debug log level")
	flag.Parse()

	db, err := sqlx.Connect("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		cnf.MySQLUser, cnf.MySQLPassword, cnf.MySQLHost, cnf.MySQLPort,
	))
	if err != nil {
		panic(err)
	}
	defer db.Close()

	rewinder, err := rewind.New(cnf, db)
	if err != nil {
		panic(err)
	}
	defer rewinder.Close()

	if err := rewinder.Rewind(); err != nil {
		panic(err)
	}
}
