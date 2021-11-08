package rewind

type Config struct {
	BinlogFile    string
	Debug         uint
	StartPosition int64
	StopPosition  int64
	StoreFile     string
	MySQLHost     string
	MySQLPort     uint
	MySQLUser     string
	MySQLPassword string
}
