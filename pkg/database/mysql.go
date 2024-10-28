package database

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

func (d *Database) ConnectionMySQL() (DBInterface, error) {
	cfg := d.config
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", cfg.Username, cfg.Password, cfg.Address, cfg.Port, cfg.DBName)
	return d.connectiondb(connStr)
}
