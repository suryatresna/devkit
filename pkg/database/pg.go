package database

import (
	"fmt"

	_ "github.com/lib/pq"
)

func (d *Database) ConnectionPostgres() (DBInterface, error) {
	cfg := d.config
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", cfg.Address, cfg.Port, cfg.Username, cfg.Password, cfg.DBName, cfg.SslMode)
	return d.connectiondb(connStr)
}
