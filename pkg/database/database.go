package database

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type DBInterface interface {
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
	Beginx() (*sqlx.Tx, error)
	BindNamed(query string, arg interface{}) (string, []interface{}, error)
	Connx(ctx context.Context) (*sqlx.Conn, error)
	DriverName() string
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	MapperFunc(mf func(string) string)
	MustBegin() *sqlx.Tx
	MustBeginTx(ctx context.Context, opts *sql.TxOptions) *sqlx.Tx
	MustExec(query string, args ...interface{}) sql.Result
	MustExecContext(ctx context.Context, query string, args ...interface{}) sql.Result
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedExecContext(ctx context.Context, query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	NamedQueryContext(ctx context.Context, query string, arg interface{}) (*sqlx.Rows, error)
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
	PrepareNamedContext(ctx context.Context, query string) (*sqlx.NamedStmt, error)
	Preparex(query string) (*sqlx.Stmt, error)
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)
	Rebind(query string) string
	Select(dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Unsafe() *sqlx.DB
}

type Config struct {
	Type            string `yaml:"type" json:"type"`
	Address         string `yaml:"address" json:"address"`
	Port            string `yaml:"port" json:"port"`
	Username        string `yaml:"username" json:"username"`
	Password        string `yaml:"password" json:"password"`
	DBName          string `yaml:"dbname" json:"dbname"`
	MaxOpenConn     int    `yaml:"maxopenconn" json:"maxopenconn"`
	MaxIdleConn     int    `yaml:"maxidleconn" json:"maxidleconn"`
	ConnMaxIdleTime int    `yaml:"connmaxidletime" json:"connmaxidletime"`
	ConnMaxLifeTime int    `yaml:"connmaxlifetime" json:"connmaxlifetime"`
	SslMode         string `yaml:"sslmode" json:"sslmode"`
}

type Database struct {
	config Config
}

func NewConnection(dbconf Config) (DBInterface, error) {
	dbcon := &Database{
		config: dbconf,
	}

	if dbconf.Type == "mysql" {
		return dbcon.ConnectionMySQL()
	}

	if dbconf.Type == "postgres" {
		return dbcon.ConnectionPostgres()
	}

	return nil, fmt.Errorf("database type %s not supported", dbconf.Type)
}

func (d *Database) connectiondb(connStr string) (DBInterface, error) {
	cfg := d.config

	db, err := sqlx.Connect(cfg.Type, connStr)
	if err != nil {
		return nil, err
	}
	if cfg.MaxOpenConn != -1 {
		db.SetMaxOpenConns(cfg.MaxOpenConn)
	}
	if cfg.MaxIdleConn != -1 {
		db.SetMaxIdleConns(cfg.MaxIdleConn)
	}
	if cfg.ConnMaxLifeTime != -1 {
		db.SetConnMaxLifetime(time.Second * time.Duration(cfg.ConnMaxLifeTime))
	}
	if cfg.ConnMaxIdleTime != -1 {
		db.SetConnMaxIdleTime(time.Second * time.Duration(cfg.ConnMaxIdleTime))
	}

	return db, nil
}

func SelectScan(rows *sqlx.Rows) ([]map[string]interface{}, error) {
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	numColumns := len(columns)

	values := make([]interface{}, numColumns)
	for i := range values {
		values[i] = new(interface{})
	}

	var results []map[string]interface{}
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		dest := make(map[string]interface{}, numColumns)
		for i, column := range columns {
			dest[column] = *(values[i].(*interface{}))
		}
		results = append(results, dest)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}
