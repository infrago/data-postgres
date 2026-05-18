package data_postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	. "github.com/infrago/base"
	"github.com/infrago/data"
	"github.com/lib/pq"
)

type (
	postgresDriver struct{}

	postgresConnection struct {
		instance *data.Instance
		db       *sql.DB
		actives  int64
	}

	postgresDialect struct{}
)

func (d *postgresDriver) Connect(inst *data.Instance) (data.Connection, error) {
	return &postgresConnection{instance: inst}, nil
}

func (c *postgresConnection) Open() error {
	dsn := strings.TrimSpace(c.instance.Config.Url)
	if dsn == "" {
		if v, ok := c.instance.Setting["dsn"].(string); ok {
			dsn = v
		}
	}
	if dsn == "" {
		return fmt.Errorf("missing pgsql dsn")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return err
	}
	c.db = db
	return nil
}

func (c *postgresConnection) Close() error {
	if c.db == nil {
		return nil
	}
	err := c.db.Close()
	c.db = nil
	return err
}

func (c *postgresConnection) Health() data.Health {
	return data.Health{Workload: atomic.LoadInt64(&c.actives)}
}

func (c *postgresConnection) DB() *sql.DB {
	return c.db
}

func (c *postgresConnection) Dialect() data.Dialect {
	return postgresDialect{}
}

func (postgresDialect) Name() string { return "pgsql" }
func (postgresDialect) Quote(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, `"`, ``)
	return `"` + s + `"`
}
func (postgresDialect) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }
func (postgresDialect) SupportsILike() bool      { return true }
func (postgresDialect) SupportsReturning() bool  { return true }
func (postgresDialect) MaxParams() int           { return 65535 }
func (postgresDialect) ClassifyError(err error) error {
	var pqerr *pq.Error
	if !errors.As(err, &pqerr) {
		return nil
	}
	switch string(pqerr.Code) {
	case "23505":
		return data.ErrDuplicate
	case "23503":
		return data.ErrForeignKey
	case "40001", "40P01":
		return data.ErrConflict
	case "57014":
		return data.ErrCanceled
	default:
		if strings.HasPrefix(string(pqerr.Code), "08") {
			return data.ErrDriver
		}
		return nil
	}
}
func (postgresDialect) BindValue(cfg Var, v any) (any, bool) {
	switch {
	case data.IsArrayVar(cfg):
		return pq.Array(v), true
	case data.IsJSONVar(cfg):
		return data.BindJSONValue(v)
	case data.IsBinaryVar(cfg):
		return data.BindBinaryValue(v)
	case data.IsUUIDVar(cfg), data.IsDecimalVar(cfg):
		return data.BindTextValue(v)
	case data.IsTimeVar(cfg):
		return data.BindTimeValue(v)
	default:
		return nil, false
	}
}
func (postgresDialect) DecodeValue(cfg Var, value any) (any, bool) {
	switch {
	case data.IsArrayVar(cfg):
		return data.DecodePGArrayValue(cfg, value)
	case data.IsJSONVar(cfg):
		return data.DecodeJSONValue(value)
	case data.IsBinaryVar(cfg):
		return data.DecodeBinaryValue(value)
	case data.IsUUIDVar(cfg), data.IsDecimalVar(cfg):
		return data.DecodeTextValue(value)
	case data.IsTimeVar(cfg):
		return data.DecodeTimeValue(value)
	default:
		return nil, false
	}
}
func (postgresDialect) BindArray(v any) any { return pq.Array(v) }
