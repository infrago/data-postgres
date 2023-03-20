package data_postgres

import (
	"github.com/infrago/data"
	_ "github.com/lib/pq" //此包自动注册名为postgres的sql驱动
)

var (
	DRIVERS = []string{
		"postgresql", "postgres", "pgsql", "pgdb", "pg",
		"cockroachdb", "cockroach", "crdb",
		"timescaledb", "timescale", "tsdb",
	}
)

// 返回驱动
func Driver() data.Driver {
	return &PostgresDriver{}
}

func init() {
	driver := Driver()
	for _, key := range DRIVERS {
		data.Register(key, driver)
	}
}
