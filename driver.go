package data_postgres

import (
	"strings"

	"github.com/infrago/data"
)

var (
	SCHEMAS = []string{
		"postgresql://",
		"postgres://",
		"pgsql://",
		"pgdb://",
		"cockroachdb://",
		"cockroach://",
		"crdb://",
		"timescale://",
		"timescaledb://",
		"tsdb://",
	}
)

type (
	PostgresDriver struct{}
)

// 驱动连接
func (drv *PostgresDriver) Connect(name string, config data.Config) (data.Connect, error) {

	//支持自定义的schema，相当于数据库名

	for _, s := range SCHEMAS {
		if strings.HasPrefix(config.Url, s) {
			config.Url = strings.Replace(config.Url, s, "postgres://", 1)
		}
	}

	schema := "public"
	if vv, ok := config.Setting["schema"].(string); ok && vv != "" {
		schema = vv
	}

	// if config.Url != "" {
	// 	durl,err := url.Parse(config.Url)
	// 	if err == nil {
	// 		if len(durl.Path) >= 1 {
	// 			schema = durl.Path[1:]
	// 		}
	// 	}
	// } else if vv,ok := config.Setting["schema"].(string); ok && vv != "" {
	// 	schema = vv
	// }

	return &PostgresConnect{
		name: name, config: config, schema: schema, db: nil, actives: int64(0),
	}, nil
}
