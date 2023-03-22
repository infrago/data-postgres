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
func (drv *PostgresDriver) Connect(inst *data.Instance) (data.Connect, error) {

	setting := PostgresSetting{
		Schema: "public",
	}

	//支持自定义的schema，相当于数据库名

	for _, s := range SCHEMAS {
		if strings.HasPrefix(inst.Config.Url, s) {
			inst.Config.Url = strings.Replace(inst.Config.Url, s, "postgres://", 1)
		}
	}

	if vv, ok := inst.Setting["schema"].(string); ok && vv != "" {
		setting.Schema = vv
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
		instance: inst, setting: setting,
	}, nil
}
