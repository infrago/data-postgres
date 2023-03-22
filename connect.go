package data_postgres

import (
	"database/sql"
	"sync"

	"github.com/infrago/data"
)

type (
	//数据库连接
	PostgresConnect struct {
		mutex    sync.RWMutex
		instance *data.Instance
		setting  PostgresSetting

		schema string

		//数据库对象
		db      *sql.DB
		actives int64
	}

	PostgresSetting struct {
		Schema string
	}
)

// 打开连接
func (this *PostgresConnect) Open() error {
	db, err := sql.Open("postgres", this.instance.Config.Url)
	if err == nil {
		this.db = db
	}
	return err
}

// 健康检查
func (this *PostgresConnect) Health() (data.Health, error) {
	this.mutex.RLock()
	defer this.mutex.RUnlock()
	return data.Health{Workload: this.actives}, nil
}

// 关闭连接
func (this *PostgresConnect) Close() error {
	if this.db != nil {
		err := this.db.Close()
		if err != nil {
			return err
		}
		this.db = nil
	}
	return nil
}

func (this *PostgresConnect) Base() data.DataBase {
	this.mutex.Lock()
	this.actives++
	this.mutex.Unlock()

	return &PostgresBase{this, this.instance.Name, this.setting.Schema, nil, nil, false, []postgresTrigger{}, nil}
}
