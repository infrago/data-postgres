package data_postgres

import (
	"database/sql"
	"sync"

	"github.com/infrago/data"
)

type (
	//数据库连接
	PostgresConnect struct {
		mutex  sync.RWMutex
		name   string
		config data.Config
		schema string

		//数据库对象
		db      *sql.DB
		actives int64
	}
)

// 打开连接
func (connect *PostgresConnect) Open() error {
	db, err := sql.Open("postgres", connect.config.Url)
	if err == nil {
		connect.db = db
	}
	return err
}

// 健康检查
func (connect *PostgresConnect) Health() (data.Health, error) {
	connect.mutex.RLock()
	defer connect.mutex.RUnlock()
	return data.Health{Workload: connect.actives}, nil
}

// 关闭连接
func (connect *PostgresConnect) Close() error {
	if connect.db != nil {
		err := connect.db.Close()
		if err != nil {
			return err
		}
		connect.db = nil
	}
	return nil
}

func (connect *PostgresConnect) Base() data.DataBase {
	connect.mutex.Lock()
	connect.actives++
	connect.mutex.Unlock()

	return &PostgresBase{connect, connect.name, connect.schema, nil, nil, false, []postgresTrigger{}, nil}
}
