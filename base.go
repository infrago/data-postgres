package data_postgres

import (
	"database/sql"
	"errors"
	"fmt"

	. "github.com/infrago/base"
	"github.com/infrago/data"
	"github.com/infrago/infra"
	"github.com/infrago/log"

	"strconv"
	"strings"
	"time"
)

type (
	postgresTrigger struct {
		Name  string
		Value Map
	}
	PostgresBase struct {
		connect *PostgresConnect

		name   string
		schema string

		tx   *sql.Tx
		exec PostgresExecutor
		// cache data.CacheBase

		//是否手动提交事务，默认false为自动
		//当调用begin时， 自动变成手动提交事务
		//triggers保存待提交的触发器，手动下有效
		manual   bool
		triggers []postgresTrigger

		lastError error
	}
)

// 记录触发器
func (base *PostgresBase) trigger(name string, values ...Map) {
	if base.manual {
		//手动时保存触发器
		var value Map
		if len(values) > 0 {
			value = values[0]
			// if vv, ok := values[0].(Map); ok {
			// 	value = vv
			// }
		}
		base.triggers = append(base.triggers, postgresTrigger{Name: name, Value: value})
	} else {
		//自动时，直接触发
		data.Trigger(name, values...)
	}

}

// 查询表，支持多个KEY遍历
func (base *PostgresBase) tableConfig(name string) *data.Table {
	keys := []string{
		fmt.Sprintf("%s.%s", base.name, name),
		fmt.Sprintf("*.%s", name),
		name,
	}

	for _, key := range keys {
		if cfg := data.GetTable(key); cfg != nil {
			return cfg
		}
	}

	return nil
}
func (base *PostgresBase) viewConfig(name string) *data.View {
	keys := []string{
		fmt.Sprintf("%s.%s", base.name, name),
		fmt.Sprintf("*.%s", name),
		name,
	}

	for _, key := range keys {
		if cfg := data.GetView(key); cfg != nil {
			return cfg
		}
	}

	return nil
}
func (base *PostgresBase) modelConfig(name string) *data.Model {
	keys := []string{
		fmt.Sprintf("%s.%s", base.name, name),
		fmt.Sprintf("*.%s", name),
		name,
	}

	for _, key := range keys {
		if cfg := data.GetModel(key); cfg != nil {
			return cfg
		}
	}

	return nil
}

func (base *PostgresBase) errorHandler(key string, err error, args ...Any) {
	if err != nil {
		//出错自动取消事务
		base.Cancel()

		errors := []Any{key, err}
		errors = append(errors, args...)

		base.lastError = err
		log.Warning(errors...)
	}
}

// 关闭数据库
func (base *PostgresBase) Close() error {
	base.connect.mutex.Lock()
	base.connect.actives--
	base.connect.mutex.Unlock()

	//好像目前不需要关闭什么东西
	if base.tx != nil {
		//关闭时候,一定要提交一次事务
		//如果手动提交了, 这里会失败, 问题不大
		//如果没有提交的话, 连接不会交回连接池. 会一直占用
		base.Cancel()
	}

	// if base.cache != nil {
	// 	base.cache.Close()
	// }

	return nil
}
func (base *PostgresBase) Erred() error {
	err := base.lastError
	base.lastError = nil
	return err
}

// // ID生成器 表版
// func (base *PostgresBase) Serial(key string, start, step int64) int64 {

// 	exec, err := base.beginExec()
// 	if err != nil {
// 		base.errorHandler("data.serial", err, key)
// 		return 0
// 	}

// 	serial := "serial"
// 	if base.connect.config.Serial != "" {
// 		serial = base.connect.config.Serial
// 	} else if vv, ok := base.connect.config.Setting["serial"].(string); ok && vv != "" {
// 		serial = vv
// 	}

// 	if step == 0 {
// 		step = 1
// 	}

// 	//`INSERT INTO %v(key,seq) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET seq=%v.seq+excluded.seq RETURNING seq;`,
// 	sql := fmt.Sprintf(
// 		`INSERT INTO %v(key,seq) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET seq=%v.seq+$3 RETURNING seq;`,
// 		serial, serial,
// 	)
// 	args := []Any{key, start, step}
// 	row := exec.QueryRow(sql, args...)

// 	seq := int64(0)

// 	err = row.Scan(&seq)
// 	if err != nil {
// 		base.errorHandler("data.serial", err, key)
// 		return 0
// 	}

// 	return seq
// }

// ID生成器 序列版
func (base *PostgresBase) Serial(key string, start, step int64) int64 {

	exec, err := base.beginExec()
	if err != nil {
		base.errorHandler("data.serial", err, key)
		return 0
	}

	key = strings.Replace(key, `"`, ``, -1)
	serial := fmt.Sprintf("serial_%s", key)

	if step == 0 {
		step = 1
	}

	sql := fmt.Sprintf(
		`CREATE SEQUENCE IF NOT EXISTS "%s" START %d INCREMENT %d; select nextval('%s');`,
		serial, start, step, serial,
	)
	row := exec.QueryRow(sql)

	seq := int64(0)

	err = row.Scan(&seq)
	if err != nil {
		base.errorHandler("data.serial", err, key)
		return 0
	}

	return seq
}

// 删除ID生成器
func (base *PostgresBase) Break(key string) {

	exec, err := base.beginExec()
	if err != nil {
		base.errorHandler("data.break", err, key)
		return
	}

	serial := "serial"
	if base.connect.instance.Config.Serial != "" {
		serial = base.connect.instance.Config.Serial
	} else if vv, ok := base.connect.instance.Config.Setting["serial"].(string); ok && vv != "" {
		serial = vv
	}

	sql := fmt.Sprintf(`DELETE FROM "%s" WHERE "key"=$1`, serial)
	_, err = exec.Exec(sql, key)
	if err != nil {
		base.errorHandler("data.break.delete", err, key)
		return
	}
}

// 获取表对象
func (base *PostgresBase) Table(name string) data.DataTable {
	if config := base.tableConfig(name); config != nil {
		//模式，表名
		schema, table, key := base.schema, name, "id"
		if config.Schema != "" {
			schema = config.Schema
		}
		if config.Table != "" {
			table = config.Table
		}
		if config.Key != "" {
			key = config.Key
		}

		fields := Vars{
			"$count": Var{Type: "int", Nullable: true, Name: "统计"},
		}
		for k, v := range config.Fields {
			fields[k] = v
		}

		table = strings.Replace(table, ".", "_", -1)
		return &PostgresTable{
			PostgresView{base, name, schema, table, key, fields},
		}
	} else {
		panic("[数据]表不存在")
	}
}

// 获取模型对象
func (base *PostgresBase) View(name string) data.DataView {
	if config := base.viewConfig(name); config != nil {

		//模式，表名
		schema, view, key := base.schema, name, "id"
		if config.Schema != "" {
			schema = config.Schema
		}
		if config.View != "" {
			view = config.View
		}
		if config.Key != "" {
			key = config.Key
		}

		fields := Vars{
			"$count": Var{Type: "int", Nullable: true, Name: "统计"},
		}
		for k, v := range config.Fields {
			fields[k] = v
		}

		view = strings.Replace(view, ".", "_", -1)
		return &PostgresView{
			base, name, schema, view, key, fields,
		}
	} else {
		panic("[数据]视图不存在")
	}
}

// 获取模型对象
func (base *PostgresBase) Model(name string) data.DataModel {
	if config := base.modelConfig(name); config != nil {

		//模式，表名
		schema, model, key := base.schema, name, "id"
		if config.Model != "" {
			model = config.Model
		}
		if config.Key != "" {
			key = config.Key
		}

		fields := Vars{
			"$count": Var{Type: "int", Nullable: true, Name: "统计"},
		}
		for k, v := range config.Fields {
			fields[k] = v
		}

		model = strings.Replace(model, ".", "_", -1)
		return &PostgresModel{
			base, name, schema, model, key, fields,
		}
	} else {
		panic("[数据]模型不存在")
	}
}

//是否开启缓存
// func (base *PostgresBase) Cache(use bool) (DataBase) {
// 	base.caching = use
// 	return base
// }

// 开启手动模式
func (base *PostgresBase) Begin() (*sql.Tx, error) {
	base.lastError = nil
	base.manual = true
	return base.beginTx()
}

// 注意，此方法为实际开始事务
func (base *PostgresBase) beginTx() (*sql.Tx, error) {
	if base.tx != nil {
		return base.tx, nil
	}

	tx, err := base.connect.db.Begin()
	if err != nil {
		return nil, err
	}

	base.tx = tx
	base.exec = tx

	return tx, err
}

// 此为取消事务
func (base *PostgresBase) endTx() error {
	base.tx = nil
	base.exec = nil
	base.manual = false
	base.triggers = []postgresTrigger{}
	return nil
}

func (base *PostgresBase) beginExec() (PostgresExecutor, error) {
	if base.manual {
		_, err := base.beginTx()
		return base.exec, err
	} else {
		return base.connect.db, nil
	}
}

// 提交事务
func (base *PostgresBase) Submit() error {
	//不管成功失败，都结束事务
	defer base.endTx()

	if base.tx == nil {
		return errors.New("[数据]无效事务")
	}

	err := base.tx.Commit()
	if err != nil {
		return err
	}

	//提交事务后,要把触发器都发掉
	for _, trigger := range base.triggers {
		data.Trigger(trigger.Name, trigger.Value)
	}

	return nil
}

// 取消事务
func (base *PostgresBase) Cancel() error {
	if base.tx == nil {
		return errors.New("[数据]无效事务")
	}

	err := base.tx.Rollback()
	if err != nil {
		return err
	}

	//提交后,要清掉事务
	base.endTx()

	return nil
}

// 批量操作，包装事务
func (base *PostgresBase) Batch(next data.BatchFunc) Res {
	base.Begin()
	defer base.Cancel()
	if res := next(); res.Fail() {
		return res
	} else {
		if err := base.Submit(); err != nil {
			return infra.Fail
		}
		if res != nil {
			return res
		}
		return infra.OK
	}
}

// 创建的时候,也需要对值来处理,
// 数组要转成{a,b,c}格式,要不然不支持
// json可能要转成字串才支持
func (base *PostgresBase) packing(value Map) Map {

	newValue := Map{}

	for k, v := range value {
		switch t := v.(type) {
		case []string:
			{
				newValue[k] = fmt.Sprintf(`{%s}`, strings.Join(t, `,`))
			}
		case []bool:
			{
				arr := []string{}
				for _, v := range t {
					if v {
						arr = append(arr, "TRUE")
					} else {
						arr = append(arr, "FALSE")
					}
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []int:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, strconv.Itoa(v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []int8:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []int16:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []int32:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []int64:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []float32:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case []float64:
			{
				arr := []string{}
				for _, v := range t {
					arr = append(arr, fmt.Sprintf("%v", v))
				}

				newValue[k] = fmt.Sprintf("{%s}", strings.Join(arr, ","))
			}
		case Map:
			{
				b, e := infra.MarshalJSON(t)
				if e == nil {
					newValue[k] = string(b)
				} else {
					newValue[k] = "{}"
				}
			}
		case []Map:
			{
				//ms := []string{}
				//for _,v := range t {
				//	ms = append(ms, util.ToString(v))
				//}
				//
				//newValue[k] = fmt.Sprintf("{%s}", strings.Join(ms, ","))

				b, e := infra.MarshalJSON(t)
				if e == nil {
					newValue[k] = string(b)
				} else {
					newValue[k] = "[]"
				}
			}
		default:
			newValue[k] = t
		}
	}
	return newValue
}

// 楼上写入前要打包处理值
// 这里当然 读取后也要解包处理
func (base *PostgresBase) unpacking(keys []string, vals []interface{}) Map {

	m := Map{}

	for i, n := range keys {
		switch v := vals[i].(type) {
		case time.Time:
			m[n] = v.Local()
		case string:
			{
				m[n] = v
			}
		case []byte:
			{
				m[n] = string(v)
			}
		default:
			m[n] = v
		}
	}

	return m
}

// 把MAP编译成sql查询条件
func (base *PostgresBase) parsing(i int, args ...Any) (string, []interface{}, string, error) {

	sql, val, odr, err := data.ParseSQL(args...)

	if err != nil {
		return "", nil, "", err
	}

	//结果要处理一下，字段包裹、参数处理
	sql = strings.Replace(sql, DELIMS, `"`, -1)
	odr = strings.Replace(odr, DELIMS, `"`, -1)
	odr = strings.Replace(odr, RANDBY, `RANDOM()`, -1)
	for range val {
		sql = strings.Replace(sql, "?", fmt.Sprintf("$%d", i), 1)
		i++
	}

	return sql, val, odr, nil
}

// //获取relate定义的parents
// func (base *PostgresBase) parents(name string) (Map) {
// 	values := Map{}

// 	if config,ok := base.tables(name); ok {
// 		if fields,ok := config["fields"].(Map); ok {
// 			base.parent(name, fields, []string{}, values)
// 		}
// 	}

// 	return values;
// }

// //获取relate定义的parents
// func (base *PostgresBase) parent(table string, fields Map, tree []string, values Map) {
// 	for k,v := range fields {
// 		config := v.(Map)
// 		trees := append(tree, k)

// 		if config["relate"] != nil {

// 			relates := []Map{}

// 			switch ttts := config["relate"].(type) {
// 			case Map:
// 				relates = append(relates, ttts)
// 			case []Map:
// 				for _,ttt := range ttts {
// 					relates = append(relates, ttt)
// 				}
// 			}

// 			for i,relating := range relates {

// 				//relating := config["relate"].(Map)
// 				parent := relating["parent"].(string)

// 				//要从模型定义中,把所有父表的 schema, table 要拿过来
// 				if tableConfig,ok := base.tables(parent); ok {

// 					schema,table := SCHEMA,parent
// 					if tableConfig["schema"] != nil && tableConfig["schema"] != "" {
// 						schema = tableConfig["schema"].(string)
// 					}
// 					if tableConfig["table"] != nil && tableConfig["table"] != "" {
// 						table = tableConfig["table"].(string)
// 					}

// 					//加入列表，带上i是可能有多个字段引用同一个表？还是引用多个表？
// 					values[fmt.Sprintf("%v:%v", strings.Join(trees, "."), i)] = Map{
// 						"schema": schema, "table": table,
// 						"field": strings.Join(trees, "."), "relate": relating,
// 					}
// 				}
// 			}

// 		} else {
// 			if json,ok := config["json"].(Map); ok {
// 				base.parent(table, json, trees, values)
// 			}
// 		}
// 	}
// }

// //获取relate定义的childs
// func (base *PostgresBase) childs(model string) (Map) {
// 	values := Map{}

// 	for modelName,modelConfig := range base.bonder.tables {

// 		schema,table := SCHEMA,modelName
// 		if modelConfig["schema"] != nil && modelConfig["schema"] != "" {
// 			schema = modelConfig["schema"].(string)
// 		}
// 		if modelConfig["table"] != nil && modelConfig["table"] != "" {
// 			table = modelConfig["table"].(string)
// 		}

// 		if fields,ok := modelConfig["field"].(Map); ok {
// 			base.child(model, modelName, schema, table, fields, []string{ }, values)
// 		}
// 	}

// 	return values;
// }

// //获取relate定义的child
// func (base *PostgresBase) child(parent,model,schema,table string, configs Map, tree []string, values Map) {
// 	for k,v := range configs {
// 		config := v.(Map)
// 		trees := append(tree, k)

// 		if config["relate"] != nil {

// 			relates := []Map{}

// 			switch ttts := config["relate"].(type) {
// 			case Map:
// 				relates = append(relates, ttts)
// 			case []Map:
// 				for _,ttt := range ttts {
// 					relates = append(relates, ttt)
// 				}
// 			}

// 			for i,relating := range relates {

// 				//relating := config["relate"].(Map)

// 				if relating["parent"] == parent {
// 					values[fmt.Sprintf("%v:%v:%v", model, strings.Join(trees, "."), i)] = Map{
// 						"schema": schema, "table": table,
// 						"field": strings.Join(trees, "."), "relate": relating,
// 					}
// 				}
// 			}

// 		} else {
// 			if json,ok := config["json"].(Map); ok {
// 				base.child(parent,model,schema,table,json, trees, values)
// 			}
// 		}
// 	}
// }
