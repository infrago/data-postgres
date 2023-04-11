package data_postgres

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type (
	PostgresView struct {
		base   *PostgresBase
		name   string //模型名称
		schema string //架构名
		view   string //视图名
		key    string //主键
		fields Vars   //字段定义
	}
)

// 统计数量
// 添加函数支持
// 函数(字段）
func (view *PostgresView) Count(args ...Any) float64 {
	view.base.lastError = nil

	//函数和字段
	//db.Table("table").Count(FUNC,FIELD, args...)
	countFunc := "COUNT"
	countField := view.key //count(*) queryrow才支持，query不支持

	if len(args) >= 2 {
		s1vv, s1ok := args[0].(string)
		s2vv, s2ok := args[1].(string)
		if s1ok && s2ok && s1vv != "" && s2vv != "" {
			countFunc = s1vv
			countField = s2vv
			args = args[2:]
		}
	}

	//支持数组aaa:1
	if dots := strings.Split(countField, ":"); len(dots) >= 2 {
		countField = fmt.Sprintf(`"%v"[%v]`, dots[0], dots[1])
	} else {
		countField = fmt.Sprintf(`"%v"`, countField)
	}

	//生成查询条件
	where, builds, _, err := view.base.parsing(1, args...)
	if err != nil {
		view.base.errorHandler("data.count.parse", err, view.name)
		return float64(0)
	}

	exec, err := view.base.beginExec()
	if err != nil {
		view.base.errorHandler("data.count.begin", err, view.name)
		return float64(0)
	}

	sql := fmt.Sprintf(`SELECT %v(%v) FROM "%s"."%s" WHERE %s`, countFunc, countField, view.schema, view.view, where)
	rows, err := exec.Query(sql, builds...)
	if err != nil {
		view.base.errorHandler("data.count.query", err, view.name, sql, builds)
		return float64(0)
	}
	defer rows.Close()

	count := float64(0)
	for rows.Next() {

		var ccc Any
		err = rows.Scan(&ccc)
		if err != nil {
			//注意，count,max,min等函数的统计，没有数据会返回nil
			view.base.errorHandler("data.count.scan", err, view.name, sql, builds)
			return float64(0)
		}

		if vv, ok := ccc.(float64); ok {
			count = vv
		} else if vv, ok := ccc.(float32); ok {
			count = float64(vv)
		} else if vv, ok := ccc.(int64); ok {
			count = float64(vv)
		} else if vv, ok := ccc.(int); ok {
			count = float64(vv)
		} else if vv, ok := ccc.(int32); ok {
			count = float64(vv)
		} else if vv, ok := ccc.([]byte); ok {
			//DECIMAL 会返回这个
			if v, e := strconv.ParseFloat(string(vv), 64); e == nil {
				count = float64(v)
			}
		}
	}

	return count
}

// 查询单条
// 171015改成*版
func (view *PostgresView) First(args ...Any) Map {
	view.base.lastError = nil

	//生成查询条件
	where, builds, orderby, err := view.base.parsing(1, args...)
	if err != nil {
		view.base.errorHandler("data.first.parse", err, view.name)
		return nil
	}

	//获取
	exec, err := view.base.beginExec()
	if err != nil {
		view.base.errorHandler("data.first.begin", err, view.name)
		return nil
	}

	sql := fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE %s %s LIMIT 1`, view.schema, view.view, where, orderby)
	rows, err := exec.Query(sql, builds...)
	if err != nil {
		view.base.errorHandler("data.first.query", err, view.name, err, sql, builds)
		return nil
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		view.base.errorHandler("data.first.columns", err, view.name, sql, cols)
		return nil
	}

	for rows.Next() {

		//扫描数据
		values := make([]interface{}, len(cols))  //真正的值
		pValues := make([]interface{}, len(cols)) //指针，指向值
		for i := range values {
			pValues[i] = &values[i]
		}

		err = rows.Scan(pValues...)
		if err != nil {
			view.base.errorHandler("data.first.scan", err, view.name)
			return nil
		}

		//这里应该有个解包
		m := view.base.unpacking(cols, values)

		//返回前使用编码生成
		//有必要的, 按模型拿到数据
		item := Map{}
		//直接使用err=会有问题，总是不会nil，就解析问题
		errm := infra.Mapping(view.fields, m, item, false, true)
		if errm.Fail() {
			view.base.errorHandler("data.first.mapping", errm, view.name)
			return nil
		}
		return item
	}

	return nil
}

// 查询列表
// 171015改成*版
func (view *PostgresView) Query(args ...Any) []Map {
	view.base.lastError = nil

	//生成查询条件
	where, builds, orderby, err := view.base.parsing(1, args...)
	if err != nil {
		view.base.errorHandler("data.query.parse", err, view.name)
		return []Map{}
	}

	exec, err := view.base.beginExec()
	if err != nil {
		view.base.errorHandler("data.query.begin", err, view.name)
		return []Map{}
	}

	sql := fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE %s %s`, view.schema, view.view, where, orderby)
	rows, err := exec.Query(sql, builds...)
	if err != nil {
		view.base.errorHandler("data.query.query", err, view.name, sql, builds)
		return []Map{}
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		view.base.errorHandler("data.query.columns", err, view.name, cols)
		return []Map{}
	}

	//遍历结果
	items := []Map{}
	for rows.Next() {
		//扫描数据
		values := make([]interface{}, len(cols))  //真正的值
		pValues := make([]interface{}, len(cols)) //指针，指向值
		for i := range values {
			pValues[i] = &values[i]
		}
		err = rows.Scan(pValues...)

		if err != nil {
			view.base.errorHandler("data.query.scan", err, view.name)
			return []Map{}
		}

		//这里应该有个打包
		m := view.base.unpacking(cols, values)

		//返回前使用编码生成
		//有必要的, 按模型拿到数据
		item := Map{}
		//直接使用err=会有问题，总是不为nil，解析就失败
		errm := infra.Mapping(view.fields, m, item, false, true)
		if errm.Fail() {
			view.base.errorHandler("data.query.mapping", errm, view.name)
			return []Map{}
		} else {
			items = append(items, item)
		}
	}

	return items
}

// 分页查询
// 171015更新为字段*版
func (view *PostgresView) Limit(offset, limit Any, args ...Any) (int64, []Map) {
	view.base.lastError = nil

	//生成查询条件
	where, builds, orderby, err := view.base.parsing(1, args...)
	if err != nil {
		view.base.errorHandler("data.limit.parse", err, view.name)
		return int64(0), []Map{}
	}

	//开启事务
	exec, err := view.base.beginExec()
	if err != nil {
		view.base.errorHandler("data.limit.begin", err, view.name)
		return int64(0), []Map{}
	}

	//先统计，COUNT(*) QueryRow支持，Query不支持
	sql := fmt.Sprintf(`SELECT COUNT("%v") FROM "%s"."%s" WHERE %s`, view.key, view.schema, view.view, where)
	row := exec.QueryRow(sql, builds...)
	if row == nil {
		view.base.errorHandler("data.limit.count", errors.New("统计失败"))
		return int64(0), []Map{}
	}

	count := int64(0)

	err = row.Scan(&count)
	if err != nil {
		view.base.errorHandler("data.limit.count", err, view.name, sql, builds)
		return int64(0), []Map{}
	}

	sql = fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE %s %s OFFSET %d LIMIT %d`, view.schema, view.view, where, orderby, offset, limit)
	rows, err := exec.Query(sql, builds...)
	if err != nil {
		view.base.errorHandler("data.limit.query", err, view.name)
		return int64(0), []Map{}
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		view.base.errorHandler("data.limit.columns", err, view.name, columns)
		return int64(0), []Map{}
	}

	//返回结果在这
	items := []Map{}

	//遍历结果
	for rows.Next() {
		//扫描数据
		values := make([]interface{}, len(columns))  //真正的值
		pValues := make([]interface{}, len(columns)) //指针，指向值
		for i := range values {
			pValues[i] = &values[i]
		}
		err = rows.Scan(pValues...)

		if err != nil {
			view.base.errorHandler("data.limit.scan", err, view.name)
			return int64(0), []Map{}
		}

		//这里应该有个打包
		m := view.base.unpacking(columns, values)

		//返回前使用编码生成
		//有必要的, 按模型拿到数据
		item := Map{}
		//直接用err= 会有问题，总是不为nil，解析就拿原始值了
		errm := infra.Mapping(view.fields, m, item, false, true)
		if errm.Fail() {
			view.base.errorHandler("data.limit.mapping", errm, view.name)
			return int64(0), []Map{}
			//items = append(items, m)
		} else {
			items = append(items, item)
		}
	}

	return count, items
}

// 查询分组
func (view *PostgresView) Group(field string, args ...Any) []Map {
	view.base.lastError = nil

	method := COUNT
	count := field
	countField := "$count"
	if len(args) >= 3 {
		vv1, ok1 := args[0].(string)
		vv2, ok2 := args[1].(string)
		if ok1 && ok2 {
			method = vv1
			count = vv2
			countField = vv2
			args = args[2:]
		}
	}

	//生成查询条件
	where, builds, orderby, err := view.base.parsing(1, args...)
	if err != nil {
		view.base.errorHandler("data.group.parsing", err, view.name)
		return []Map{}
	}

	exec, err := view.base.beginExec()
	if err != nil {
		view.base.errorHandler("data.group.begin", err, view.name)
		return []Map{}
	}

	if orderby == "" {
		orderby = `ORDER BY "` + countField + `" DESC`
	}

	keys := []string{field, countField}

	sql := fmt.Sprintf(`SELECT "%s",%s("%s") as "%s" FROM "%s"."%s" WHERE %s GROUP BY "%s" %s`, field, method, count, countField, view.schema, view.view, where, field, orderby)
	// if limit > 0 {
	// 	sql += fmt.Sprintf(` LIMIT %d`, limit)
	// }
	rows, err := exec.Query(sql, builds...)
	if err != nil {
		view.base.errorHandler("data.group.query", err, view.name, sql)
		return []Map{}
	}
	defer rows.Close()

	//返回结果在这
	items := []Map{}

	//遍历结果
	for rows.Next() {

		//扫描数据
		values := make([]interface{}, len(keys))  //真正的值
		pValues := make([]interface{}, len(keys)) //指针，指向值
		for i := range values {
			pValues[i] = &values[i]
		}
		err = rows.Scan(pValues...)

		if err != nil {
			view.base.errorHandler("data.group.scan", err, view.name, sql)
			return []Map{}
		}

		//这里应该有个打包
		m := view.base.unpacking(keys, values)

		fields := Vars{}
		for _, key := range keys {
			fields[key] = view.fields[key]
		}
		// if method == COUNT {
		// 	fields[count] = Var{Type: "int", Required: true}
		// } else {
		// 	fields[count] = Var{Type: "float", Required: true}
		// }

		//返回前使用编码生成
		//有必要的, 按模型拿到数据
		item := Map{}
		//直接使用err=会有问题，总是不为nil，解析就拿到原始值了
		errm := infra.Mapping(fields, m, item, true, true)
		if errm.Fail() {
			view.base.errorHandler("data.group.mapping", errm, view.name)
			return []Map{}
		} else {
			items = append(items, item)
		}

	}
	return items
}

//查询分组，无count版本
//func (view *PostgresView) Group(field string, args ...Any) []Map {
//	view.base.lastError = nil
//
//	//生成查询条件
//	where, builds, orderby, err := view.base.parsing(1, args...)
//	if err != nil {
//		view.base.errorHandler("data.group.parsing", err, view.name)
//		return []Map{}
//	}
//
//	exec, err := view.base.beginExec()
//	if err != nil {
//		view.base.errorHandler("data.group.begin", err, view.name)
//		return []Map{}
//	}
//
//	keys := []string{field}
//
//	sql := fmt.Sprintf(`SELECT "%s" FROM "%s"."%s" WHERE %s GROUP BY "%s" %s`, field, view.schema, view.view, where, field, orderby)
//	rows, err := exec.Query(sql, builds...)
//	if err != nil {
//		view.base.errorHandler("data.group.query", err, view.name, sql)
//		return []Map{}
//	}
//
//	defer rows.Close()
//
//	//返回结果在这
//	items := []Map{}
//
//	//遍历结果
//	for rows.Next() {
//
//		//扫描数据
//		values := make([]interface{}, len(keys))  //真正的值
//		pValues := make([]interface{}, len(keys)) //指针，指向值
//		for i := range values {
//			pValues[i] = &values[i]
//		}
//		err = rows.Scan(pValues...)
//
//		if err != nil {
//			view.base.errorHandler("data.group.scan", err, view.name, sql)
//			return []Map{}
//		}
//
//		//这里应该有个打包
//		m := view.base.unpacking(keys, values)
//
//		//返回前使用编码生成
//		//有必要的, 按模型拿到数据
//		item := Map{}
//		//直接使用err=会有问题，总是不为nil，解析就拿到原始值了
//		errm := infra.Mapping(view.fields, m, item, false, true)
//		if errm.Fail() {
//			view.base.errorHandler("data.group.mapping", errm, view.name)
//			return []Map{}
//		} else {
//			items = append(items, item)
//		}
//
//	}
//	return items
//}

// 查询唯一对象
// 换成字段*版
func (view *PostgresView) Entity(id Any) Map {
	view.base.lastError = nil

	//开启事务
	exec, err := view.base.beginExec()
	if err != nil {
		view.base.errorHandler("data.entity.begin", err, view.name)
		return nil
	}

	//可以用*了，因为可以拿到字段列表
	sql := fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE "%s"=$1`, view.schema, view.view, view.key)
	rows, err := exec.Query(sql, id) //QueryRow不支持获取字段列表
	if err != nil {
		view.base.errorHandler("data.entity.query", err, view.name, sql)
		return nil
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		view.base.errorHandler("data.entity.columns", err, view.name, columns)
		return nil
	}

	for rows.Next() {

		//扫描数据
		values := make([]interface{}, len(columns))  //真正的值
		pValues := make([]interface{}, len(columns)) //指针，指向值
		for i := range values {
			pValues[i] = &values[i]
		}

		err = rows.Scan(pValues...)
		if err != nil {
			view.base.errorHandler("data.entity.scan", err, view.name, sql)
			return nil
		}

		//这里应该有个打包
		m := view.base.unpacking(columns, values)

		//返回前使用编码生成
		//有必要的, 按模型拿到数据
		item := Map{}
		errm := infra.Mapping(view.fields, m, item, false, true)
		if errm.Fail() {
			//return m,nil
			view.base.errorHandler("data.entity.mapping", errm, view.name)
			return nil
		} else {
			return item
		}
	}

	return nil
}
