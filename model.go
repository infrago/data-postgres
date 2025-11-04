package data_postgres

import (
	"fmt"

	. "github.com/infrago/base"
	"github.com/infrago/data"
	"github.com/infrago/infra"
)

type (
	PostgresModel struct {
		base   *PostgresBase
		name   string //模型名称
		schema string //架构名
		model  string //这里可能是表名，视图名，或是集合名（mongodb)
		key    string //主键
		fields Vars   //字段定义
	}
)

// 查询单条
// 应该不需要用MAP，直接写SQL的
func (model *PostgresModel) First(args ...Any) Map {
	model.base.lastError = nil

	//生成查询条件
	where, builds, orderby, err := model.base.parsing(1, args...)
	if err != nil {
		model.base.errorHandler("model.first.parse", err, model.name)
		return nil
	}

	exec, err := model.base.beginExec()
	if err != nil {
		model.base.errorHandler("model.first.begin", err, model.name)
		return nil
	}

	//先拿字段列表
	//不能用*，必须指定字段列表
	//要不然下拉scan的时候，数据库返回的字段和顺序不一定对
	keys := []string{}
	for k, _ := range model.fields {
		keys = append(keys, k)
	}

	sql := fmt.Sprintf(`%s %s`, where, orderby)
	row := exec.QueryRow(sql, builds...)
	if row == nil {
		model.base.errorHandler("model.first.query", err, model.name, sql)
		return nil
	}

	//扫描数据
	values := make([]interface{}, len(keys))  //真正的值
	pValues := make([]interface{}, len(keys)) //指针，指向值
	for i := range values {
		pValues[i] = &values[i]
	}

	err = row.Scan(pValues...)
	if err != nil {
		model.base.errorHandler("model.first.scan", err, model.name)
		return nil
	}

	//这里应该有个打包
	m := model.base.unpacking(keys, values)

	//返回前使用编码生成
	//有必要的, 按模型拿到数据
	item := Map{}
	errm := infra.Mapping(model.fields, m, item, false, true)
	if errm.Fail() {
		model.base.errorHandler("model.first.mapping", errm, model.name)
		return nil
	}

	return item
}

// 查询列表
func (model *PostgresModel) Query(args ...Any) []Map {
	model.base.lastError = nil

	//生成查询条件
	where, builds, orderby, err := model.base.parsing(1, args...)
	if err != nil {
		model.base.errorHandler("model.query.parse", err, model.name)
		return []Map{}
	}

	exec, err := model.base.beginExec()
	if err != nil {
		model.base.errorHandler("model.query.begin", err, model.name)
		return []Map{}
	}

	sql := fmt.Sprintf(`%s %s`, where, orderby)
	rows, err := exec.Query(sql, builds...)
	if err != nil {
		model.base.errorHandler("model.query.query", err, model.name, sql, builds)
		return []Map{}
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		model.base.errorHandler("model.query.columns", err, model.name, cols)
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
			model.base.errorHandler("model.query.scan", err, model.name)
			return []Map{}
		}

		//这里应该有个打包
		m := model.base.unpacking(cols, values)

		//返回前使用编码生成
		//有必要的, 按模型拿到数据
		item := Map{}
		//直接使用err=会有问题，总是不为nil，解析就失败
		errm := infra.Mapping(model.fields, m, item, false, true)
		if errm.Fail() {
			model.base.errorHandler("model.query.mapping", errm, model.name)
			return []Map{}
		} else {
			items = append(items, item)
		}
	}

	return items
}

func (model *PostgresModel) Range(next data.RangeFunc, args ...Any) Res {
	return model.LimitRange(0, next, args...)
}

// 查询列表
func (model *PostgresModel) LimitRange(limit int64, next data.RangeFunc, args ...Any) Res {
	if next == nil {
		return infra.Fail
	}
	if limit < 0 {
		return infra.Fail
	}

	model.base.lastError = nil

	//生成查询条件
	where, builds, orderby, err := model.base.parsing(1, args...)
	if err != nil {
		model.base.errorHandler("model.range.parse", err, model.name)
		return infra.Fail
	}

	exec, err := model.base.beginExec()
	if err != nil {
		model.base.errorHandler("model.range.begin", err, model.name)
		return infra.Fail
	}

	sql := fmt.Sprintf(`%s %s`, where, orderby)
	rows, err := exec.Query(sql, builds...)
	if err != nil {
		model.base.errorHandler("model.range.query", err, model.name, sql, builds)
		return infra.Fail
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		model.base.errorHandler("model.range.columns", err, model.name, cols)
		return infra.Fail
	}

	//遍历结果
	// items := []Map{}
	for rows.Next() {
		//扫描数据
		values := make([]interface{}, len(cols))  //真正的值
		pValues := make([]interface{}, len(cols)) //指针，指向值
		for i := range values {
			pValues[i] = &values[i]
		}
		err = rows.Scan(pValues...)

		if err != nil {
			model.base.errorHandler("model.range.scan", err, model.name)
			return infra.Fail
		}

		//这里应该有个打包
		m := model.base.unpacking(cols, values)

		//返回前使用编码生成
		//有必要的, 按模型拿到数据
		item := Map{}
		//直接使用err=会有问题，总是不为nil，解析就失败
		errm := infra.Mapping(model.fields, m, item, false, true)
		if errm.Fail() {
			model.base.errorHandler("model.range.mapping", errm, model.name)
			return infra.Fail
		} else {
			if res := next(item); res.Fail() {
				return res
			}
			if limit > 0 {
				limit--
				if limit <= 0 {
					break
				}
			}
		}
	}

	return infra.OK
}
