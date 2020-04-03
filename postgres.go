package pg

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/xooooooox/utils"
	"reflect"
	"strings"
)

var (
	idname string = "id" // 主键名称
	escape string = "\"" // SQL转义字符
	dollar string = "$"  // SQL占位符
)

var DB *sql.DB

type Curd struct {
	table  string                 // 查询的表名
	alias  string                 // 表名别名
	column string                 // 查询列名
	update map[string]interface{} // 更新列信息
	join   string                 // 联合查询
	where  string                 // 条件语句
	group  string                 // 分组信息
	order  string                 // 排序信息
	limit  int64                  // 查询条数
	offset int64                  // 跳过的数据条数
	page   int64                  // 页码
	dollar int                    // $n SQL占位符序号
	sql    string                 // SQL
	args   []interface{}          // SQL args
	error  error                  // error
	tx     *sql.Tx                // transaction
	print  bool                   // 是否打印执行的SQL脚本及参数
}

// derive 多种数据类型推算出表名
func derive(table interface{}) string {
	rt := reflect.TypeOf(table)
	kind := rt.Kind()
	if kind == reflect.Ptr {
		rt = rt.Elem()
		kind = rt.Kind()
		if kind == reflect.Struct {
			return utils.PascalToUnderline(rt.Name())
		}
		if kind == reflect.String {
			return strings.ToLower(reflect.ValueOf(table).Elem().Interface().(string))
		}
		return ""
	}
	if kind == reflect.Struct {
		return utils.PascalToUnderline(rt.Name())
	}
	if kind == reflect.String {
		return strings.ToLower(table.(string))
	}
	return ""
}

// escapes SQL转义查询列名
func escapes(name string) string {
	// "u"."id"
	if strings.Index(name, escape) >= 0 {
		return name
	}
	// u.id,u.name,u.email
	if strings.Index(name, ",") >= 0 {
		return name
	}
	// count(*) as count
	if strings.Index(name, "(") >= 0 {
		return name
	}
	// 存在 AS 关键字的情况下
	length := len(name)
	index := strings.Index(name, "as")
	if index < 0 {
		index = strings.Index(name, "AS")
	}
	if index > 0 && index < length {
		return fmt.Sprintf("%s AS %s", escaped(name[:index]), escaped(name[index+2:]))
	}
	// 没有AS关键字, 普通字段 如: u.id, name
	return escaped(name)
}

// escaped SQL转义 email => "email", u.id => "u"."id"
func escaped(name string) string {
	if strings.Index(name, escape) >= 0 {
		return name
	}
	if strings.Index(name, ",") >= 0 {
		return name
	}
	name = strings.TrimSpace(name)
	if strings.Index(name, " ") >= 0 {
		return name
	}
	return fmt.Sprintf(`%s%s%s`, escape, strings.Replace(name, ".", fmt.Sprintf(`%s.%s`, escape, escape), -1), escape)
}

// dollars Postgres 有序的占位符
func dollars(index int) string {
	return fmt.Sprintf("%s%d", dollar, index)
}

func Table(table interface{}) *Curd {
	x := &Curd{}
	x.table = escaped(derive(table))
	return x
}

func Begin() *Curd {
	x := &Curd{}
	tx, err := DB.Begin()
	if err != nil {
		x.error = err
		return x
	}
	x.tx = tx
	return x
}

func (x *Curd) RollBack() {
	x.error = x.tx.Rollback()
}

func (x *Curd) Commit() {
	x.error = x.tx.Commit()
}

func (x *Curd) Print(print ...bool) *Curd {
	if len(print) > 0 && x.print != print[0] {
		x.print = print[0]
	} else {
		x.print = true
	}
	return x
}

func (x *Curd) Error() error {
	return x.error
}

func (x *Curd) Exec(execute string, args ...interface{}) (rows int64) {
	var err error
	defer func() {
		x.error = err
	}()
	if x.print {
		fmt.Println(execute, args) // 输出执行的SQL脚本和对应参数
	}
	if x.tx != nil {
		stmt, err := x.tx.Prepare(execute)
		if err != nil {
			x.RollBack()
			return
		}
		result, err := stmt.Exec(args...)
		if err != nil {
			x.RollBack()
			return
		}
		rows, err = result.RowsAffected()
		if err != nil {
			x.RollBack()
			return
		}
		return
	}
	stmt, err := DB.Prepare(execute)
	if err != nil {
		return
	}
	result, err := stmt.Exec(args...)
	if err != nil {
		return
	}
	rows, err = result.RowsAffected()
	if err != nil {
		return
	}
	return
}

func (x *Curd) Add(insert interface{}) (id int64) {
	if insert == nil {
		return
	}
	t, v := reflect.TypeOf(insert), reflect.ValueOf(insert)
	if t.Kind() != reflect.Ptr {
		return
	}
	t, v = t.Elem(), v.Elem()
	if t.Kind() != reflect.Struct {
		return
	}
	cols, vals := "", ""
	args := []interface{}{}
	index := 0
	column := ""
	sqlInsert := ""
	for i := 0; i < t.NumField(); i++ {
		column = utils.PascalToUnderline(t.Field(i).Name)
		if column == idname {
			continue
		}
		args = append(args, v.Field(i).Interface())
		index++
		if cols == "" {
			cols = escaped(column)
			vals = dollars(index)
			continue
		}
		cols = fmt.Sprintf(`%s, %s`, cols, escaped(column))
		vals = fmt.Sprintf("%s, %s", vals, dollars(index))
	}
	sqlInsert = fmt.Sprintf(`INSERT INTO %s ( %s ) VALUES ( %s ) RETURNING %s`, escaped(utils.PascalToUnderline(t.Name())), cols, vals, escaped(idname))
	if x.print {
		fmt.Println(sqlInsert, args) // 输出执行的SQL脚本和对应参数
	}
	if x.tx != nil {
		x.error = x.tx.QueryRow(sqlInsert, args...).Scan(&id)
		if x.error != nil {
			x.RollBack()
		}
		return
	}
	x.error = DB.QueryRow(sqlInsert, args...).Scan(&id)
	return
}

func (x *Curd) Adds(batch ...interface{}) (rows int64) {
	// 传入当前函数的所有结构体信息
	type insert struct {
		Table  string
		Column string
		Values string
		Args   []interface{}
	}
	// 需要执行的sql结构体信息
	type exec struct {
		Sql  string
		Args []interface{}
	}
	length := len(batch)
	// 需要插入的数据总条数,每一条的信息
	inserts := make([]insert, length, length)
	// 针对每个表的占位符索引
	sqlIndex := map[string]int{}
	for i := 0; i < length; i++ {
		// 不能有空指针
		if batch[i] == nil {
			return
		}
		t, v := reflect.TypeOf(batch[i]), reflect.ValueOf(batch[i])
		// 确保参数的每一个参数是指针
		if t.Kind() != reflect.Ptr {
			return
		}
		t, v = t.Elem(), v.Elem()
		// 确保参数的每一个参数是结构体指针
		if t.Kind() != reflect.Struct {
			return
		}
		// 当前这个结构体的所映射的表名
		table := utils.PascalToUnderline(t.Name())
		if _, ok := sqlIndex[table]; !ok {
			sqlIndex[table] = 1 // 占位符索引从1开始
		}
		for j := 0; j < v.NumField(); j++ {
			// 如果column名称是id 跳过 (主键,自动递增)
			if utils.PascalToUnderline(t.Field(j).Name) == idname {
				continue
			}
			inserts[i].Table = escaped(table)
			inserts[i].Args = append(inserts[i].Args, v.Field(j).Interface())
			if inserts[i].Column == "" {
				inserts[i].Column = fmt.Sprintf("%s", escaped(utils.PascalToUnderline(t.Field(j).Name)))
				inserts[i].Values = fmt.Sprintf("%s", dollars(sqlIndex[table]))
				sqlIndex[table]++
				continue
			}
			inserts[i].Column = fmt.Sprintf("%s, %s", inserts[i].Column, escaped(utils.PascalToUnderline(t.Field(j).Name)))
			inserts[i].Values = fmt.Sprintf("%s, %s", inserts[i].Values, fmt.Sprintf("%s", dollars(sqlIndex[table])))
			sqlIndex[table]++
		}
	}
	execs := map[string]exec{}
	for i := 0; i < length; i++ {
		table := inserts[i].Table
		if execs[table].Sql == "" {
			execs[table] = exec{
				Sql:  fmt.Sprintf("INSERT INTO %s ( %s ) VALUES ( %s )", inserts[i].Table, inserts[i].Column, inserts[i].Values),
				Args: inserts[i].Args,
			}
			continue
		}
		execs[table] = exec{
			Sql:  fmt.Sprintf("%s, ( %s )", execs[table].Sql, inserts[i].Values),
			Args: append(execs[table].Args, inserts[i].Args...),
		}
	}
	for _, val := range execs {
		rows += x.Exec(val.Sql, val.Args...)
	}
	return
}

func (x *Curd) Del() int64 {
	defer x.Clear()
	x.sql = fmt.Sprintf("DELETE FROM %s", x.table)
	if x.where != "" {
		x.sql = fmt.Sprintf("%s WHERE ( %s )", x.sql, x.where)
	}
	rows := x.Exec(x.sql, x.args...)
	return rows
}

func (x *Curd) Ups(ups ...map[string]interface{}) int64 {
	defer x.Clear()
	if x.update == nil {
		x.update = map[string]interface{}{}
	}
	for _, v := range ups {
		for col, val := range v {
			x.update[col] = val
		}
	}
	aws := x.args
	set := ""
	x.dollar = 0
	x.args = []interface{}{}
	for k, v := range x.update {
		x.dollar++
		x.args = append(x.args, v)
		if set == "" {
			set = fmt.Sprintf("%s = %s", escaped(k), dollars(x.dollar))
			continue
		}
		set = fmt.Sprintf("%s, %s = %s", set, escaped(k), dollars(x.dollar))
	}
	if set == "" {
		return 0
	}
	x.sql = fmt.Sprintf("UPDATE %s SET %s", x.table, set)
	if x.where != "" {
		countDollarInWhere := strings.Count(x.where, dollar)
		for i := 1; i <= countDollarInWhere; i++ {
			x.dollar++
			x.where = strings.Replace(x.where, dollars(i), dollars(x.dollar), -1)
		}
		x.sql = fmt.Sprintf("%s WHERE ( %s )", x.sql, x.where)
	}
	x.args = append(x.args, aws...)
	rows := x.Exec(x.sql, x.args...)
	return rows
}

// result *AnyStruct LIMIT 1
// result *[]*AnyStruct LIMIT N, N>1
func (x *Curd) Get(result interface{}) (err error) {
	defer x.Clear()
	defer func() {
		x.error = err
	}()
	err = errors.New("error format args")
	rt, rv := reflect.TypeOf(result), reflect.ValueOf(result)
	kind := rt.Kind()
	if kind != reflect.Ptr {
		return
	}
	rt1 := rt.Elem()
	kind = rt1.Kind()
	if x.column == "" {
		x.column = "*"
	}
	x.sql = fmt.Sprintf("SELECT %s FROM %s", x.column, x.table)
	if x.alias != "" {
		x.sql = fmt.Sprintf("%s %s", x.sql, x.alias)
	}
	if x.join != "" {
		x.sql = fmt.Sprintf("%s%s", x.sql, x.join)
	}
	if x.where != "" {
		x.sql = fmt.Sprintf("%s WHERE ( %s )", x.sql, x.where)
	}
	if x.group != "" {
		x.sql = fmt.Sprintf("%s GROUP BY %s", x.sql, x.group)
	}
	if x.order != "" {
		x.sql = fmt.Sprintf("%s ORDER BY %s", x.sql, x.order)
	}
	if x.limit == 0 {
		x.limit = 1
	}
	x.sql = fmt.Sprintf("%s LIMIT %d", x.sql, x.limit)
	if x.page != 0 {
		x.offset = (x.page - 1) * x.limit
	}
	x.sql = fmt.Sprintf("%s OFFSET %d", x.sql, x.offset)
	if x.limit == 1 {
		if kind != reflect.Struct {
			return
		}
	}
	if x.limit > 1 {
		if kind != reflect.Slice {
			return
		}
		rt1 = rt1.Elem()
		kind = rt1.Kind()
		if kind != reflect.Ptr {
			return
		}
		rt1 = rt1.Elem()
		kind = rt1.Kind()
		if kind != reflect.Struct {
			return
		}
	}
	if x.print {
		fmt.Println(x.sql, x.args) // 输出执行的SQL脚本和对应参数
	}
	// 执行查询SQL
	rows, err := DB.Query(x.sql, x.args...)
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	// 查询一条
	if x.limit == 1 {
		data := rv.Elem()       // 最终返回的数据
		rzv := reflect.Value{}  // reflect zero value, 反射包零值
		cols := []interface{}{} // 列名集合
		for _, cn := range columns {
			cnv := data.FieldByName(utils.UnderlineToPascal(strings.ToLower(cn))) // 列名全部转换成小写, 下划线命名转帕斯卡命名
			if cnv == rzv || !cnv.CanSet() {
				// 结构体缺少cn字段, 或者结构体的cn字段不可访问(小写字母开头)
				err = errors.New(fmt.Sprintf("structure is missing fields: %s", utils.UnderlineToPascal(cn)))
				return
			}
			cols = append(cols, cnv.Addr().Interface())
		}
		for rows.Next() {
			err = rows.Scan(cols...)
			if err != nil {
				return err
			}
			break
		}
		reflect.ValueOf(result).Elem().Set(data)
		return
	}
	// 查询多条
	rzv := reflect.Value{} // reflect zero value, 反射包零值
	data := rv.Elem()      // 最终返回的数据
	for rows.Next() {
		row := reflect.New(rt.Elem().Elem().Elem()) // struct
		rowVal := reflect.Indirect(row)
		cols := []interface{}{} // 列名集合
		for _, cn := range columns {
			cnv := rowVal.FieldByName(utils.UnderlineToPascal(strings.ToLower(cn)))
			if cnv == rzv || !cnv.CanSet() {
				// 结构体缺少cn字段, 或者结构体的cn字段不可访问(小写字母开头)
				err = errors.New(fmt.Sprintf("structure is missing fields: %s", utils.UnderlineToPascal(cn)))
				return
			}
			cols = append(cols, cnv.Addr().Interface())
		}
		err = rows.Scan(cols...)
		if err != nil {
			return err
		}
		data = reflect.Append(data, row)
	}
	reflect.ValueOf(result).Elem().Set(data)
	return
}

func (x *Curd) Table(table interface{}) *Curd {
	x.table = escaped(derive(table))
	return x
}

func (x *Curd) Mod(column string, value interface{}) *Curd {
	if x.update == nil {
		x.update = map[string]interface{}{}
	}
	x.update[column] = value
	return x
}

func (x *Curd) Alias(alias interface{}) *Curd {
	x.alias = escaped(derive(alias))
	return x
}

func (x *Curd) Cols(cols ...string) *Curd {
	for _, v := range cols {
		v = escapes(v)
		if x.column == "" {
			x.column = v
		} else {
			x.column = fmt.Sprintf("%s, %s", x.column, v)
		}
	}
	return x
}

func (x *Curd) Join(table interface{}, alias interface{}, col1 string, col2 string) *Curd {
	x.LeftJoin(table, alias, col1, col2)
	return x
}

func (x *Curd) LeftJoin(table interface{}, alias interface{}, col1 string, col2 string) *Curd {
	x.join = fmt.Sprintf("%s LEFT JOIN %s %s ON %s = %s", x.join, escaped(derive(table)), escaped(derive(alias)), escaped(col1), escaped(col2))
	return x
}

func (x *Curd) InnerJoin(table interface{}, alias interface{}, col1 string, col2 string) *Curd {
	x.join = fmt.Sprintf("%s INNER JOIN %s %s ON %s = %s", x.join, escaped(derive(table)), escaped(derive(alias)), escaped(col1), escaped(col2))
	return x
}

func (x *Curd) RightJoin(table interface{}, alias interface{}, col1 string, col2 string) *Curd {
	x.join = fmt.Sprintf("%s RIGHT JOIN %s %s ON %s = %s", x.join, escaped(derive(table)), escaped(derive(alias)), escaped(col1), escaped(col2))
	return x
}

func (x *Curd) Where(where string, args ...interface{}) *Curd {
	x.where = where
	x.args = args
	return x
}

func (x *Curd) whereLogic(logic string) string {
	x.where = strings.TrimSpace(x.where)
	if x.where == "" {
		return ""
	}
	if strings.HasSuffix(x.where, "(") {
		return fmt.Sprintf("%s ", x.where)
	}
	return fmt.Sprintf("%s %s ", x.where, logic)
}

func (x *Curd) WhereBracketsLeft() *Curd {
	if x.where == "" {
		x.where = "("
	} else {
		x.where = fmt.Sprintf("%s %s", x.where, "AND (")
	}
	return x
}

func (x *Curd) WhereOrBracketsLeft() *Curd {
	if x.where == "" {
		x.where = "("
	} else {
		x.where = fmt.Sprintf("%s %s", x.where, "OR (")
	}
	return x
}

func (x *Curd) WhereBracketsRight() *Curd {
	x.where = fmt.Sprintf("%s %s", x.where, ")")
	return x
}

func (x *Curd) WhereEqual(col string, val interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s = %s", x.whereLogic("AND"), col, dollars(x.dollar))
	return x
}

func (x *Curd) WhereNotEqual(col string, val interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s <> %s", x.whereLogic("AND"), col, dollars(x.dollar))
	return x
}

func (x *Curd) WhereMoreThan(col string, val interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s > %s", x.whereLogic("AND"), col, dollars(x.dollar))
	return x
}

func (x *Curd) WhereMoreThanEqual(col string, val interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s >= %s", x.whereLogic("AND"), col, dollars(x.dollar))
	return x
}

func (x *Curd) WhereLessThan(col string, val interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s < %s", x.whereLogic("AND"), col, dollars(x.dollar))
	return x
}

func (x *Curd) WhereLessThanEqual(col string, val interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s <= %s", x.whereLogic("AND"), col, dollars(x.dollar))
	return x
}

func (x *Curd) WhereIn(col string, val ...interface{}) *Curd {
	col = escaped(col)
	ins := ""
	for _, v := range val {
		x.dollar++
		x.args = append(x.args, v)
		if ins == "" {
			ins = dollars(x.dollar)
		} else {
			ins = fmt.Sprintf("%s, %s", ins, dollars(x.dollar))
		}
	}
	x.where = fmt.Sprintf("%s%s IN ( %s )", x.whereLogic("AND"), col, ins)
	return x
}

func (x *Curd) WhereNotIn(col string, val ...interface{}) *Curd {
	col = escaped(col)
	ins := ""
	for _, v := range val {
		x.dollar++
		x.args = append(x.args, v)
		if ins == "" {
			ins = dollars(x.dollar)
		} else {
			ins = fmt.Sprintf("%s, %s", ins, dollars(x.dollar))
		}
	}
	x.where = fmt.Sprintf("%s%s NOT IN ( %s )", x.whereLogic("AND"), col, ins)
	return x
}

func (x *Curd) WhereBetween(col string, val1 interface{}, val2 interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val1)
	x.args = append(x.args, val2)
	x.where = fmt.Sprintf("%s%s BETWEEN %s AND %s", x.whereLogic("AND"), col, dollars(x.dollar), dollars(x.dollar+1))
	x.dollar++
	return x
}

func (x *Curd) WhereOrEqual(col string, val interface{}) *Curd {
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s = %s", x.whereLogic("OR"), escaped(col), dollars(x.dollar))
	return x
}

func (x *Curd) WhereOrNotEqual(col string, val interface{}) *Curd {
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s <> %s", x.whereLogic("OR"), escaped(col), dollars(x.dollar))
	return x
}

func (x *Curd) WhereOrMoreThan(col string, val interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s > %s", x.whereLogic("OR"), col, dollars(x.dollar))
	return x
}

func (x *Curd) WhereOrMoreThanEqual(col string, val interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s >= %s", x.whereLogic("OR"), col, dollars(x.dollar))
	return x
}

func (x *Curd) WhereOrLessThan(col string, val interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s < %s", x.whereLogic("OR"), col, dollars(x.dollar))
	return x
}

func (x *Curd) WhereOrLessThanEqual(col string, val interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s <= %s", x.whereLogic("OR"), col, dollars(x.dollar))
	return x
}

func (x *Curd) WhereOrIn(col string, val ...interface{}) *Curd {
	col = escaped(col)
	ins := ""
	for _, v := range val {
		x.dollar++
		x.args = append(x.args, v)
		if ins == "" {
			ins = dollars(x.dollar)
		} else {
			ins = fmt.Sprintf("%s, %s", ins, dollars(x.dollar))
		}
	}
	x.where = fmt.Sprintf("%s%s IN ( %s )", x.whereLogic("OR"), col, ins)
	return x
}

func (x *Curd) WhereOrNotIn(col string, val ...interface{}) *Curd {
	col = escaped(col)
	ins := ""
	for _, v := range val {
		x.dollar++
		x.args = append(x.args, v)
		if ins == "" {
			ins = dollars(x.dollar)
		} else {
			ins = fmt.Sprintf("%s, %s", ins, dollars(x.dollar))
		}
	}
	x.where = fmt.Sprintf("%s%s NOT IN ( %s )", x.whereLogic("OR"), col, ins)
	return x
}

func (x *Curd) WhereOrBetween(col string, val1 interface{}, val2 interface{}) *Curd {
	col = escaped(col)
	x.dollar++
	x.args = append(x.args, val1)
	x.args = append(x.args, val2)
	x.where = fmt.Sprintf("%s%s BETWEEN %s AND %s", x.whereLogic("OR"), col, dollars(x.dollar), dollars(x.dollar+1))
	x.dollar++
	return x
}

func (x *Curd) Group(group string) *Curd {
	group = escaped(group)
	if x.group == "" {
		x.group = group
	} else {
		x.group = fmt.Sprintf("%s, %s", x.group, group)
	}
	return x
}

func (x *Curd) Asc(name string) *Curd {
	name = escaped(name)
	if x.order == "" {
		x.order = fmt.Sprintf("%s ASC", name)
	} else {
		x.order = fmt.Sprintf("%s, %s ASC", x.order, name)
	}
	return x
}

func (x *Curd) Desc(name string) *Curd {
	name = escaped(name)
	if x.order == "" {
		x.order = fmt.Sprintf("%s DESC", name)
	} else {
		x.order = fmt.Sprintf("%s, %s DESC", x.order, name)
	}
	return x
}

func (x *Curd) Limit(limit int64) *Curd {
	x.limit = limit
	return x
}

func (x *Curd) Offset(offset int64) *Curd {
	x.offset = offset
	return x
}

func (x *Curd) Page(page int64) *Curd {
	x.page = page
	return x
}

func (x *Curd) Clear() {
	x.alias = ""
	x.column = ""
	x.update = map[string]interface{}{}
	x.join = ""
	x.where = ""
	x.group = ""
	x.order = ""
	x.limit = 0
	x.offset = 0
	x.page = 0
	x.dollar = 0
	x.sql = ""
	x.args = []interface{}{}
}
