package pg

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	pk  string = "id"
	c32 string = " "
	c34 string = `"`
	c44 string = ","
	c36 string = "$"
	c63 string = "?"
	DB  *sql.DB
)

type InsertMoreRows struct {
	Table  string
	Column string
	Values string
	Args   []interface{}
}

type ExecMoreSql struct {
	Sql  string
	Args []interface{}
}

func PascalToUnderline(s string) string {
	tmp := []byte{}
	j := false
	num := len(s)
	for i := 0; i < num; i++ {
		d := s[i]
		if i > 0 && d >= 'A' && d <= 'Z' && j {
			tmp = append(tmp, '_')
		}
		if d != '_' {
			j = true
		}
		tmp = append(tmp, d)
	}
	return strings.ToLower(string(tmp[:]))
}

func UnderlineToPascal(s string) string {
	tmp := []byte{}
	bytes := []byte(s)
	length := len(bytes)
	nextLetterNeedToUpper := true
	for i := 0; i < length; i++ {
		if bytes[i] == '_' {
			nextLetterNeedToUpper = true
			continue
		}
		if nextLetterNeedToUpper && bytes[i] >= 'a' && bytes[i] <= 'z' {
			tmp = append(tmp, bytes[i]-32)
		} else {
			tmp = append(tmp, bytes[i])
		}
		nextLetterNeedToUpper = false
	}
	return string(tmp[:])
}

func Derive(table interface{}) string {
	rt := reflect.TypeOf(table)
	kind := rt.Kind()
	if kind == reflect.Ptr {
		rt = rt.Elem()
		kind = rt.Kind()
		if kind == reflect.Struct {
			return PascalToUnderline(rt.Name())
		}
		if kind == reflect.String {
			return strings.ToLower(reflect.ValueOf(table).Elem().Interface().(string))
		}
		return ""
	}
	if kind == reflect.Struct {
		return PascalToUnderline(rt.Name())
	}
	if kind == reflect.String {
		return strings.ToLower(table.(string))
	}
	return ""
}

func Escape(name string) string {
	if strings.Index(name, c34) >= 0 {
		return name
	}
	if strings.Index(name, c44) >= 0 {
		return name
	}
	name = strings.TrimSpace(name)
	if strings.Index(name, c32) >= 0 {
		return name
	}
	return fmt.Sprintf(`%s%s%s`, c34, strings.Replace(name, ".", fmt.Sprintf(`%s.%s`, c34, c34), -1), c34)
}

func InsertOneSql(insert interface{}) (sqlStr string, args []interface{}, err error) {
	if insert == nil {
		err = errors.New("insert object is nil, require *struct")
		return
	}
	t, v := reflect.TypeOf(insert), reflect.ValueOf(insert)
	if t.Kind() != reflect.Ptr {
		err = errors.New("insert object is not a ptr, require *struct")
		return
	}
	t, v = t.Elem(), v.Elem()
	if t.Kind() != reflect.Struct {
		err = errors.New("insert object is not a struct ptr, require *struct")
		return
	}
	cols, vals := "", ""
	args = []interface{}{}
	index := 0
	column := ""
	for i := 0; i < t.NumField(); i++ {
		column = PascalToUnderline(t.Field(i).Name)
		if column == pk {
			continue
		}
		args = append(args, v.Field(i).Interface())
		index++
		if cols == "" {
			cols = Escape(column)
			vals = c63
			continue
		}
		cols = fmt.Sprintf(`%s, %s`, cols, Escape(column))
		vals = fmt.Sprintf("%s, %s", vals, c63)
	}
	sqlStr = fmt.Sprintf(`INSERT INTO %s ( %s ) VALUES ( %s ) RETURNING %s`, Escape(PascalToUnderline(t.Name())), cols, vals, Escape(pk))
	return
}

func InsertMoreSql(batch ...interface{}) (execs map[string]*ExecMoreSql, err error) {
	var t reflect.Type
	var v reflect.Value
	length := len(batch)
	table := ""
	inserts := make([]InsertMoreRows, length, length)
	sqlIndex := map[string]int{}
	for i := 0; i < length; i++ {
		if batch[i] == nil {
			err = errors.New(fmt.Sprintf("insert object is nil, number %d, require *struct", i))
			return
		}
		t, v = reflect.TypeOf(batch[i]), reflect.ValueOf(batch[i])
		if t.Kind() != reflect.Ptr {
			err = errors.New(fmt.Sprintf("insert object is not ptr, number %d, require *struct", i))
			return
		}
		t, v = t.Elem(), v.Elem()
		if t.Kind() != reflect.Struct {
			err = errors.New(fmt.Sprintf("insert object is not struct ptr, number %d, require *struct", i))
			return
		}
		table = PascalToUnderline(t.Name())
		if _, ok := sqlIndex[table]; !ok {
			sqlIndex[table] = 1
		}
		for j := 0; j < v.NumField(); j++ {
			if PascalToUnderline(t.Field(j).Name) == pk {
				continue
			}
			inserts[i].Table = Escape(table)
			inserts[i].Args = append(inserts[i].Args, v.Field(j).Interface())
			if inserts[i].Column == "" {
				inserts[i].Column = fmt.Sprintf("%s", Escape(PascalToUnderline(t.Field(j).Name)))
				inserts[i].Values = fmt.Sprintf("%s", c63)
				sqlIndex[table]++
				continue
			}
			inserts[i].Column = fmt.Sprintf("%s, %s", inserts[i].Column, Escape(PascalToUnderline(t.Field(j).Name)))
			inserts[i].Values = fmt.Sprintf("%s, %s", inserts[i].Values, fmt.Sprintf("%s", c63))
			sqlIndex[table]++
		}
	}
	execs = map[string]*ExecMoreSql{}
	for i := 0; i < length; i++ {
		table = inserts[i].Table
		if _, ok := execs[table]; !ok {
			execs[table] = &ExecMoreSql{}
		}
		if execs[table].Sql == "" {
			execs[table] = &ExecMoreSql{
				Sql:  fmt.Sprintf("INSERT INTO %s ( %s ) VALUES ( %s )", inserts[i].Table, inserts[i].Column, inserts[i].Values),
				Args: inserts[i].Args,
			}
			continue
		}
		execs[table] = &ExecMoreSql{
			Sql:  fmt.Sprintf("%s, ( %s )", execs[table].Sql, inserts[i].Values),
			Args: append(execs[table].Args, inserts[i].Args...),
		}
	}
	return
}

func FormatPostgres(s string) string {
	count := strings.Count(s, c63)
	for i := 1; i <= count; i++ {
		s = strings.Replace(s, c63, fmt.Sprintf("%s%d", c36, i), 1)
	}
	return s
}

type Curd struct {
	table  string                 // 查询的表名
	alias  string                 // 表名别名
	cols   string                 // 查询列名
	mods   map[string]interface{} // 更新列信息
	join   string                 // 联合查询
	where  string                 // 条件语句
	group  string                 // 分组信息
	order  string                 // 排序信息
	limit  int64                  // 查询条数
	offset int64                  // 跳过的数据条数
	page   int64                  // 页码
	result int64                  // 受影响的行数|pk value
	sql    string                 // SQL
	args   []interface{}          // SQL args
	error  error                  // error
	tx     *sql.Tx                // transaction
	print  bool                   // 是否打印执行的SQL脚本及参数
}

// dollars postgres ordered placeholders
//func dollars(index int) string {
//	return fmt.Sprintf("%s%d", dollar, index)
//}

func Table(name ...interface{}) *Curd {
	x := &Curd{}
	x.Table(name...)
	return x
}

func Begin() *Curd {
	x := &Curd{}
	x.tx, x.error = DB.Begin()
	return x
}

func (x *Curd) RollBack() *Curd {
	x.error = x.tx.Rollback()
	return x
}

func (x *Curd) Commit() *Curd {
	x.error = x.tx.Commit()
	return x
}

func (x *Curd) Print(print ...bool) *Curd {
	if len(print) > 0 {
		x.print = print[0]
	} else {
		x.print = true
	}
	return x
}

func (x *Curd) printSql() {
	fmt.Println(x.sql, x.args)
}

func (x *Curd) Error() error {
	return x.error
}

func (x *Curd) Result() int64 {
	return x.result
}

func (x *Curd) Exec(query string, args ...interface{}) *Curd {
	query = FormatPostgres(query)
	x.sql, x.args = query, args
	var err error
	var rows int64
	defer func() {
		x.sql = ""
		x.args = []interface{}{}
		x.error = err
		x.result = rows
	}()
	if x.print {
		x.printSql()
	}
	if x.tx != nil {
		stmt, err := x.tx.Prepare(x.sql)
		if err != nil {
			x.RollBack()
			return x
		}
		result, err := stmt.Exec(x.args...)
		if err != nil {
			x.RollBack()
			return x
		}
		rows, err = result.RowsAffected()
		if err != nil {
			x.RollBack()
			return x
		}
		return x
	}
	stmt, err := DB.Prepare(x.sql)
	if err != nil {
		return x
	}
	result, err := stmt.Exec(x.args...)
	if err != nil {
		return x
	}
	rows, err = result.RowsAffected()
	if err != nil {
		return x
	}
	return x
}

func (x *Curd) Add(insert interface{}) *Curd {
	var id int64
	defer func() {
		x.result = id
		x.sql = ""
		x.args = []interface{}{}
	}()
	x.sql, x.args, x.error = InsertOneSql(insert)
	if x.error != nil {
		return x
	}
	x.sql = FormatPostgres(x.sql)
	if x.print {
		x.printSql()
	}
	if x.tx != nil {
		x.error = x.tx.QueryRow(x.sql, x.args...).Scan(&id)
		if x.error != nil {
			x.RollBack()
		}
		return x
	}
	x.error = DB.QueryRow(x.sql, x.args...).Scan(&id)
	return x
}

func (x *Curd) Adds(batch ...interface{}) *Curd {
	var rows int64
	var execs map[string]*ExecMoreSql
	defer func() {
		x.result = rows
	}()
	execs, x.error = InsertMoreSql(batch...)
	for _, v := range execs {
		x.Exec(v.Sql, v.Args...)
		rows += x.result
	}
	return x
}

func (x *Curd) Del() {
	defer func() {
		x.where = ""
	}()
	x.sql = fmt.Sprintf("DELETE FROM %s", x.table)
	if x.where != "" {
		x.sql = fmt.Sprintf("%s WHERE ( %s )", x.sql, x.where)
	}
	x.Exec(x.sql, x.args...)
	return
}

func (x *Curd) Ups(ups ...map[string]interface{}) {
	defer func() {
		x.where = ""
		x.mods = map[string]interface{}{}
	}()
	if x.mods == nil {
		x.mods = map[string]interface{}{}
	}
	for _, v := range ups {
		for col, val := range v {
			x.mods[col] = val
		}
	}
	aws := x.args
	set := ""
	x.args = []interface{}{}
	for k, v := range x.mods {
		x.args = append(x.args, v)
		if set == "" {
			set = fmt.Sprintf("%s = %s", Escape(k), c63)
			continue
		}
		set = fmt.Sprintf("%s, %s = %s", set, Escape(k), c63)
	}
	if set == "" {
		return
	}
	x.sql = fmt.Sprintf("UPDATE %s SET %s", x.table, set)
	if x.where != "" {
		x.sql = fmt.Sprintf("%s WHERE ( %s )", x.sql, x.where)
	}
	x.args = append(x.args, aws...)
	x.Exec(x.sql, x.args...)
	return
}

func (x *Curd) spliceQuerySql() *Curd {
	defer func() {
		x.alias = ""
		x.cols = ""
		x.join = ""
		x.where = ""
		x.group = ""
		x.order = ""
		x.limit = 0
		x.page = 0
		x.offset = 0
	}()
	if x.cols == "" {
		x.cols = "*"
	}
	x.sql = fmt.Sprintf("SELECT %s FROM %s", x.cols, x.table)
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
	x.sql = FormatPostgres(x.sql)
	if x.print {
		x.printSql()
	}
	return x
}

// result *AnyStruct LIMIT 1
// result *[]*AnyStruct LIMIT N, N>1

func (x *Curd) One(result interface{}) {
	var err error
	defer func() {
		x.error = err
		x.sql = ""
		x.args = []interface{}{}
	}()
	rt, rv := reflect.TypeOf(result), reflect.ValueOf(result)
	kind := rt.Kind()
	if kind != reflect.Ptr {
		err = errors.New("need a pointer parameter")
		return
	}
	if kind != reflect.Struct {
		err = errors.New("querying a piece of data requires structure pointer parameters")
		return
	}
	if x.sql == "" {
		x.spliceQuerySql()
	}
	rows, err := DB.Query(x.sql, x.args...)
	if err != nil {
		return
	}
	columns, err := rows.Columns()
	if err != nil {
		return
	}
	data := rv.Elem()       // 最终返回的数据
	rzv := reflect.Value{}  // reflect zero value, 反射包零值
	cols := []interface{}{} // 列名集合
	for _, cn := range columns {
		cnv := data.FieldByName(UnderlineToPascal(strings.ToLower(cn))) // 列名全部转换成小写, 下划线命名转帕斯卡命名
		if cnv == rzv || !cnv.CanSet() {
			// 结构体缺少cn字段, 或者结构体的cn字段不可访问(小写字母开头)
			err = errors.New(fmt.Sprintf("struct is missing field: %s", UnderlineToPascal(cn)))
			return
		}
		cols = append(cols, cnv.Addr().Interface())
	}
	for rows.Next() {
		err = rows.Scan(cols...)
		if err != nil {
			return
		}
		break
	}
	reflect.ValueOf(result).Elem().Set(data)
	return
}

func (x *Curd) More(result interface{}) {
	var err error
	defer func() {
		x.error = err
		x.sql = ""
		x.args = []interface{}{}
	}()
	rt, rv := reflect.TypeOf(result), reflect.ValueOf(result)
	kind := rt.Kind()
	if kind != reflect.Ptr {
		err = errors.New("need a pointer parameter")
		return
	}
	rt1 := rt.Elem()
	kind = rt1.Kind()
	if kind != reflect.Slice {
		err = errors.New("query multiple data, need slice pointer parameters")
		return
	}
	rt1 = rt1.Elem()
	kind = rt1.Kind()
	if kind != reflect.Ptr {
		err = errors.New("query multiple data, need to be a pointer inside the slice")
		return
	}
	rt1 = rt1.Elem()
	kind = rt1.Kind()
	if kind != reflect.Struct {
		err = errors.New("query multiple data, need to be a structure pointer inside the slice")
		return
	}
	if x.sql == "" {
		x.spliceQuerySql()
	}
	rows, err := DB.Query(x.sql, x.args...)
	if err != nil {
		return
	}
	columns, err := rows.Columns()
	if err != nil {
		return
	}
	rzv := reflect.Value{} // reflect zero value, 反射包零值
	data := rv.Elem()      // 最终返回的数据
	for rows.Next() {
		row := reflect.New(rt.Elem().Elem().Elem()) // struct
		rowVal := reflect.Indirect(row)
		cols := []interface{}{} // 列名集合
		for _, cn := range columns {
			cnv := rowVal.FieldByName(UnderlineToPascal(strings.ToLower(cn)))
			if cnv == rzv || !cnv.CanSet() {
				// 结构体缺少cn字段, 或者结构体的cn字段不可访问(小写字母开头)
				err = errors.New(fmt.Sprintf("struct is missing field: %s", UnderlineToPascal(cn)))
				return
			}
			cols = append(cols, cnv.Addr().Interface())
		}
		err = rows.Scan(cols...)
		if err != nil {
			return
		}
		data = reflect.Append(data, row)
	}
	reflect.ValueOf(result).Elem().Set(data)
	return
}

func (x *Curd) Table(name ...interface{}) *Curd {
	tmp := ""
	for _, v := range name {
		tmp = Escape(Derive(v))
		if x.table == "" {
			x.table = tmp
			continue
		}
		x.table = fmt.Sprintf("%s, %s", x.table, tmp)
	}
	return x
}

func (x *Curd) Mod(column string, value interface{}) *Curd {
	if x.mods == nil {
		x.mods = map[string]interface{}{}
	}
	x.mods[column] = value
	return x
}

func (x *Curd) Alias(alias interface{}) *Curd {
	x.alias = Escape(Derive(alias))
	return x
}

func (x *Curd) Cols(cols ...string) *Curd {
	for _, v := range cols {
		v = Escape(v)
		if x.cols == "" {
			x.cols = v
		} else {
			x.cols = fmt.Sprintf("%s, %s", x.cols, v)
		}
	}
	return x
}

func (x *Curd) Join(table interface{}, alias interface{}, col1 string, col2 string) *Curd {
	x.LeftJoin(table, alias, col1, col2)
	return x
}

func (x *Curd) LeftJoin(table interface{}, alias interface{}, col1 string, col2 string) *Curd {
	x.join = fmt.Sprintf("%s LEFT JOIN %s %s ON %s = %s", x.join, Escape(Derive(table)), Escape(Derive(alias)), Escape(col1), Escape(col2))
	return x
}

func (x *Curd) InnerJoin(table interface{}, alias interface{}, col1 string, col2 string) *Curd {
	x.join = fmt.Sprintf("%s INNER JOIN %s %s ON %s = %s", x.join, Escape(Derive(table)), Escape(Derive(alias)), Escape(col1), Escape(col2))
	return x
}

func (x *Curd) RightJoin(table interface{}, alias interface{}, col1 string, col2 string) *Curd {
	x.join = fmt.Sprintf("%s RIGHT JOIN %s %s ON %s = %s", x.join, Escape(Derive(table)), Escape(Derive(alias)), Escape(col1), Escape(col2))
	return x
}

func (x *Curd) Where(where string, args ...interface{}) *Curd {
	if where == "" {
		return x
	}
	x.where = where
	x.args = args
	return x
}

func (x *Curd) WhereAppend(where string, args ...interface{}) *Curd {
	if where == "" {
		return x
	}
	x.where = fmt.Sprintf("%s %s", x.where, where)
	x.args = append(x.args, args...)
	return x
}

func (x *Curd) whereSplice(logic string) string {
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
	col = Escape(col)
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s = %s", x.whereSplice("AND"), col, c63)
	return x
}

func (x *Curd) WhereNotEqual(col string, val interface{}) *Curd {
	col = Escape(col)
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s <> %s", x.whereSplice("AND"), col, c63)
	return x
}

func (x *Curd) WhereMoreThan(col string, val interface{}) *Curd {
	col = Escape(col)
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s > %s", x.whereSplice("AND"), col, c63)
	return x
}

func (x *Curd) WhereMoreThanEqual(col string, val interface{}) *Curd {
	col = Escape(col)
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s >= %s", x.whereSplice("AND"), col, c63)
	return x
}

func (x *Curd) WhereLessThan(col string, val interface{}) *Curd {
	col = Escape(col)
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s < %s", x.whereSplice("AND"), col, c63)
	return x
}

func (x *Curd) WhereLessThanEqual(col string, val interface{}) *Curd {
	col = Escape(col)
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s <= %s", x.whereSplice("AND"), col, c63)
	return x
}

func (x *Curd) WhereIn(col string, val ...interface{}) *Curd {
	col = Escape(col)
	ins := ""
	for _, v := range val {
		x.args = append(x.args, v)
		if ins == "" {
			ins = c63
		} else {
			ins = fmt.Sprintf("%s, %s", ins, c63)
		}
	}
	x.where = fmt.Sprintf("%s%s IN ( %s )", x.whereSplice("AND"), col, ins)
	return x
}

func (x *Curd) WhereNotIn(col string, val ...interface{}) *Curd {
	col = Escape(col)
	ins := ""
	for _, v := range val {
		x.args = append(x.args, v)
		if ins == "" {
			ins = c63
		} else {
			ins = fmt.Sprintf("%s, %s", ins, c63)
		}
	}
	x.where = fmt.Sprintf("%s%s NOT IN ( %s )", x.whereSplice("AND"), col, ins)
	return x
}

func (x *Curd) WhereBetween(col string, val1 interface{}, val2 interface{}) *Curd {
	col = Escape(col)
	x.args = append(x.args, val1)
	x.args = append(x.args, val2)
	x.where = fmt.Sprintf("%s%s BETWEEN %s AND %s", x.whereSplice("AND"), col, c63, c63)

	return x
}

func (x *Curd) WhereOrEqual(col string, val interface{}) *Curd {
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s = %s", x.whereSplice("OR"), Escape(col), c63)
	return x
}

func (x *Curd) WhereOrNotEqual(col string, val interface{}) *Curd {

	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s <> %s", x.whereSplice("OR"), Escape(col), c63)
	return x
}

func (x *Curd) WhereOrMoreThan(col string, val interface{}) *Curd {
	col = Escape(col)
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s > %s", x.whereSplice("OR"), col, c63)
	return x
}

func (x *Curd) WhereOrMoreThanEqual(col string, val interface{}) *Curd {
	col = Escape(col)
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s >= %s", x.whereSplice("OR"), col, c63)
	return x
}

func (x *Curd) WhereOrLessThan(col string, val interface{}) *Curd {
	col = Escape(col)
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s < %s", x.whereSplice("OR"), col, c63)
	return x
}

func (x *Curd) WhereOrLessThanEqual(col string, val interface{}) *Curd {
	col = Escape(col)
	x.args = append(x.args, val)
	x.where = fmt.Sprintf("%s%s <= %s", x.whereSplice("OR"), col, c63)
	return x
}

func (x *Curd) WhereOrIn(col string, val ...interface{}) *Curd {
	col = Escape(col)
	ins := ""
	for _, v := range val {
		x.args = append(x.args, v)
		if ins == "" {
			ins = c63
		} else {
			ins = fmt.Sprintf("%s, %s", ins, c63)
		}
	}
	x.where = fmt.Sprintf("%s%s IN ( %s )", x.whereSplice("OR"), col, ins)
	return x
}

func (x *Curd) WhereOrNotIn(col string, val ...interface{}) *Curd {
	col = Escape(col)
	ins := ""
	for _, v := range val {
		x.args = append(x.args, v)
		if ins == "" {
			ins = c63
		} else {
			ins = fmt.Sprintf("%s, %s", ins, c63)
		}
	}
	x.where = fmt.Sprintf("%s%s NOT IN ( %s )", x.whereSplice("OR"), col, ins)
	return x
}

func (x *Curd) WhereOrBetween(col string, val1 interface{}, val2 interface{}) *Curd {
	col = Escape(col)
	x.args = append(x.args, val1)
	x.args = append(x.args, val2)
	x.where = fmt.Sprintf("%s%s BETWEEN %s AND %s", x.whereSplice("OR"), col, c63, c63)

	return x
}

func (x *Curd) Group(group string) *Curd {
	group = Escape(group)
	if x.group == "" {
		x.group = group
	} else {
		x.group = fmt.Sprintf("%s, %s", x.group, group)
	}
	return x
}

func (x *Curd) Asc(name string) *Curd {
	name = Escape(name)
	if x.order == "" {
		x.order = fmt.Sprintf("%s ASC", name)
	} else {
		x.order = fmt.Sprintf("%s, %s ASC", x.order, name)
	}
	return x
}

func (x *Curd) Desc(name string) *Curd {
	name = Escape(name)
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
