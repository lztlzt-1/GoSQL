package Records

import (
	"GoSQL/src/structType"
	"errors"
	"strconv"
	"strings"
)

type Column struct {
	name    string
	value   any
	itsType string
}

type Table struct {
	name    string
	column  []Column
	records []structType.Record
	length  int
}

func NewTable(name string, str string) (*Table, error) {
	list := strings.Split(str, " ")
	if len(list)&1 != 0 {
		return nil, errors.New("invalid string, please check")
	}
	var column []Column
	for i := 0; i < len(list); i++ {
		name := list[i]
		itsType := list[i+1]
		i++
		column = append(column, Column{name: name, itsType: itsType})
	}
	return &Table{name: name, column: column, length: 0}, nil
}

func (this *Table) Insert(str string) error {
	items := strings.Split(str, " ")
	if len(items) != len(this.column) {
		return errors.New("error: While inserting into Table, count is not same")
	}
	record := structType.Record{}
	//将所有传入的值转化成对应value，并检查错误
	for i := 0; i < len(items); i++ {
		switch this.column[i].itsType {
		case "int":
			d, err := strconv.Atoi(items[i])
			if err != nil {
				return err
			}
			record.Value = append(record.Value, d)
		case "bool":
			if items[i] == "true" {
				record.Value = append(record.Value, true)
			} else if items[i] == "false" {
				record.Value = append(record.Value, false)
			} else {
				return errors.New("error: While inserting into Table, expect true or false")
			}
		case "string":
			record.Value = append(record.Value, items[i])
		}
	}
	this.records = append(this.records, record)
	this.length++
	return nil
}

func (this *Table) queryidx(key string) (int, error) {
	for i := 0; i < len(this.column); i++ {
		if this.column[i].name == key {
			return i, nil
		}
	}
	return 0, errors.New("error: invalid column name")
}

// Query 这个查询属于比较底层的，所以可以通过前面的步骤过滤到提供两个list,表示每一个key对应的value是数组里的值则拿出
func (this *Table) Query(key []string, value []any) ([]structType.Record, error) {
	n := len(this.records) // 总共查询记录的个数
	var queryRecords []structType.Record
	idx, err := this.queryidx(key[0]) // column[idx]表示要查询的记录值
	if err != nil {
		return nil, err
	}
	for i := 0; i < n; i++ {
		if this.records[i].Value[idx] == value[0] {
			queryRecords = append(queryRecords, this.records[i])
		}
	}
	for j := 1; j < len(value); j++ {
		n := len(queryRecords)
		var localRecords []structType.Record
		idx, err := this.queryidx(key[j]) // column[idx]表示要查询的记录值
		if err != nil {
			return nil, err
		}
		for i := 0; i < n; i++ {
			if queryRecords[i].Value[idx] == value[j] {
				localRecords = append(localRecords, queryRecords[i])
			}
		}
		queryRecords = localRecords
	}
	return queryRecords, nil
}

// Update 这个查询属于比较底层的，所以可以通过前面的步骤过滤到提供两个list
func (this *Table) Update(key []string, value []any, updateKey []string, updateValue []any) error {
	n := len(this.records) // 总共查询记录的个数
	var queryPos []int
	idx, err := this.queryidx(key[0]) // column[idx]表示要查询的记录值
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if this.records[i].Value[idx] == value[0] {
			queryPos = append(queryPos, i)
		}
	}
	for j := 1; j < len(value); j++ {
		n := len(queryPos)
		var localPos []int
		idx, err := this.queryidx(key[j]) // column[idx]表示要查询的记录值
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if this.records[queryPos[i]].Value[idx] == value[j] {
				localPos = append(localPos, queryPos[i])
			}
		}
		queryPos = localPos
	}
	n = len(queryPos)
	for j := 0; j < len(updateValue); j++ {
		queryidx, err := this.queryidx(updateKey[j])
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			this.records[queryPos[i]].Value[queryidx] = updateValue[j]
		}
	}
	return nil
}

// Delete 这个查询属于比较底层的，所以可以通过前面的步骤过滤到提供两个list
func (this *Table) Delete(keys []string, values []any) error {
	if len(keys) != len(values) {
		return errors.New("error: key and value slices must have the same length")
	}

	for i := 0; i < len(this.records); i++ {
		record := this.records[i]
		match := true
		for j := 0; j < len(keys); j++ {
			key := keys[j]
			value := values[j]
			// 查找键在列中的索引
			index, err := this.queryidx(key)
			if err != nil {
				return err
			}
			// 检查记录中的值是否匹配
			if record.Value[index] != value {
				match = false
				break
			}
		}
		if match {
			this.records = append(this.records[:i], this.records[i+1:]...)
			this.length--
			i--
		}
	}
	return nil
}
