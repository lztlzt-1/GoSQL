package Records

import (
	"errors"
	"strconv"
	"strings"
)

type Column struct {
	name    string
	value   any
	itsType string
}

type Record struct {
	value []any
}

type Table struct {
	column  []Column
	records []Record
	length  int
}

func NewTable(str string) (*Table, error) {
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
	return &Table{column: column, length: 0}, nil
}

func (this *Table) Insert(str string) error {
	items := strings.Split(str, " ")
	if len(items) != len(this.column) {
		return errors.New("error: While inserting into Table, count is not same")
	}
	record := Record{}
	//将所有传入的值转化成对应value，并检查错误
	for i := 0; i < len(items); i++ {
		switch this.column[i].itsType {
		case "int":
			d, err := strconv.Atoi(items[i])
			if err != nil {
				return err
			}
			record.value = append(record.value, d)
		case "bool":
			if items[i] == "true" {
				record.value = append(record.value, true)
			} else if items[i] == "false" {
				record.value = append(record.value, false)
			} else {
				return errors.New("error: While inserting into Table, expect true or false")
			}
		case "string":
			record.value = append(record.value, items[i])
		}
	}
	this.records = append(this.records, record)
	this.length++
	return nil
}
