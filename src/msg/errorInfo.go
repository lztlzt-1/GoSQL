package msg

import (
	"GoSQL/src/dataTypes"
	"fmt"
)

func Nofile(path string) string {
	return fmt.Sprint("Error: Can not open flie \"", path, "\". No such file or direct")
}

func ReadErr(err error) string {
	return fmt.Sprint("Error: reading error")
}

func WritePageErr(id dataTypes.PageId) string {
	return fmt.Sprint("Error: can not write data to page")
}
