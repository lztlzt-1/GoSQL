package msg

import (
	"fmt"
)

func Nofile(path string) string {
	return fmt.Sprint("Error: Can not open flie \"", path, "\". No such file or direct")
}

func ReadErr(err error) string {
	return fmt.Sprint("Error: reading error")
}

func WritePageErr(id PageId) string {
	return fmt.Sprint("Error: can not write data to page")
}
