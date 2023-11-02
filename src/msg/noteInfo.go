package msg

import "fmt"

func SuccessWritePage(pagePos int) string {
	return fmt.Sprint("Success writing data to page: ", pagePos)
}
