package glob

import (
	"crypto/md5"
	"fmt"
)

func HashMd5(str string) string {
	fmt.Println("Source string: ", str)
	return fmt.Sprintf("%x", md5.Sum([]byte(str)))
}
