package common

import (
	"fmt"
	"time"
)

func Now() int64 {
	return time.Now().UTC().UnixNano() / int64(time.Millisecond)
}

func ToString(value float64, precision int) string {
	s := fmt.Sprint("%.", precision, "f")
	return fmt.Sprintf(s, value)
}
