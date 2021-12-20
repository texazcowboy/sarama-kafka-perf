package common

import (
	"fmt"
	"math/rand"
	"time"
)

func GenerateRandomString(l int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, l)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[:l]
}