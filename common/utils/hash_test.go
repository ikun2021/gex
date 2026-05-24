package utils

import "testing"

func TestBcryptHash(t *testing.T) {
	hash := BcryptHash("123456")
	t.Log(hash)
}
