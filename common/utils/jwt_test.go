package utils

import (
	"log"
	"testing"
)

func TestJWTCreateAndParse(t *testing.T) {
	conf := &JwtConf{
		SignKey:        "test-sign-key",
		ValidTime:      "1h",
		PrivateKeyType: SigningMethodHS256,
	}
	jwtClient := NewJWT(conf)
	token, expireAt, err := jwtClient.CreateToken(1001)
	if err != nil {
		t.Fatal(err)
	}
	if token == "" || expireAt <= 0 {
		t.Fatalf("unexpected token=%s expireAt=%d", token, expireAt)
	}
	claims, err := jwtClient.ParseToken(token)
	if err != nil {
		t.Fatal(err)
	}
	if claims.Extra.UserId != 1001 {
		t.Fatalf("userId: got %d want 1001", claims.Extra.UserId)
	}
	log.Println("ok", token)
}
