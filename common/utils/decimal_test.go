package utils

import (
	"github.com/shopspring/decimal"
	"log"
	"math/big"
	"testing"
)

func TestBig(t *testing.T) {

	bigint := NewFromString("10000000.1234567890123456789")
	log.Println(bigint)
	bigint1 := NewFromString("1123")
	log.Println(bigint1)
	bigint2 := NewFromString("1123.121212")
	log.Println(bigint2)
	mul1 := NewFromString("10000000.1234567890123456789")
	mul2 := NewFromString("10000000.1234567890123456789")
	mul3 := mul1.Mul(mul2)
	log.Println(mul3)
	mul4 := mul3.RoundDown(18)
	log.Println(mul4)
}
func TestMul(t *testing.T) {
	d, _ := decimal.NewFromString("1")
	d1, _ := decimal.NewFromString("2")
	mul := d.Div(d1).Floor()
	log.Println(mul)
}
func TestFromString(t *testing.T) {
	mul2 := NewFromString("10000000.12345678901234500")
	log.Println(mul2.StringFixedBank(3))
}
func TestPrecCut(t *testing.T) {
	result1 := PrecCut("89.23", 1)
	result2 := PrecCut("89.23", 6)
	result3 := PrecCut("89", 6)
	result4 := PrecCut("891", -2)
	result5 := PrecCut("891.23232", -2)
	result6 := PrecCut("", 2)
	log.Println(result1)
	log.Println(result2)
	log.Println(result3)
	log.Println(result4)
	log.Println(result5)
	log.Println(result6)
}
func TestExp(t *testing.T) {
	d := decimal.New(1, 18)
	log.Println(d)
	d1 := decimal.NewFromBigInt(big.NewInt(1), 0)
	round := d1.DivRound(d, 18)
	log.Println(round)

}
