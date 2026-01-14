package utils

import (
	"github.com/shopspring/decimal"
	"math/big"
	"strings"
)

const MaxPrec = 18

var (
	DecimalZeroMaxPrec = NewFromStringMaxPrec("0") // 0
	maxDecimal         = [18]byte{
		'0', '0', '0', '0', '0', '0',
		'0', '0', '0', '0', '0', '0',
		'0', '0', '0', '0', '0', '0',
	}
)

/*
decimal类型比较和计算的时候最好是统一指数，但是乘除会修改指数

s数字字符串

prec精度，小数位位数
*/
func NewFromStringMaxPrec(s string) decimal.Decimal {
	return NewFromString(s, MaxPrec)
}

func NewFromString(s string, prec int32) decimal.Decimal {
	var index = strings.IndexByte(s, '.')
	// 小数部分所有数字
	if prec > MaxPrec {
		prec = MaxPrec
	}
	right := maxDecimal
	dc := right[:prec]
	var value big.Int
	// 整数
	if index == -1 {
		value.SetString(s+string(dc), 10)
		return decimal.NewFromBigInt(&value, -prec)
	}

	// 整数部分,小数部分
	integer, d := s[:index], s[index+1:]

	// 小数部分数字位数
	decimalPlace := int32(len(d))

	// 如果小数精度超长，则截取
	if decimalPlace > prec {
		d = d[:prec]
	}

	// 拷贝小数部分有效数字位数
	copy(dc, d)

	var total = integer + string(dc)
	value.SetString(total, 10)
	return decimal.NewFromBigInt(&value, -prec)
}

// 535 -1 530
// 535.1234 1 535.1
// 535 1 535.0
// 535.1234 -1 530
// 535.1234 6 535.123400
func PrecCut(v string, prec int32) string {
	if v == "" {
		return v
	}
	pos := strings.IndexByte(v, '.')
	d := []byte(v)
	var result []byte
	switch {
	case pos == -1 && prec < 0:
		result = make([]byte, len(v))
		l := len(v) + int(prec)
		copy(result[:l], d)
		copy(result[l:], maxDecimal[:])
	case pos > -1 && prec < 0:
		result = make([]byte, pos)
		l := pos + int(prec)
		copy(result[:l], d)
		copy(result[l:], maxDecimal[:])
	case pos == -1 && prec > 0:
		result = make([]byte, len(v)+1+int(prec))
		copy(result, d[:])
		copy(result[len(d):], []byte{'.'})
		//填充零
		copy(result[len(d)+1:], maxDecimal[:])
	case pos > -1 && prec > 0:
		result = make([]byte, pos+int(prec)+1)
		copy(result, d[:])
		//填充零
		if len(d)-pos < int(prec) {
			copy(result[len(d):], maxDecimal[:])
		}
	}

	return string(result)
}
