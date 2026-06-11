//go:generate errgen -p admin.go
package errs

const (
	// PasswordNotMatchCode  密码不匹配
	PasswordNotMatchCode Code = AdminCodeInit + iota + 1 //密码不匹配
)

var PasswordNotMatch = PasswordNotMatchCode.Error("")
