package errs

import (
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Code codes.Code

const (
	CommonCodeInit Code = 100000 * (iota + 1)
	AccountCodeInit
	AdminCodeInit
	MatchCodeInit
	OrderCodeInit
	QuoteCodeInit
)

func WarpMessage(err error, msg string) error {
	s, ok := status.FromError(err)
	if ok {
		msg = s.Message() + ":" + msg
		return status.New(s.Code(), msg).Err()
	}
	return errors.Wrap(err, msg)
}

func (c Code) Translate(lang string) string {
	if lang == "" {
		lang = "zh"
	}
	return Translate(lang, cast.ToString(uint32(c)))
}

func (c Code) Error(msg string) error {
	return status.New(codes.Code(c), msg).Err()
}

func (c Code) String() string {
	return ""
}
