package errs

import (
	"errors"
	"github.com/zeromicro/go-zero/core/logx"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/nicksnyder/go-i18n/v2/i18n"
	"go.uber.org/atomic"
	"golang.org/x/text/language"
)

const (
	defaultMsgId  = "999999"
	defaultFormat = "toml"
	unKnownMsg    = "unknown error"
)

var (
	_defaultLang       = language.English
	_defaultTranslator = atomic.Pointer[Translator]{}
)

func SetDefaultTranslator(translator *Translator) {
	_defaultTranslator.Store(translator)
}

type Translator struct {
	*i18n.Bundle
	defaultMsgs map[string]string
}

func NewTranslator(langFilePath string) (*Translator, error) {
	bundle := i18n.NewBundle(_defaultLang)
	bundle.RegisterUnmarshalFunc(defaultFormat, toml.Unmarshal)
	var defaultMsgs = map[string]string{}

	err := filepath.WalkDir(langFilePath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			logx.Errorf("walk dir error: %v", err)
			return err
		}
		if d.IsDir() {
			return nil
		}
		if ext := strings.TrimLeft(filepath.Ext(d.Name()), "."); ext != defaultFormat {
			return nil
		}
		msgFile, err := bundle.LoadMessageFile(path)
		if err != nil {
			return err
		}

		for _, v := range msgFile.Messages {
			if v.ID == defaultMsgId {
				lang := msgFile.Tag.String()
				defaultMsgs[lang] = v.Other
				break
			}

		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Translator{Bundle: bundle, defaultMsgs: defaultMsgs}, nil

}

func (t *Translator) Translate(lang, msgId string) string {
	localizer := i18n.NewLocalizer(t.Bundle, lang)
	msg, err := localizer.Localize(&i18n.LocalizeConfig{
		DefaultMessage: &i18n.Message{
			ID:    msgId,
			Other: t.defaultMsgs[lang],
		},
	})
	var e *i18n.MessageNotFoundErr
	if err == nil || errors.As(err, &e) {
		return msg
	}
	return unKnownMsg
}

func Translate(lang, msgId string) string {
	return _defaultTranslator.Load().Translate(lang, msgId)
}
