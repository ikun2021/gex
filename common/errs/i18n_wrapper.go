package errs

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/nicksnyder/go-i18n/v2/i18n"
	"go.uber.org/atomic"
	"golang.org/x/text/language"
)

/*
匹配语言文件规则
1.示例一 文件匹配规则
zh-HK
 1. zh-HK.toml
 2. zh-CN.toml
 3. zh.toml
 4. default.toml(默认语言文件,默认为en)

2.示例二
zh

	1.zh
	2.zh-CN
	3. default.toml(默认语言文件,默认为en)

code 匹配不到的情况
使用本文件code为999999
如果不存在999999消息ID，返回默认语言文件(en)的999999消息ID
*/
const (
	_defaultMsgId    = "999999"
	_defaultFormat   = "toml"
	_unKnownMsg      = "internal error"
	_defaultLangCode = "en"
)

type I18nConf struct {
	LanguagePath    string `json:"LanguagePath"`
	DefaultLanguage string `json:"DefaultLanguage,optional"`
}

var (
	_defaultLang = language.English
	//并发安全，支持动态修改
	_defaultAtomicTranslator = atomic.Pointer[Translator]{}
	//并发不安装
	_defaultTranslator *Translator
	//并发安全模块访问
	_concurrentSafeMode = false
)

// WrapAtomic 切换为并发安全模式访问
func WrapAtomic() {
	_defaultAtomicTranslator.Store(_defaultTranslator)
	_concurrentSafeMode = true
}

func SetDefaultTranslator(translator *Translator) {
	_defaultTranslator = translator
}

type Translator struct {
	*i18n.Bundle
	defaultMsgs map[string]string
}
type LangData struct {
	Lang string
	Data []byte
}

func NewTranslatorFormBytes(data []*LangData) (*Translator, error) {
	bundle := i18n.NewBundle(_defaultLang)
	bundle.RegisterUnmarshalFunc(_defaultFormat, toml.Unmarshal)
	var defaultMsgs = map[string]string{}
	for _, v := range data {
		msgFile, err := bundle.ParseMessageFileBytes(v.Data, v.Lang+"."+_defaultFormat)
		if err != nil {
			return nil, err
		}
		for _, v := range msgFile.Messages {
			if v.ID == _defaultMsgId {
				lang := msgFile.Tag.String()
				defaultMsgs[lang] = v.Other
				break
			}

		}
	}
	return &Translator{Bundle: bundle, defaultMsgs: defaultMsgs}, nil

}

func NewTranslatorFormFile(langFilePath string) (*Translator, error) {
	bundle := i18n.NewBundle(_defaultLang)
	bundle.RegisterUnmarshalFunc(_defaultFormat, toml.Unmarshal)
	var defaultMsgs = map[string]string{}
	err := filepath.WalkDir(langFilePath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d == nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if ext := strings.TrimLeft(filepath.Ext(d.Name()), "."); ext != _defaultFormat {
			return nil
		}
		msgFile, err := bundle.LoadMessageFile(path)
		if err != nil {
			return err
		}

		for _, v := range msgFile.Messages {
			if v.ID == _defaultMsgId {
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

func (t *Translator) Translate(lang, msgId string, templateData ...map[string]interface{}) (string, error) {
	localizer := i18n.NewLocalizer(t.Bundle, lang)
	var templateDataMap map[string]interface{}
	if len(templateData) > 0 {
		templateDataMap = templateData[0]
	}
	msg, err := localizer.Localize(&i18n.LocalizeConfig{
		MessageID:    "",
		TemplateData: templateDataMap,
		PluralCount:  nil,
		DefaultMessage: &i18n.Message{
			ID:    msgId,
			Other: t.defaultMsgs[lang],
		},
		Funcs:          nil,
		TemplateParser: nil,
	})
	if msg != "" {
		return msg, nil
	}
	var nerr *i18n.MessageNotFoundErr
	if errors.As(err, &nerr) {
		return t.defaultMsgs[_defaultLangCode], nil
	}

	return msg, err

}

func (t *Translator) TranslateErrCode(lang, msgId string, templateData ...map[string]interface{}) string {
	localizer := i18n.NewLocalizer(t.Bundle, lang)
	var templateDataMap map[string]interface{}
	if len(templateData) > 0 {
		templateDataMap = templateData[0]
	}
	msg, err := localizer.Localize(&i18n.LocalizeConfig{
		DefaultMessage: &i18n.Message{
			ID:    msgId,
			Other: t.defaultMsgs[lang],
		},
		TemplateData: templateDataMap,
	})
	var e *i18n.MessageNotFoundErr
	if err == nil || (errors.As(err, &e) && msg != "") {
		return msg
	}
	return _unKnownMsg
}
func Translate(lang, msgId string, templateData ...map[string]interface{}) string {
	if _concurrentSafeMode {
		return _defaultAtomicTranslator.Load().TranslateErrCode(lang, msgId, templateData...)
	}
	return _defaultTranslator.TranslateErrCode(lang, msgId, templateData...)
}
