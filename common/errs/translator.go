package errs

import (
	"github.com/ikun2021/gex/common/pkg/confx"
	"github.com/ikun2021/gex/common/pkg/etcd"
	"github.com/zeromicro/go-zero/core/logx"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	DefaultLanguage = "zh-CN"
	DefaultCode     = InternalCode
	DefaultMsg      = "server error"
	EtcdPrefixKey   = "language/"
)

var translator *Translator

type Translator struct {
	Codes *sync.Map
}

func InitTranslator(path string) {
	dir, err := os.ReadDir(path)
	if err != nil {
		logx.Severef("init language file error err =%v", err)
	}
	var (
		m sync.Map
		t Translator
	)
	for _, v := range dir {
		var lang string
		if v.IsDir() {
			continue
		}
		l := strings.Split(v.Name(), ".")
		if len(l) != 2 {
			logx.Severef("init language file error err =%v", err)
		}
		lang = l[0]
		fullPath := filepath.Join(path, v.Name())
		data, err := os.ReadFile(fullPath)

		if err != nil {
			logx.Severef("read file  failed err =%v", err)
		}
		d := map[Code]string{}
		if err := yaml.Unmarshal(data, d); err != nil {
			logx.Severef("yaml unmarshal file failed err =%v", err)
		}
		m.Store(lang, d)
	}
	t.Codes = &m
	translator = &t
}

func InitTranslatorFromEtcd(etcdConfig etcd.EtcdConfig) {
	m := &sync.Map{}
	confx.MustLoadFromEtcd(EtcdPrefixKey, etcdConfig, m, confx.WithCustomInitLoadFunc(func(kvs []*mvccpb.KeyValue, target any) {
		for _, v := range kvs {
			key := strings.Split(string(v.Key), "/")
			if len(key) < 2 {
				continue
			}
			lang := key[1]
			d := map[Code]string{}
			if err := yaml.Unmarshal(v.Value, d); err != nil {
				logx.Severef("yaml unmarshal file failed %v", err)
			}
			m.Store(lang, d)
		}
	}), confx.WithCustomWatchFunc(func(evs []*clientv3.Event, target any) {

		for _, v := range evs {
			key := strings.Split(string(v.Kv.Key), "/")
			if len(key) < 2 {
				continue
			}
			lang := key[1]
			d := map[Code]string{}
			if err := yaml.Unmarshal(v.Kv.Value, d); err != nil {
				logx.Severef("yaml unmarshal file failed %v", err)
			}
			m.Store(lang, d)
		}
	}))
	m.Range(func(key, value any) bool {
		logx.Infof("key = %v,value =%v", key, value)
		return true
	})
	translator = &Translator{Codes: m}
}
func (t *Translator) translate(lang string, c Code) string {
	if lang == "" {
		lang = DefaultLanguage
	}
	v, ok := t.Codes.Load(lang)
	if !ok {
		lang = DefaultLanguage
	}
	v, ok = t.Codes.Load(DefaultLanguage)
	if !ok {
		return DefaultMsg
	}
	code := v.(map[Code]string)
	msg, ok := code[c]
	if !ok {
		return code[DefaultCode]
	}
	return msg
}
