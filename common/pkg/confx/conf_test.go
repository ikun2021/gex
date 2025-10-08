package confx

//import (
//	"fmt"
//	"github.com/zeromicro/go-zero/core/conf"
//	"gopkg.in/yaml.v3"
//	"log"
//	"testing"
//)
//
//func TestUnmarshal(t *testing.T) {
//	c := `
//Name: match.rpc
//ListenOn: 0.0.0.0:20003
//Timeout: 0
//Etcd:
//  Hosts:
//    - 192.168.2.159:2379
//  Key: match.rpc
//
//PulsarConfig:
//  hosts:
//    - 192.168.2.159:6650
//LoggerConfig:
//  Level: debug
//  Stacktrace: true
//  AddCaller: true
//  Mode: console
//  FileName: github.com/ikun2021/gex-match-std.log
//  ErrorFileName: github.com/ikun2021/gex-match-err.log
//  MaxSize: 10
//  MaxAge: 30
//  MaxBackup: 20
//  Async: true
//  Json: false
//  Compress: true
//
//WsConf:
//  Etcd:
//    Key: proxy
//    Hosts:
//      - 192.168.2.159:2379
//SymbolInfo:
//    SymbolName: IKUN_USDT
//    SymbolID: 1
//    BaseCoinID: 1
//    BaseCoinName: IKUN
//    QuoteCoinID: 2
//    QuoteCoinName: USDT
//    BaseCoinPrec: 4
//    QuoteCoinPrec: 4`
//	//var c1 Config
//	if err := conf.LoadFromYamlBytes([]byte(c), &c1); err != nil {
//		log.Println(err)
//	}
//	log.Println(c1)
//	t1 := T{}
//
//	err := yaml.Unmarshal([]byte(data), &t1)
//	if err != nil {
//		log.Fatalf("error: %v", err)
//	}
//	fmt.Printf("--- t:\n%v\n\n", t1)
//}
//
//var data = `
//a: Easy!
//b:
//  c: 2
//  d: [3, 4]
//`
//
//// Note: struct fields must be public in order for unmarshal to
//// correctly populate the data.
//type T struct {
//	A string
//	B struct {
//		RenamedC int   `yaml:"c"`
//		D        []int `yaml:",flow"`
//	}
//}
