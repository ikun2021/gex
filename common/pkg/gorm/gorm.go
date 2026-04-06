package config

import (
	logger "github.com/ikun2021/zlog"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"moul.io/zapgorm2"
)

type GormConf struct {
	Ip           string        `json:"ip"`
	Port         int32         `json:"port"`
	Username     string        `json:"username"`
	Password     string        `json:"password"`
	DbName       string        `json:"dbname"`
	MaxIdleConns int           `json:"maxIdleConns"`
	MaxOpenConns int           `json:"maxOpenConns"`
	Logger       logger.Config `json:"logger,optional"`
}

func (gc *GormConf) dns() string {
	return gc.Username + ":" + gc.Password + "@tcp(" + gc.Ip + ":" + cast.ToString(gc.Port) + ")/" + gc.DbName + "?charset=utf8mb4&parseTime=True&loc=Local"
}
func (gc *GormConf) MustNewGormClient() *gorm.DB {
	if db, err := gorm.Open(mysql.Open(gc.dns()), gc.gormConfig()); err != nil {
		logx.Severef("init gorm failed %v", err)
		return nil
	} else {
		sqlDB, _ := db.DB()
		sqlDB.SetMaxIdleConns(gc.MaxIdleConns)
		sqlDB.SetMaxOpenConns(gc.MaxOpenConns)
		return db
	}
}

func (gc *GormConf) gormConfig() *gorm.Config {
	config := &gorm.Config{}
	config.SkipDefaultTransaction = true

	var l zap.Logger
	if gc.Logger.Mode == "" {
		l = *logger.GetZapLogger()
	} else {
		l = *gc.Logger.Build()
	}
	gl := zapgorm2.New(l.WithOptions(zap.AddCallerSkip(1)))
	gl.IgnoreRecordNotFoundError = true
	gl.LogLevel = gormlogger.Info

	config.Logger = gl

	return config
}
