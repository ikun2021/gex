package logic

import (
	"context"
	"github.com/ikun2021/gex/common/errs"
	"gopkg.in/yaml.v3"
	"log"

	"github.com/ikun2021/gex/app/admin/api/internal/svc"
	"github.com/ikun2021/gex/app/admin/api/internal/types"

	"github.com/zeromicro/go-zero/core/logx"
)

type SyncErrorCodeLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewSyncErrorCodeLogic(ctx context.Context, svcCtx *svc.ServiceContext) *SyncErrorCodeLogic {
	return &SyncErrorCodeLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *SyncErrorCodeLogic) SyncErrorCode(req *types.Empty) (resp *types.Empty, err error) {
	errorCode := l.svcCtx.AdminQuery.ErrorCode
	languages := []string{"zh-CN", "en-US"}
	for _, v := range languages {
		codes, err := errorCode.WithContext(l.ctx).Where(errorCode.Language.Eq(v)).Find()
		if err != nil {
			logx.Errorw("find code error", logx.Field("err", err))
			return nil, err
		}
		m := make(map[int32]string)
		for _, v := range codes {
			m[v.ErrorCodeID] = v.ErrorCodeName
		}
		d, err := yaml.Marshal(m)
		log.Println(string(d))
		if err != nil {
			logx.Errorw("sync error code to etcd failed", logx.Field("err", err))
			return nil, err
		}
		if _, err := l.svcCtx.EtcdCli.Put(l.ctx, errs.EtcdPrefixKey+v, string(d)); err != nil {
			logx.Errorw("sync error code to etcd failed", logx.Field("err", err))
			return nil, err
		}
	}

	return
}
