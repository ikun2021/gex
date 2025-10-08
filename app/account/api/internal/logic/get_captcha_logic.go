package logic

import (
	"context"
	"github.com/ikun2021/gex/app/account/api/internal/svc"
	"github.com/ikun2021/gex/app/account/api/internal/types"
	"github.com/mojocn/base64Captcha"
	"github.com/zeromicro/go-zero/core/logx"
)

type GetCaptchaLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetCaptchaLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetCaptchaLogic {
	return &GetCaptchaLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetCaptchaLogic) GetCaptcha() (resp *types.GetCaptchaResp, err error) {
	driver := base64Captcha.NewDriverDigit(80, 240, 6, 0.7, 80)
	cp := base64Captcha.NewCaptcha(driver, l.svcCtx.CaptchaStore)
	id, b64s, _, err := cp.Generate()
	if err != nil {
		logx.Errorw("generate captcha failed", logx.Field("err", err))
		return nil, err
	}
	return &types.GetCaptchaResp{
		CaptchaPic:    b64s,
		CaptchaId:     id,
		CaptchaLength: 6,
	}, nil

}
