package accountservicelogic

import (
	"context"
	"errors"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/errs"
	"github.com/ikun2021/gex/common/proto/define"
	"github.com/ikun2021/gex/common/utils"
	logger "github.com/luxun9527/zlog"
	"gorm.io/gorm"

	"github.com/gookit/goutil/strutil"
	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/zeromicro/go-zero/core/logx"
)

type LoginLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLoginLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LoginLogic {
	return &LoginLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// 登录
func (l *LoginLogic) Login(in *pb.LoginReq) (*pb.LoginResp, error) {
	user := l.svcCtx.Query.User

	result, err := user.WithContext(l.ctx).
		Where(user.Username.Eq(in.Username)).
		First()
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errs.LoginFailed
		}
		logx.Errorw("find user failed", logger.ErrorField(err))
		return nil, err
	}
	if !utils.BcryptCheck(in.Password, result.Password) {
		return nil, errs.LoginFailed
	}
	claims := l.svcCtx.JwtClient.CreateClaims(utils.JwtContent{
		UserID:   int64(result.ID),
		Username: result.Username,
		NickName: "",
	})
	//生成token
	token, err := l.svcCtx.JwtClient.CreateToken(claims)
	if err != nil {
		logx.Errorf("create token failed err", err)
		return nil, err
	}
	tokenMd5 := strutil.Md5(token)
	if err := l.svcCtx.RedisCli.Set(context.Background(), define.AccountToken.WithParams(tokenMd5), "", 3600*24).Err(); err != nil {
		logx.Errorw("set token failed", logger.ErrorField(err))
		return nil, err
	}

	return &pb.LoginResp{
		Uid:        int64(result.ID),
		Username:   in.Username,
		Token:      token,
		ExpireTime: claims.ExpiresAt.Unix(),
	}, nil

}
