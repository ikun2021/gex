package accountservicelogic

import (
	"context"
	"fmt"
	"strings"

	"github.com/ikun2021/gex/app/account/rpc/internal/svc"
	"github.com/ikun2021/gex/app/account/rpc/pb"
	"github.com/ikun2021/gex/common/rediskeys"
	"github.com/ikun2021/gex/common/utils"
	"github.com/spf13/cast"
	"github.com/zeromicro/go-zero/core/logx"
)

type GetUserAssetListLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetUserAssetListLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetUserAssetListLogic {
	return &GetUserAssetListLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

// GetUserAssetList 从 Redis balance Hash 获取用户所有币种资产。
// Hash 字段：{coin} 为可用余额，{coin}_frozen 为冻结余额，值为放大后的 int64 字符串。
func (l *GetUserAssetListLogic) GetUserAssetList(in *pb.GetUserAssetListReq) (*pb.GetUserAssetListResp, error) {
	tag := rediskeys.UserSlotTag(in.Uid)
	balanceKey := rediskeys.UserBalanceKey(tag, in.Uid)

	fields, err := l.svcCtx.RedisCli.HGetAll(l.ctx, balanceKey).Result()
	if err != nil {
		logx.Errorw("hgetall user balance failed", logx.Field("uid", in.Uid), logx.Field("err", err))
		return nil, fmt.Errorf("get user balance from redis failed: %w", err)
	}
	if len(fields) == 0 {
		return &pb.GetUserAssetListResp{AssetList: []*pb.Asset{}}, nil
	}

	availMap, frozenMap := parseBalanceFields(fields)
	coinByName := make(map[string]int32, len(l.svcCtx.Config.Coin))
	for _, c := range l.svcCtx.Config.Coin {
		coinByName[c.Name] = c.Id
	}

	assets := make([]*pb.Asset, 0, len(availMap)+len(frozenMap))
	seen := make(map[string]struct{}, len(availMap)+len(frozenMap))
	for coinName := range availMap {
		seen[coinName] = struct{}{}
	}
	for coinName := range frozenMap {
		seen[coinName] = struct{}{}
	}

	for coinName := range seen {
		availInt := cast.ToInt64(availMap[coinName])
		frozenInt := cast.ToInt64(frozenMap[coinName])
		if availInt == 0 && frozenInt == 0 {
			continue
		}
		assets = append(assets, &pb.Asset{
			UserId:          in.Uid,
			CoinId:          coinByName[coinName],
			CoinName:        coinName,
			AvailableAmount: utils.FromDBInteger(coinName, availInt),
			FrozenAmount:    utils.FromDBInteger(coinName, frozenInt),
		})
	}

	return &pb.GetUserAssetListResp{AssetList: assets}, nil
}

func parseBalanceFields(fields map[string]string) (avail, frozen map[string]string) {
	avail = make(map[string]string)
	frozen = make(map[string]string)
	const frozenSuffix = "_frozen"
	for field, val := range fields {
		if strings.HasSuffix(field, frozenSuffix) {
			coinName := strings.TrimSuffix(field, frozenSuffix)
			frozen[coinName] = val
			continue
		}
		avail[field] = val
	}
	return avail, frozen
}
