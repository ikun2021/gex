package test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

type responseBody struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type loginReq struct {
	Username  string `json:"username"`
	Password  string `json:"password"`
	Captcha   string `json:"captcha"`
	CaptchaID string `json:"captcha_id"`
}

type loginResp struct {
	Uid        int64  `json:"uid"`
	Username   string `json:"username"`
	Token      string `json:"token"`
	ExpireTime int64  `json:"expire_time"`
}

type createOrderReq struct {
	SymbolName  string `json:"symbol_name"`
	Side        int32  `json:"side"`
	OrderType   int32  `json:"order_type"`
	Price       string `json:"price"`
	BaseAmount  string `json:"base_amount"`
	QuoteAmount string `json:"quote_amount"`
}

type assetInfo struct {
	Id           int64  `json:"id"`
	CoinName     string `json:"coin_name"`
	CoinID       int32  `json:"coin_id"`
	AvailableQty string `json:"available_qty"`
	FrozenQty    string `json:"frozen_qty"`
}

type getUserAssetListResp struct {
	AssetList []*assetInfo `json:"asset_list"`
}

type gatewayHTTPProxy struct {
	baseURL string
	token   string
	client  *http.Client
}

func (p *gatewayHTTPProxy) getAssets(ctx context.Context, t *testing.T) []*assetInfo {
	t.Helper()
	rb := p.postJSON(ctx, t, "/account/v1/get_user_asset_list", struct{}{})
	if rb.Code != 0 {
		t.Fatalf("get_user_asset_list failed, code=%d msg=%q data=%s", rb.Code, rb.Msg, string(rb.Data))
	}
	data := mustUnmarshal[getUserAssetListResp](t, rb.Data)
	return data.AssetList
}

type assetSnapshot struct {
	Available string
	Frozen    string
}

func snapshotAssets(t *testing.T, assets []*assetInfo, coins ...string) map[string]assetSnapshot {
	t.Helper()
	out := make(map[string]assetSnapshot, len(coins))
	for _, c := range coins {
		a := findAssetByCoin(t, assets, c)
		if a == nil {
			t.Fatalf("missing asset %q", c)
		}
		out[c] = assetSnapshot{Available: a.AvailableQty, Frozen: a.FrozenQty}
	}
	return out
}

func assertAssetsEqual(t *testing.T, assets []*assetInfo, want map[string]assetSnapshot) {
	t.Helper()
	for coin, w := range want {
		a := findAssetByCoin(t, assets, coin)
		if a == nil {
			t.Fatalf("missing asset %q", coin)
		}
		if a.AvailableQty != w.Available || a.FrozenQty != w.Frozen {
			t.Fatalf("%s mismatch: available=%q frozen=%q, want available=%q frozen=%q",
				coin, a.AvailableQty, a.FrozenQty, w.Available, w.Frozen)
		}
	}
}

func newGatewayHTTPProxy(t *testing.T) *gatewayHTTPProxy {
	t.Helper()

	baseURL := os.Getenv("GEX_TEST_BASE_URL")
	if baseURL == "" {
		baseURL = "http://dev.api.gex.com"
	}

	return &gatewayHTTPProxy{
		baseURL: baseURL,
		client:  &http.Client{Timeout: 15 * time.Second},
	}
}

func (p *gatewayHTTPProxy) postJSON(ctx context.Context, t *testing.T, path string, req any) responseBody {
	t.Helper()

	var bodyBytes []byte
	var err error
	if req == nil {
		bodyBytes = []byte(`{}`)
	} else {
		bodyBytes, err = json.Marshal(req)
		if err != nil {
			t.Fatalf("marshal request failed: %v", err)
		}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		t.Fatalf("new request failed: %v", err)
	}
	httpReq.Header.Set("Accept", "*/*")
	httpReq.Header.Set("Content-Type", "application/json")
	if p.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+p.token)
	}

	resp, err := p.client.Do(httpReq)
	if err != nil {
		t.Fatalf("http request failed: %v", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body failed: %v", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("unexpected http status=%d, body=%s", resp.StatusCode, string(raw))
	}

	var rb responseBody
	if err := json.Unmarshal(raw, &rb); err != nil {
		t.Fatalf("unmarshal response failed: %v, body=%s", err, string(raw))
	}
	return rb
}

func (p *gatewayHTTPProxy) login(ctx context.Context, t *testing.T) string {
	t.Helper()

	// 允许显式传入 token（调试/复用）。
	if tok := os.Getenv("GEX_TEST_TOKEN"); tok != "" {
		p.token = tok
		return tok
	}

	username := os.Getenv("GEX_TEST_USERNAME")
	if username == "" {
		username = "zhangsan"
	}
	password := os.Getenv("GEX_TEST_PASSWORD")
	if password == "" {
		password = "123456"
	}
	captcha := os.Getenv("GEX_TEST_CAPTCHA")
	if captcha == "" {
		captcha = "laboris exercitation dolore sunt"
	}
	captchaID := os.Getenv("GEX_TEST_CAPTCHA_ID")
	if captchaID == "" {
		captchaID = "1"
	}

	rb := p.postJSON(ctx, t, "/account/v1/login", loginReq{
		Username:  username,
		Password:  password,
		Captcha:   captcha,
		CaptchaID: captchaID,
	})
	if rb.Code != 0 {
		t.Fatalf("login failed, code=%d msg=%q data=%s", rb.Code, rb.Msg, string(rb.Data))
	}

	data := mustUnmarshal[loginResp](t, rb.Data)
	if data.Token == "" {
		t.Fatalf("login returned empty token, raw=%s", string(rb.Data))
	}
	p.token = data.Token
	return data.Token
}

func mustUnmarshal[T any](t *testing.T, raw json.RawMessage) T {
	t.Helper()
	var v T
	if len(raw) == 0 {
		t.Fatalf("empty json raw message")
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("unmarshal data failed: %v, data=%s", err, string(raw))
	}
	return v
}

func findAssetByCoin(t *testing.T, assets []*assetInfo, coin string) *assetInfo {
	t.Helper()
	for _, a := range assets {
		if a != nil && a.CoinName == coin {
			return a
		}
	}
	return nil
}

func waitWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func dec(t *testing.T, s string) decimal.Decimal {
	t.Helper()
	d, err := decimal.NewFromString(s)
	if err != nil {
		t.Fatalf("invalid decimal %q: %v", s, err)
	}
	return d
}

func TestGatewayHTTP_OrderAndAssetFlow(t *testing.T) {
	t.Parallel()

	p := newGatewayHTTPProxy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 0) 先登录拿 token
	_ = p.login(ctx, t)

	beforeAssets := p.getAssets(ctx, t)
	beforeSnap := snapshotAssets(t, beforeAssets, "USDT", "IKUN")

	// 1) 下单
	createResp := p.postJSON(ctx, t, "/order/v1/create_order", createOrderReq{
		SymbolName:  "IKUN_USDT",
		Side:        1,
		OrderType:   2,
		Price:       "22",
		BaseAmount:  "1",
		QuoteAmount: "22",
	})
	if createResp.Code != 0 {
		t.Fatalf("create_order failed, code=%d msg=%q data=%s", createResp.Code, createResp.Msg, string(createResp.Data))
	}

	// 2) 轮询资产：期望买单会导致 USDT 冻结发生变化（至少不同于下单前）。
	deadline := time.Now().Add(15 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting asset frozen change after buy order")
		}
		assets := p.getAssets(ctx, t)
		usdt := findAssetByCoin(t, assets, "USDT")
		if usdt != nil && usdt.FrozenQty != beforeSnap["USDT"].Frozen {
			return
		}
		if err := waitWithContext(ctx, 300*time.Millisecond); err != nil {
			t.Fatalf("wait retry failed: %v", err)
		}
	}
}

func TestGatewayHTTP_SelfMatch_AssetsNetZero(t *testing.T) {
	t.Parallel()

	p := newGatewayHTTPProxy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_ = p.login(ctx, t)

	// 自撮合：同一用户买卖互相成交，按当前结算逻辑（无手续费）预期最终净持仓不变，
	// 并且所有冻结回到下单前水平（通常为 0）。
	beforeAssets := p.getAssets(ctx, t)
	beforeSnap := snapshotAssets(t, beforeAssets, "USDT", "IKUN")

	price := "22"
	baseAmount := "1"
	quoteAmount := "22"

	// 买单（冻结 USDT）
	rb := p.postJSON(ctx, t, "/order/v1/create_order", createOrderReq{
		SymbolName:  "IKUN_USDT",
		Side:        1,
		OrderType:   2,
		Price:       price,
		BaseAmount:  baseAmount,
		QuoteAmount: quoteAmount,
	})
	if rb.Code != 0 {
		t.Fatalf("create buy order failed, code=%d msg=%q data=%s", rb.Code, rb.Msg, string(rb.Data))
	}

	// 卖单（冻结 IKUN）
	rb = p.postJSON(ctx, t, "/order/v1/create_order", createOrderReq{
		SymbolName:  "IKUN_USDT",
		Side:        2,
		OrderType:   2,
		Price:       price,
		BaseAmount:  baseAmount,
		QuoteAmount: quoteAmount,
	})
	if rb.Code != 0 {
		t.Fatalf("create sell order failed, code=%d msg=%q data=%s", rb.Code, rb.Msg, string(rb.Data))
	}

	// 等待撮合与结算异步完成：最终以“冻结归零 + 净持仓不变”为准。
	sleepMs := 1500 * time.Millisecond
	if v := os.Getenv("GEX_TEST_MATCH_SLEEP_MS"); v != "" {
		if d, err := time.ParseDuration(v + "ms"); err == nil {
			sleepMs = d
		}
	}
	if err := waitWithContext(ctx, sleepMs); err != nil {
		t.Fatalf("wait before poll failed: %v", err)
	}

	deadline := time.Now().Add(25 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting self-match settle (assets)")
		}
		assets := p.getAssets(ctx, t)
		usdt := findAssetByCoin(t, assets, "USDT")
		ikun := findAssetByCoin(t, assets, "IKUN")
		if usdt != nil && ikun != nil {
			if usdt.FrozenQty == beforeSnap["USDT"].Frozen && ikun.FrozenQty == beforeSnap["IKUN"].Frozen {
				assertAssetsEqual(t, assets, beforeSnap)
				return
			}
		}
		if err := waitWithContext(ctx, 800*time.Millisecond); err != nil {
			t.Fatalf("wait retry failed: %v", err)
		}
	}
}

func TestGatewayHTTP_BuyFreeze_TwoDecimals(t *testing.T) {
	t.Parallel()

	p := newGatewayHTTPProxy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	_ = p.login(ctx, t)

	beforeAssets := p.getAssets(ctx, t)
	beforeUSDT := findAssetByCoin(t, beforeAssets, "USDT")
	if beforeUSDT == nil {
		t.Fatalf("missing USDT asset before")
	}

	// 2 位小数金额冻结：USDT 使用 scale=6，FromDBInteger 会输出去掉尾随 0 的字符串。
	freezeQuote := "22.12"
	rb := p.postJSON(ctx, t, "/order/v1/create_order", createOrderReq{
		SymbolName:  "IKUN_USDT",
		Side:        1,
		OrderType:   2,
		Price:       "22.12",
		BaseAmount:  "1",
		QuoteAmount: freezeQuote,
	})
	if rb.Code != 0 {
		t.Fatalf("create_order failed, code=%d msg=%q data=%s", rb.Code, rb.Msg, string(rb.Data))
	}

	wantMinDelta := dec(t, freezeQuote)
	beforeFrozen := dec(t, beforeUSDT.FrozenQty)

	deadline := time.Now().Add(20 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting USDT frozen >= %s", freezeQuote)
		}

		assets := p.getAssets(ctx, t)
		usdt := findAssetByCoin(t, assets, "USDT")
		if usdt == nil {
			t.Fatalf("missing USDT asset after")
		}
		afterFrozen := dec(t, usdt.FrozenQty)
		delta := afterFrozen.Sub(beforeFrozen)

		// 由于测试环境可能存在其他未结订单，这里采用“至少增加指定冻结额”的断言。
		if delta.GreaterThanOrEqual(wantMinDelta) {
			return
		}
		if err := waitWithContext(ctx, 300*time.Millisecond); err != nil {
			t.Fatalf("wait retry failed: %v", err)
		}
	}
}

func TestGatewayHTTP_ConcurrentSelfMatch_TwoDecimals_NetZero(t *testing.T) {
	t.Parallel()

	p := newGatewayHTTPProxy(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	_ = p.login(ctx, t)

	beforeAssets := p.getAssets(ctx, t)
	beforeSnap := snapshotAssets(t, beforeAssets, "USDT", "IKUN")

	// 并发自撮合：每个 goroutine 下买 + 卖一对，冻结涉及 2 位小数。
	n := 5
	if v := os.Getenv("GEX_TEST_CONCURRENCY"); v != "" {
		// 简单容错：解析失败就用默认 5
		if vv, err := decimal.NewFromString(v); err == nil && vv.IsInteger() {
			nn := int(vv.IntPart())
			if nn > 0 && nn <= 30 {
				n = nn
			}
		}
	}

	price := "22.12"
	baseAmount := "1.23"   // IKUN 冻结 2 位小数
	quoteAmount := "27.20" // USDT 冻结 2 位小数（不强校验精确等于 price*base）

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			// 买单
			rb := p.postJSON(ctx, t, "/order/v1/create_order", createOrderReq{
				SymbolName:  "IKUN_USDT",
				Side:        1,
				OrderType:   2,
				Price:       price,
				BaseAmount:  baseAmount,
				QuoteAmount: quoteAmount,
			})
			if rb.Code != 0 {
				// 在 goroutine 内用 t.Fatalf 会导致竞态；这里直接 panic 让测试失败并输出栈。
				panic("create buy order failed: " + rb.Msg)
			}
			// 卖单
			rb = p.postJSON(ctx, t, "/order/v1/create_order", createOrderReq{
				SymbolName:  "IKUN_USDT",
				Side:        2,
				OrderType:   2,
				Price:       price,
				BaseAmount:  baseAmount,
				QuoteAmount: quoteAmount,
			})
			if rb.Code != 0 {
				panic("create sell order failed: " + rb.Msg)
			}
		}()
	}
	wg.Wait()

	// 等待冻结归零并最终资产回到下单前（无手续费场景的“可预判结果”）。
	deadline := time.Now().Add(35 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting concurrent self-match settle (assets)")
		}
		assets := p.getAssets(ctx, t)
		usdt := findAssetByCoin(t, assets, "USDT")
		ikun := findAssetByCoin(t, assets, "IKUN")
		if usdt != nil && ikun != nil {
			if usdt.FrozenQty == beforeSnap["USDT"].Frozen && ikun.FrozenQty == beforeSnap["IKUN"].Frozen {
				assertAssetsEqual(t, assets, beforeSnap)
				return
			}
		}
		if err := waitWithContext(ctx, 800*time.Millisecond); err != nil {
			t.Fatalf("wait retry failed: %v", err)
		}
	}
}
