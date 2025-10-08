package handler

import (
    "github.com/zeromicro/go-zero/rest/httpx"
    "net/http"
    "github.com/ikun2021/gex/common/pkg/response"
    "github.com/ikun2021/gex/common/errs"
    {{.ImportPackages}}

)

func {{.HandlerName}}(svcCtx *svc.ServiceContext) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        {{if .HasRequest}}var req types.{{.RequestType}}
        if err := httpx.Parse(r, &req); err != nil {
			response.Response(w, r, nil, errs.WarpMessage(errs.ParamValidateFailed,err.Error()))
            return
        }{{end}}

        l := logic.New{{.LogicType}}(r.Context(), svcCtx)
        {{if .HasResp}}resp, {{end}}err := l.{{.Call}}({{if .HasRequest}}&req{{end}})
        {{if .HasResp}}response.Response(w,r, resp, err){{else}}response.Response(w, nil, err){{end}}

    }
}