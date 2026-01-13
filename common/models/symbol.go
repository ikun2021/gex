package models

type Symbol struct {
	Name        string `json:"name"`
	BaseCoinId  int32  `json:"baseCoinId"`
	BaseCoin    string `json:"baseCoin"`
	QuoteCoin   string `json:"quoteCoin"`
	QuoteCoinId int32  `json:"quoteCoinId"`
	Id          int32  `json:"id"`
}
