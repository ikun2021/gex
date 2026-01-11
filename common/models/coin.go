package models

type Coin struct {
	Name      string `json:"name"`
	Id        int32  `json:"id"`
	Precision int32  `json:"precision"`
}
