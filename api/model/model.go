package model


// ParamCluster is be used to create new or modify cluster
type ParamCluster struct {
	Name string `json:"name" validate:"required"`
	Appids []string `json:"appids,split" validate:"gte=0,dive,gte=0"`
	Spec string `json:"spec" validate:"required"`
	Version string `json:"version" validate:"required"`
	CacheType string `json:"cache_type" validate:"required"`
	Number int `json:"number" validate:"required"`
}


// QueryPage is the pagenation binder.
type QueryPage struct {
	PageNum int `form:"pn,default=1" validate:"gt=0"`
	PageCount int `form:"pc,default=1000" validate:"gt=0"`
}

// ParamFilterCluster is the cluster filter.
type ParamFilterCluster struct {
	Name string `form:"name"`
	Appid string `form:"appid"`
}
