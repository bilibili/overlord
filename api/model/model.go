package model

// ParamCluster is be used to create new or modify cluster
type ParamCluster struct {
	Name      string   `json:"name" validate:"required"`
	Appids    []string `json:"appids,split" validate:"gte=0,dive,gte=0"`
	Spec      string   `json:"spec" validate:"required"`
	Version   string   `json:"version" validate:"required"`
	CacheType string   `json:"cache_type" validate:"required"`
	Number    int      `json:"number" validate:"required"`
}

// ParamScale parase from data to used to scale cluster
type ParamScale struct {
	Name   string `json:"name" validate:"required"`
	Number int    `json:"number" validate:"required,ne=0"`
}

// QueryPage is the pagenation binder.
type QueryPage struct {
	PageNum   int `form:"pn,default=1" validate:"gt=0"`
	PageCount int `form:"pc,default=1000" validate:"gt=0"`
}

// ParamFilterCluster is the cluster filter.
type ParamFilterCluster struct {
	Name  string `json:"name"`
	Appid string `json:"appid"`
}

// ParamAssign is the model used for server.assgnAppid and server.unassignAppid
type ParamAssign struct {
	ClusterName string `json:"cluster_name" validate:"required"`
	Appid       string `json:"appid" validate:"required"`
}
