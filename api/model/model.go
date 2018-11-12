package model


// ParamCluster is be used to create new or modify cluster
type ParamCluster struct {
	Name string `form:"name" validate:"required"`
	Appids string `form:"appids,split" validate:"gte=0,dive,gte=0"`
	CacheType string `form:"cache_type" validate:"required"`
	Spec string `form:"spec" validate:"required"`
	MasterNum int `form:"master_num" validate:"required,gte=4"`
	Version string `form:"version" validate:"required"`
}
