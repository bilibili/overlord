package model

import (
	"fmt"
	"strings"
)

// ParamCluster is be used to create new or modify cluster
type ParamCluster struct {
	Name        string   `json:"name" validate:"required"`
	Appids      []string `json:"appids,split" validate:"gte=0,dive,gte=0"`
	Spec        string   `json:"spec" validate:"required"`
	Version     string   `json:"version" validate:"required"`
	CacheType   string   `json:"cache_type" validate:"required"`
	TotalMemory float64  `json:"total_memory" validate:"required"`
	Group       string   `json:"group" validate:"required"`

	Number     int     `json:"-"`
	SpecCPU    float64 `json:"-"`
	SpecMemory float64 `json:"-"`
}

// Validate will check if the cluster param is right enough.
func (pc *ParamCluster) Validate() error {
	// check appids
	for _, appid := range pc.Appids {
		if !strings.Contains(appid, ".") {
			return fmt.Errorf("error: appid %s must contains period(.)", appid)
		}
	}
	return nil
}

// ValidateGroup will check if the group param is in the predefined groups.
func (pc *ParamCluster) ValidateGroup(groups []*Group) error {
	for _, group := range groups {
		if group.Name == pc.Group {
			return nil
		}
	}
	return fmt.Errorf("error: group %s unknown", pc.Group)
}

// ParamScale parase from data to used to scale cluster
type ParamScale struct {
	Name   string `json:"name" validate:"required"`
	Number int    `json:"number"`
	Memory int    `json:"memory"`
}

// QueryPage is the pagenation binder.
type QueryPage struct {
	PageNum   int `form:"pn,default=1" validate:"gt=0"`
	PageCount int `form:"pc,default=1000" validate:"gt=0"`
}

// Bounds returns the upper and lower bounds begins with 0 for this query path.
func (p *QueryPage) Bounds() (int, int) {
	return p.PageCount * (p.PageNum - 1), p.PageCount * p.PageNum
}

// ParamFilterCluster is the cluster filter.
type ParamFilterCluster struct {
	Name  string `json:"name"`
	Appid string `json:"appid"`
}

// ParamAppid is the alias of appid for create
type ParamAppid = ParamAssign

// ParamAssign is the model used for server.assgnAppid and server.unassignAppid
type ParamAssign struct {
	Appid string `json:"appid" validate:"required"`
}

// Validate check the param is ok
func (p *ParamAssign) Validate() error {
	if !strings.Contains(p.Appid, ".") {
		return fmt.Errorf("error: appid %s must contains period(.)", p.Appid)
	}
	return nil
}

// ParamScaleWeight change the weight of cluster
type ParamScaleWeight struct {
	Weight int `json:"weight" validate:"required,ne=0"`
}
