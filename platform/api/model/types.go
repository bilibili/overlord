package model

import (
	"fmt"
	"strings"
)

// Version is the info or version dir
type Version struct {
	CacheType string   `json:"cache_type"`
	Versions  []string `json:"versions"`
}

// Group is the group with slug and cn name
type Group struct {
	Name   string `json:"name"`
	NameCN string `json:"name_cn"`
}

// Job is the json-encodable struct
type Job struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Param string `json:"param"`
}

// define cluster and instance state
const (
	StateWaiting = "waiting"
	StateDone    = "done"
	StateError   = "error"
)

// Cluster is the special struct in model
type Cluster struct {
	Name         string   `json:"name"`
	Appids       []string `json:"appids"`
	State        string   `json:"state"`
	FrontEndPort int      `json:"front_end_port"`

	CacheType string  `json:"cache_type"`
	Thread    int     `json:"cpu" validate:"required"`
	MaxMemory float64 `json:"max_memory"`
	Version   string  `json:"version" validate:"required"`
	Number    int     `json:"number" validate:"required"`
	Group     string  `json:"group"`
	Monitor   string  `json:"monitor"`

	DialTimeout   int  `json:"dial_timeout"`
	ReadTimeout   int  `json:"read_timeout"`
	WriteTimeout  int  `json:"write_timeout"`
	NodeConns     int  `json:"node_connections"`
	PingFailLimit int  `json:"ping_fail_limit"`
	PingAutoEject bool `json:"ping_auto_eject"`

	Instances []*Instance `json:"instances"`
}

// Instance is the struct for each cache
type Instance struct {
	IP     string `json:"ip"`
	Port   int    `json:"port"`
	Weight int    `json:"weight"`
	Alias  string `json:"alias"`
	State  string `json:"state"`
	Role   string `json:"role"`
}

// Appid is the struct conttains many cluster name
type Appid struct {
	Name     string   `json:"name"`
	Clusters []string `json:"clusters"`
}

// GroupedAppid is the struct contains grouped appids
type GroupedAppid struct {
	Name            string             `json:"name"`
	GroupedClusters []*GroupedClusters `json:"grouped_clusters"`
}

// GroupedClusters is the struct cotnains clusters and grouped by group
type GroupedClusters struct {
	Group    string     `json:"group"`
	Clusters []*Cluster `json:"clusters"`
}

// TreeAppid is the struct used for tree
type TreeAppid struct {
	NameLabel
	Children []*NameLabel `json:"children"`
}

// NameLabel contains the name and label
type NameLabel struct {
	Name  string `json:"name"`
	Label string `json:"label"`
}

// BuildTreeAppids creates appid tree by appid list
func BuildTreeAppids(appids []string) []*TreeAppid {
	nameMapper := make(map[string][]string)
	for _, appid := range appids {
		aps := strings.Split(appid, ".")
		prefix := strings.Join(aps[:len(aps)-1], ".")
		if _, ok := nameMapper[prefix]; ok {
			nameMapper[prefix] = append(nameMapper[prefix], aps[len(aps)-1])
		} else {
			nameMapper[prefix] = []string{aps[len(aps)-1]}
		}
	}
	tas := make([]*TreeAppid, len(nameMapper))
	count := 0
	for prefix, suffixs := range nameMapper {
		children := make([]*NameLabel, len(suffixs))
		for idx, suffix := range suffixs {
			children[idx] = &NameLabel{Name: fmt.Sprintf("%s.%s", prefix, suffix), Label: suffix}
		}
		ta := &TreeAppid{NameLabel: NameLabel{Name: prefix, Label: prefix}, Children: children}
		tas[count] = ta
		count++
	}
	return tas
}
