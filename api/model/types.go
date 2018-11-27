package model

// Job is the json-encodable struct
type Job struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// Cluster is the special struct in model
type Cluster struct {
	Name   string   `json:"name"`
	Appids []string `json:"appids"`
	State  string   `json:"state"`

	CacheType string  `json:"cache_type"`
	Thread    int     `json:"cpu" validate:"required"`
	MaxMemory float64 `json:"max_memory"`
	Version   string  `json:"version" validate:"required"`
	Number    int     `json:"number" validate:"required"`

	Instances []*Instance `json:"instances"`
}

// Instance is the struct for each cache
type Instance struct {
	IP    string `json:"ip"`
	Port  int    `json:"port"`
	Weight int   `json:"weight"`
	State string `json:"state"`
}

// Appid is the struct conttains many cluster name
type Appid struct {
	Name     string   `json:"name"`
	Clusters []string `json:"clusters"`
}
