package server

import (
	"net/http"
	"overlord/api/model"
	"overlord/job"
	"overlord/lib/log"

	"github.com/gin-gonic/gin"
	"go.etcd.io/etcd/client"
)

// POST /clusters
func createCluster(c *gin.Context) {
	p := new(model.ParamCluster)
	if err := c.BindJSON(p); err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}
	log.Infof("create new cluster with param %v", *p)

	jobid, err := svc.CreateCluster(p)
	if err != nil {
		eJSON(c, err)
		return
	}

	c.JSON(http.StatusOK, &model.Job{ID: jobid, State: job.StatePending})
}

// GET /clusters/:cluster_id
func getCluster(c *gin.Context) {
	clusterName := c.Param("cluster_name")

	cluster, err := svc.GetCluster(clusterName)
	if err != nil {
		eJSON(c, err)
		return
	}

	c.JSON(http.StatusOK, cluster)
}

// GET /clusters
func getClusters(c *gin.Context) {
	clusters, err := svc.GetClusters()
	if err != nil {
		eJSON(c, err)
		return
	}

	listJSON(c, clusters, len(clusters))
}

// DELETE /clusters/:cluster_name
func removeCluster(c *gin.Context) {
	cname := c.Param("cluster_name")
	jobID, err := svc.RemoveCluster(cname)
	if err != nil {
		eJSON(c, err)
		return
	}

	c.JSON(http.StatusOK, &model.Job{ID: jobID, State: job.StatePending})
}

// PATCH /clusters/:cluster_name/instances
func scaleCluster(c *gin.Context) {
	p := new(model.ParamScale)
	if err := c.BindJSON(p); err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}

	jobID, err := svc.ScaleCluster(p)
	if err != nil {
		if client.IsKeyNotFound(err) {
			c.JSON(http.StatusNotFound, err)
			return
		}
		eJSON(c, err)
		return
	}
	c.JSON(http.StatusOK, &model.Job{ID: jobID, State: job.StatePending})
}
