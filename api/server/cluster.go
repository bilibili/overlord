package server

import (
	"net/http"
	"overlord/api/model"
	"overlord/job"
	"overlord/lib/log"

	"github.com/gin-gonic/gin"
)

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

func getCluster(c *gin.Context) {
}
