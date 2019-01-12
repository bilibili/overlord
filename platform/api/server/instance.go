package server

import (
	"net/http"
	"overlord/platform/api/model"
	"overlord/platform/job"

	"github.com/gin-gonic/gin"
)

// GET /clusters/:cluster_id/instances
func getInstances(c *gin.Context) {
}

func changeInstanceWeight(c *gin.Context) {
	p := model.ParamScaleWeight{}
	if err := c.ShouldBind(&p); err != nil {
		eJSON(c, err)
		return
	}
	instance := c.Param("instance_addr")

	err := svc.SetInstanceWeight(instance, p.Weight)
	if err != nil {
		eJSON(c, err)
		return
	}
	done(c)
}

func restartInstance(c *gin.Context) {
	cname := c.Param("cluster_name")
	addr := c.Param("instance_addr")
	jobid, err := svc.RestartInstance(cname, addr)
	if err != nil {
		eJSON(c, err)
		return
	}
	c.JSON(http.StatusOK, &model.Job{ID: jobid, State: job.StatePending})
}
