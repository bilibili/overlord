package server

import (
	"overlord/api/model"

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
