package server

import (
	"overlord/api/model"
	"overlord/api/service"
	"overlord/config"

	"net/http"

	"overlord/lib/log"

	"github.com/gin-gonic/gin"
)

var (
	svc *service.Service
)

// Run the whole overlord app
func Run(cfg *config.ServerConfig, s *service.Service) {
	svc = s
	engine := gin.Default()
	initRouter(engine)
	if err := engine.Run(cfg.Listen); err != nil {
		log.Error("engine start fail due to %v", err)
		panic(err)
	}
}

func initRouter(e *gin.Engine) {
	clusters := e.Group("/clusters")
	clusters.POST("/", createCluster)
}

func createCluster(c *gin.Context) {
	p := new(model.ParamCluster)
	if err := c.Bind(p); err != nil {
		return
	}

	taskid, err := svc.CreateCluster(p)
	if err != nil {
		c.JSON(http.StatusInternalServerError, err)
		return
	}

	c.JSON(http.StatusOK, map[string]int{"task_id": int(taskid)})
}
