package server

import (
	"overlord/api/service"
	"overlord/config"

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
	clusters.GET("/", getCluster)

	tasks := e.Group("/jobs")
	tasks.GET("/:task_id", getJob)
}
