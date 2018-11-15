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
		log.Errorf("engine start fail due to %v", err)
		panic(err)
	}
}

func initRouter(e *gin.Engine) {
	clusters := e.Group("/clusters")
	clusters.POST("/", createCluster)
	clusters.GET("/", getCluster)

	jobs := e.Group("/jobs")
	jobs.GET("/", getJobs)
	jobs.GET("/:job_id", getJob)
}
