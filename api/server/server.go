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
	clusters.GET("/", getClusters)
	clusters.GET("/:cluster_name", getCluster)
	clusters.DELETE("/:cluster_name", removeCluster)

	clusters.PATCH("/:cluster_name/instances", scaleCluster)
	clusters.GET("/:cluster_name/instances", getInstances)

	// clusters.POST("/:cluster_name/appids", )

	cmds := e.Group("/commands")
	cmds.POST("/:ip/:port", executeCommand)

	jobs := e.Group("/jobs")
	jobs.GET("/", getJobs)
	jobs.GET("/:job_id", getJob)

	job := e.Group("/job")
	job.POST("/", approveJob)

	specs := e.Group("/specs")
	specs.GET("/", getSpecs)
	specs.DELETE("/", removeSpecs)

	appids := e.Group("/appids")
	appids.GET("/", getAppids)
	appids.DELETE("/:appid", removeAppid)
}
