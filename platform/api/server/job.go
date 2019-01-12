package server

import (
	"net/http"

	"strings"

	"github.com/gin-gonic/gin"
	"go.etcd.io/etcd/client"
)

// getJob get the job by given number
func getJob(c *gin.Context) {
	jobID := c.Param("job_id")
	jobID = strings.Replace(jobID, ".", "/", -1)
	t, err := svc.GetJob(jobID)
	if client.IsKeyNotFound(err) {
		c.JSON(http.StatusNotFound, err)
		return
	} else if err != nil {
		eJSON(c, err)
		return
	}

	c.JSON(http.StatusOK, t)
}

func getJobs(c *gin.Context) {
	j, err := svc.GetJobs()
	if err != nil {
		eJSON(c, err)
		return
	}

	listJSON(c, j, len(j))
}
