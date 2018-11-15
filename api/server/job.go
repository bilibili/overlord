package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.etcd.io/etcd/client"
)

// getJob get the job by given number
func getJob(c *gin.Context) {
	jobID := c.Param("job_id")
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

type list struct {
	Count int         `json:"count"`
	Items interface{} `json:"items"`
}

func empty() *list {
	return &list{
		Count: 0,
		Items: []struct{}{},
	}
}

func listJSON(c *gin.Context, vals interface{}, count int) {
	if count == 0 {
		c.JSON(http.StatusOK, empty())
		return
	}

	c.JSON(http.StatusOK, &list{
		Count: count,
		Items: vals,
	})
}
