package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.etcd.io/etcd/client"
)

// getTask get the task by given number
func getTask(c *gin.Context) {
	taskID := c.Param("task_id")
	t, err := svc.GetTask(taskID)
	if client.IsKeyNotFound(err) {
		c.JSON(http.StatusNotFound, err)
		return
	} else if err != nil {
		eJSON(c, err)
		return
	}

	c.JSON(http.StatusOK, t)
}
