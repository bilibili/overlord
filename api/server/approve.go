package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func approveJob(c *gin.Context) {
	id := c.PostForm("job_id")
	if id == "" {
		c.JSON(http.StatusBadRequest, "job_id not exists")
		return
	}

	err := svc.ApproveJob(id)
	if err != nil {
		eJSON(c, err)
		return
	}

	c.JSON(http.StatusOK, map[string]string{"job": "approved"})
}
