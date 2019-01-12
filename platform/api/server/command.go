package server

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// POST /commands/:ip/:port
func executeCommand(c *gin.Context) {
	ip := c.Param("ip")
	port := c.Param("port")

	cmd := c.PostForm("command")

	rcmd, err := svc.Execute(fmt.Sprintf("%s:%s", ip, port), cmd)
	if err != nil {
		eJSON(c, err)
		return
	}

	c.JSON(http.StatusOK, rcmd)
}
