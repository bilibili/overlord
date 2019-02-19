package server

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// POST /commands/:ip/:port
func executeCommand(c *gin.Context) {
	ip := c.Param("ip")
	port := c.Param("port")

	cmd := c.PostForm("command")
	args := strings.Split(cmd, " ")
	rcmd, err := svc.Execute(fmt.Sprintf("%s:%s", ip, port), args[0], args[1:]...)
	if err != nil {
		eJSON(c, err)
		return
	}

	c.JSON(http.StatusOK, rcmd)
}
