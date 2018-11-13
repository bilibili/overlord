package server

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// eJSON will report error json into body
func eJSON(c *gin.Context, err error) {
	c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": fmt.Sprintf("%v", err)})
}
