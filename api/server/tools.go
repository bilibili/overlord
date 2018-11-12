package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// eJSON will report error json into body
func eJSON(c *gin.Context, err error) {
	c.JSON(http.StatusInternalServerError, map[string]interface{}{"error": err})
}
