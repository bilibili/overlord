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

func done(c *gin.Context) {
	c.JSON(http.StatusOK, struct {
		Message string `json:"message"`
	}{
		Message: "done",
	})
}
