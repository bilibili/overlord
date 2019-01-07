package server

import (
	"fmt"
	"net/http"
	"overlord/api/model"

	"github.com/gin-gonic/gin"
	"go.etcd.io/etcd/client"
)

// eJSON will report error json into body
func eJSON(c *gin.Context, err error) {
	merr := map[string]interface{}{"error": fmt.Sprintf("%v", err)}

	if client.IsKeyNotFound(err) {
		c.JSON(http.StatusNotFound, err)
		return
	}

	if err == model.ErrNotFound {
		c.JSON(http.StatusNotFound, merr)
		return
	}
	if err == model.ErrConflict {
		c.JSON(http.StatusConflict, merr)
		return
	}

	c.JSON(http.StatusInternalServerError, merr)
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
