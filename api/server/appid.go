package server

import (
	"net/http"
	"overlord/api/model"

	"github.com/gin-gonic/gin"
)

func getAppids(c *gin.Context) {
	page := new(model.QueryPage)
	if err := c.BindQuery(page); err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}

	name := c.Query("name")
	appids, err := svc.SearchAppids(name, page)
	if err != nil {
		eJSON(c, err)
		return
	}
	listJSON(c, appids, len(appids))
}

func removeAppid(c *gin.Context) {
}
