package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func getAppids(c *gin.Context) {
	appids, err := svc.GetTreeAppid()
	if err != nil {
		eJSON(c, err)
		return
	}
	listJSON(c, appids, len(appids))
}

func getAppid(c *gin.Context) {
	appid := c.Param("appid")
	ga, err := svc.GetGroupedAppid(appid)
	if err != nil {
		eJSON(c, err)
		return
	}
	c.JSON(http.StatusOK, ga)
}

func removeAppid(c *gin.Context) {
	appid := c.Param("appid")
	err := svc.RemoveAppid(appid)
	if err != nil {
		eJSON(c, err)
		return
	}
	done(c)
}
