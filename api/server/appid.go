package server

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func getAppids(c *gin.Context) {
	format := c.DefaultQuery("format", "plain")
	if format == "tree" {
		appids, err := svc.GetTreeAppid()
		if err != nil {
			eJSON(c, err)
			return
		}
		listJSON(c, appids, len(appids))
		return
	} else if format == "plain" {
		appids, err := svc.GetPlainAppid()
		if err != nil {
			eJSON(c, err)
			return
		}
		listJSON(c, appids, len(appids))
		return
	}

	c.JSON(http.StatusBadRequest, map[string]string{"error": "output format must be one of plain|tree"})
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
