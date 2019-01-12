package server

import "github.com/gin-gonic/gin"

// e.GET("/versions", getAllVersion)
func getAllVersions(c *gin.Context) {
	versions, err := svc.GetAllVersions()
	if err != nil {
		eJSON(c, err)
		return
	}
	listJSON(c, versions, len(versions))
}
