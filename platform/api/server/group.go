package server

import "github.com/gin-gonic/gin"

// e.GET("/groups", getAllGroups)
func getAllGroups(c *gin.Context) {
	groups := svc.GetAllGroups()

	listJSON(c, groups, len(groups))
}
