package server

import "github.com/gin-gonic/gin"

// GET /specs
func getSpecs(c *gin.Context) {
	specs, err := svc.GetAllSpecs()
	if err != nil {
		eJSON(c, err)
		return
	}
	listJSON(c, specs, len(specs))
}

// DELETE /specs/:spec
func removeSpecs(c *gin.Context) {
	spec := c.Param("spec")
	err := svc.RemoveSpec(spec)
	if err != nil {
		eJSON(c, err)
		return
	}
	done(c)
}
