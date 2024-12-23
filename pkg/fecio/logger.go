package fecio

import (
	"github.com/facebookincubator/go-belt/tool/logger"
)

var (
	Logger logger.Logger = logger.Default().WithLevel(logger.LevelWarning)
)
