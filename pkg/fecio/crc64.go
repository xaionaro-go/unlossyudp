package fecio

import (
	"hash/crc64"
)

var crc64Table = crc64.MakeTable(crc64.ECMA)
