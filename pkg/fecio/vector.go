package fecio

import (
	"fmt"
)

type vector struct {
	ID               uint32
	DataPackets      []*dataPacketWithMetadata
	DataPacketsSet   uint
	ParityPackets    []*parityPacketWithMetadata
	ParityPacketsSet uint
	Reconstructed    bool
}

func (vec *vector) Reset() {
	if len(vec.DataPackets) != 0 {
		panic(fmt.Errorf("internal error: len(vec.DataPackets) != 0: it was supposed to be freed by the sender handler"))
	}
	vec.DataPacketsSet = 0

	if len(vec.ParityPackets) != 0 {
		panic(fmt.Errorf("internal error: len(vec.ParityPayloads) != 0: it was supposed to be freed by the sender handler"))
	}
	vec.ParityPacketsSet = 0
}
