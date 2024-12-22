package fecio

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/facebookincubator/go-belt/tool/logger"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/templexxx/reedsolomon"
	"github.com/xaionaro-go/unlossyudp/pkg/ringbuffer"
)

type rsCache = lru.Cache[RedundancyConfiguration, *reedsolomon.RS]

type FECReader struct {
	locker                sync.Mutex
	backend               io.ReadCloser
	rsCache               *rsCache
	receivedVectors       *ringbuffer.RingBuffer[*vector]
	payloadPool           sync.Pool
	vectorPool            sync.Pool
	dataPacketPool        sync.Pool
	parityPacketPool      sync.Pool
	tmpHaveIndexes        []int
	tmpReconstructIndexes []int
	payloadsBuffer        [][]byte
	oobChan               chan []byte

	packetsQueue          chan *dataPacketWithMetadata
	currentPacket         *dataPacketWithMetadata
	currentSubpacketIndex int
}

var _ io.Reader = (*FECReader)(nil)

func NewFECReader(
	r io.ReadCloser,
	allowReorder bool,
	maxPacketSize uint16,
	maxWindowSize uint32,
) (*FECReader, error) {
	if !allowReorder {
		return nil, fmt.Errorf("guaranteeing the order is not supported, yet")
	}
	if !isPowerOfTwo(maxWindowSize) {
		return nil, fmt.Errorf("due to design limitations of the reader, the maxWindowSize needs to be a power of 2, but it is %d", maxWindowSize)
	}
	rsCache, err := lru.New[RedundancyConfiguration, *reedsolomon.RS](MaxDataPacketsPerVector * MaxRedundancyPacketsPerVector)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize the cache structure: %w", err)
	}
	fecR := &FECReader{
		backend: r,
		rsCache: rsCache,
		payloadPool: sync.Pool{New: func() any {
			return ptr(make([]byte, 0, maxPacketSize))
		}},
		vectorPool: sync.Pool{New: func() any {
			return &vector{}
		}},
		dataPacketPool: sync.Pool{New: func() any {
			return &dataPacketWithMetadata{
				DataPacket: &DataPacket{},
			}
		}},
		parityPacketPool: sync.Pool{New: func() any {
			return &parityPacketWithMetadata{
				ParityPacket: &ParityPacket{},
			}
		}},
		oobChan:      make(chan []byte, 256),
		packetsQueue: make(chan *dataPacketWithMetadata, 1+MaxRedundancyPacketsPerVector),
	}
	fecR.receivedVectors = ringbuffer.New[*vector](uint(maxWindowSize), fecR.onVectorForget)
	return fecR, nil
}

func (r *FECReader) onVectorForget(vec *vector) {
	for _, d := range vec.DataPackets {
		if d == nil {
			continue
		}
		r.freeDataPacket(d)
	}
	vec.DataPackets = vec.DataPackets[:0]

	for _, p := range vec.ParityPackets {
		if p == nil {
			continue
		}
		p.Serialized = p.Serialized[:0]
		if reuseMemory {
			r.parityPacketPool.Put(p)
		}
	}
	vec.ParityPackets = vec.ParityPackets[:0]
}

func (r *FECReader) Read(
	p []byte,
) (int, error) {
	r.locker.Lock()
	defer r.locker.Unlock()

	if r.currentPacket == nil {
		r.tryObtainingNextPacket()
	}
	if r.currentPacket != nil {
		return r.copyBackMessage(r.getNextPayload(), p)
	}

	err := r.readFromBackend()
	if err != nil {
		return 0, fmt.Errorf("unable to read a message from the backend reader: %w", err)
	}

	if r.currentPacket != nil {
		return r.copyBackMessage(r.getNextPayload(), p)
	}

	return 0, fmt.Errorf("internal error: the buffer of parsed messages is empty")
}

func (r *FECReader) Close() error {
	return r.backend.Close()
}

func (r *FECReader) tryObtainingNextPacket() {
	Logger.Tracef("tryObtainingNextPacket")
	select {
	case r.currentPacket = <-r.packetsQueue:
		r.currentSubpacketIndex = 0
		Logger.Tracef("set r.currentPacket to %p: %#+v", r.currentPacket, r.currentPacket.DataPacket)
	default:
		Logger.Tracef("nothing to set to r.currentPacket")
	}
}

func (r *FECReader) getNextPayload() []byte {
	subPkt := r.currentPacket.Subpackets[r.currentSubpacketIndex]
	r.currentSubpacketIndex++
	if r.currentSubpacketIndex >= len(r.currentPacket.Subpackets) {
		r.freeCurrentPacket()
	}
	Logger.Tracef("next payload: %X", subPkt.Payload)
	return subPkt.Payload
}

func (r *FECReader) freeCurrentPacket() {
	r.freeDataPacket(r.currentPacket)
	r.currentPacket = nil
}

func (r *FECReader) freeDataPacket(d *dataPacketWithMetadata) {
	if d == nil {
		panic("internal error: d == nil")
	}
	if reuseMemory {
		for _, buf := range d.Buffers {
			r.payloadPool.Put(buf)
		}
	}
	d.Buffers = d.Buffers[:0]
	if reuseMemory {
		r.dataPacketPool.Put(d)
	}
}

func (r *FECReader) copyBackMessage(src, dst []byte) (int, error) {
	copy(dst, src)
	if len(dst) < len(src) {
		return len(dst), io.ErrShortBuffer
	}

	return len(src), nil
}

func (r *FECReader) readFromBackend() (_err error) {
	Logger.Tracef("readFromBackend")
	defer func() { Logger.Tracef("/readFromBackend: %v", _err) }()

	bufPtr := getSliceFromPool[byte](&r.payloadPool, -1)
	for {
		Logger.Tracef("receiving a message via the backend")
		n, err := r.backend.Read(*bufPtr)
		Logger.Tracef("/receiving a message via the backend: %d %v", n, err)
		if err != nil {
			return err
		}
		if n >= len(*bufPtr) {
			return fmt.Errorf("the received packet is larger than %d", len(*bufPtr)-1)
		}
		if n < int(packetHeadersSize) {
			return fmt.Errorf("the received packet is too small: %d < %d", n, packetHeadersSize)
		}

		Logger.Tracef("magic value is: %X", (*bufPtr)[:4])
		switch {
		case bytes.Equal((*bufPtr)[:4], magicDataPacket[:]):
			err := r.parseDataPacket(bufPtr, n)
			if err != nil {
				return fmt.Errorf("unable to parse a data packet: %w", err)
			}
			r.tryObtainingNextPacket()
			if r.currentPacket == nil {
				return fmt.Errorf("received a packet, but lost it somehow")
			}
		case bytes.Equal((*bufPtr)[:4], magicParityPacket[:]):
			err := r.parseParityPacket(bufPtr, n)
			if err != nil {
				return fmt.Errorf("unable to parse a parity packet: %w", err)
			}
			r.tryObtainingNextPacket()
			if r.currentPacket == nil {
				Logger.Tracef("received a parity packet, but no data packet obtained as a result; trying again")
				continue
			}
		case bytes.Equal((*bufPtr)[:4], magicOOBPacket[:]):
			err := r.parseOOBPacket(bufPtr, n)
			if err != nil {
				Logger.Errorf("unable to parse an OOB packet: %v", err)
			}
			continue
		}
		return nil
	}
}

func (r *FECReader) parseOOBPacket(
	unparsedPtr *[]byte,
	length int,
) (_err error) {
	Logger.Tracef("parseOOBPacket")
	defer func() { Logger.Tracef("/parseOOBPacket: %v", _err) }()
	select {
	case r.oobChan <- copySlice((*unparsedPtr)[4:length]):
		return nil
	default:
		return fmt.Errorf("the OOB messages queue is full")
	}
}

func (r *FECReader) parseDataPacket(
	unparsedPtr *[]byte,
	length int,
) (_err error) {
	Logger.Tracef("parseDataPacket")
	defer func() { Logger.Tracef("/parseDataPacket: %v", _err) }()
	return readerParsePacket(
		r,
		unparsedPtr,
		length,
		r.dataPacketPool.Get().(*dataPacketWithMetadata),
		func(
			vec *vector,
			pkt *dataPacketWithMetadata,
		) error {
			if int(pkt.InVectorID) >= len(vec.DataPackets) {
				if pkt.InVectorID > MaxDataPacketsPerVector {
					return fmt.Errorf("InVectorID of a data packet is greater than MaxDataPacketsPerVector: %d > %d", pkt.InVectorID, MaxDataPacketsPerVector)
				}
				dataPacketsNum := int(pkt.InVectorID) + 1
				vec.DataPackets = resizeSlice(vec.DataPackets, dataPacketsNum, dataPacketsNum)
			}

			if vec.DataPackets[pkt.InVectorID] != nil {
				// a duplicate
				Logger.Tracef("received a duplicate data packet")
				return nil
			}

			Logger.Tracef("sending an original packet %d:%d", pkt.VectorID, pkt.InVectorID)
			r.onNewDataPacket(vec, pkt)
			return nil
		},
	)
}

func readerParsePacket[T packetWithMetadata](
	r *FECReader,
	unparsedPtr *[]byte,
	length int,
	pkt T,
	callback func(*vector, T) error,
) error {
	pkt.Reset()
	err := pkt.Parse(unparsedPtr, length)
	if err != nil {
		Logger.Tracef("the unparsed packet: %X", (*unparsedPtr)[:length])
		return fmt.Errorf("unable to parse the data packet: %w", err)
	}

	err = pkt.VerifyChecksum()
	if err != nil {
		Logger.Tracef("the corrupted packet: %X", (*unparsedPtr)[:length])
		return fmt.Errorf("checksum does not match: %w", err)
	}

	vectorID := pkt.GetVectorID()

	vec, _ := r.receivedVectors.Get(int(vectorID))
	if vec == nil {
		vec = r.vectorPool.Get().(*vector)
		vec.Reset()
		vec.ID = vectorID
		r.receivedVectors.Set(int(vectorID), vec)
	}

	return callback(vec, pkt)
}

func (r *FECReader) onNewDataPacket(
	vec *vector,
	pkt *dataPacketWithMetadata,
) {
	Logger.Tracef("onNewDataPacket: %p: %#+v", pkt, pkt.DataPacket)
	defer Logger.Tracef("/onNewDataPacket")
	if r.currentPacket != nil {
		panic(fmt.Errorf("there is already another packet in the process of reading from: r.currentPacket != nil"))
	}

	if vec.DataPackets[pkt.InVectorID] != nil {
		Logger.Tracef("found a duplicate data packet, skipping")
		return
	}

	vec.DataPacketsSet++
	vec.DataPackets[pkt.InVectorID] = pkt
	r.packetsQueue <- pkt
}

func (r *FECReader) parseParityPacket(
	unparsedPtr *[]byte,
	length int,
) (_err error) {
	Logger.Tracef("parseParityPacket")
	defer func() { Logger.Tracef("/parseParityPacket: %v", _err) }()
	return readerParsePacket(
		r,
		unparsedPtr,
		length,
		r.parityPacketPool.Get().(*parityPacketWithMetadata),
		func(
			vec *vector,
			pkt *parityPacketWithMetadata,
		) error {
			if pkt.RedundancyConfiguration.DataPackets > MaxDataPacketsPerVector {
				return fmt.Errorf("cfg.DataPackets is greater MaxDataPacketsPerVector: %d > %d", pkt.RedundancyConfiguration.DataPackets, MaxDataPacketsPerVector)
			}
			if pkt.RedundancyConfiguration.RedundancyPackets > MaxRedundancyPacketsPerVector {
				return fmt.Errorf("cfg.RedundancyPackets is greater MaxRedundancyPacketsPerVector: %d > %d", pkt.RedundancyConfiguration.RedundancyPackets, MaxRedundancyPacketsPerVector)
			}
			if int(pkt.InVectorID) < int(pkt.RedundancyConfiguration.DataPackets) {
				return fmt.Errorf("invalid InVectorID of a parity packet, should be >= %d, but it is %d", pkt.RedundancyConfiguration.DataPackets, pkt.InVectorID)
			}
			idx := int(pkt.InVectorID) - int(pkt.RedundancyConfiguration.DataPackets)
			if idx > int(pkt.RedundancyConfiguration.RedundancyPackets) {
				return fmt.Errorf("InVectorID of a data packet is greater than cfg.DataPackets + cfg.RedundancyPackets: %d > %d", pkt.InVectorID, pkt.RedundancyConfiguration.DataPackets+pkt.RedundancyConfiguration.RedundancyPackets)
			}

			vec.ParityPackets = resizeSlice(
				vec.ParityPackets,
				int(pkt.RedundancyConfiguration.RedundancyPackets),
				int(pkt.RedundancyConfiguration.RedundancyPackets),
			)

			if vec.ParityPackets[idx] != nil {
				// a duplicate
				Logger.Tracef("received a parity data packet")
				return nil
			}

			vec.ParityPacketsSet++
			vec.ParityPackets[idx] = pkt
			return r.onNewParityPacket(vec, pkt)
		},
	)
}

func (r *FECReader) onNewParityPacket(
	vec *vector,
	pkt *parityPacketWithMetadata,
) error {
	err := r.reconstructVector(vec, pkt.RedundancyConfiguration)
	if err != nil {
		return fmt.Errorf("unable to reconstruct the vector: %w", err)
	}

	return nil
}

func (r *FECReader) reconstructVector(
	vec *vector,
	cfg RedundancyConfiguration,
) error {
	if vec.Reconstructed {
		return nil
	}

	if vec.DataPacketsSet+vec.ParityPacketsSet < uint(cfg.DataPackets) {
		return nil
	}
	if vec.DataPacketsSet >= uint(cfg.DataPackets) {
		return nil
	}

	r.tmpReconstructIndexes = r.tmpReconstructIndexes[:0]
	r.tmpHaveIndexes = r.tmpHaveIndexes[:0]
	vec.DataPackets = resizeSlice(vec.DataPackets, int(cfg.DataPackets), int(cfg.DataPackets))
	totalPackets := int(cfg.DataPackets) + int(cfg.RedundancyPackets)
	r.payloadsBuffer = resizeSlice(r.payloadsBuffer, totalPackets, totalPackets)

	itemSize := 0
	for idx, pkt := range vec.ParityPackets {
		if pkt == nil {
			continue
		}
		if pkt.RedundancyConfiguration != cfg {
			return fmt.Errorf("redundancy configuration mismatch: %#+v != %#+v", pkt.RedundancyConfiguration, cfg)
		}
		r.tmpHaveIndexes = append(r.tmpHaveIndexes, int(cfg.DataPackets)+idx)
		payload := pkt.Serialized[parityPacketHeadersSize:]
		r.payloadsBuffer[idx+int(cfg.DataPackets)] = payload
		itemSize = len(payload)
	}

	for idx, pkt := range vec.DataPackets {
		if pkt == nil {
			r.tmpReconstructIndexes = append(r.tmpReconstructIndexes, idx)
			continue
		}
		r.tmpHaveIndexes = append(r.tmpHaveIndexes, idx)
		r.payloadsBuffer[idx] = resizeSlice(pkt.Serialized[packetHeadersSize:], itemSize, itemSize)
	}

	for idx := range r.payloadsBuffer {
		if len(r.payloadsBuffer[idx]) == itemSize {
			continue
		}
		r.payloadsBuffer[idx] = resizeSlice(r.payloadsBuffer[idx], itemSize, itemSize)
	}

	rs, err := r.getReedSolomon(cfg)
	if err != nil {
		return fmt.Errorf("unable to get a Reed-Solomon handler: %w", err)
	}

	if Logger.Level() == logger.LevelTrace {
		for idx, payload := range r.payloadsBuffer {
			Logger.Tracef("r.payloadsBuffer[%d] == %X (len: %d)", idx, payload, len(payload))
		}
		Logger.Tracef("r.tmpHaveIndexes == %v, r.tmpReconstructIndexes == %v; itemSize == %d", r.tmpHaveIndexes, r.tmpReconstructIndexes, itemSize)
	}
	err = rs.Reconst(r.payloadsBuffer, r.tmpHaveIndexes, r.tmpReconstructIndexes)
	if err != nil {
		return fmt.Errorf("rs.Reconst returned an error: %w", err)
	}
	vec.Reconstructed = true

	if Logger.Level() == logger.LevelTrace {
		for idx, payload := range r.payloadsBuffer {
			Logger.Tracef("reconstructed r.payloadsBuffer[%d] == %X", idx, payload)
		}
	}

	for _, idx := range r.tmpReconstructIndexes {
		Logger.Tracef("rebuilding packet with InVectorID %d", idx)
		pkt := r.dataPacketPool.Get().(*dataPacketWithMetadata)
		pkt.PacketHeaders.VectorID = vec.ID
		pkt.PacketHeaders.InVectorID = uint8(idx)
		payload := r.payloadsBuffer[idx]
		_, err := pkt.DataPacket.ReadPayloadFrom(bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("unable to parse the reconstructed packet: %w", err)
		}
		Logger.Tracef("sending an reconstructed packet %d:%d", pkt.VectorID, pkt.InVectorID)
		r.onNewDataPacket(vec, pkt)
	}
	return nil
}

func (r *FECReader) getReedSolomon(
	cfg RedundancyConfiguration,
) (*reedsolomon.RS, error) {
	rs, ok := r.rsCache.Get(cfg)
	if ok {
		return rs, nil
	}

	rs, err := reedsolomon.New(int(cfg.DataPackets), int(cfg.RedundancyPackets))
	if err != nil {
		return nil, fmt.Errorf("unable to initialize a Reed-Solomon handler: %w", err)
	}

	r.rsCache.Add(cfg, rs)
	return rs, nil
}
