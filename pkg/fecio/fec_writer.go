package fecio

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/facebookincubator/go-belt/tool/logger"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/templexxx/reedsolomon"
)

type fecWriter struct {
	backend                      io.WriteCloser
	rsConfigs                    []RedundancyConfiguration
	rsCache                      *rsCache
	isClosed                     bool
	cancelFunc                   context.CancelFunc
	currentVectorID              uint32
	nextInVectorID               uint8
	currentDataPacket            *dataPacketWithMetadata
	sendingBuffer                chan *dataPacketWithMetadata
	locker                       sync.Mutex
	maxPacketSize                uint16
	accumulateTime               time.Duration
	launchTimerIfNotLaunchedChan chan struct{}
	triggerSendingNowChan        chan struct{}
	writerConfig                 writerConfig
	payloadPool                  sync.Pool
	dataPacketPool               sync.Pool
	parityPacket                 parityPacketWithMetadata
	maxDataPacketsPerVector      uint8
	oobPacket                    []byte
}

type FECWriter struct {
	*fecWriter
}

var _ io.Writer = (*FECWriter)(nil)

type RedundancyConfiguration struct {
	DataPackets       uint8
	RedundancyPackets uint8
}

func NewFECWriter(
	w io.WriteCloser,
	cfgs []RedundancyConfiguration,
	accumulateTime time.Duration,
	maxPacketSize uint16,
	opts ...WriterOpt,
) (*FECWriter, error) {
	if len(cfgs) == 0 {
		return nil, fmt.Errorf("zero redundancy configurations given")
	}

	var err error
	sort.Slice(cfgs, func(i, j int) bool {
		cfgA := cfgs[i]
		cfgB := cfgs[j]

		if cfgA.DataPackets == cfgB.DataPackets {
			err = fmt.Errorf("received two equivalent configurations: %#+v == %#+v", cfgA, cfgB)
		}
		return cfgA.DataPackets > cfgB.DataPackets
	})
	if err != nil {
		return nil, err
	}

	Logger.Debugf("cfgs == %v", cfgs)

	var maxDataPacketsPerVector uint8
	for _, cfg := range cfgs {
		if cfg.DataPackets <= 0 {
			return nil, fmt.Errorf("a configuration with non-positive value of data packets")
		}
		if cfg.DataPackets > MaxDataPacketsPerVector {
			return nil, fmt.Errorf("DataPackets is greater than MaxDataPacketsPerVector: %d > %d", cfg.DataPackets, MaxDataPacketsPerVector)
		}
		if cfg.RedundancyPackets > MaxRedundancyPacketsPerVector {
			return nil, fmt.Errorf("RedundancyPackets is greater than MaxRedundancyPacketsPerVector: %d > %d", cfg.RedundancyPackets, MaxRedundancyPacketsPerVector)
		}
		if uint8(cfg.DataPackets) > maxDataPacketsPerVector {
			maxDataPacketsPerVector = uint8(cfg.DataPackets)
		}
	}

	rsCache, err := lru.New[RedundancyConfiguration, *reedsolomon.RS](MaxDataPacketsPerVector * MaxRedundancyPacketsPerVector)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize the cache structure: %w", err)
	}

	fecW := &FECWriter{
		fecWriter: &fecWriter{
			backend:                      w,
			rsConfigs:                    cfgs,
			rsCache:                      rsCache,
			accumulateTime:               accumulateTime,
			maxPacketSize:                maxPacketSize,
			launchTimerIfNotLaunchedChan: make(chan struct{}, 1),
			triggerSendingNowChan:        make(chan struct{}, 1),
			sendingBuffer:                make(chan *dataPacketWithMetadata, 2*maxDataPacketsPerVector),
			writerConfig:                 WriterOpts(opts).config(),
			payloadPool: sync.Pool{
				New: func() any {
					return ptr(make([]byte, 0, maxPacketSize))
				},
			},
			dataPacketPool: sync.Pool{
				New: func() any {
					d := &dataPacketWithMetadata{
						DataPacket: &DataPacket{},
					}
					copy(d.Magic[:], magicDataPacket[:])
					return d
				},
			},
			parityPacket: parityPacketWithMetadata{
				ParityPacket: &ParityPacket{},
				Serialized:   make([]byte, maxPacketSize),
			},
			maxDataPacketsPerVector: maxDataPacketsPerVector,
		},
	}
	copy(fecW.parityPacket.Magic[:], magicParityPacket[:])
	copy(fecW.parityPacket.Serialized, magicParityPacket[:])
	fecW.oobPacket = append(fecW.oobPacket, magicOOBPacket[:]...)

	fecW.currentDataPacket = fecW.dataPacketPool.Get().(*dataPacketWithMetadata)
	fecW.currentDataPacket.Reset()

	fecW.init()
	return fecW, nil
}

func (w *FECWriter) init() {
	ctx, cancelFn := context.WithCancel(context.Background())
	w.cancelFunc = cancelFn
	go func() {
		w.fecWriter.loop(ctx)
	}()

	runtime.SetFinalizer(w, func(w *FECWriter) {
		go w.Close()
	})
}

func (w *fecWriter) Close() error {
	w.locker.Lock()
	defer w.locker.Unlock()
	w.isClosed = true
	w.cancelFunc()
	return w.backend.Close()
}

func (w *fecWriter) loop(ctx context.Context) {
	iterationTime := w.accumulateTime / 10
	if iterationTime == 0 {
		iterationTime = time.Nanosecond
	}
	t := myClock.NewTicker(iterationTime)
	defer func() {
		Logger.Debugf("/loop")
		t.Stop()
		go w.Close()
	}()
	timerStarted := false
	nextTriggerAt := myClock.Now().Add(time.Hour * 24 * 365 * 100)
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.launchTimerIfNotLaunchedChan:
			if timerStarted {
				Logger.Tracef("received a launch timer signal: but it is already started")
				continue
			}
			nextTriggerAt = myClock.Now().Add(w.accumulateTime)
			timerStarted = true
			Logger.Tracef("received a launch timer signal: started")
		case <-w.triggerSendingNowChan:
			Logger.Tracef("<-w.triggerSendingNowChan")
			nextTriggerAt = myClock.Now().Add(time.Hour * 24 * 365 * 100)
			timerStarted = false
			if err := w.sendEverythingNow(); err != nil {
				Logger.Debugf("received an error on sending the packets: %v", err)
				return
			}
		case <-t.Chan():
			now := myClock.Now()
			Logger.Tracef("<-t.C: %v %v", now, nextTriggerAt)
			if !timerStarted {
				Logger.Tracef("<-t.C: !timerStarted")
				continue
			}
			if !now.After(nextTriggerAt) {
				Logger.Tracef("<-t.C: !time.Now().After(nextTriggerAt)")
				continue
			}
			Logger.Tracef("<-t.C: w.sendTimer.Reset(time.Nanosecond)")
			nextTriggerAt = myClock.Now().Add(time.Hour * 24 * 365 * 100)
			if err := w.sendEverythingNow(); err != nil {
				Logger.Debugf("received an error on sending the packets: %v", err)
				return
			}
		}
	}
}

func (w *fecWriter) WriteOOB(
	msg []byte,
) (int, error) {
	w.locker.Lock()
	defer w.locker.Unlock()
	if w.isClosed {
		return 0, fmt.Errorf("already closed: %w", io.ErrClosedPipe)
	}
	w.oobPacket = resizeSlice(w.oobPacket, len(magicOOBPacket), len(magicOOBPacket)+len(msg))
	Logger.Tracef("/sending message %X (len %d) with the OOB packet via the backend", msg, len(msg))
	n, err := w.backend.Write(w.oobPacket)
	Logger.Tracef("/sending message %X (len %d) with the OOB packet via the backend: %d %v", msg, len(msg), n, err)
	return n, err
}

func (w *fecWriter) Write(
	msg []byte,
) (int, error) {
	Logger.Tracef("writing a message of size %d", len(msg))
	minimalPacketSize := len(msg) + int(packetHeadersSize) + int(dataSubpacketHeadersSize)
	if minimalPacketSize > int(w.maxPacketSize)-int(redundancyConfiguration) {
		return 0, fmt.Errorf("the packet is too large: %d > %d", minimalPacketSize, w.maxPacketSize-redundancyConfiguration)
	}

	if len(msg) == 0 {
		return 0, nil
	}

	msgPtr := &msg
	if !w.writerConfig.NoCopy {
		msgPtr = copySliceUsingPool(&w.payloadPool, msg)
	}

	w.locker.Lock()
	defer w.locker.Unlock()
	if w.isClosed {
		return 0, fmt.Errorf("already closed: %w", io.ErrClosedPipe)
	}

	w.launchTimerIfNotLaunched()
	for w.nextInVectorID >= w.maxDataPacketsPerVector {
		w.locker.Unlock()
		runtime.Gosched()
		w.locker.Lock()
		w.triggerSendingNow()
	}

	curPkt := w.currentDataPacket
	if curPkt.SizeIfAddSubpacket(msg) > uint32(w.maxPacketSize)-uint32(redundancyConfiguration) {
		err := w.sendCurrentPacket()
		if err != nil {
			return len(msg), fmt.Errorf("unable to send the currently accumulated packet: %w", err)
		}
	}
	curPkt.AddDataSubpacket(msgPtr)
	if curPkt.CurrentSize == w.maxPacketSize-redundancyConfiguration {
		err := w.sendCurrentPacket()
		if err != nil {
			return len(msg), fmt.Errorf("unable to send the currently accumulated packet: %w", err)
		}
	}
	return len(msg), nil
}

func (w *fecWriter) sendCurrentPacket() (_err error) {
	Logger.Tracef("sendCurrentPacket")
	defer func() { Logger.Tracef("/sendCurrentPacket: %v", _err) }()

	curPkt := w.currentDataPacket
	if curPkt.CurrentSize <= dataSubpacketHeadersSize {
		return fmt.Errorf("an empty packet")
	}
	if len(curPkt.Subpackets) == 0 {
		panic(fmt.Errorf("internal error: len(curPkt.Subpackets) == 0"))
	}
	if len(curPkt.Subpackets[0].Payload) == 0 {
		panic(fmt.Errorf("internal error: len(curPkt.Subpackets[0].Payload) == 0"))
	}
	w.currentDataPacket = w.dataPacketPool.Get().(*dataPacketWithMetadata)
	w.currentDataPacket.Reset()

	curPkt.VectorID = w.currentVectorID
	curPkt.InVectorID = w.nextInVectorID
	w.nextInVectorID++

	msg := curPkt.Serialize(w.payloadPool.Get().(*[]byte))

	Logger.Tracef("sending message %X (len: %d) with the data packet via the backend", msg, len(msg))
	n, err := w.backend.Write(msg)
	Logger.Tracef("/sending message %X (len: %d) with the data packet via the backend: %d %v", msg, len(msg), n, err)
	if err != nil {
		return fmt.Errorf("unable to write a data packet via the backend writer: %w", err)
	}
	if n != len(msg) {
		return fmt.Errorf("wrote a data packet of a wrong size: %d != %d", n, len(msg))
	}

	Logger.Tracef("sending packet %d:%d for parity calculations", curPkt.VectorID, curPkt.InVectorID)
	for {
		select {
		case w.sendingBuffer <- curPkt:
		default: // to avoid a possible deadlock we allow the buffer to flush in the goroutine that executes method `loop`.
			w.locker.Unlock()
			runtime.Gosched()
			w.locker.Lock()
			continue
		}
		break
	}

	if len(w.sendingBuffer) >= int(w.maxDataPacketsPerVector) {
		w.triggerSendingNow()
	}
	return nil
}

func (w *fecWriter) launchTimerIfNotLaunched() {
	select {
	case w.launchTimerIfNotLaunchedChan <- struct{}{}:
		Logger.Tracef("launchTimerIfNotLaunched: sent a signal")
	default:
		Logger.Tracef("launchTimerIfNotLaunched: the queue is already full")
	}
}

func (w *fecWriter) triggerSendingNow() {
	select {
	case w.triggerSendingNowChan <- struct{}{}:
		Logger.Tracef("triggerSendingNowChan: sent a signal")
	default:
		Logger.Tracef("triggerSendingNowChan: the queue is already full")
	}
}

func (w *fecWriter) flushTriggers() {
	select {
	case <-w.triggerSendingNowChan:
	default:
	}
}

func (w *fecWriter) sendEverythingNow() (_err error) {
	w.locker.Lock()
	defer w.locker.Unlock()
	w.flushTriggers()
	Logger.Tracef("sendEverythingNow")
	defer func() { Logger.Tracef("/sendEverythingNow: %v", _err) }()

	if len(w.currentDataPacket.Subpackets) == 0 {
		Logger.Tracef("nothing to send")
	} else {
		if err := w.sendPendingDataNow(); err != nil {
			return fmt.Errorf("unable to send pending data packets: %w", err)
		}
	}
	if err := w.sendParityNow(); err != nil {
		return fmt.Errorf("unable to send parity packets: %w", err)
	}
	w.currentVectorID++
	w.nextInVectorID = 0
	return nil
}

func (w *fecWriter) sendPendingDataNow() error {
	err := w.sendCurrentPacket()
	return err
}

func (w *fecWriter) sendParityNow() error {
	for {
		pendingCount := uint(len(w.sendingBuffer))
		if pendingCount == 0 {
			return nil
		}
		cfg := w.findRSConfig(pendingCount)
		if int(cfg.DataPackets) > int(pendingCount) {
			return fmt.Errorf("internal error: found a ReedSolomon handler with DataNum greater than the amount of Data messages we have: %d > %d", cfg.DataPackets, pendingCount)
		}
		cfg.DataPackets = uint8(len(w.sendingBuffer))
		rs, err := w.getReedSolomon(cfg)
		if err != nil {
			return fmt.Errorf("unable to initialize a Reed Solomon handler: %w", err)
		}

		var (
			messageBufs    []*[]byte
			messages       [][]byte
			maxMessageSize int
			packetHeaders  PacketHeaders
		)
		for range cfg.DataPackets {
			select {
			case pkt := <-w.sendingBuffer:
				Logger.Tracef("a data packet %d:%d for parity calculations", pkt.VectorID, pkt.InVectorID)
				messageBufs = append(messageBufs, pkt.Buffers...)
				pkt.Buffers = pkt.Buffers[:0]
				if len(messages) == 0 {
					packetHeaders = pkt.PacketHeaders
					packetHeaders.InVectorID = 0
					packetHeaders.CRC64 = 0
				}
				cmpPacketHdr := pkt.PacketHeaders
				if int(cmpPacketHdr.InVectorID) != len(messages) {
					return fmt.Errorf("internal error: invalid InVectorID: %d != %d", pkt.PacketHeaders.InVectorID, len(messages))
				}
				cmpPacketHdr.InVectorID = 0
				cmpPacketHdr.CRC64 = 0
				if packetHeaders != cmpPacketHdr {
					return fmt.Errorf("internal error: packet headers do not match: %#+v != %#+v", packetHeaders, cmpPacketHdr)
				}

				msg := (pkt.Serialized)[packetHeadersSize:]
				messages = append(messages, msg)
				if len(msg) > maxMessageSize {
					maxMessageSize = len(msg)
				}
				pkt.Serialized = nil
				pkt.Reset()
				if reuseMemory {
					w.dataPacketPool.Put(pkt)
				}
			default:
				return fmt.Errorf("internal error: do not have enough messages, read %d, expected %d", len(messages), cfg.DataPackets)
			}
		}

		for i := range messages {
			messages[i] = messages[i][:maxMessageSize]
		}

		for range cfg.RedundancyPackets {
			msgPtr := getSliceFromPool[byte](&w.payloadPool, maxMessageSize)
			messageBufs = append(messageBufs, msgPtr)
			messages = append(messages, *msgPtr)
		}
		if Logger.Level() == logger.LevelTrace {
			for idx, payload := range messages {
				Logger.Tracef("messages[%d] == %X", idx, payload)
			}
		}

		err = rs.Encode(messages)
		if err != nil {
			return fmt.Errorf("unable to build parity messages: %w", err)
		}

		parityMessages := messages[cfg.DataPackets:]
		pkt := &w.parityPacket
		pkt.Serialized = pkt.Serialized[:int(parityPacketHeadersSize)+maxMessageSize]
		pkt.SetVectorID(packetHeaders.VectorID)
		pkt.SetRedundancyConfiguration(RedundancyConfiguration{
			DataPackets:       uint8(cfg.DataPackets),
			RedundancyPackets: uint8(cfg.RedundancyPackets),
		})
		for idx, msg := range parityMessages {
			pkt.SetInVectorID(uint8(int(cfg.DataPackets) + idx))
			pkt.SetPayload(msg)
			pkt.CalculateChecksum()
			Logger.Tracef("sending message %X:%#+v (len: %d) with a parity packet via the backend", pkt.Serialized, pkt.ParityPacket, len(pkt.Serialized))
			n, err := w.backend.Write(pkt.Serialized)
			Logger.Tracef("/sending message %X:%#+v (len: %d) with a parity packet via the backend: %d %v", pkt.Serialized, pkt.ParityPacket, len(pkt.Serialized), n, err)
			if err != nil {
				return fmt.Errorf("unable to write the parity packet via the backend writer: %w", err)
			}
			if n != len(pkt.Serialized) {
				return fmt.Errorf("wrote a parity packet of a wrong size: %d != %d", n, len(pkt.Serialized))
			}
		}
		for _, msgBuf := range messageBufs {
			*msgBuf = (*msgBuf)[:0]
			if reuseMemory {
				w.payloadPool.Put(msgBuf)
			}
		}
	}
}

func (w *fecWriter) getReedSolomon(
	cfg RedundancyConfiguration,
) (*reedsolomon.RS, error) {
	rs, ok := w.rsCache.Get(cfg)
	if ok {
		return rs, nil
	}

	rs, err := reedsolomon.New(int(cfg.DataPackets), int(cfg.RedundancyPackets))
	if err != nil {
		return nil, fmt.Errorf("unable to initialize a Reed-Solomon handler: %w", err)
	}

	w.rsCache.Add(cfg, rs)
	return rs, nil
}

func (w *fecWriter) findRSConfig(dataPacketsNum uint) RedundancyConfiguration {
	idx := sort.Search(len(w.rsConfigs), func(i int) bool {
		return int(dataPacketsNum) >= int(w.rsConfigs[i].DataPackets)
	})
	Logger.Tracef("findRSConfig: idx == %d", idx)
	if idx >= len(w.rsConfigs) {
		idx = len(w.rsConfigs) - 1
	}
	Logger.Tracef("findRSConfig: corrected idx == %d", idx)

	cfg := w.rsConfigs[idx]
	if int(cfg.DataPackets) > int(dataPacketsNum) {
		panic(fmt.Errorf("internal error: rs.DataNum > int(dataPacketsNum): %d > %d", cfg.DataPackets, int(dataPacketsNum)))
	}
	return cfg
}

func (w *fecWriter) MaxDataPacketsPerVector() uint8 {
	return w.maxDataPacketsPerVector
}
