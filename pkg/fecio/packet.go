package fecio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"math"
)

var BinaryEndian = binary.BigEndian

var magicDataPacket = [4]byte{0x18, 0x95, 0x1a, 0x8e}
var magicParityPacket = [4]byte{0xeb, 0xc0, 0x49, 0x9c}
var magicOOBPacket = [4]byte{0x67, 0x19, 0xe0, 0x57}

type Packet interface {
	GetVectorID() uint32
	Parse([]byte) error
	VerifyChecksum() error
	Reset()
}

type packetWithMetadata interface {
	GetVectorID() uint32
	Parse(*[]byte, int) error
	VerifyChecksum() error
	Reset()
}

type PacketHeaders struct {
	Magic      [4]byte
	CRC64      uint64
	VectorID   uint32
	InVectorID uint8
}

func (hdr *PacketHeaders) GetVectorID() uint32 {
	return hdr.VectorID
}

type ParityPacketHeaders struct {
	PacketHeaders
	RedundancyConfiguration
}

type ParityPacket struct {
	ParityPacketHeaders
	ParityPayload []byte
}

func (p *ParityPacket) Read(msg []byte) (int, error) {
	r := bytes.NewReader(msg)
	err := binary.Read(r, BinaryEndian, &p.ParityPacketHeaders)
	if err != nil {
		return int(r.Size()) - r.Len(), fmt.Errorf("unable to read the headers: %w", err)
	}

	p.ParityPayload = resizeSlice(p.ParityPayload, len(p.ParityPayload), len(p.ParityPayload))
	err = binary.Read(r, BinaryEndian, p.ParityPayload)
	if err != nil {
		return int(r.Size()) - r.Len(), fmt.Errorf("unable to read the payload: %w", err)
	}
	return int(r.Size()), nil
}

type parityPacketWithMetadata struct {
	*ParityPacket
	Serialized []byte
}

func (p *parityPacketWithMetadata) Reset() {
}

func (p *parityPacketWithMetadata) Parse(
	msg *[]byte,
	length int,
) error {
	p.Serialized = (*msg)[:length]
	_, err := p.ParityPacket.Read((*msg)[:length])
	return err
}

func (p *parityPacketWithMetadata) SetVectorID(id uint32) {
	p.VectorID = id
	BinaryEndian.PutUint32(p.Serialized[12:], id)
}

func (p *parityPacketWithMetadata) SetInVectorID(id uint8) {
	p.InVectorID = id
	p.Serialized[16] = id
}

func (p *parityPacketWithMetadata) SetRedundancyConfiguration(
	cfg RedundancyConfiguration,
) {
	p.RedundancyConfiguration = cfg
	p.Serialized[packetHeadersSize+0] = byte(cfg.DataPackets)
	p.Serialized[packetHeadersSize+1] = byte(cfg.RedundancyPackets)
}

func (p *parityPacketWithMetadata) SetPayload(payload []byte) {
	p.ParityPayload = payload
	copy(p.Serialized[packetHeadersSize+2:], payload)
}

func (p *parityPacketWithMetadata) CalculateChecksum() {
	BinaryEndian.PutUint64(p.Serialized[4:], 0)
	Logger.Tracef("parityPacketWithMetadata.CalculateChecksum; the packet: %X", p.Serialized[:int(parityPacketHeadersSize)+len(p.ParityPayload)])
	p.CRC64 = crc64.Checksum(p.Serialized, crc64Table)
	BinaryEndian.PutUint64(p.Serialized[4:], p.CRC64)
	Logger.Tracef("parityPacketWithMetadata.CalculateChecksum; result: %X", p.CRC64)
}

func (p *parityPacketWithMetadata) VerifyChecksum() error {
	crc64 := p.CRC64
	p.CalculateChecksum()
	var newCRC64 uint64
	p.CRC64, newCRC64 = crc64, p.CRC64
	BinaryEndian.PutUint64(p.Serialized[4:], crc64)

	if crc64 != newCRC64 {
		return fmt.Errorf("(parity packet) CRC64 does not match: 0x%016X != 0x%016X", crc64, newCRC64)
	}
	return nil
}

type DataPacket struct {
	PacketHeaders
	Subpackets []DataSubpacket
}

func (p *DataPacket) Write(buf []byte) (int, error) {
	out := bytes.NewBuffer(buf[:0])
	err := binary.Write(out, BinaryEndian, p.PacketHeaders)
	if err != nil {
		return out.Len(), fmt.Errorf("unable to write the headers: %w", err)
	}
	for idx, subPkt := range p.Subpackets {
		if len(subPkt.Payload) == 0 {
			return out.Len(), fmt.Errorf("unable to write subpacket #%d payload is empty", idx)
		}
		if int(subPkt.DataSubpacketHeaders.Size) != len(subPkt.Payload) {
			return out.Len(), fmt.Errorf("invalid header: the size should be %d, but it is %d", subPkt.DataSubpacketHeaders.Size, len(subPkt.Payload))
		}
		err := binary.Write(out, BinaryEndian, subPkt.DataSubpacketHeaders)
		if err != nil {
			return out.Len(), fmt.Errorf("unable to write subpacket #%d headers: %w", idx, err)
		}
		err = binary.Write(out, BinaryEndian, subPkt.Payload)
		if err != nil {
			return out.Len(), fmt.Errorf("unable to write subpacket #%d payload: %w", idx, err)
		}
	}
	if out.Len() > len(buf) {
		return out.Len(), fmt.Errorf("internal error: invalid length of the output: %d > %d", out.Len(), len(buf))
	}
	return out.Len(), nil
}

func (p *DataPacket) ReadPayloadFrom(r io.Reader) (int64, error) {
	n := int64(0)
	for {
		p.Subpackets = append(p.Subpackets, DataSubpacket{})
		idx := len(p.Subpackets) - 1
		subPkt := &p.Subpackets[idx]

		err := binary.Read(r, BinaryEndian, &subPkt.DataSubpacketHeaders)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			p.Subpackets = p.Subpackets[:idx]
			break
		}
		if err != nil {
			return n, fmt.Errorf("unable to read the subpacket #%d headers: %w", idx, err)
		}
		n += int64(dataSubpacketHeadersSize)
		if subPkt.Size == 0 {
			Logger.Tracef("the payload size is zero")
			return n, nil
		}
		Logger.Tracef("new subPkt headers: %#+v", subPkt.DataSubpacketHeaders)

		subPkt.Payload = resizeSlice(subPkt.Payload, int(subPkt.Size), int(subPkt.Size))
		err = binary.Read(r, BinaryEndian, subPkt.Payload)
		if err != nil {
			return n, fmt.Errorf("unable to read the subpacket #%d payload of size %d (hdr: %#+v): %w", idx, subPkt.Size, subPkt.DataSubpacketHeaders, err)
		}
		n += int64(subPkt.Size)
		Logger.Tracef("new subPkt payload: %X", subPkt.Payload)
	}

	return n, nil
}

func (p *DataPacket) Read(msg []byte) (int, error) {
	r := bytes.NewReader(msg)
	err := binary.Read(r, BinaryEndian, &p.PacketHeaders)
	if err != nil {
		return int(r.Size()) - r.Len(), fmt.Errorf("unable to read the headers: %w", err)
	}

	_, err = p.ReadPayloadFrom(r)
	return int(r.Size()) - r.Len(), err
}

type dataPacketWithMetadata struct {
	*DataPacket
	Buffers     []*[]byte
	CurrentSize uint16
	Serialized  []byte
}

func (d *dataPacketWithMetadata) Parse(
	msg *[]byte,
	length int,
) error {
	d.Serialized = *msg
	d.CurrentSize = uint16(length)
	_, err := d.DataPacket.Read((*msg)[:length])
	return err
}

func (d *dataPacketWithMetadata) SizeIfAddSubpacket(msg []byte) uint32 {
	return uint32(d.CurrentSize) + uint32(len(msg)) + uint32(dataSubpacketHeadersSize)
}

func (d *dataPacketWithMetadata) AddDataSubpacket(msg *[]byte) {
	d.Buffers = append(d.Buffers, msg)
	newSize := d.SizeIfAddSubpacket(*msg)
	if newSize > math.MaxUint16 {
		panic(fmt.Errorf("internal error: supposed to be impossible, the accumulated packet size is bigger than a limit by design"))
	}
	d.CurrentSize = uint16(newSize)
	d.DataPacket.Subpackets = append(
		d.DataPacket.Subpackets,
		DataSubpacket{
			DataSubpacketHeaders: DataSubpacketHeaders{
				Size: uint16(len(*msg)),
			},
			Payload: *msg,
		},
	)
	Logger.Tracef("added a subpacket (with payload of size %d), now there are %d of them", len(*msg), len(d.DataPacket.Subpackets))
}

func (d *dataPacketWithMetadata) Reset() {
	d.CurrentSize = packetHeadersSize
	d.InVectorID = 0
	d.VectorID = 0
	d.Subpackets = d.DataPacket.Subpackets[:0]
	if len(d.Buffers) != 0 {
		panic(fmt.Errorf("internal error: len(d.Buffers) != 0: it was supposed to be freed by the sender handler"))
	}
	if d.Serialized != nil {
		panic(fmt.Errorf("internal error: d.Serialized != nil: it was supposed to be freed by the sender handler"))
	}
}

func (d *dataPacketWithMetadata) Serialize(bufPtr *[]byte) []byte {
	if d.Serialized != nil {
		return d.Serialized
	}
	*bufPtr = resizeSlice(*bufPtr, int(d.CurrentSize), int(d.CurrentSize))
	d.CRC64 = 0
	msg := (*bufPtr)[:d.CurrentSize]
	_, err := d.DataPacket.Write(msg)
	if err != nil {
		panic(fmt.Errorf("internal error: unable to serialize the packet: %w", err))
	}
	d.Serialized = msg
	Logger.Tracef("dataPacketWithMetadata.Serialize; the packet: %X", msg)
	BinaryEndian.PutUint64((d.Serialized)[4:], 0)
	d.CRC64 = crc64.Checksum(msg, crc64Table)
	BinaryEndian.PutUint64(msg[len(d.Magic):], d.CRC64)
	return msg
}

func (d *dataPacketWithMetadata) VerifyChecksum() error {
	oldCRC64 := d.CRC64
	msg := d.Serialized[:d.CurrentSize]
	BinaryEndian.PutUint64(msg[4:], 0)
	Logger.Tracef("dataPacketWithMetadata.VerifyChecksum; the packet: %X", msg)
	newCRC64 := crc64.Checksum(msg, crc64Table)
	BinaryEndian.PutUint64(msg[4:], oldCRC64)
	if oldCRC64 != newCRC64 {
		return fmt.Errorf("(data packet) CRC64 does not match: 0x%016X != 0x%016X", oldCRC64, newCRC64)
	}
	return nil
}

type DataSubpacketHeaders struct {
	Size uint16
}

var (
	parityPacketHeadersSize  uint16
	redundancyConfiguration  uint16
	packetHeadersSize        uint16
	dataSubpacketHeadersSize uint16
)

func init() {
	parityPacketHeadersSize = uint16(binary.Size(ParityPacketHeaders{}))
	redundancyConfiguration = uint16(binary.Size(RedundancyConfiguration{}))
	packetHeadersSize = uint16(binary.Size(PacketHeaders{}))
	dataSubpacketHeadersSize = uint16(binary.Size(DataSubpacketHeaders{}))
}

type DataSubpacket struct {
	DataSubpacketHeaders
	Payload []byte
}
