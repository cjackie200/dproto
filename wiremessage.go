// Craig Hesling <craig@hesling.com>
// Started December 15, 2016
//
// This file houses the lowest level interface for dproto. These structures
// and methods allow access to wire-level message data.
// You can use this interface if you do not want dproto to manage
// associations.
//
// Notes: We should have a stream Reader interface that can read and
//        and interpret bytes from a buffer synchronously. Not just the readall
//        and process later methods.

package dproto

import (
	"fmt"

	"io"

	"errors"

	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
)

// ErrMalformedProtoBuf is returned when some operation has determined that
// that a message field/types association does not agree with a message
var ErrMalformedProtoBuf = errors.New("Malformed protobuf buffer")

// ErrMessageFieldMissing is returned when some messgae get could not find
// the specified field
var ErrMessageFieldMissing = errors.New("Message field not found")

// ErrInvalidProtoBufType is returned when an invalid Protobuf type was
// specified
var ErrInvalidProtoBufType = errors.New("Invalid protobuf type")

// WireMessage holds the data elements of a marshalled Protobuf message.
// A marshalled Protobuf message is simply the concatenation of all the
// below key-values, where the key is the field number and the value is
// is converted to a wiretype.
//
// This design is not exactly efficient for sending the fields in FielndNum
// order. For this reason, this implementation may be changed out in a later
// date. Sending fields in numerical order is reccomended on the Protobuf
// website.
type WireMessage struct {
	varint  map[FieldNum]WireVarint
	fixed32 map[FieldNum]WireFixed32
	fixed64 map[FieldNum]WireFixed64
	bytes   map[FieldNum][]byte
}

// NewWireMessage creates a new Wiremessage object.
func NewWireMessage() *WireMessage {
	var m = new(WireMessage)
	m.Reset()
	return m
}

// Reset clears the WireMessage m
func (m *WireMessage) Reset() {
	m.varint = make(map[FieldNum]WireVarint)
	m.fixed32 = make(map[FieldNum]WireFixed32)
	m.fixed64 = make(map[FieldNum]WireFixed64)
	m.bytes = make(map[FieldNum][]byte)
}

/*******************************************************
 *             Low-Level Wire Interface                *
 *******************************************************/

// AddVarint adds a WireVarint wiretype to the wire message m
func (m *WireMessage) AddVarint(field FieldNum, value WireVarint) {
	m.varint[field] = value
}

// AddFixed32 adds a WireFixed32 wiretype to the wire message m
func (m *WireMessage) AddFixed32(field FieldNum, value WireFixed32) {
	m.fixed32[field] = value
}

// AddFixed64 adds a WireFixed64 wiretype to the wire message m
func (m *WireMessage) AddFixed64(field FieldNum, value WireFixed64) {
	m.fixed64[field] = value
}

// AddBytes adds a byte buffer wiretype to the wire message m
func (m *WireMessage) AddBytes(field FieldNum, buf []byte) {
	m.bytes[field] = buf
}

// Remove removes the wiretype field previously added
func (m *WireMessage) Remove(field FieldNum) {
	delete(m.varint, field)
	delete(m.fixed32, field)
	delete(m.fixed64, field)
	delete(m.bytes, field)
}

// GetFieldCount gets the number of fields in the WireMessage
func (m *WireMessage) GetFieldCount() int {
	return len(m.varint) + len(m.fixed32) + len(m.fixed64) + len(m.bytes)
}

// GetFieldNums gets all field numbers contained in the WireMessage
func (m *WireMessage) GetFieldNums() []FieldNum {
	fields := make([]FieldNum, 0, m.GetFieldCount())
	for k, _ := range m.varint {
		fields = append(fields, k)
	}
	for k, _ := range m.fixed32 {
		fields = append(fields, k)
	}
	for k, _ := range m.fixed64 {
		fields = append(fields, k)
	}
	for k, _ := range m.bytes {
		fields = append(fields, k)
	}
	return fields
}

// GetField fetches the raw wire field from m and returns it
// as the proper wire type
func (m *WireMessage) GetField(field FieldNum) (interface{}, bool) {

	/* Check all data field types to find specified field */

	if val, ok := m.varint[field]; ok {
		return val, true
	}
	if val, ok := m.fixed32[field]; ok {
		return val, true
	}
	if val, ok := m.fixed64[field]; ok {
		return val, true
	}
	if val, ok := m.bytes[field]; ok {
		return val, true
	}
	return nil, false
}

// GetVarint fetches a varint wire field from m
func (m *WireMessage) GetVarint(field FieldNum) (WireVarint, bool) {
	val, ok := m.varint[field]
	return val, ok
}

// GetFixed32 fetches a fixed32 wire field from m
func (m *WireMessage) GetFixed32(field FieldNum) (WireFixed32, bool) {
	val, ok := m.fixed32[field]
	return val, ok
}

// GetFixed64 fetches a fixed64 wire field from m
func (m *WireMessage) GetFixed64(field FieldNum) (WireFixed64, bool) {
	val, ok := m.fixed64[field]
	return val, ok
}

// GetBytes fetches a byte array wire field from m
func (m *WireMessage) GetBytes(field FieldNum) ([]byte, bool) {
	val, ok := m.bytes[field]
	return val, ok
}

/*******************************************************
 *                High-Level Interface                 *
 *******************************************************/

/////////////////////////////// Decoding /////////////////////////////////////

// DecodeInt32 fetches the wiretype field and decodes it as a Protobuf int32
func (m *WireMessage) DecodeInt32(field FieldNum) (int32, bool) {
	val, ok := m.GetVarint(field)
	return val.AsInt32(), ok
}

// DecodeInt64 fetches the field from m and decodes it as a Protobuf int64
func (m *WireMessage) DecodeInt64(field FieldNum) (int64, bool) {
	val, ok := m.GetVarint(field)
	return val.AsInt64(), ok
}

// DecodeUint32 fetches the field from m and decodes it as a Protobuf uint32
func (m *WireMessage) DecodeUint32(field FieldNum) (uint32, bool) {
	val, ok := m.GetVarint(field)
	return val.AsUint32(), ok
}

// DecodeUint64 fetches the field from m and decodes it as a Protobuf uint64
func (m *WireMessage) DecodeUint64(field FieldNum) (uint64, bool) {
	val, ok := m.GetVarint(field)
	return val.AsUint64(), ok
}

// DecodeSint32 fetches the field from m and decodes it as a Protobuf sint32
func (m *WireMessage) DecodeSint32(field FieldNum) (int32, bool) {
	val, ok := m.GetVarint(field)
	return val.AsSint32(), ok
}

// DecodeSint64 fetches the field from m and decodes it as a Protobuf sint64
func (m *WireMessage) DecodeSint64(field FieldNum) (int64, bool) {
	val, ok := m.GetVarint(field)
	return val.AsSint64(), ok
}

// DecodeBool fetches the field from m and decodes it as a Protobuf bool
func (m *WireMessage) DecodeBool(field FieldNum) (bool, bool) {
	val, ok := m.GetVarint(field)
	return val.AsBool(), ok
}

// DecodeFixed32 fetches the field from m and decodes it as a Protobuf fixed32
func (m *WireMessage) DecodeFixed32(field FieldNum) (uint32, bool) {
	val, ok := m.GetFixed32(field)
	return val.AsFixed32(), ok
}

// DecodeSfixed32 fetches the field from m and decodes it as a Protobuf sfixed32
func (m *WireMessage) DecodeSfixed32(field FieldNum) (int32, bool) {
	val, ok := m.GetFixed32(field)
	return val.AsSfixed32(), ok
}

// DecodeFloat fetches the field from m and decodes it as a Protobuf float
func (m *WireMessage) DecodeFloat(field FieldNum) (float32, bool) {
	val, ok := m.GetFixed32(field)
	return val.AsFloat(), ok
}

// DecodeFixed64 fetches the field from m and decodes it as a Protobuf fixed64
func (m *WireMessage) DecodeFixed64(field FieldNum) (uint64, bool) {
	val, ok := m.GetFixed64(field)
	return val.AsFixed64(), ok
}

// DecodeSfixed64 fetches the field from m and decodes it as a Protobuf sfixed64
func (m *WireMessage) DecodeSfixed64(field FieldNum) (int64, bool) {
	val, ok := m.GetFixed64(field)
	return val.AsSfixed64(), ok
}

// DecodeDouble fetches the field and decodes it as a Protobuf double
func (m *WireMessage) DecodeDouble(field FieldNum) (float64, bool) {
	val, ok := m.GetFixed64(field)
	return val.AsDouble(), ok
}

// DecodeString fetches the field from m and decodes it as a Protobuf string
func (m *WireMessage) DecodeString(field FieldNum) (string, bool) {
	// TODO: Check correctness for unicode/7bit ASCII text
	if val, ok := m.GetBytes(field); ok {
		return string(val), true
	}
	return "", false
}

// DecodeBytes fetches the field from m and decodes it as a Protobuf bytes type
func (m *WireMessage) DecodeBytes(field FieldNum) ([]byte, bool) {
	val, ok := m.GetBytes(field)
	return val, ok
}

// DecodeMessage fetches the field from m and decodes it as an embedded message
func (m *WireMessage) DecodeMessage(field FieldNum) (*WireMessage, error) {
	if bytes, ok := m.GetBytes(field); ok {
		emmsg := NewWireMessage()
		return emmsg, emmsg.Unmarshal(bytes)
	}
	return nil, ErrMessageFieldMissing
}

// DecodeAs fetches the field from m and decodes it as the specified
// Protobuf type
func (m *WireMessage) DecodeAs(field FieldNum, pbtype descriptor.FieldDescriptorProto_Type) (val interface{}, err error) {
	val = 0
	err = nil
	ok := true

	switch pbtype {
	case descriptor.FieldDescriptorProto_TYPE_INT32:
		val, ok = m.DecodeInt32(field)
	case descriptor.FieldDescriptorProto_TYPE_INT64:
		val, ok = m.DecodeInt64(field)
	case descriptor.FieldDescriptorProto_TYPE_UINT32:
		val, ok = m.DecodeUint32(field)
	case descriptor.FieldDescriptorProto_TYPE_UINT64:
		val, ok = m.DecodeUint64(field)
	case descriptor.FieldDescriptorProto_TYPE_SINT32:
		val, ok = m.DecodeSint32(field)
	case descriptor.FieldDescriptorProto_TYPE_SINT64:
		val, ok = m.DecodeSint64(field)
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		val, ok = m.DecodeBool(field)
	case descriptor.FieldDescriptorProto_TYPE_FIXED32:
		val, ok = m.DecodeFixed32(field)
	case descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		val, ok = m.DecodeSfixed32(field)
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		val, ok = m.DecodeFloat(field)
	case descriptor.FieldDescriptorProto_TYPE_FIXED64:
		val, ok = m.DecodeFixed64(field)
	case descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		val, ok = m.DecodeSfixed64(field)
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		val, ok = m.DecodeDouble(field)
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		val, ok = m.DecodeString(field)
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		val, ok = m.DecodeBytes(field)
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		val, err = m.DecodeMessage(field)
	default:
		val, err = 0, ErrInvalidProtoBufType
	}

	if !ok {
		err = ErrMessageFieldMissing
	}
	return
}

/////////////////////////////// Encoding /////////////////////////////////////

// EncodeInt32 adds value to the WireMessage encoded as a Protobuf int32
func (m *WireMessage) EncodeInt32(field FieldNum, value int32) {
	m.AddVarint(field, new(WireVarint).FromInt32(value))
}

// EncodeInt64 adds value to the WireMessage encoded as a Protobuf int64
func (m *WireMessage) EncodeInt64(field FieldNum, value int64) {
	m.AddVarint(field, new(WireVarint).FromInt64(value))
}

// EncodeUint32 adds value to the WireMessage encoded as a Protobuf uint32
func (m *WireMessage) EncodeUint32(field FieldNum, value uint32) {
	m.AddVarint(field, new(WireVarint).FromUint32(value))
}

// EncodeUint64 adds value to the WireMessage encoded as a Protobuf uint64
func (m *WireMessage) EncodeUint64(field FieldNum, value uint64) {
	m.AddVarint(field, new(WireVarint).FromUint64(value))
}

// EncodeSint32 adds value to the WireMessage encoded as a Protobuf sint32
func (m *WireMessage) EncodeSint32(field FieldNum, value int32) {
	m.AddVarint(field, new(WireVarint).FromSint32(value))
}

// EncodeSint64 adds value to the WireMessage encoded as a Protobuf sint64
func (m *WireMessage) EncodeSint64(field FieldNum, value int64) {
	m.AddVarint(field, new(WireVarint).FromSint64(value))
}

// EncodeBool adds value to the WireMessage encoded as a Protobuf bool
func (m *WireMessage) EncodeBool(field FieldNum, value bool) {
	m.AddVarint(field, new(WireVarint).FromBool(value))
}

// EncodeFixed32 adds value to the WireMessage encoded as a Protobuf fixed32
func (m *WireMessage) EncodeFixed32(field FieldNum, value uint32) {
	m.AddFixed32(field, new(WireFixed32).FromFixed32(value))
}

// EncodeSfixed32 adds value to the WireMessage encoded as a Protobuf sfixed32
func (m *WireMessage) EncodeSfixed32(field FieldNum, value int32) {
	m.AddFixed32(field, new(WireFixed32).FromSfixed32(value))
}

// EncodeFloat adds value to the WireMessage encoded as a Protobuf float
func (m *WireMessage) EncodeFloat(field FieldNum, value float32) {
	m.AddFixed32(field, new(WireFixed32).FromFloat(value))
}

// EncodeFixed64 adds value to the WireMessage encoded as a Protobuf fixed64
func (m *WireMessage) EncodeFixed64(field FieldNum, value uint64) {
	m.AddFixed64(field, new(WireFixed64).FromFixed64(value))
}

// EncodeSfixed64 adds value to the WireMessage encoded as a Protobuf sfixed64
func (m *WireMessage) EncodeSfixed64(field FieldNum, value int64) {
	m.AddFixed64(field, new(WireFixed64).FromSfixed64(value))
}

// EncodeDouble fetches the field and decodes it as a Protobuf double
func (m *WireMessage) EncodeDouble(field FieldNum, value float64) {
	m.AddFixed64(field, new(WireFixed64).FromDouble(value))
}

// EncodeString adds value to the WireMessage encoded as a Protobuf string
func (m *WireMessage) EncodeString(field FieldNum, value string) {
	// TODO: Check correctness for unicode/7bit ASCII text
	m.AddBytes(field, []byte(value))
}

// EncodeBytes adds value to the WireMessage encoded as a Protobuf bytes type
func (m *WireMessage) EncodeBytes(field FieldNum, value []byte) {
	m.AddBytes(field, value)
}

// EncodeMessage adds value to the WireMessage encoded as an embedded message
func (m *WireMessage) EncodeMessage(field FieldNum, value *WireMessage) error {
	bytes, err := value.Marshal()
	if err != nil {
		return err
	}
	m.AddBytes(field, bytes)
	return nil
}

// EncodeAs adds value to the WireMessage encoded as the specified Protobuf type
//
// Errors will ensue if the generic type is not compatible with the specified
// Protobuf type.
func (m *WireMessage) EncodeAs(field FieldNum, value interface{}, pbtype descriptor.FieldDescriptorProto_Type) error {
	err := ErrInvalidProtoBufType

	switch pbtype {
	case descriptor.FieldDescriptorProto_TYPE_INT32:
		if v, ok := value.(int32); ok {
			m.EncodeInt32(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_INT64:
		if v, ok := value.(int64); ok {
			m.EncodeInt64(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_UINT32:
		if v, ok := value.(uint32); ok {
			m.EncodeUint32(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_UINT64:
		if v, ok := value.(uint64); ok {
			m.EncodeUint64(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_SINT32:
		if v, ok := value.(int32); ok {
			m.EncodeSint32(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_SINT64:
		if v, ok := value.(int64); ok {
			m.EncodeSint64(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_BOOL:
		if v, ok := value.(bool); ok {
			m.EncodeBool(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_FIXED32:
		if v, ok := value.(uint32); ok {
			m.EncodeFixed32(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_SFIXED32:
		if v, ok := value.(int32); ok {
			m.EncodeSfixed32(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_FLOAT:
		if v, ok := value.(float32); ok {
			m.EncodeFloat(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_FIXED64:
		if v, ok := value.(uint64); ok {
			m.EncodeFixed64(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_SFIXED64:
		if v, ok := value.(int64); ok {
			m.EncodeSfixed64(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_DOUBLE:
		if v, ok := value.(float64); ok {
			m.EncodeDouble(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_STRING:
		if v, ok := value.(string); ok {
			m.EncodeString(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_BYTES:
		if v, ok := value.([]byte); ok {
			m.EncodeBytes(field, v)
			err = nil
		}
	case descriptor.FieldDescriptorProto_TYPE_MESSAGE:
		if v, ok := value.(*WireMessage); ok {
			err = m.EncodeMessage(field, v)
		}
	}
	return err
}

// Unmarshal sorts a ProtoBuf message into it's constituent
// parts to be such that it's field can be accessed in constant time
//
// This implementation has been adapted from the proto.Buffer.DebugPrint()
func (m *WireMessage) Unmarshal(buf []byte) error {
	pbuf := proto.NewBuffer(buf)

	var u uint64

	// obuf := pbuf.buf
	// index := pbuf.index
	// pbuf.buf = b
	// pbuf.index = 0
	depth := 0

	// fmt.Printf("\n--- %s ---\n", s)

out:
	for {
		for i := 0; i < depth; i++ {
			fmt.Print("  ")
		}

		// index := p.index
		// if index == len(pbuf.Bytes()) {
		// 	break
		// }

		// Fetch the next tag (field/type)
		tag, err := pbuf.DecodeVarint()
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				// We are finished
				break out
			}
			// TODO: Other error ?
			// fmt.Printf("%3d: fetching op err %v\n", index, err)
			// break out
			return err
		}

		// Decompose the tag into the field number and the wiretype
		field, wire := WireVarint(tag).AsTag()

		// Switch on the wire type
		switch wire {
		default:
			// Ignore unknown wiretypes

			// fmt.Printf("%3d: t=%3d unknown wire=%d\n",
			// index, tag, wire)
			// break out

		case proto.WireBytes:
			var r []byte

			r, err = pbuf.DecodeRawBytes(false)
			if err != nil {
				// break out
				return err
			}
			m.AddBytes(field, r)

		case proto.WireFixed32:
			u, err = pbuf.DecodeFixed32()
			if err != nil {
				// fmt.Printf("%3d: t=%3d fix32 err %v\n", index, tag, err)
				// break out
				return ErrMalformedProtoBuf
			}
			// fmt.Printf("%3d: t=%3d fix32 %d\n", index, tag, u)
			m.AddFixed32(field, WireFixed32(u))

		case proto.WireFixed64:
			u, err = pbuf.DecodeFixed64()
			if err != nil {
				// fmt.Printf("%3d: t=%3d fix64 err %v\n", index, tag, err)
				// break out
				return ErrMalformedProtoBuf
			}
			// fmt.Printf("%3d: t=%3d fix64 %d\n", index, tag, u)
			m.AddFixed64(field, WireFixed64(u))

		case proto.WireVarint:
			u, err = pbuf.DecodeVarint()
			if err != nil {
				// fmt.Printf("%3d: t=%3d varint err %v\n", index, tag, err)
				// break out
				return ErrMalformedProtoBuf
			}
			// fmt.Printf("%3d: t=%3d varint %d\n", index, tag, u)
			m.AddVarint(field, WireVarint(u))

		case proto.WireStartGroup:
			// fmt.Printf("%3d: t=%3d start\n", index, tag)
			depth++

		case proto.WireEndGroup:
			depth--
			// fmt.Printf("%3d: t=%3d end\n", index, tag)
		}
	}

	if depth != 0 {
		// fmt.Printf("%3d: start-end not balanced %d\n", p.index, depth)
		return ErrMalformedProtoBuf
	}
	// fmt.Printf("\n")

	// p.buf = obuf
	// p.index = index

	return nil
}

type fieldNumArray []FieldNum

func (fs fieldNumArray) Len() int           { return len(fs) }
func (fs fieldNumArray) Swap(i, j int)      { fs[i], fs[j] = fs[j], fs[i] }
func (fs fieldNumArray) Less(i, j int) bool { return fs[i] < fs[j] }

func (m *WireMessage) Marshal() ([]byte, error) {
	fields := fieldNumArray(m.GetFieldNums())
	sort.Sort(fields) // protobuf encoding should be in increaing key order
	pbuf := proto.NewBuffer(make([]byte, 0, 1))

	// Add all fields in the previously created sorted order
	for _, fnum := range []FieldNum(fields) {

		field, ok := m.GetField(fnum)
		if !ok {
			return nil, ErrMessageFieldMissing
		}

		switch field.(type) {
		case WireVarint:
			// Make field the appropriate type
			f := field.(WireVarint)
			// Write tag header
			var tag WireVarint
			tag.FromTag(fnum, proto.WireVarint)
			err := pbuf.EncodeVarint(uint64(tag))
			if err != nil {
				return nil, err
			}
			// Write the field data
			err = pbuf.EncodeVarint(uint64(f))
			if err != nil {
				return nil, err
			}
		case WireFixed32:
			// Make field the appropriate type
			f := field.(WireFixed32)
			// Write tag header
			var tag WireVarint
			tag.FromTag(fnum, proto.WireFixed32)
			err := pbuf.EncodeVarint(uint64(tag))
			if err != nil {
				return nil, err
			}
			// Write the field data
			err = pbuf.EncodeFixed32(uint64(f))
			if err != nil {
				return nil, err
			}
		case WireFixed64:
			// Make field the appropriate type
			f := field.(WireFixed64)
			// Write tag header
			var tag WireVarint
			tag.FromTag(fnum, proto.WireFixed64)
			err := pbuf.EncodeVarint(uint64(tag))
			if err != nil {
				return nil, err
			}
			// Write the field data
			err = pbuf.EncodeFixed64(uint64(f))
			if err != nil {
				return nil, err
			}
		case []byte:
			// Make field the appropriate type
			f := field.([]byte)
			// Write tag header
			var tag WireVarint
			tag.FromTag(fnum, proto.WireBytes)
			err := pbuf.EncodeVarint(uint64(tag))
			if err != nil {
				return nil, err
			}
			// Write the field data
			err = pbuf.EncodeRawBytes(f)
			if err != nil {
				return nil, err
			}
		}
	}

	return pbuf.Bytes(), nil
}
