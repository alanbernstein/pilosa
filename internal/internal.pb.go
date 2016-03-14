// Code generated by protoc-gen-gogo.
// source: internal/internal.proto
// DO NOT EDIT!

/*
Package internal is a generated protocol buffer package.

It is generated from these files:
	internal/internal.proto

It has these top-level messages:
	Bitmap
	Chunk
	Pair
	Biclique
	Bit
	Profile
	Attr
	AttrMap
	QueryRequest
	QueryResponse
	QueryResult
	ImportRequest
	ImportResponse
	Cache
	SliceMaxResponse
*/
package internal

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type Bitmap struct {
	Chunks           []*Chunk `protobuf:"bytes,1,rep,name=Chunks" json:"Chunks,omitempty"`
	Attrs            []*Attr  `protobuf:"bytes,2,rep,name=Attrs" json:"Attrs,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *Bitmap) Reset()         { *m = Bitmap{} }
func (m *Bitmap) String() string { return proto.CompactTextString(m) }
func (*Bitmap) ProtoMessage()    {}

func (m *Bitmap) GetChunks() []*Chunk {
	if m != nil {
		return m.Chunks
	}
	return nil
}

func (m *Bitmap) GetAttrs() []*Attr {
	if m != nil {
		return m.Attrs
	}
	return nil
}

type Chunk struct {
	Key              *uint64  `protobuf:"varint,1,req,name=Key" json:"Key,omitempty"`
	Value            []uint64 `protobuf:"varint,2,rep,name=Value" json:"Value,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *Chunk) Reset()         { *m = Chunk{} }
func (m *Chunk) String() string { return proto.CompactTextString(m) }
func (*Chunk) ProtoMessage()    {}

func (m *Chunk) GetKey() uint64 {
	if m != nil && m.Key != nil {
		return *m.Key
	}
	return 0
}

func (m *Chunk) GetValue() []uint64 {
	if m != nil {
		return m.Value
	}
	return nil
}

type Pair struct {
	Key              *uint64 `protobuf:"varint,1,req,name=Key" json:"Key,omitempty"`
	Count            *uint64 `protobuf:"varint,2,req,name=Count" json:"Count,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Pair) Reset()         { *m = Pair{} }
func (m *Pair) String() string { return proto.CompactTextString(m) }
func (*Pair) ProtoMessage()    {}

func (m *Pair) GetKey() uint64 {
	if m != nil && m.Key != nil {
		return *m.Key
	}
	return 0
}

func (m *Pair) GetCount() uint64 {
	if m != nil && m.Count != nil {
		return *m.Count
	}
	return 0
}

type Biclique struct {
	Tiles            []uint64 `protobuf:"varint,1,rep,name=Tiles" json:"Tiles,omitempty"`
	Count            *uint64  `protobuf:"varint,2,req,name=Count" json:"Count,omitempty"`
	Score            *uint64  `protobuf:"varint,3,req,name=Score" json:"Score,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *Biclique) Reset()         { *m = Biclique{} }
func (m *Biclique) String() string { return proto.CompactTextString(m) }
func (*Biclique) ProtoMessage()    {}

func (m *Biclique) GetTiles() []uint64 {
	if m != nil {
		return m.Tiles
	}
	return nil
}

func (m *Biclique) GetCount() uint64 {
	if m != nil && m.Count != nil {
		return *m.Count
	}
	return 0
}

func (m *Biclique) GetScore() uint64 {
	if m != nil && m.Score != nil {
		return *m.Score
	}
	return 0
}

type Bit struct {
	BitmapID         *uint64 `protobuf:"varint,1,req,name=BitmapID" json:"BitmapID,omitempty"`
	ProfileID        *uint64 `protobuf:"varint,2,req,name=ProfileID" json:"ProfileID,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Bit) Reset()         { *m = Bit{} }
func (m *Bit) String() string { return proto.CompactTextString(m) }
func (*Bit) ProtoMessage()    {}

func (m *Bit) GetBitmapID() uint64 {
	if m != nil && m.BitmapID != nil {
		return *m.BitmapID
	}
	return 0
}

func (m *Bit) GetProfileID() uint64 {
	if m != nil && m.ProfileID != nil {
		return *m.ProfileID
	}
	return 0
}

type Profile struct {
	ID               *uint64 `protobuf:"varint,1,req,name=ID" json:"ID,omitempty"`
	Attrs            []*Attr `protobuf:"bytes,2,rep,name=Attrs" json:"Attrs,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Profile) Reset()         { *m = Profile{} }
func (m *Profile) String() string { return proto.CompactTextString(m) }
func (*Profile) ProtoMessage()    {}

func (m *Profile) GetID() uint64 {
	if m != nil && m.ID != nil {
		return *m.ID
	}
	return 0
}

func (m *Profile) GetAttrs() []*Attr {
	if m != nil {
		return m.Attrs
	}
	return nil
}

type Attr struct {
	Key              *string `protobuf:"bytes,1,req,name=Key" json:"Key,omitempty"`
	StringValue      *string `protobuf:"bytes,2,opt,name=StringValue" json:"StringValue,omitempty"`
	UintValue        *uint64 `protobuf:"varint,3,opt,name=UintValue" json:"UintValue,omitempty"`
	BoolValue        *bool   `protobuf:"varint,4,opt,name=BoolValue" json:"BoolValue,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *Attr) Reset()         { *m = Attr{} }
func (m *Attr) String() string { return proto.CompactTextString(m) }
func (*Attr) ProtoMessage()    {}

func (m *Attr) GetKey() string {
	if m != nil && m.Key != nil {
		return *m.Key
	}
	return ""
}

func (m *Attr) GetStringValue() string {
	if m != nil && m.StringValue != nil {
		return *m.StringValue
	}
	return ""
}

func (m *Attr) GetUintValue() uint64 {
	if m != nil && m.UintValue != nil {
		return *m.UintValue
	}
	return 0
}

func (m *Attr) GetBoolValue() bool {
	if m != nil && m.BoolValue != nil {
		return *m.BoolValue
	}
	return false
}

type AttrMap struct {
	Attrs            []*Attr `protobuf:"bytes,1,rep,name=Attrs" json:"Attrs,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *AttrMap) Reset()         { *m = AttrMap{} }
func (m *AttrMap) String() string { return proto.CompactTextString(m) }
func (*AttrMap) ProtoMessage()    {}

func (m *AttrMap) GetAttrs() []*Attr {
	if m != nil {
		return m.Attrs
	}
	return nil
}

type QueryRequest struct {
	DB               *string  `protobuf:"bytes,1,req,name=DB" json:"DB,omitempty"`
	Query            *string  `protobuf:"bytes,2,req,name=Query" json:"Query,omitempty"`
	Slices           []uint64 `protobuf:"varint,3,rep,name=Slices" json:"Slices,omitempty"`
	Profiles         *bool    `protobuf:"varint,4,opt,name=Profiles" json:"Profiles,omitempty"`
	Timestamp        *int64   `protobuf:"varint,5,opt,name=Timestamp" json:"Timestamp,omitempty"`
	Quantum          *uint32  `protobuf:"varint,6,opt,name=Quantum" json:"Quantum,omitempty"`
	Remote           *bool    `protobuf:"varint,7,opt,name=Remote" json:"Remote,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *QueryRequest) Reset()         { *m = QueryRequest{} }
func (m *QueryRequest) String() string { return proto.CompactTextString(m) }
func (*QueryRequest) ProtoMessage()    {}

func (m *QueryRequest) GetDB() string {
	if m != nil && m.DB != nil {
		return *m.DB
	}
	return ""
}

func (m *QueryRequest) GetQuery() string {
	if m != nil && m.Query != nil {
		return *m.Query
	}
	return ""
}

func (m *QueryRequest) GetSlices() []uint64 {
	if m != nil {
		return m.Slices
	}
	return nil
}

func (m *QueryRequest) GetProfiles() bool {
	if m != nil && m.Profiles != nil {
		return *m.Profiles
	}
	return false
}

func (m *QueryRequest) GetTimestamp() int64 {
	if m != nil && m.Timestamp != nil {
		return *m.Timestamp
	}
	return 0
}

func (m *QueryRequest) GetQuantum() uint32 {
	if m != nil && m.Quantum != nil {
		return *m.Quantum
	}
	return 0
}

func (m *QueryRequest) GetRemote() bool {
	if m != nil && m.Remote != nil {
		return *m.Remote
	}
	return false
}

type QueryResponse struct {
	Err              *string        `protobuf:"bytes,1,opt,name=Err" json:"Err,omitempty"`
	Results          []*QueryResult `protobuf:"bytes,2,rep,name=Results" json:"Results,omitempty"`
	Profiles         []*Profile     `protobuf:"bytes,3,rep,name=Profiles" json:"Profiles,omitempty"`
	XXX_unrecognized []byte         `json:"-"`
}

func (m *QueryResponse) Reset()         { *m = QueryResponse{} }
func (m *QueryResponse) String() string { return proto.CompactTextString(m) }
func (*QueryResponse) ProtoMessage()    {}

func (m *QueryResponse) GetErr() string {
	if m != nil && m.Err != nil {
		return *m.Err
	}
	return ""
}

func (m *QueryResponse) GetResults() []*QueryResult {
	if m != nil {
		return m.Results
	}
	return nil
}

func (m *QueryResponse) GetProfiles() []*Profile {
	if m != nil {
		return m.Profiles
	}
	return nil
}

type QueryResult struct {
	Bitmap           *Bitmap     `protobuf:"bytes,1,opt,name=Bitmap" json:"Bitmap,omitempty"`
	N                *uint64     `protobuf:"varint,2,opt,name=N" json:"N,omitempty"`
	Pairs            []*Pair     `protobuf:"bytes,3,rep,name=Pairs" json:"Pairs,omitempty"`
	Changed          *bool       `protobuf:"varint,4,opt,name=Changed" json:"Changed,omitempty"`
	Bicliques        []*Biclique `protobuf:"bytes,5,rep,name=Bicliques" json:"Bicliques,omitempty"`
	XXX_unrecognized []byte      `json:"-"`
}

func (m *QueryResult) Reset()         { *m = QueryResult{} }
func (m *QueryResult) String() string { return proto.CompactTextString(m) }
func (*QueryResult) ProtoMessage()    {}

func (m *QueryResult) GetBitmap() *Bitmap {
	if m != nil {
		return m.Bitmap
	}
	return nil
}

func (m *QueryResult) GetN() uint64 {
	if m != nil && m.N != nil {
		return *m.N
	}
	return 0
}

func (m *QueryResult) GetPairs() []*Pair {
	if m != nil {
		return m.Pairs
	}
	return nil
}

func (m *QueryResult) GetChanged() bool {
	if m != nil && m.Changed != nil {
		return *m.Changed
	}
	return false
}

func (m *QueryResult) GetBicliques() []*Biclique {
	if m != nil {
		return m.Bicliques
	}
	return nil
}

type ImportRequest struct {
	DB               *string  `protobuf:"bytes,1,req,name=DB" json:"DB,omitempty"`
	Frame            *string  `protobuf:"bytes,2,req,name=Frame" json:"Frame,omitempty"`
	Slice            *uint64  `protobuf:"varint,3,req,name=Slice" json:"Slice,omitempty"`
	BitmapIDs        []uint64 `protobuf:"varint,4,rep,name=BitmapIDs" json:"BitmapIDs,omitempty"`
	ProfileIDs       []uint64 `protobuf:"varint,5,rep,name=ProfileIDs" json:"ProfileIDs,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *ImportRequest) Reset()         { *m = ImportRequest{} }
func (m *ImportRequest) String() string { return proto.CompactTextString(m) }
func (*ImportRequest) ProtoMessage()    {}

func (m *ImportRequest) GetDB() string {
	if m != nil && m.DB != nil {
		return *m.DB
	}
	return ""
}

func (m *ImportRequest) GetFrame() string {
	if m != nil && m.Frame != nil {
		return *m.Frame
	}
	return ""
}

func (m *ImportRequest) GetSlice() uint64 {
	if m != nil && m.Slice != nil {
		return *m.Slice
	}
	return 0
}

func (m *ImportRequest) GetBitmapIDs() []uint64 {
	if m != nil {
		return m.BitmapIDs
	}
	return nil
}

func (m *ImportRequest) GetProfileIDs() []uint64 {
	if m != nil {
		return m.ProfileIDs
	}
	return nil
}

type ImportResponse struct {
	Err              *string `protobuf:"bytes,1,opt,name=Err" json:"Err,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *ImportResponse) Reset()         { *m = ImportResponse{} }
func (m *ImportResponse) String() string { return proto.CompactTextString(m) }
func (*ImportResponse) ProtoMessage()    {}

func (m *ImportResponse) GetErr() string {
	if m != nil && m.Err != nil {
		return *m.Err
	}
	return ""
}

type Cache struct {
	BitmapIDs        []uint64 `protobuf:"varint,1,rep,name=BitmapIDs" json:"BitmapIDs,omitempty"`
	XXX_unrecognized []byte   `json:"-"`
}

func (m *Cache) Reset()         { *m = Cache{} }
func (m *Cache) String() string { return proto.CompactTextString(m) }
func (*Cache) ProtoMessage()    {}

func (m *Cache) GetBitmapIDs() []uint64 {
	if m != nil {
		return m.BitmapIDs
	}
	return nil
}

type SliceMaxResponse struct {
	SliceMax         *uint64 `protobuf:"varint,1,req,name=SliceMax" json:"SliceMax,omitempty"`
	XXX_unrecognized []byte  `json:"-"`
}

func (m *SliceMaxResponse) Reset()         { *m = SliceMaxResponse{} }
func (m *SliceMaxResponse) String() string { return proto.CompactTextString(m) }
func (*SliceMaxResponse) ProtoMessage()    {}

func (m *SliceMaxResponse) GetSliceMax() uint64 {
	if m != nil && m.SliceMax != nil {
		return *m.SliceMax
	}
	return 0
}

func init() {
	proto.RegisterType((*Bitmap)(nil), "internal.Bitmap")
	proto.RegisterType((*Chunk)(nil), "internal.Chunk")
	proto.RegisterType((*Pair)(nil), "internal.Pair")
	proto.RegisterType((*Biclique)(nil), "internal.Biclique")
	proto.RegisterType((*Bit)(nil), "internal.Bit")
	proto.RegisterType((*Profile)(nil), "internal.Profile")
	proto.RegisterType((*Attr)(nil), "internal.Attr")
	proto.RegisterType((*AttrMap)(nil), "internal.AttrMap")
	proto.RegisterType((*QueryRequest)(nil), "internal.QueryRequest")
	proto.RegisterType((*QueryResponse)(nil), "internal.QueryResponse")
	proto.RegisterType((*QueryResult)(nil), "internal.QueryResult")
	proto.RegisterType((*ImportRequest)(nil), "internal.ImportRequest")
	proto.RegisterType((*ImportResponse)(nil), "internal.ImportResponse")
	proto.RegisterType((*Cache)(nil), "internal.Cache")
	proto.RegisterType((*SliceMaxResponse)(nil), "internal.SliceMaxResponse")
}
