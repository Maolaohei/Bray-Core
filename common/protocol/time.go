package protocol

type Timestamp int64

type TimestampGenerator func() Timestamp
