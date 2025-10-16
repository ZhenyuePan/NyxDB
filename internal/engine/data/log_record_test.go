package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord_WithMeta(t *testing.T) {
	rec := &LogRecord{
		Key:        []byte("name"),
		Value:      []byte("bitcask-go"),
		Type:       LogRecordNormal,
		CommitTs:   42,
		PrevFid:    7,
		PrevOffset: -128,
	}

	encoded, n := EncodeLogRecord(rec)
	assert.NotNil(t, encoded)
	assert.Greater(t, n, int64(len(rec.Key)+len(rec.Value)))

	header, headerSize := decodeLogRecordHeader(encoded)
	assert.NotNil(t, header)
	assert.True(t, (encoded[4]&logRecordMetaFlag) != 0)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.EqualValues(t, len(rec.Key), header.keySize)
	assert.EqualValues(t, len(rec.Value), header.valueSize)
	assert.Equal(t, rec.CommitTs, header.commitTs)
	assert.Equal(t, rec.PrevFid, header.prevFid)
	assert.Equal(t, rec.PrevOffset, header.prevOffset)

	keyStart := int(headerSize)
	assert.Equal(t, rec.Key, encoded[keyStart:keyStart+len(rec.Key)])
	assert.Equal(t, rec.Value, encoded[keyStart+len(rec.Key):])
}

func TestEncodeLogRecord_WithoutMeta(t *testing.T) {
	rec := &LogRecord{
		Key:   []byte("foo"),
		Value: []byte("bar"),
		Type:  LogRecordDeleted,
	}

	encoded, _ := EncodeLogRecord(rec)
	header, headerSize := decodeLogRecordHeader(encoded)

	assert.NotNil(t, header)
	assert.Equal(t, byte(LogRecordDeleted), encoded[4])
	assert.Equal(t, uint64(0), header.commitTs)
	assert.Equal(t, int64(0), header.prevOffset)

	keyStart := int(headerSize)
	assert.Equal(t, rec.Key, encoded[keyStart:keyStart+len(rec.Key)])
	assert.Equal(t, rec.Value, encoded[keyStart+len(rec.Key):])
}

func TestGetLogRecordCRC(t *testing.T) {
	rec := &LogRecord{
		Key:        []byte("hello"),
		Value:      []byte("world"),
		Type:       LogRecordNormal,
		CommitTs:   7,
		PrevFid:    3,
		PrevOffset: 12345,
	}
	encoded, _ := EncodeLogRecord(rec)
	header, headerSize := decodeLogRecordHeader(encoded)

	crc := getLogRecordCRC(rec, encoded[crc32.Size:int(headerSize)])
	assert.Equal(t, header.crc, crc)
}
