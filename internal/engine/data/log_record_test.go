package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord_WithMeta(t *testing.T) {
	entry := &LogEntry{
		Record: LogRecord{
			Key:   []byte("name"),
			Value: []byte("bitcask-go"),
			Type:  LogRecordNormal,
		},
		Meta: LogMeta{
			CommitTs:   42,
			PrevFid:    7,
			PrevOffset: -128,
		},
	}

	encoded, n := EncodeLogEntry(entry)
	assert.NotNil(t, encoded)
	assert.Greater(t, n, int64(len(entry.Record.Key)+len(entry.Record.Value)))

	header, headerSize := decodeLogRecordHeader(encoded)
	assert.NotNil(t, header)
	assert.True(t, (encoded[4]&logRecordMetaFlag) != 0)
	assert.Equal(t, LogRecordNormal, header.recordType)
	assert.EqualValues(t, len(entry.Record.Key), header.keySize)
	assert.EqualValues(t, len(entry.Record.Value), header.valueSize)
	assert.Equal(t, entry.Meta.CommitTs, header.meta.CommitTs)
	assert.Equal(t, entry.Meta.PrevFid, header.meta.PrevFid)
	assert.Equal(t, entry.Meta.PrevOffset, header.meta.PrevOffset)

	keyStart := int(headerSize)
	assert.Equal(t, entry.Record.Key, encoded[keyStart:keyStart+len(entry.Record.Key)])
	assert.Equal(t, entry.Record.Value, encoded[keyStart+len(entry.Record.Key):])
}

func TestEncodeLogRecord_WithoutMeta(t *testing.T) {
	entry := &LogEntry{
		Record: LogRecord{
			Key:   []byte("foo"),
			Value: []byte("bar"),
			Type:  LogRecordDeleted,
		},
	}

	encoded, _ := EncodeLogEntry(entry)
	header, headerSize := decodeLogRecordHeader(encoded)

	assert.NotNil(t, header)
	assert.Equal(t, byte(LogRecordDeleted), encoded[4])
	assert.Equal(t, uint64(0), header.meta.CommitTs)
	assert.Equal(t, int64(0), header.meta.PrevOffset)

	keyStart := int(headerSize)
	assert.Equal(t, entry.Record.Key, encoded[keyStart:keyStart+len(entry.Record.Key)])
	assert.Equal(t, entry.Record.Value, encoded[keyStart+len(entry.Record.Key):])
}

func TestGetLogRecordCRC(t *testing.T) {
	entry := &LogEntry{
		Record: LogRecord{
			Key:   []byte("hello"),
			Value: []byte("world"),
			Type:  LogRecordNormal,
		},
		Meta: LogMeta{
			CommitTs:   7,
			PrevFid:    3,
			PrevOffset: 12345,
		},
	}
	encoded, _ := EncodeLogEntry(entry)
	header, headerSize := decodeLogRecordHeader(encoded)

	crc := getLogEntryCRC(entry, encoded[crc32.Size:int(headerSize)])
	assert.Equal(t, header.crc, crc)
}
