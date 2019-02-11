package dory

import (
	"bytes"
	"encoding/binary"
	"errors"
	"time"

	"github.com/golang/protobuf/proto"
)

const (
	MSGSIZEFIELDSIZE    = 4
	APIKEYFIELDSIZE     = 2
	APIVERSIONFIELDSIZE = 2

	FLAGSFIELDSIZE        = 2
	PARTITIONKEYFIELDSIZE = 4
	TOPICSIZEFIELDSIZE    = 2
	TIMESTAMPFIELDSIZE    = 8
	KEYSIZEFIELDSIZE      = 4
	VALUESIZEFIELDSIZE    = 4

	ANYPARTITIONFIXEDBYTES = MSGSIZEFIELDSIZE + APIKEYFIELDSIZE + APIVERSIONFIELDSIZE +
		FLAGSFIELDSIZE + TOPICSIZEFIELDSIZE +
		TIMESTAMPFIELDSIZE + KEYSIZEFIELDSIZE +
		VALUESIZEFIELDSIZE

	PARTITIONKEYFIXEDBYTES = ANYPARTITIONFIXEDBYTES + PARTITIONKEYFIELDSIZE

	ANYPARTITIONAPIKEY     = 256
	ANYPARTITIONAPIVERSION = 0

	PARTITIONKEYAPIKEY     = 257
	PARTITIONKEYAPIVERSION = 0
)

const (
	MAX_TOPIC_SIZE = 0x7fff
	MAX_MSG_SIZE   = 0x7fffffff
)

func CreateAnyPartitionMsg(message *ReportLog, topic string) ([]byte, error) {
	p, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}
	return createAnyPartitionMsg(GetEpochMilliseconds(), topic, "", p)
}

func createAnyPartitionMsg(timestamp int64, topic, key string, value []byte) ([]byte, error) {
	topicBytes := []byte(topic)
	valueBytes := value
	keyBytes := []byte(key)

	if len(topicBytes) > MAX_TOPIC_SIZE {
		return nil, errors.New("Kafka topic is too large")
	}

	msgSize := ANYPARTITIONFIXEDBYTES + len(topicBytes) + len(keyBytes) + len(valueBytes)

	if msgSize > MAX_MSG_SIZE {
		return nil, errors.New("Cannot create a message that large")
	}

	b := new(bytes.Buffer)

	pData := []interface{}{
		int32(msgSize),
		uint16(ANYPARTITIONAPIKEY),
		uint16(ANYPARTITIONAPIVERSION),
		uint16(0),
		int16(len(topicBytes)),
	}

	for _, v := range pData {
		err := binary.Write(b, binary.BigEndian, v)
		if err != nil {
			return nil, err
		}
	}

	b.Write(topicBytes)
	binary.Write(b, binary.BigEndian, timestamp)
	binary.Write(b, binary.BigEndian, int32(len(keyBytes)))
	b.Write(keyBytes)
	binary.Write(b, binary.BigEndian, int32(len(valueBytes)))
	b.Write(valueBytes)
	result := make([]byte, b.Len())
	b.Read(result)

	return result, nil
}

func createPartitionKeyMsg(timestamp int64, partitionkey int, topic, key, value string) ([]byte, error) {
	topicBytes := []byte(topic)
	valueBytes := []byte(value)
	keyBytes := []byte(key)

	if len(topicBytes) > MAX_TOPIC_SIZE {
		return nil, errors.New("Kafka topic is too large")
	}

	msgSize := PARTITIONKEYFIXEDBYTES + len(topicBytes) + len(keyBytes) + len(valueBytes)
	if msgSize > MAX_MSG_SIZE {
		return nil, errors.New("Cannot create a message that large")
	}

	b := new(bytes.Buffer)
	pData := []interface{}{
		int32(msgSize),
		uint16(PARTITIONKEYAPIKEY),
		uint16(PARTITIONKEYAPIVERSION),
		uint16(0),
		uint32(partitionkey),
		int16(len(topicBytes)),
	}

	for _, v := range pData {
		err := binary.Write(b, binary.BigEndian, v)
		if err != nil {
			return nil, err
		}
	}

	b.Write(topicBytes)
	binary.Write(b, binary.BigEndian, timestamp)
	binary.Write(b, binary.BigEndian, int32(len(keyBytes)))
	b.Write(keyBytes)
	binary.Write(b, binary.BigEndian, int32(len(valueBytes)))
	b.Write(valueBytes)
	p := make([]byte, b.Len())
	b.Read(p)

	return p, nil
}

func GetEpochMilliseconds() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
