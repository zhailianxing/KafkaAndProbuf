package models

import (
	"errors"
	"fmt"
	"KafkaAndProbuf/models/dory"
	"github.com/golang/protobuf/proto"
)

func DecodeFromProtobuf(p []byte) (m map[string]string, ip string, logTimestamp int64, err error) {
	dst := new(dory.ReportLog)
	err = proto.Unmarshal(p, dst)
	if err != nil {
		return
	}

	ip = *dst.Ip
	logTimestamp = *dst.LogTimestamp
	if dst.Field == nil {
		err = errors.New("指针错误")
		return
	}
	m = make(map[string]string, len(dst.Field.Map))
	for _, v := range dst.Field.Map {
		value := v.GetValue()
		switch {
		case value.StringType != nil:
			m[v.GetKey()] = *value.StringType
			continue
		case value.FloatType != nil:
			m[v.GetKey()] = fmt.Sprintf("%f", *value.FloatType)
			continue
		case value.IntType != nil:
			m[v.GetKey()] = fmt.Sprintf("%d", *value.IntType)
			continue
		case value.LongType != nil:
			m[v.GetKey()] = fmt.Sprintf("%d", *value.LongType)
			continue
		}
	}
	return
}

