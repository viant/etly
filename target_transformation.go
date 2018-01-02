package etly

import (
	"bytes"
	"fmt"
	"strings"
)

type TargetTransformations map[string]*TargetTransformation

func (t *TargetTransformations) Length() int {
	var result = 0
	for _, v := range *t {
		result += len(v.targetRecords)
	}
	return result
}

func (t *TargetTransformations) Size() int {
	var result = 0
	for _, v := range *t {
		for _, record := range v.targetRecords {
			result += len(record)
		}
	}
	return result
}

func (t *TargetTransformations) Upload(transfer *Transfer) ([]*ProcessedTransfer, error) {
	var result = make([]*ProcessedTransfer, 0)
	if len(*t) > 0 {
		for _, transformed := range *t {
			content := strings.Join(transformed.targetRecords, "\n")
			compressedData, err := encodeData(transfer.Target.Compression, []byte(content))
			if err != nil {
				return nil, err
			}
			storageService, err := getStorageService(transfer.Target.Resource)
			if err != nil {
				return nil, err
			}

			//Disable MD5 <---- AW. why this is hardcoded here !!!!
			fileName := transformed.ProcessedTransfer.Transfer.Target.Name + "?disableMD5=true"
			err = storageService.Upload(fileName, bytes.NewReader(compressedData))
			if err != nil {
				return nil, fmt.Errorf("failed to upload: %v %v", transfer.Target, err)
			}
			result = append(result, transformed.ProcessedTransfer)
		}
	}
	return result, nil
}
