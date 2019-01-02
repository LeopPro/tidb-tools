package file_uploader

import (
	"fmt"
	"hash"
)

type FileHash interface {
	hash.Hash
	fmt.Stringer
}

type FileUploaderDriver interface {
	Upload(sliceInfo *Slice) (string, error)
	Hash() FileHash
}

type AWSS3FileUploaderDriver struct {
}

func NewAWSS3FileUploaderDriver() *AWSS3FileUploaderDriver {
	panic("312")
}
